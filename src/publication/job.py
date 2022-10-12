import os

from src.io.source import Drug, ClinicalTrial, Pubmed
from src.publication.publication import drug_graph, top_journals


def start_job_drug_graph(spark):
    """
    steps :
    1 - extraction des données depuis les sources
    2 - transformation des données
    3 - chargement du graph dans un fichier de sortie json

    :param spark: spark dependencies
    :return: transformed dataframe
    """

    spark_sess, spark_logger, config_dict = spark.start_spark()

    spark_logger.info("Start job drug graph")
    # SOURCE FILES
    source_path = config_dict['source']['path']
    drug_path = os.path.join(source_path, config_dict['source']['drug_file'])
    clinical_path = os.path.join(source_path, config_dict['source']['clinical_trial_file'])
    pubmed_csv_path = os.path.join(source_path, config_dict['source']['pubmed_csv_file'])
    pubmed_json_path = os.path.join(source_path, config_dict['source']['pubmed_json_file'])

    # EXTRACT
    spark_logger.info("Extraction Drug")
    drug_df = Drug(spark_sess, drug_path).read()
    spark_logger.info("Extraction Drug done !")

    spark_logger.info("Extraction Clinical trials")
    clinical_trial_df = ClinicalTrial(spark_sess, clinical_path).read()
    spark_logger.info("Extraction Clinical trials done !")

    spark_logger.info("Extraction PubMed")
    pubmed_df = Pubmed(spark_sess, pubmed_csv_path, pubmed_json_path).read()
    spark_logger.info("Extraction PubMed done !")

    # TRANSFORM
    transformed_df = drug_graph(drug_df, clinical_trial_df, pubmed_df)

    # LOAD
    transformed_df.write.json(os.path.join(config_dict['sink']['path'], config_dict['sink']['drug_graph']),
                              mode="overwrite")

    spark_logger.info(f"File saved in {config_dict['sink']['path']}")
    spark_logger.info("End job drug graph")

    return transformed_df


def start_job_top_journals(spark):
    """
    steps :
    1 - chargement du dataframe
    2 - transformation du  dataframe
    3 - chargement de top journals dans un fichier de sortie json

    :param spark: spark dependencies
    :param df: transformed dataframe from job drug graph
    :return:
    """
    spark_sess, spark_logger, config_dict = spark.start_spark()
    spark_logger.info("Start job top journals")

    # SOURCE FILES

    # EXTRACT
    df = spark_sess.read.format("json").load(
        os.path.join(config_dict['sink']['path'], config_dict['sink']['drug_graph']))

    # TRANSFORM
    top_journals_df = top_journals(df)
    top_journals_df.show()

    # LOAD
    top_journals_df.write.json(os.path.join(config_dict['sink']['path'], config_dict['sink']['top_journals']),
                               mode="overwrite")
    spark_logger.info(f"File saved in {config_dict['sink']['path']}")

    spark_logger.info("End job top journals")

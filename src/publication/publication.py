from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, upper, col, count

from src.utils import constants


def drug_graph(drug_df: DataFrame, clinical_trial_df: DataFrame, pubmed_df: DataFrame):
    """
    Votre data pipeline doit produire en sortie un fichier JSON qui représente un graphe de liaison entre les
    différents médicaments et leurs mentions respectives dans les différentes publications PubMed, les différentes
    publications scientifiques et enfin les journaux avec la date associée à chacune de ces mentions. La
    représentation ci-dessous permet de visualiser ce qui est attendu. Il peut y avoir plusieurs manières de
    modéliser cet output et vous pouvez justifier votre vision :
    Règles de gestion :

    - Un drug est considéré comme mentionné dans un article PubMed ou un essai clinique s’il est mentionné dans
     le titre de la publication.
    - Un drug est considéré comme mentionné par un journal s’il est mentionné dans une publication émise par ce journal.

    :param drug_df: Drug Dataframe
    :param clinical_trial_df: Clinical Trial Dataframe
    :param pubmed_df: Pubmed Dataframe
    :return: Dataframe drug graph
    """
    join_drug_clinical = drug_df.join(clinical_trial_df,
                                      upper(clinical_trial_df[constants.CLINICAL_TRIAL_SCIENTIFIC_TITLE])
                                      .contains(upper(drug_df[constants.DRUG_NAME])), "left").withColumn("type",
                                                                                                         lit("clinical"))

    join_drug_pubmed = drug_df.join(pubmed_df, upper(pubmed_df[constants.PUBMED_TITLE])
                                    .contains(upper(drug_df[constants.DRUG_NAME])), "left").withColumn("type",
                                                                                                       lit("pubmed"))

    union_df = join_drug_clinical.union(join_drug_pubmed).filter("journal is not null").drop(constants.DRUG_ATCCODE,
                                                                                             constants.CLINICAL_TRIAL_ID).withColumnRenamed(
        constants.CLINICAL_TRIAL_SCIENTIFIC_TITLE, constants.PUBMED_TITLE).sort(constants.DRUG_NAME, "type")

    return union_df


def top_journals(df: DataFrame):
    """
    Vous devez aussi mettre en place (hors de la data pipeline, vous pouvez considérer que c’est une partie annexe)
    une feature permettant de répondre à la problématique suivante :
    • Extraire depuis le json produit par la data  pipeline le nom du journal qui mentionne le plus de médicaments
     différents ?

    :param df: Dataframe transformed
    :return: Dataframe top journals
    """
    rows = (
        df.select(col(constants.DRUG_NAME), col(constants.PUBMED_JOURNAL))
        .groupBy(col(constants.PUBMED_JOURNAL))
        .agg(count("*").alias("drugs_count"))
        .sort(col("drugs_count").desc()).limit(3)
    )

    return rows

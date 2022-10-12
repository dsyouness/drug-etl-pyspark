import sys

from src.publication import job
from src.dependencies import spark

if __name__ == "__main__":
    spark_sess, spark_logger, config_dict = spark.start_spark()
    # set LogLevel to info
    spark_sess.sparkContext.setLogLevel('info')
    if len(sys.argv) > 1:
        if sys.argv[1] == "drug_graph":
            job.start_job_drug_graph(spark)
        elif sys.argv[1] == "top_journals":
            job.start_job_top_journals(spark)
    else:
        spark_logger.warn("No parameter defined")
        job.start_job_drug_graph(spark)
        job.start_job_top_journals(spark)

    # stop spark
    spark_sess.stop()

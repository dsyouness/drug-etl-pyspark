from pyspark.sql import SparkSession

from src.schema import schemas
from src.utils import utils


class Drug(object):
    def __init__(self, spark: SparkSession, path: str):
        self.spark = spark
        self.path = path
        self.schema = schemas.drug_schema

    def read(self):
        """
        Read csv input file.
        :return: pyspark.sql.DataFrame
        """
        return utils.read_csv(spark=self.spark, filepath=self.path, schema=self.schema)


class ClinicalTrial(object):
    def __init__(self, spark: SparkSession, path: str):
        self.spark = spark
        self.path = path
        self.schema = schemas.clinical_trial_schema

    def read(self):
        """
        Read csv input file.
        :return: pyspark.sql.DataFrame
        """
        return utils.read_csv(spark=self.spark, filepath=self.path, schema=self.schema)


class Pubmed(object):
    def __init__(self, spark: SparkSession, csv_path: str, json_path: str):
        self.spark = spark
        self.csv_path = csv_path
        self.json_path = json_path
        self.schema = schemas.pubmed_schema

    def read(self):
        """
        Read csv and json input files.
        :return: pyspark.sql.DataFrame
        """
        return utils.read_csv(spark=self.spark, filepath=self.csv_path, schema=self.schema).union(
            utils.read_json(spark=self.spark, filepath=self.json_path, schema=self.schema)
        )

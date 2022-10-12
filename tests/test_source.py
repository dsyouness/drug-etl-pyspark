import unittest

from pyspark.sql import SparkSession

from src.io.source import Drug


class TestSource(unittest.TestCase):
    def test_read(self):
        spark_sess = SparkSession.builder.appName("test-app").getOrCreate()
        drug_df = Drug(spark_sess, "data/source/drugs.csv").read()
        self.assertEqual(drug_df.count(), 7)

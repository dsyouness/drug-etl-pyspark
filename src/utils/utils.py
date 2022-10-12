from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


def get_spark_session(app: str):
    return SparkSession.builder.appName(app).getOrCreate()


def read_csv(spark: SparkSession, filepath: str, schema: StructType, separator: str = ","):
    return (
        spark.read.schema(schema)
        .format("csv")
        .option("sep", separator)
        .option("header", "true")
        .load(filepath)
    )


def read_json(spark: SparkSession, filepath: str, schema: StructType):
    return spark.read.option("multiline", "true").schema(schema).json(filepath)

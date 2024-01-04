from pyspark.sql import SparkSession


def init_sparksession():
    spark = (
        SparkSession
        .Builder()
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .getOrCreate()
    )

    # log level
    spark.sparkContext.setLogLevel("ERROR")

    return spark

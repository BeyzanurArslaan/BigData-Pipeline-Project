"""
Spark session factory.
"""

from pyspark.sql import SparkSession

from processing.config import (
    HDFS_URI,
    SPARK_APP_NAME,
    SPARK_MASTER,
)


def create_spark_session() -> SparkSession:

    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)

        # Hadoop
        .config("spark.hadoop.fs.defaultFS", HDFS_URI)

        # Resources
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.cores", "2")

        # Parallelism
        .config("spark.default.parallelism", "2")
        .config("spark.sql.shuffle.partitions", "2")

        # Dynamic Allocation
        .config("spark.dynamicAllocation.enabled", "false")

        # Serializer
        .config(
            "spark.serializer",
            "org.apache.spark.serializer.KryoSerializer"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark
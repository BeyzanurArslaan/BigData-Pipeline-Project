from pyspark.sql import functions as F

from processing.config import HDFS_PARQUET_URI, HDFS_WAREHOUSE_URI, join_hdfs_uri
from processing.spark_session import create_spark_session


def build_dim_date():
    spark = create_spark_session()

    try:
        orders = spark.read.parquet(
            join_hdfs_uri(HDFS_PARQUET_URI, "olist_orders_dataset")
        )

        dim_date = (
            orders
            .select(
                F.to_date(F.col("order_purchase_timestamp")).alias("date")
            )
            .withColumn(
                "date_key",
                F.date_format(F.col("date"), "yyyyMMdd").cast("int"),
            )
            .withColumn("year", F.year(F.col("date")))
            .withColumn("month", F.month(F.col("date")))
            .withColumn("day", F.dayofmonth(F.col("date")))
            .dropDuplicates(["date_key"])
            .select("date_key", "date", "year", "month", "day")
        )

        dim_date.write.mode("overwrite").parquet(
            join_hdfs_uri(HDFS_WAREHOUSE_URI, "dim_date")
        )

        print("=" * 60)
        print("DIM_DATE CREATED")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    build_dim_date()

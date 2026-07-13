from pyspark.sql import functions as F

from processing.config import HDFS_PARQUET_URI, HDFS_WAREHOUSE_URI, join_hdfs_uri
from processing.spark_session import create_spark_session


def build_dim_sellers():
    spark = create_spark_session()

    try:
        sellers = spark.read.parquet(
            join_hdfs_uri(HDFS_PARQUET_URI, "olist_sellers_dataset")
        )

        dim_sellers = (
            sellers
            .select(
                "seller_id",
                "seller_city",
                "seller_state",
                F.col("seller_zip_code_prefix").cast("int").alias("seller_zip_code_prefix"),
            )
            .dropDuplicates(["seller_id"])
        )

        dim_sellers.write.mode("overwrite").parquet(
            join_hdfs_uri(HDFS_WAREHOUSE_URI, "dim_sellers")
        )

        print("=" * 60)
        print("DIM_SELLERS CREATED")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    build_dim_sellers()

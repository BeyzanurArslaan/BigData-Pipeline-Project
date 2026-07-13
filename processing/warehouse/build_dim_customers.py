from pyspark.sql import functions as F

from processing.config import HDFS_PARQUET_URI, HDFS_WAREHOUSE_URI, join_hdfs_uri
from processing.spark_session import create_spark_session


def build_dim_customers():
    spark = create_spark_session()

    try:
        customers = spark.read.parquet(
            join_hdfs_uri(HDFS_PARQUET_URI, "olist_customers_dataset")
        )

        dim_customers = (
            customers
            .select(
                "customer_id",
                "customer_unique_id",
                "customer_city",
                "customer_state",
                F.col("customer_zip_code_prefix").cast("int").alias("customer_zip_code_prefix"),
            )
            .dropDuplicates(["customer_id"])
        )

        dim_customers.write.mode("overwrite").parquet(
            join_hdfs_uri(HDFS_WAREHOUSE_URI, "dim_customers")
        )

        print("=" * 60)
        print("DIM_CUSTOMERS CREATED")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    build_dim_customers()

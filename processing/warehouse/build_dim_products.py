from pyspark.sql import functions as F

from processing.config import HDFS_PARQUET_URI, HDFS_WAREHOUSE_URI, join_hdfs_uri
from processing.spark_session import create_spark_session


def build_dim_products():
    spark = create_spark_session()

    try:
        products = spark.read.parquet(
            join_hdfs_uri(HDFS_PARQUET_URI, "olist_products_dataset")
        )

        dim_products = (
            products
            .select(
                "product_id",
                "product_category_name",
                F.col("product_weight_g").cast("double").alias("product_weight_g"),
                F.col("product_length_cm").cast("double").alias("product_length_cm"),
                F.col("product_height_cm").cast("double").alias("product_height_cm"),
                F.col("product_width_cm").cast("double").alias("product_width_cm"),
            )
            .dropDuplicates(["product_id"])
        )

        dim_products.write.mode("overwrite").parquet(
            join_hdfs_uri(HDFS_WAREHOUSE_URI, "dim_products")
        )

        print("=" * 60)
        print("DIM_PRODUCTS CREATED")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    build_dim_products()

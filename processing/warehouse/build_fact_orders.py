from pyspark.sql import functions as F
from pyspark.sql.window import Window

from processing.config import HDFS_PARQUET_URI, HDFS_WAREHOUSE_URI, join_hdfs_uri
from processing.spark_session import create_spark_session


def build_fact_orders():
    spark = create_spark_session()

    try:
        orders = spark.read.parquet(
            join_hdfs_uri(HDFS_PARQUET_URI, "olist_orders_dataset")
        ).select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
        )

        items = (
            spark.read.parquet(
                join_hdfs_uri(HDFS_PARQUET_URI, "olist_order_items_dataset")
            )
            .select(
                "order_id",
                F.col("order_item_id").cast("int").alias("order_item_id"),
                "product_id",
                "seller_id",
                F.col("price").cast("decimal(18,2)").alias("price"),
                F.col("freight_value").cast("decimal(18,2)").alias("freight_value"),
            )
            .withColumn(
                "item_gross_value",
                F.coalesce(F.col("price"), F.lit(0).cast("decimal(18,2)"))
                + F.coalesce(F.col("freight_value"), F.lit(0).cast("decimal(18,2)")),
            )
        )

        item_totals = (
            items.groupBy("order_id")
            .agg(
                F.sum("item_gross_value").alias("order_items_gross_total"),
            )
        )

        payments = (
            spark.read.parquet(
                join_hdfs_uri(HDFS_PARQUET_URI, "olist_order_payments_dataset")
            )
            .select(
                "order_id",
                F.col("payment_sequential").cast("int").alias("payment_sequential"),
                "payment_type",
                F.col("payment_installments").cast("int").alias(
                    "payment_installments"
                ),
                F.col("payment_value").cast("decimal(18,2)").alias("payment_value"),
            )
        )

        order_payments = payments.groupBy("order_id").agg(
            F.sum("payment_value").alias("order_payment_value")
        )

        primary_payment_window = Window.partitionBy("order_id").orderBy(
            F.col("payment_sequential").asc(),
            F.col("payment_type").asc_nulls_last(),
        )
        primary_payments = (
            payments.withColumn("payment_rank", F.row_number().over(primary_payment_window))
            .filter(F.col("payment_rank") == 1)
            .select(
                "order_id",
                F.col("payment_type").alias("primary_payment_type"),
                F.col("payment_installments").alias("primary_payment_installments"),
            )
        )

        reviews = (
            spark.read.parquet(
                join_hdfs_uri(HDFS_PARQUET_URI, "olist_order_reviews_dataset")
            )
            .select(
                "order_id",
                F.col("review_score").cast("int").alias("review_score"),
            )
            .dropDuplicates(["order_id"])
        )

        fact_orders = (
            items.join(orders, "order_id", "left")
            .join(item_totals, "order_id", "left")
            .join(order_payments, "order_id", "left")
            .join(primary_payments, "order_id", "left")
            .join(reviews, "order_id", "left")
            .withColumn(
                "date_key",
                F.date_format(F.to_date(F.col("order_purchase_timestamp")), "yyyyMMdd").cast(
                    "int"
                ),
            )
            .withColumn(
                "allocated_payment_value",
                F.when(
                    F.coalesce(F.col("order_items_gross_total"), F.lit(0).cast("decimal(18,2)"))
                    > 0,
                    F.coalesce(F.col("order_payment_value"), F.lit(0).cast("decimal(18,2)"))
                    * F.coalesce(F.col("item_gross_value"), F.lit(0).cast("decimal(18,2)"))
                    / F.col("order_items_gross_total"),
                ).otherwise(F.lit(0).cast("decimal(18,2)")),
            )
            .select(
                "order_id",
                "order_item_id",
                "customer_id",
                "seller_id",
                "product_id",
                "date_key",
                "order_status",
                "item_gross_value",
                "order_items_gross_total",
                "order_payment_value",
                "allocated_payment_value",
                "primary_payment_type",
                "primary_payment_installments",
                "price",
                "freight_value",
                "review_score",
            )
        )

        fact_orders.write.mode("overwrite").parquet(
            join_hdfs_uri(HDFS_WAREHOUSE_URI, "fact_orders")
        )

        print("=" * 60)
        print("FACT_ORDERS CREATED")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    build_fact_orders()

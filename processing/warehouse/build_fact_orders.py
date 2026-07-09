from pathlib import Path

from pyspark.sql.functions import col, date_format, to_date

from processing.config import PARQUET_OUTPUT_DIR
from processing.spark_session import create_spark_session


def build_fact_orders():

    spark = create_spark_session()

    parquet_dir = Path(PARQUET_OUTPUT_DIR)

    orders = spark.read.parquet(
        f"file://{parquet_dir / 'olist_orders_dataset'}"
    )

    items = spark.read.parquet(
        f"file://{parquet_dir / 'olist_order_items_dataset'}"
    )

    payments = spark.read.parquet(
        f"file://{parquet_dir / 'olist_order_payments_dataset'}"
    )

    reviews = spark.read.parquet(
        f"file://{parquet_dir / 'olist_order_reviews_dataset'}"
    )

    fact_orders = (
        orders
        .join(items, "order_id", "left")
        .join(payments, "order_id", "left")
        .join(
            reviews.select(
                "order_id",
                "review_score"
            ),
            "order_id",
            "left"
        )
        .withColumn(
            "date_key",
            date_format(
                to_date(col("order_purchase_timestamp")),
                "yyyyMMdd"
            ).cast("int")
        )
        .select(
            "order_id",
            "customer_id",
            "seller_id",
            "product_id",
            "date_key",
            "payment_value",
            "price",
            "freight_value",
            "payment_type",
            "payment_installments",
            "review_score",
            "order_status"
        )
    )

    rows = fact_orders.count()

    output = parquet_dir / "fact_orders"

    (
        fact_orders
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(f"file://{output}")
    )

    print("=" * 60)
    print("FACT_ORDERS CREATED")
    print(f"Rows : {rows}")
    print(f"Saved: {output}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    build_fact_orders()
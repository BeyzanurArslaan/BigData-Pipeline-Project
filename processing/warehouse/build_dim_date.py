from pathlib import Path

from pyspark.sql.functions import (
    col,
    date_format,
    to_date,
    year,
    month,
    dayofmonth
)

from processing.config import PARQUET_OUTPUT_DIR
from processing.spark_session import create_spark_session


def build_dim_date():

    spark = create_spark_session()

    parquet_dir = Path(PARQUET_OUTPUT_DIR)

    orders = spark.read.parquet(
        f"file://{parquet_dir / 'olist_orders_dataset'}"
    )

    dim_date = (
        orders
        .select("order_purchase_timestamp")
        .withColumn(
            "date",
            to_date(col("order_purchase_timestamp"))
        )
        .dropDuplicates(["date"])
        .withColumn(
            "date_key",
            date_format(col("date"), "yyyyMMdd").cast("int")
        )
        .withColumn(
            "year",
            year(col("date"))
        )
        .withColumn(
            "month",
            month(col("date"))
        )
        .withColumn(
            "day",
            dayofmonth(col("date"))
        )
        .select(
            "date_key",
            "date",
            "year",
            "month",
            "day"
        )
    )

    rows = dim_date.count()

    output = parquet_dir / "dim_date"

    (
        dim_date
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(f"file://{output}")
    )

    print("=" * 60)
    print("DIM_DATE CREATED")
    print(f"Rows : {rows}")
    print(f"Saved: {output}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    build_dim_date()
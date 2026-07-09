from pathlib import Path

from processing.config import PARQUET_OUTPUT_DIR
from processing.spark_session import create_spark_session


def build_dim_customers():

    spark = create_spark_session()

    parquet_dir = Path(PARQUET_OUTPUT_DIR)

    customers = spark.read.parquet(
        f"file://{parquet_dir / 'olist_customers_dataset'}"
    )

    dim_customers = (
        customers
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state",
            "customer_zip_code_prefix"
        )
        .dropDuplicates(["customer_id"])
    )

    rows = dim_customers.count()

    output = parquet_dir / "dim_customers"

    (
        dim_customers
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(f"file://{output}")
    )

    print("=" * 60)
    print("DIM_CUSTOMERS CREATED")
    print(f"Rows : {rows}")
    print(f"Saved: {output}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    build_dim_customers()
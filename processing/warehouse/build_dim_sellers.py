from pathlib import Path

from processing.config import PARQUET_OUTPUT_DIR
from processing.spark_session import create_spark_session


def build_dim_sellers():

    spark = create_spark_session()

    parquet_dir = Path(PARQUET_OUTPUT_DIR)

    sellers = spark.read.parquet(
        f"file://{parquet_dir / 'olist_sellers_dataset'}"
    )

    dim_sellers = (
        sellers
        .select(
            "seller_id",
            "seller_city",
            "seller_state",
            "seller_zip_code_prefix"
        )
        .dropDuplicates(["seller_id"])
    )

    rows = dim_sellers.count()

    output = parquet_dir / "dim_sellers"

    (
        dim_sellers
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(f"file://{output}")
    )

    print("=" * 60)
    print("DIM_SELLERS CREATED")
    print(f"Rows : {rows}")
    print(f"Saved: {output}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    build_dim_sellers()
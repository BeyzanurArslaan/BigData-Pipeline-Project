from pathlib import Path

from processing.config import PARQUET_OUTPUT_DIR
from processing.spark_session import create_spark_session


def build_dim_products():

    spark = create_spark_session()

    parquet_dir = Path(PARQUET_OUTPUT_DIR)

    products = spark.read.parquet(
        f"file://{parquet_dir / 'olist_products_dataset'}"
    )

    dim_products = (
        products
        .select(
            "product_id",
            "product_category_name",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm"
        )
        .dropDuplicates(["product_id"])
    )

    rows = dim_products.count()

    output = parquet_dir / "dim_products"

    (
        dim_products
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(f"file://{output}")
    )

    print("=" * 60)
    print("DIM_PRODUCTS CREATED")
    print(f"Rows : {rows}")
    print(f"Saved: {output}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    build_dim_products()
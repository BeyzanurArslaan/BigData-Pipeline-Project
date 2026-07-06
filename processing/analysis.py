from pathlib import Path
import gc
import traceback

from processing.config import PARQUET_OUTPUT_DIR, RAW_DATA_DIR
from processing.logger import get_logger
from processing.spark_session import create_spark_session


def main():

    logger = get_logger()

    spark = create_spark_session()

    PARQUET_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(Path(RAW_DATA_DIR).glob("*.csv"))

    logger.info("Found %d datasets", len(csv_files))

    for csv_file in csv_files:

        dataset_name = csv_file.stem
        output_path = PARQUET_OUTPUT_DIR / dataset_name

        try:

            logger.info("=" * 60)
            logger.info("Processing %s", dataset_name)

            df = (
                spark.read
                .option("header", True)
                .option("inferSchema", False)
                .csv(f"file://{csv_file}")
                .repartition(2)
            )

            logger.info("%s loaded", dataset_name)

            (
                df.write
                .mode("overwrite")
                .parquet(f"file://{output_path}")
            )

            logger.info("%s saved", dataset_name)

            spark.catalog.clearCache()
            del df
            gc.collect()

        except Exception as e:

            logger.error("FAILED: %s", dataset_name)
            logger.error(str(e))
            traceback.print_exc()
            break

    spark.stop()


if __name__ == "__main__":
    main()
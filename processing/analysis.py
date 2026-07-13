"""Phase 1 ingestion job for the Olist datasets."""

import logging
import sys
from pathlib import Path
from typing import List, Optional

from pyspark.sql import SparkSession

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from processing.config import HDFS_PARQUET_URI, RAW_DATA_DIR, join_hdfs_uri


logger = logging.getLogger(__name__)


def build_spark_session() -> SparkSession:
    """Create the Spark session used for the ingestion job."""

    return SparkSession.builder.appName("olist-phase1-ingestion").getOrCreate()


def load_csv_to_parquet(spark: SparkSession, csv_file: Path) -> None:
    """Read one CSV file and write it to the configured HDFS Parquet location."""

    dataset_name = csv_file.stem
    csv_uri = csv_file.resolve().as_uri()
    output_uri = join_hdfs_uri(HDFS_PARQUET_URI, dataset_name)

    logger.info("Processing %s -> %s", csv_uri, output_uri)

    dataframe = (
        spark.read.option("header", "true").option("inferSchema", "true").csv(csv_uri)
    )
    dataframe.write.mode("overwrite").parquet(output_uri)


def main() -> int:
    """Process all CSV datasets and return a shell-friendly exit code."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    spark: Optional[SparkSession] = None
    failed_datasets: List[str] = []

    try:
        spark = build_spark_session()

        if not RAW_DATA_DIR.exists():
            logger.error("Raw data directory does not exist: %s", RAW_DATA_DIR)
            return 1

        csv_files = sorted(RAW_DATA_DIR.glob("*.csv"))
        if not csv_files:
            logger.error("No CSV files found in %s", RAW_DATA_DIR)
            return 1

        logger.info("Found %d datasets", len(csv_files))

        for csv_file in csv_files:
            try:
                logger.info("=" * 60)
                logger.info("Processing %s", csv_file.stem)
                load_csv_to_parquet(spark, csv_file)
                logger.info("%s saved", csv_file.stem)
            except Exception:
                failed_datasets.append(csv_file.stem)
                logger.exception("Failed to process dataset %s", csv_file.name)

        if failed_datasets:
            logger.error("Completed with failures: %s", ", ".join(failed_datasets))
            return 1

        logger.info("All datasets processed successfully")
        return 0
    except Exception:
        logger.exception("Fatal error while running the Phase 1 ingestion job")
        return 1
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                logger.exception("Failed to stop Spark cleanly")


if __name__ == "__main__":
    raise SystemExit(main())

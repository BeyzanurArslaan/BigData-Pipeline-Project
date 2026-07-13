"""Configuration for the Phase 1 ingestion pipeline.

`RAW_DATA_DIR` stays as a local `Path` because Spark reads the mounted CSV
files from inside the container filesystem under `/app`.
"""

from pathlib import Path
import os

# Local mount point for the raw CSV files inside the Spark container.
RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "/app/data/raw"))

# HDFS endpoint used by Spark when writing processed Parquet output.
HDFS_URI = os.getenv("HDFS_URI", "hdfs://namenode:9000")

# Base HDFS location for the Phase 1 Parquet datasets.
HDFS_PARQUET_OUTPUT_URI = os.getenv(
    "HDFS_PARQUET_OUTPUT_URI",
    f"{HDFS_URI.rstrip('/')}/olist/parquet",
)

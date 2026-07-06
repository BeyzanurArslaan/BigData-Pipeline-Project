"""
Configuration module for the Olist Big Data Analytics Pipeline.
"""

from pathlib import Path

# ---------------------------------------------------------------------
# Project directories
# ---------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PARQUET_OUTPUT_DIR = PROJECT_ROOT / "data" / "parquet"

# ---------------------------------------------------------------------
# Spark configuration
# ---------------------------------------------------------------------

SPARK_APP_NAME = "Olist Big Data Analytics Pipeline"

SPARK_MASTER = "spark://spark-master:7077"

HDFS_URI = "hdfs://namenode:9000"
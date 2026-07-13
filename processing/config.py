"""Central configuration for the Olist pipeline.

`RAW_DATA_DIR` remains a local `Path` because the CSV files are mounted inside
the container filesystem. HDFS locations are kept as URI strings and are never
represented as `Path` objects.
"""

import os
import posixpath
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit


PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Local filesystem locations used inside the container.
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

# Spark runtime settings.
SPARK_APP_NAME = "Olist Big Data Analytics Pipeline"
SPARK_MASTER = "spark://spark-master:7077"

# Base HDFS URI used for curated Parquet inputs and the analytical warehouse.
HDFS_URI = os.getenv("HDFS_URI", "hdfs://namenode:9000")


def join_hdfs_uri(base_uri, *parts):
    """Join HDFS URI path segments without converting the URI to a Path."""

    parsed = urlsplit(base_uri)
    joined_path = posixpath.join(
        parsed.path or "/",
        *[part.strip("/") for part in parts if part],
    )
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            joined_path,
            parsed.query,
            parsed.fragment,
        )
    )


HDFS_PARQUET_URI = os.getenv(
    "HDFS_PARQUET_URI",
    join_hdfs_uri(HDFS_URI, "olist", "parquet"),
)

HDFS_WAREHOUSE_URI = os.getenv(
    "HDFS_WAREHOUSE_URI",
    join_hdfs_uri(HDFS_URI, "olist", "warehouse"),
)

# Backward-compatible alias for legacy code that still expects this name.
HDFS_PARQUET_OUTPUT_URI = HDFS_PARQUET_URI

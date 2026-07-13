"""Central configuration for the Olist pipeline.

Local `Path` objects are used only for files mounted inside the container
filesystem. HDFS locations stay as URI strings so they can be passed directly
to Spark without converting them into filesystem paths.
"""

import os
import posixpath
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit


PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Local mounts used by Phase 1 and legacy scripts.
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PARQUET_OUTPUT_DIR = PROJECT_ROOT / "data" / "parquet"

# Spark runtime settings.
SPARK_APP_NAME = "Olist Big Data Analytics Pipeline"
SPARK_MASTER = "spark://spark-master:7077"

# HDFS base URI for Phase 2 warehouse outputs and shared Parquet inputs.
HDFS_URI = os.getenv("HDFS_URI", "hdfs://namenode:9000")


def join_hdfs_uri(base_uri, *parts):
    """Join URI path segments without converting the location to a Path."""

    parsed = urlsplit(base_uri)
    joined_path = posixpath.join(parsed.path or "/", *[part.strip("/") for part in parts if part])
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

"""
Utility functions used across the project.

Currently this module provides logging utilities that help monitor
pipeline execution and simplify debugging.
"""

import logging


def get_logger() -> logging.Logger:
    """
    Create and configure the application logger.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )

    return logging.getLogger("olist_pipeline")
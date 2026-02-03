"""
Shared SparkSession utilities for all examples.

This module provides a consistent way to create SparkSession instances
across all chapter examples, with sensible defaults for local development.

Logging is configured via conf/log4j2.properties to:
- Write INFO logs to .logs/spark.log
- Only show ERROR on console (keeping output clean for learning)
"""

import os
from pathlib import Path

from pyspark import SparkContext
from pyspark.sql import SparkSession

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Path to log4j2 config
LOG4J2_CONFIG = PROJECT_ROOT / "conf" / "log4j2.properties"

# Base application name prefix for all Spark sessions
# Final app name will be: APP_NAME_PREFIX-<script_name>
APP_NAME_PREFIX = "DataAlgorithms"


def _ensure_logs_dir() -> None:
    """Ensure .logs directory exists."""
    logs_dir = PROJECT_ROOT / ".logs"
    logs_dir.mkdir(exist_ok=True)


def _snake_to_title(snake_str: str) -> str:
    """
    Convert snake_case string to TitleCase.

    Examples:
        average_by_key_reducebykey -> AverageByKeyReducebykey
        dna_base_count_basic -> DnaBaseCountBasic
        map_vs_flatmap -> MapVsFlatmap

    Args:
        snake_str: A snake_case string

    Returns:
        TitleCase version of the string
    """
    return "".join(word.capitalize() for word in snake_str.split("_"))


def _parse_script_identifier(script_id: str | None) -> str | None:
    """
    Parse a script identifier, which can be either a file path or a name.

    If it looks like a file path (contains / or ends with .py), extract
    the filename and convert from snake_case to TitleCase.

    Args:
        script_id: Either a file path (__file__) or a direct name

    Returns:
        Processed script name in TitleCase, or None
    """
    if script_id is None:
        return None

    # Detect if it's a file path
    if "/" in script_id or script_id.endswith(".py"):
        # Extract filename without extension
        stem = Path(script_id).stem
        return _snake_to_title(stem)

    # Already a formatted name, return as-is
    return script_id


def _build_app_name(script_name: str | None = None) -> str:
    """
    Build the full application name.

    Args:
        script_name: Optional script identifier (e.g., "AverageByKey")

    Returns:
        Full app name like "DataAlgorithms" or "DataAlgorithms-AverageByKey"
    """
    if script_name:
        return f"{APP_NAME_PREFIX}-{script_name}"
    return APP_NAME_PREFIX


def create_spark_session(
    script_name: str | None = None,
    master: str = "local[*]",
) -> SparkSession:
    """
    Create a SparkSession with common configurations.

    Logging is configured to write detailed logs to .logs/spark.log
    while only showing errors on the console.

    Args:
        script_name: Identifier for this script. Can be either:
                     - A file path like __file__ (auto-converts snake_case to TitleCase)
                     - A direct name like "AverageByKey"
                     Results in app name like "DataAlgorithms-AverageByKey"
        master: Spark master URL (default: local[*] for local development)

    Returns:
        Configured SparkSession instance

    Examples:
        # Using __file__ (recommended - DRY)
        spark = create_spark_session(__file__)

        # Using direct name
        spark = create_spark_session("MyCustomName")
    """
    _ensure_logs_dir()

    parsed_name = _parse_script_identifier(script_name)
    app_name = _build_app_name(parsed_name)

    # Change working directory context for log4j file output
    original_cwd = os.getcwd()
    os.chdir(PROJECT_ROOT)

    try:
        builder = SparkSession.builder.appName(app_name).master(master)

        # Configure log4j2 if config exists
        if LOG4J2_CONFIG.exists():
            builder = builder.config(
                "spark.driver.extraJavaOptions",
                f"-Dlog4j.configurationFile=file:{LOG4J2_CONFIG}",
            )

        spark = (
            builder.config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "2g")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )

        # Set log level for any logs after startup
        spark.sparkContext.setLogLevel("ERROR")

        return spark
    finally:
        os.chdir(original_cwd)


def get_spark_context(script_name: str | None = None) -> SparkContext:
    """
    Get SparkContext from a SparkSession.

    Useful for RDD-based examples from the book.

    Args:
        script_name: Identifier for this script (file path or name)

    Returns:
        SparkContext instance
    """
    spark = create_spark_session(script_name)
    return spark.sparkContext

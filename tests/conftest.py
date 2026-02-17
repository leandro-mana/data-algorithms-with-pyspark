"""
Pytest configuration and shared fixtures for PySpark tests.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Create a SparkSession for testing.

    Uses session scope to reuse the same Spark context across all tests,
    which significantly speeds up test execution.
    """
    spark = (
        SparkSession.builder
        .appName("pytest-pyspark")
        .master("local[2]")  # Use 2 cores for testing
        .config("spark.sql.shuffle.partitions", "2")  # Reduce partitions for faster tests
        .config("spark.ui.enabled", "false")  # Disable Spark UI for tests
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

    # Set log level to reduce noise during tests
    spark.sparkContext.setLogLevel("WARN")

    yield spark

    spark.stop()


@pytest.fixture(scope="session")
def sc(spark: SparkSession):
    """
    Get SparkContext from the SparkSession fixture.

    Useful for RDD-based tests.
    """
    return spark.sparkContext

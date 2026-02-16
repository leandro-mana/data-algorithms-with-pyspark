"""
Chapter 1: ETL Example with DataFrames

Demonstrates a complete ETL (Extract, Transform, Load) pipeline using
PySpark DataFrames with US Census 2010 sample data.

Pipeline:
    Extract  → Read JSON data into a DataFrame
    Transform → Filter seniors (age > 54) and add total population column
    Load     → Write results to local CSV output
"""

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from src.common.data_loader import PROJECT_ROOT, get_chapter_data_path
from src.common.spark_session import create_spark_session

# ---------------------------------------------------------------------------
# ETL functions — each step is a pure function operating on DataFrames
# ---------------------------------------------------------------------------


def extract(spark: SparkSession, input_path: str) -> DataFrame:
    """Read JSON data into a DataFrame."""
    return spark.read.json(input_path)


def transform(df: DataFrame) -> DataFrame:
    """Filter seniors (age > 54) and add a total population column."""
    seniors = df.filter(col("age") > 54)
    return seniors.withColumn("total", col("males") + col("females"))


def load(df: DataFrame, output_path: str) -> None:
    """Write the transformed DataFrame to CSV output."""
    df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the ETL pipeline: Census JSON → filter seniors → CSV output."""
    spark = create_spark_session(__file__)

    input_path = str(get_chapter_data_path("chapter_01", "census_2010.json"))
    output_path = str(PROJECT_ROOT / ".output" / "chapter_01" / "seniors")

    # Allow overriding the input path via command line
    if len(sys.argv) > 1:
        input_path = sys.argv[1]

    # --- Extract ---
    print("=== ETL Step 1: Extraction ===\n")
    census_df = extract(spark, input_path)
    print(f"Records extracted: {census_df.count()}")
    census_df.printSchema()
    census_df.show(5)

    # --- Transform ---
    print("=== ETL Step 2: Transformation ===\n")
    seniors_df = transform(census_df)
    print(f"Senior records (age > 54): {seniors_df.count()}")
    seniors_df.show()

    # --- Load ---
    print("=== ETL Step 3: Loading ===\n")
    load(seniors_df, output_path)
    print(f"Data written to: {output_path}")

    # Verify the load by reading back
    verification_df = spark.read.csv(output_path, header=True, inferSchema=True)
    print(f"Verification — records loaded: {verification_df.count()}")
    verification_df.show()

    spark.stop()


if __name__ == "__main__":
    main()

"""
Chapter 5: Physical Partitioning — Write Data Partitioned by Columns

Demonstrates partitioning DataFrames to disk by column values,
the key technique for optimizing queries in Athena, BigQuery, and Hive.

Two output formats are shown:
1. Text (CSV) — row-based, human-readable
2. Parquet — columnar, compressed, production-grade

The resulting directory structure enables partition pruning:
  output/year=2023/month=03/part-00000.parquet
  output/year=2023/month=06/part-00000.parquet
  ...

Downstream query tools scan only the relevant partition folders.
"""

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, split

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OUTPUT_BASE = ".output/chapter_05"


# ---------------------------------------------------------------------------
# Pure transformation functions
# ---------------------------------------------------------------------------


def load_transactions(spark: SparkSession, input_path: str) -> DataFrame:
    """Load transaction CSV into a DataFrame.

    Input format: customer_id,date,transaction_id,amount
    where date = DD/MM/YYYY
    """
    return spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)


def add_partition_columns(df: DataFrame) -> DataFrame:
    """Extract year and month from date string for partitioning.

    Input date format: DD/MM/YYYY
    Adds 'year' and 'month' columns derived from the date field.
    """
    date_parts = split(col("date"), "/")
    return df.withColumn("year", date_parts[2].cast("int")).withColumn(
        "month", date_parts[1].cast("int")
    )


def write_partitioned_csv(df: DataFrame, output_path: str) -> None:
    """Write DataFrame partitioned by year and month as CSV (text format).

    Creates directory structure: output_path/year=YYYY/month=MM/*.csv
    """
    (
        df.write.mode("overwrite")
        .partitionBy("year", "month")
        .option("header", "true")
        .csv(output_path)
    )


def write_partitioned_parquet(df: DataFrame, output_path: str) -> None:
    """Write DataFrame partitioned by year and month as Parquet.

    Uses repartition() before write to produce one file per partition.
    Creates directory structure: output_path/year=YYYY/month=MM/*.parquet
    """
    (
        df.repartition("year", "month")
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(output_path)
    )


def query_partitioned_data(spark: SparkSession, parquet_path: str) -> DataFrame:
    """Read partitioned Parquet and demonstrate partition pruning.

    When filtering on partition columns (year, month), Spark only
    reads the relevant partition directories — not the entire dataset.
    """
    df = spark.read.parquet(parquet_path)
    return df.filter((col("year") == 2024) & (col("month") == 9))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Partition transaction data by year/month in CSV and Parquet formats."""
    spark = create_spark_session(__file__)

    input_path = str(get_chapter_data_path("chapter_05", "transactions.csv"))
    if len(sys.argv) > 1:
        input_path = sys.argv[1]

    csv_output = f"{OUTPUT_BASE}/partitioned_csv"
    parquet_output = f"{OUTPUT_BASE}/partitioned_parquet"

    # --- Load and prepare ---
    print("=== Physical Partitioning: Write Data by Columns ===\n")
    raw_df = load_transactions(spark, input_path)
    print(f"Input: {input_path}")
    print(f"Total records: {raw_df.count()}\n")

    print("Raw data (first 5 rows):")
    raw_df.show(5, truncate=False)

    df = add_partition_columns(raw_df)
    print("With partition columns (year, month) added:")
    df.show(5, truncate=False)

    # --- Write partitioned CSV ---
    print(f"--- Writing partitioned CSV to {csv_output} ---")
    write_partitioned_csv(df, csv_output)
    print("Done.\n")

    # --- Write partitioned Parquet ---
    print(f"--- Writing partitioned Parquet to {parquet_output} ---")
    write_partitioned_parquet(df, parquet_output)
    print("Done.\n")

    # --- Query with partition pruning ---
    print("--- Query: year=2024, month=9 (partition pruning) ---")
    result = query_partitioned_data(spark, parquet_output)
    result.show(truncate=False)
    print(f"Records matching filter: {result.count()}")

    # --- Show distinct partitions ---
    print("\n--- Distinct partitions in dataset ---")
    partitions = (
        spark.read.parquet(parquet_output)
        .select("year", "month")
        .distinct()
        .orderBy("year", "month")
    )
    partitions.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

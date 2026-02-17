"""
Chapter 7: CSV and JSON Operations — Reading and Writing Common Formats

Demonstrates the DataFrameReader and DataFrameWriter APIs for CSV and JSON,
the two most common text-based data interchange formats:

CSV:
- Reading with headers and inferSchema
- Reading without headers (default _c0, _c1, ... column names)
- Reading with an explicit StructType schema
- Writing with custom separator and header options

JSON:
- Reading line-delimited JSON (one JSON object per line)
- Schema inference including nested types (arrays)
- Writing JSON output

Key insight: Spark's read/write API follows a consistent builder pattern:
  spark.read.format(...).option(...).load(path)
  df.write.format(...).option(...).save(path)
"""

import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session

OUTPUT_DIR = Path(".output/chapter_07")


# ---------------------------------------------------------------------------
# CSV — Reading
# ---------------------------------------------------------------------------


def read_csv_with_header(spark: SparkSession, path: str) -> DataFrame:
    """Read CSV with header row and automatic schema inference.

    Options:
      header=true  — first row is column names (not data)
      inferSchema=true — Spark samples the data to detect types
    """
    return (
        spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path)
    )


def read_csv_no_header(spark: SparkSession, path: str) -> DataFrame:
    """Read CSV without a header — columns get default names _c0, _c1, ...

    Without inferSchema, everything is StringType.
    """
    return (
        spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(path)
    )


def read_csv_explicit_schema(spark: SparkSession, path: str) -> DataFrame:
    """Read CSV using a user-defined StructType schema.

    Explicit schemas:
    - Avoid the overhead of schema inference (scanning data twice)
    - Guarantee exact column names and types
    - Are required for production pipelines
    """
    sensor_schema = StructType(
        [
            StructField("sensor_id", StringType(), nullable=False),
            StructField("temperature", DoubleType(), nullable=True),
            StructField("humidity", DoubleType(), nullable=True),
            StructField("reading_date", StringType(), nullable=True),
        ]
    )
    return spark.read.format("csv").option("header", "false").schema(sensor_schema).load(path)


# ---------------------------------------------------------------------------
# CSV — Writing
# ---------------------------------------------------------------------------


def write_csv_with_options(df: DataFrame, output_path: str) -> None:
    """Write DataFrame as CSV with custom separator and header.

    Write modes: append, overwrite, ignore, error (default).
    """
    (
        df.coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .option("sep", "|")
        .mode("overwrite")
        .save(output_path)
    )


# ---------------------------------------------------------------------------
# JSON — Reading
# ---------------------------------------------------------------------------


def read_json(spark: SparkSession, path: str) -> DataFrame:
    """Read line-delimited JSON (one JSON object per line).

    Spark automatically infers the schema from JSON, including
    nested types like arrays and structs.
    """
    return spark.read.json(path)


# ---------------------------------------------------------------------------
# JSON — Writing
# ---------------------------------------------------------------------------


def write_json(df: DataFrame, output_path: str) -> None:
    """Write DataFrame as JSON (one JSON object per line)."""
    df.coalesce(1).write.mode("overwrite").json(output_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Demonstrate CSV and JSON read/write operations."""
    spark = create_spark_session(__file__)

    csv_path = str(get_chapter_data_path("chapter_07", "employees.csv"))
    no_header_path = str(get_chapter_data_path("chapter_07", "sensors_no_header.csv"))
    json_path = str(get_chapter_data_path("chapter_07", "employees.json"))

    print("=== CSV & JSON Operations: DataFrameReader / DataFrameWriter ===\n")

    # --- CSV with header + inferSchema ---
    print("--- 1. CSV with header + inferSchema ---")
    employees = read_csv_with_header(spark, csv_path)
    employees.show(truncate=False)
    employees.printSchema()

    # --- CSV without header ---
    print("--- 2. CSV without header (default column names) ---")
    sensors_default = read_csv_no_header(spark, no_header_path)
    sensors_default.show(truncate=False)
    sensors_default.printSchema()

    # --- CSV with explicit schema ---
    print("--- 3. CSV with explicit StructType schema ---")
    sensors_typed = read_csv_explicit_schema(spark, no_header_path)
    sensors_typed.show(truncate=False)
    sensors_typed.printSchema()

    # --- Write CSV with pipe delimiter ---
    csv_output = str(OUTPUT_DIR / "employees_pipe_delimited")
    print(f"--- 4. Writing CSV with '|' separator to {csv_output} ---")
    write_csv_with_options(employees, csv_output)
    print("Written. Reading back:")
    pipe_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", "|")
        .option("inferSchema", "true")
        .load(csv_output)
    )
    pipe_df.show(truncate=False)

    # --- JSON read ---
    print("--- 5. JSON read with schema inference ---")
    emp_json = read_json(spark, json_path)
    emp_json.show(truncate=False)
    emp_json.printSchema()

    # --- Explode JSON array column ---
    print("--- 6. Exploding JSON array (skills) ---")
    skills_flat = emp_json.select(col("name"), explode(col("skills")).alias("skill"))
    skills_flat.show(truncate=False)

    # --- Write JSON ---
    json_output = str(OUTPUT_DIR / "employees_json_output")
    print(f"--- 7. Writing JSON to {json_output} ---")
    write_json(emp_json.select("name", "department", "salary"), json_output)
    print("Written. Reading back:")
    spark.read.json(json_output).show(truncate=False)

    # --- SQL on DataFrames ---
    print("--- 8. SQL queries on DataFrames (registerTempTable pattern) ---")
    employees.createOrReplaceTempView("employees")
    spark.sql(
        "SELECT department, COUNT(*) as headcount, ROUND(AVG(salary)) as avg_salary "
        "FROM employees GROUP BY department ORDER BY avg_salary DESC"
    ).show(truncate=False)

    # Cleanup output directory
    shutil.rmtree(str(OUTPUT_DIR), ignore_errors=True)

    spark.stop()


if __name__ == "__main__":
    main()

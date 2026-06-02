"""
Chapter 7: Parquet Operations — Columnar Storage for Analytics

Demonstrates reading and writing Parquet files with PySpark:

- Writing a DataFrame as Parquet (schema is embedded in the file)
- Reading Parquet back with automatic schema preservation
- Predicate pushdown — WHERE filters pushed to the storage layer
- Column pruning — only requested columns are read from disk
- Partition discovery — reading partitioned Parquet directories
- SQL queries directly on Parquet files (without loading first)

Key insight: Parquet is the default and recommended format for Spark
analytics. It's columnar (fast aggregations), self-describing (schema
embedded), and supports predicate pushdown and column pruning.
"""

import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count
from pyspark.sql.functions import round as spark_round

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session

OUTPUT_DIR = Path(".output/chapter_07")


# ---------------------------------------------------------------------------
# Parquet — Write
# ---------------------------------------------------------------------------


def write_parquet(df: DataFrame, output_path: str) -> None:
    """Write DataFrame as Parquet with snappy compression (default)."""
    df.write.mode("overwrite").parquet(output_path)


def write_partitioned_parquet(df: DataFrame, output_path: str, partition_col: str) -> None:
    """Write Parquet partitioned by a column for efficient filtering.

    Creates directory structure: output_path/partition_col=value/...
    This enables partition pruning — Spark skips entire directories
    when the WHERE clause filters on the partition column.
    """
    (
        df.repartition(partition_col)
        .write.mode("overwrite")
        .partitionBy(partition_col)
        .parquet(output_path)
    )


# ---------------------------------------------------------------------------
# Parquet — Read
# ---------------------------------------------------------------------------


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Read Parquet files — schema is automatically preserved."""
    return spark.read.parquet(path)


def read_parquet_with_column_pruning(
    spark: SparkSession, path: str, columns: list[str]
) -> DataFrame:
    """Read only specific columns from Parquet.

    Column pruning means Spark only reads the requested columns from disk,
    skipping the rest entirely. This is a major performance win for wide tables.
    """
    return spark.read.parquet(path).select(*columns)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Demonstrate Parquet read, write, partitioning, and query patterns."""
    spark = create_spark_session(__file__)

    csv_path = str(get_chapter_data_path("chapter_07", "employees.csv"))
    employees = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(csv_path)
    )

    print("=== Parquet Operations: Columnar Storage for Analytics ===\n")
    print("Source DataFrame:")
    employees.show(truncate=False)

    # --- 1. Write as Parquet ---
    parquet_path = str(OUTPUT_DIR / "employees.parquet")
    print(f"--- 1. Writing Parquet to {parquet_path} ---")
    write_parquet(employees, parquet_path)
    print("Done.\n")

    # --- 2. Read Parquet back (schema preserved) ---
    print("--- 2. Reading Parquet back (schema auto-preserved) ---")
    parquet_df = read_parquet(spark, parquet_path)
    parquet_df.show(truncate=False)
    parquet_df.printSchema()

    # --- 3. Column pruning ---
    print("--- 3. Column pruning (only name, salary) ---")
    pruned = read_parquet_with_column_pruning(spark, parquet_path, ["name", "salary"])
    pruned.show(truncate=False)

    # --- 4. Predicate pushdown ---
    print("--- 4. Predicate pushdown (salary > 90000) ---")
    high_earners = parquet_df.filter(col("salary") > 90000)
    high_earners.show(truncate=False)
    print(
        "Note: Spark pushes the filter to the Parquet reader, so rows\n"
        "that don't match are skipped at the storage layer (not in memory).\n"
    )

    # --- 5. Partitioned Parquet ---
    partitioned_path = str(OUTPUT_DIR / "employees_by_dept.parquet")
    print(f"--- 5. Writing partitioned Parquet (by department) to {partitioned_path} ---")
    write_partitioned_parquet(employees, partitioned_path, "department")
    print("Done. Reading back:\n")

    partitioned_df = read_parquet(spark, partitioned_path)
    partitioned_df.show(truncate=False)

    # Partition pruning: only Engineering partition is read
    print("--- 6. Partition pruning (WHERE department = 'Engineering') ---")
    eng_only = partitioned_df.filter(col("department") == "Engineering")
    eng_only.show(truncate=False)
    print(
        "With partitioned Parquet, Spark skips the Finance/ and Marketing/\n"
        "directories entirely — only the Engineering/ partition is read.\n"
    )

    # --- 7. SQL queries on Parquet ---
    print("--- 7. SQL queries on Parquet (createOrReplaceTempView) ---")
    parquet_df.createOrReplaceTempView("emp_parquet")
    spark.sql(
        "SELECT department, "
        "COUNT(*) as headcount, "
        "ROUND(AVG(salary)) as avg_salary, "
        "MAX(salary) as max_salary "
        "FROM emp_parquet "
        "GROUP BY department "
        "ORDER BY avg_salary DESC"
    ).show(truncate=False)

    # --- 8. Parquet vs CSV comparison ---
    print("--- 8. Format comparison: Parquet is self-describing ---")
    print("CSV schema (inferred — requires extra scan):")
    employees.printSchema()
    print("Parquet schema (embedded — no inference needed):")
    parquet_df.printSchema()

    # --- 9. Aggregation directly on Parquet ---
    print("--- 9. Aggregation on Parquet (column pruning + predicate pushdown) ---")
    result = (
        parquet_df.filter(col("age") >= 35)
        .groupBy("department")
        .agg(
            count("*").alias("senior_count"),
            spark_round(avg("salary"), 2).alias("avg_salary"),
        )
        .orderBy(col("avg_salary").desc())
    )
    result.show(truncate=False)

    # Cleanup output directory
    shutil.rmtree(str(OUTPUT_DIR), ignore_errors=True)

    spark.stop()


if __name__ == "__main__":
    main()

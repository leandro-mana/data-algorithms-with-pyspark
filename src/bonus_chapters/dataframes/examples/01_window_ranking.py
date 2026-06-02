"""
Bonus Chapter: DataFrame Window Functions - Example 1 (Ranking)

Window functions compute a value for each row based on a group of related
rows (the "window") without collapsing them into a single aggregate — unlike
groupBy, every input row produces exactly one output row.

This example covers ranking functions, which assign a position to each row
within its partition based on an ordering criterion.

Ranking functions:
    row_number()  — unique sequential rank (1,2,3,4): ties broken arbitrarily
    rank()        — same rank for ties, gaps after (1,1,3,4)
    dense_rank()  — same rank for ties, no gaps  (1,1,2,3)
    percent_rank()— relative rank as a fraction between 0.0 and 1.0
    ntile(n)      — divide rows into n equal-sized buckets

Window specification:
    partitionBy() — like GROUP BY: defines the group each row is ranked within
    orderBy()     — defines the ranking order within each partition
"""

import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import Window

from src.common.spark_session import create_spark_session

DEFAULT_INPUT = Path(__file__).parent.parent / "data" / "sales.csv"


def main() -> None:
    input_path = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_INPUT)

    spark = create_spark_session(__file__)

    print("=== Window Functions: Ranking ===\n")

    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.printSchema()

    # --- Ranking within department by total sales across all months ---
    total_sales = df.groupBy("employee", "department").agg(F.sum("sales").alias("total_sales"))

    # Window partitioned by department, ordered by total sales descending
    dept_window = Window.partitionBy("department").orderBy(F.col("total_sales").desc())

    ranked = total_sales.select(
        "employee",
        "department",
        "total_sales",
        F.row_number().over(dept_window).alias("row_number"),
        F.rank().over(dept_window).alias("rank"),
        F.dense_rank().over(dept_window).alias("dense_rank"),
        F.percent_rank().over(dept_window).alias("percent_rank"),
        F.ntile(2).over(dept_window).alias("ntile_2"),  # top / bottom half
    )

    print("--- Employee rankings within department (by total sales) ---")
    ranked.orderBy("department", "row_number").show(truncate=False)

    # --- Practical use: top performer per department ---
    top_per_dept = (
        ranked.filter(F.col("row_number") == 1)
        .select("department", "employee", "total_sales")
        .orderBy("department")
    )

    print("--- Top performer per department ---")
    top_per_dept.show(truncate=False)

    # --- rank() vs dense_rank() vs row_number() — when they differ ---
    # Introduce a tie by giving Alice and Carol equal sales
    tied_data = [
        ("Alice", "Eng", 15000),
        ("Carol", "Eng", 15000),  # tie with Alice
        ("Eve", "Eng", 11000),
    ]
    tied_df = spark.createDataFrame(tied_data, ["employee", "dept", "sales"])
    tie_window = Window.partitionBy("dept").orderBy(F.col("sales").desc())

    print("--- rank() vs dense_rank() vs row_number() with tied values ---")
    tied_df.select(
        "employee",
        "sales",
        F.row_number().over(tie_window).alias("row_number"),  # 1, 2, 3
        F.rank().over(tie_window).alias("rank"),  # 1, 1, 3
        F.dense_rank().over(tie_window).alias("dense_rank"),  # 1, 1, 2
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

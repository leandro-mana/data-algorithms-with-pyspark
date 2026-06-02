"""
Bonus Chapter: DataFrame Window Functions - Example 2 (Analytics)

Analytic window functions compute values that depend on neighbouring rows
within the same partition. Unlike ranking functions, they look forward or
backward in time (lag/lead) or accumulate values across rows (running totals,
moving averages).

Analytic functions covered:
    lag(col, n)     — value from n rows BEFORE the current row
    lead(col, n)    — value from n rows AFTER the current row
    sum() OVER ...  — running (cumulative) total within partition
    avg() OVER ...  — moving average over a sliding row frame

Window frames (rowsBetween / rangeBetween):
    UNBOUNDED PRECEDING → current row  : cumulative (running total)
    n PRECEDING         → current row  : trailing n-row window (moving avg)
    current row         → UNBOUNDED FOLLOWING : suffix aggregate
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

    print("=== Window Functions: Analytics ===\n")

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Window ordered by month within each employee — for time-series functions
    employee_time = Window.partitionBy("employee").orderBy("month")

    # --- lag / lead: month-over-month change ---
    mom_df = (
        df.select(
            "employee",
            "month",
            "sales",
            F.lag("sales", 1).over(employee_time).alias("prev_month_sales"),
            F.lead("sales", 1).over(employee_time).alias("next_month_sales"),
        )
        .withColumn(
            "mom_change",
            F.col("sales") - F.col("prev_month_sales"),
        )
        .withColumn(
            "mom_pct",
            F.round(
                (F.col("sales") - F.col("prev_month_sales")) / F.col("prev_month_sales") * 100,
                1,
            ),
        )
    )

    print("--- Month-over-month sales change per employee ---")
    mom_df.orderBy("employee", "month").show(truncate=False)

    # --- Running total (cumulative sum) ---
    # Frame: from the start of the partition up to and including the current row
    cumulative_window = employee_time.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    running_total_df = df.select(
        "employee",
        "month",
        "sales",
        F.sum("sales").over(cumulative_window).alias("cumulative_sales"),
    )

    print("--- Running cumulative sales per employee ---")
    running_total_df.orderBy("employee", "month").show(truncate=False)

    # --- 2-month trailing moving average ---
    # Frame: current row and 1 row before (2-row window)
    moving_avg_window = employee_time.rowsBetween(-1, Window.currentRow)

    moving_avg_df = df.select(
        "employee",
        "month",
        "sales",
        F.round(F.avg("sales").over(moving_avg_window), 0).alias("2mo_moving_avg"),
    )

    print("--- 2-month trailing moving average per employee ---")
    moving_avg_df.orderBy("employee", "month").show(truncate=False)

    # --- Department-level: each employee's share of department total ---
    dept_total_window = Window.partitionBy("department", "month")

    share_df = df.select(
        "employee",
        "department",
        "month",
        "sales",
        F.sum("sales").over(dept_total_window).alias("dept_total"),
    ).withColumn(
        "sales_share_pct",
        F.round(F.col("sales") / F.col("dept_total") * 100, 1),
    )

    print("--- Each employee's share of department sales per month ---")
    share_df.orderBy("month", "department", F.col("sales_share_pct").desc()).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

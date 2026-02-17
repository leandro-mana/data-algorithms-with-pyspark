"""
Chapter 11: Map-Side Join (Broadcast Join)

Optimizes joins by broadcasting the smaller table to all workers,
eliminating the expensive shuffle phase entirely.

Pattern:
  1. Collect the dimension table (small) into a dict
  2. Broadcast the dict to all executors
  3. Map over the fact table (large), looking up values from the broadcast

Requirements:
  - Dimension table must fit in executor memory
  - Only works for equi-joins on the dimension key

This is how Spark's broadcast join hint works under the hood.
"""

import sys

from pyspark.rdd import RDD
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session

# ---------------------------------------------------------------------------
# RDD-based map-side join
# ---------------------------------------------------------------------------


def rdd_map_side_join(
    fact_rdd: RDD,
    dimension_dict: dict,
    sc,
) -> RDD:
    """Join a large fact RDD with a small dimension dict via broadcast.

    fact_rdd: RDD[(dept_id, (emp_id, name, salary))]
    dimension_dict: {dept_id: (dept_name, location)}

    Returns: RDD[(emp_id, name, salary, dept_name, location)]
    """
    broadcasted = sc.broadcast(dimension_dict)

    def lookup_department(record):
        dept_id = record[0]
        emp_id, name, salary = record[1]
        dept_info = broadcasted.value.get(dept_id)
        if dept_info:
            dept_name, location = dept_info
            return (emp_id, name, salary, dept_name, location)
        return (emp_id, name, salary, "UNKNOWN", "UNKNOWN")

    return fact_rdd.map(lookup_department)


# ---------------------------------------------------------------------------
# DataFrame-based map-side join with UDFs
# ---------------------------------------------------------------------------


def df_map_side_join(
    fact_df: DataFrame,
    dimension_dict: dict,
    spark: SparkSession,
) -> DataFrame:
    """Join a DataFrame with a broadcast dict using UDFs.

    This approach avoids the shuffle entirely — each executor looks up
    the dimension value from a local copy of the broadcast variable.
    """
    sc = spark.sparkContext
    dept_broadcast = sc.broadcast(dimension_dict)

    def get_dept_name(dept_id: int) -> str:
        info = dept_broadcast.value.get(dept_id)
        return info[0] if info else "UNKNOWN"

    def get_dept_location(dept_id: int) -> str:
        info = dept_broadcast.value.get(dept_id)
        return info[1] if info else "UNKNOWN"

    dept_name_udf = F.udf(get_dept_name, StringType())
    dept_location_udf = F.udf(get_dept_location, StringType())

    return fact_df.withColumn("dept_name", dept_name_udf(F.col("dept_id"))).withColumn(
        "location", dept_location_udf(F.col("dept_id"))
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Demonstrate map-side join with broadcast variables."""
    spark: SparkSession = create_spark_session(__file__)
    sc = spark.sparkContext

    emp_path = str(get_chapter_data_path("chapter_11", "employees_dept.csv"))
    dept_path = str(get_chapter_data_path("chapter_11", "departments.csv"))

    if len(sys.argv) > 2:
        emp_path = sys.argv[1]
        dept_path = sys.argv[2]

    print("=" * 60)
    print("Chapter 11: Map-Side Join (Broadcast Join)")
    print("=" * 60)

    # Load dimension table into a dict (it's small — fits in memory)
    dept_df = spark.read.csv(dept_path, header=True, inferSchema=True)
    print("\nDimension table (departments — small, broadcast to all workers):")
    dept_df.show(truncate=False)

    # Build dict: {dept_id: (dept_name, location)}
    dept_dict: dict[int, tuple[str, str]] = {
        row.dept_id: (row.dept_name, row.location) for row in dept_df.collect()
    }

    # Load fact table
    emp_df = spark.read.csv(emp_path, header=True, inferSchema=True)
    print("Fact table (employees — large in production):")
    emp_df.show(truncate=False)

    # --- RDD-based map-side join ---
    print("--- RDD Map-Side Join (broadcast dict) ---\n")

    emp_rdd = emp_df.rdd.map(lambda row: (row.dept_id, (row.emp_id, row.name, row.salary)))
    joined_rdd = rdd_map_side_join(emp_rdd, dept_dict, sc)

    print("Result (emp_id, name, salary, dept_name, location):")
    for record in sorted(joined_rdd.collect()):
        print(f"  {record}")

    # --- DataFrame-based map-side join with UDFs ---
    print("\n--- DataFrame Map-Side Join (broadcast + UDF) ---\n")

    joined_df = df_map_side_join(emp_df, dept_dict, spark)
    joined_df.orderBy("emp_id").show(truncate=False)

    # --- Compare with Spark's built-in broadcast join ---
    print("--- Spark Built-in Broadcast Join (for comparison) ---\n")

    broadcast_joined = emp_df.join(F.broadcast(dept_df), "dept_id", "inner")
    broadcast_joined.orderBy("emp_id").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

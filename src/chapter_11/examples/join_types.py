"""
Chapter 11: Join Design Patterns — Join Types

Demonstrates three join types using both DataFrames and a custom
RDD-based MapReduce implementation (without using Spark's join()).

Join types:
  - INNER: only rows with matching keys in BOTH tables
  - LEFT:  all rows from left table, NULLs where right has no match
  - RIGHT: all rows from right table, NULLs where left has no match

The custom RDD join shows how joins work under the hood in MapReduce:
  1. Tag each record with its source table
  2. Union and groupByKey
  3. Compute Cartesian product of matching values
"""

import itertools
import sys

from pyspark.rdd import RDD
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session

# ---------------------------------------------------------------------------
# DataFrame joins (built-in)
# ---------------------------------------------------------------------------


def demonstrate_df_joins(t1: DataFrame, t2: DataFrame) -> None:
    """Show inner, left, and right joins using DataFrame API."""
    print("--- DataFrame Joins ---\n")

    print("Table T1:")
    t1.show(truncate=False)
    print("Table T2:")
    t2.show(truncate=False)

    print("INNER JOIN (only matching keys):")
    t1.join(t2, "key", "inner").orderBy("key", F.col("T1_value")).show(truncate=False)

    print("LEFT JOIN (all T1 rows, NULLs for missing T2):")
    t1.join(t2, "key", "left").orderBy("key", F.col("T1_value")).show(truncate=False)

    print("RIGHT JOIN (all T2 rows, NULLs for missing T1):")
    t1.join(t2, "key", "right").orderBy("key", F.col("T2_value")).show(truncate=False)


# ---------------------------------------------------------------------------
# Custom RDD join (MapReduce style — without using Spark's join())
# ---------------------------------------------------------------------------


def tag_record(table_name: str):
    """Return a function that tags each (key, value) with its source table."""

    def _tag(pair: tuple) -> tuple:
        return (pair[0], (table_name, pair[1]))

    return _tag


def cartesian_product_inner(entry: tuple) -> list[tuple]:
    """Compute Cartesian product of T1 and T2 values for a given key.

    This implements the reduce phase of a MapReduce join:
      1. Separate values by source table tag
      2. Produce all (t1_val, t2_val) combinations
    """
    key = entry[0]
    values = list(entry[1])

    t1_vals = [v for tag, v in values if tag == "T1"]
    t2_vals = [v for tag, v in values if tag == "T2"]

    if not t1_vals or not t2_vals:
        return []

    return [(key, combo) for combo in itertools.product(t1_vals, t2_vals)]


def custom_rdd_inner_join(t1_rdd: RDD, t2_rdd: RDD) -> RDD:
    """Implement inner join using union + groupByKey + Cartesian product.

    This is how joins work in MapReduce under the hood:
      Map:    tag each record with source table
      Shuffle: groupByKey
      Reduce:  Cartesian product of T1 × T2 values per key
    """
    tagged_t1 = t1_rdd.map(tag_record("T1"))
    tagged_t2 = t2_rdd.map(tag_record("T2"))

    combined = tagged_t1.union(tagged_t2)
    grouped = combined.groupByKey()

    return grouped.flatMap(cartesian_product_inner)


def demonstrate_rdd_join(sc, t1_path: str, t2_path: str) -> None:
    """Show custom RDD-based MapReduce join."""
    print("--- Custom RDD Join (MapReduce Style) ---\n")

    def parse_kv(line: str) -> tuple[str, int]:
        tokens = line.split(",")
        return (tokens[0], int(tokens[1]))

    raw_t1 = sc.textFile(t1_path)
    header_t1 = raw_t1.first()
    t1_rdd = raw_t1.filter(lambda line: line != header_t1).map(parse_kv)

    raw_t2 = sc.textFile(t2_path)
    header_t2 = raw_t2.first()
    t2_rdd = raw_t2.filter(lambda line: line != header_t2).map(parse_kv)

    print("T1 (tagged):")
    for item in t1_rdd.map(tag_record("T1")).collect():
        print(f"  {item}")

    print("\nT2 (tagged):")
    for item in t2_rdd.map(tag_record("T2")).collect():
        print(f"  {item}")

    joined = custom_rdd_inner_join(t1_rdd, t2_rdd)
    print("\nCustom INNER JOIN result (key, (t1_val, t2_val)):")
    for key, (t1_val, t2_val) in sorted(joined.collect()):
        print(f"  key={key}  T1={t1_val}  T2={t2_val}")

    # Compare with Spark's built-in join
    spark_joined = t1_rdd.join(t2_rdd)
    print("\nSpark built-in join (same result):")
    for key, (t1_val, t2_val) in sorted(spark_joined.collect()):
        print(f"  key={key}  T1={t1_val}  T2={t2_val}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Demonstrate join types with DataFrames and custom RDD join."""
    spark: SparkSession = create_spark_session(__file__)
    sc = spark.sparkContext

    t1_path = str(get_chapter_data_path("chapter_11", "table_t1.csv"))
    t2_path = str(get_chapter_data_path("chapter_11", "table_t2.csv"))

    if len(sys.argv) > 2:
        t1_path = sys.argv[1]
        t2_path = sys.argv[2]

    print("=" * 60)
    print("Chapter 11: Join Design Patterns — Join Types")
    print("=" * 60)

    # DataFrame joins
    t1_df = spark.read.csv(t1_path, header=True, inferSchema=True).withColumnRenamed(
        "value", "T1_value"
    )
    t2_df = spark.read.csv(t2_path, header=True, inferSchema=True).withColumnRenamed(
        "value", "T2_value"
    )
    demonstrate_df_joins(t1_df, t2_df)

    # Custom RDD join
    demonstrate_rdd_join(sc, t1_path, t2_path)

    print()
    spark.stop()


if __name__ == "__main__":
    main()

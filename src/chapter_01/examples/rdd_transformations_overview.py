"""
Chapter 1: RDD Transformations Overview

Demonstrates key RDD transformations: filter, flatMap, groupByKey, reduceByKey,
sortBy, cartesian, and takeOrdered.
"""

from src.common.spark_session import create_spark_session


def main() -> None:
    """Main entry point demonstrating various RDD transformations."""
    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print("=== RDD Transformations Overview ===\n")

    # --- Filter Transformation ---
    numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    evens = numbers.filter(lambda x: x % 2 == 0)
    print(f"filter() - Even numbers: {evens.collect()}")

    # --- FlatMap Transformation ---
    sentences = sc.parallelize(
        ["hello world", "spark is great", "pyspark rocks", "Python is also great"]
    )
    words = sentences.flatMap(lambda s: s.split())
    print(f"flatMap() - Words: {words.collect()}")

    # --- GroupByKey Transformation ---
    pairs = sc.parallelize([("a", 1), ("b", 2), ("a", 3), ("b", 4), ("a", 5)])
    grouped = pairs.groupByKey().mapValues(list)
    print(f"groupByKey() - Grouped: {grouped.collect()}")

    # --- ReduceByKey Transformation ---
    word_counts = words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
    print(f"reduceByKey() - Word counts: {sorted(word_counts.collect())}")

    # --- SortBy Transformation ---
    sorted_counts = word_counts.sortBy(lambda x: x[1], ascending=False)  # type: ignore[type-var]
    print(f"sortBy() - Sorted by count: {sorted_counts.collect()}")

    # --- Cartesian Transformation ---
    rdd1 = sc.parallelize([1, 2])
    rdd2 = sc.parallelize(["a", "b"])
    cartesian = rdd1.cartesian(rdd2)
    print(f"cartesian() - All pairs: {cartesian.collect()}")

    # --- TakeOrdered ---
    scores: list[tuple[str, int]] = [
        ("alice", 85),
        ("bob", 92),
        ("carol", 78),
        ("dave", 95),
    ]
    scores_rdd = sc.parallelize(scores)
    top_2 = scores_rdd.takeOrdered(2, key=lambda x: -x[1])  # type: ignore[type-var]
    print(f"takeOrdered() - Top 2 scores: {top_2}")

    spark.stop()


if __name__ == "__main__":
    main()

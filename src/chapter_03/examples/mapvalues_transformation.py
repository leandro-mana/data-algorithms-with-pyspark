"""
Chapter 3: mapValues() Transformation

Demonstrates mapValues() which applies a function only to the values
of key-value pairs, preserving the keys.
"""

from src.common.spark_session import create_spark_session


def main() -> None:
    """Main entry point demonstrating mapValues transformation."""
    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print("=== mapValues() Transformation Demo ===\n")

    # Example 1: Modify values while keeping keys
    pairs: list[tuple[str, int]] = [("a", 1), ("b", 2), ("c", 3), ("a", 4), ("b", 5)]
    print(f"Input pairs: {pairs}\n")

    rdd = sc.parallelize(pairs)

    # Double all values
    doubled = rdd.mapValues(lambda v: v * 2)
    print(f"mapValues(v * 2): {doubled.collect()}")

    # Square all values
    squared = rdd.mapValues(lambda v: v**2)
    print(f"mapValues(v ** 2): {squared.collect()}")

    # Convert values to strings
    stringified = rdd.mapValues(lambda v: f"value_{v}")
    print(f"mapValues(f'value_{{v}}'): {stringified.collect()}")

    # Example 2: Transform tuples in values
    print("\n--- Example 2: Working with tuple values ---\n")

    score_pairs: list[tuple[str, tuple[int, int]]] = [
        ("alice", (85, 1)),
        ("bob", (90, 1)),
        ("alice", (92, 1)),
        ("bob", (88, 1)),
    ]
    print(f"Score pairs: {score_pairs}")

    score_rdd = sc.parallelize(score_pairs)

    # First, reduce to get totals
    totals = score_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    print("\nAfter reduceByKey (sum scores and counts):")
    print(f"  {totals.collect()}")

    # Then use mapValues to compute average
    averages = totals.mapValues(lambda v: v[0] / v[1])
    print("\nAfter mapValues (compute average):")
    print(f"  {averages.collect()}")

    # Example 3: Comparison with map()
    print("\n--- Comparison: map() vs mapValues() ---\n")

    data: list[tuple[str, int]] = [("x", 10), ("y", 20)]
    data_rdd = sc.parallelize(data)

    # Using map() - must handle both key and value
    with_map = data_rdd.map(lambda kv: (kv[0], kv[1] + 100))
    print(f"Using map(): {with_map.collect()}")

    # Using mapValues() - cleaner for value-only transforms
    with_mapvalues = data_rdd.mapValues(lambda v: v + 100)
    print(f"Using mapValues(): {with_mapvalues.collect()}")

    print("\nmapValues() is cleaner when you only need to transform values!")

    spark.stop()


if __name__ == "__main__":
    main()

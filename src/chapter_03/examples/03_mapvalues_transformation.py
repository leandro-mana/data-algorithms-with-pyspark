"""
Chapter 3: mapValues() Transformation

Demonstrates mapValues() which applies a function only to the values
of key-value pairs, preserving the keys and partitioning.

Algorithm:
    1. Input: RDD of (key, value) pairs
    2. mapValues() applies function to each value
    3. Keys remain unchanged
    4. Partitioning is preserved (important for subsequent operations)

Trade-offs:
    - Cleaner than map() when only transforming values
    - Preserves partitioning (map() may not)
    - More efficient for hash-partitioned RDDs
    - Cannot modify keys (use map() if key changes are needed)
    - Common pattern: reduceByKey() followed by mapValues() for averages
"""

from pyspark import RDD, SparkContext

from src.common.spark_session import create_spark_session


def double_value(value: int) -> int:
    """
    Double the input value.

    Args:
        value: Integer to double

    Returns:
        The value multiplied by 2
    """
    return value * 2


def square_value(value: int) -> int:
    """
    Square the input value.

    Args:
        value: Integer to square

    Returns:
        The value raised to the power of 2
    """
    return value**2


def format_value(value: int) -> str:
    """
    Format an integer value as a descriptive string.

    Args:
        value: Integer to format

    Returns:
        Formatted string like "value_5"
    """
    return f"value_{value}"


def add_score_tuples(a: tuple[int, int], b: tuple[int, int]) -> tuple[int, int]:
    """
    Add two (score, count) tuples together.

    Used with reduceByKey to aggregate scores for averaging.

    Args:
        a: First (score, count) tuple
        b: Second (score, count) tuple

    Returns:
        Combined (total_score, total_count) tuple
    """
    return (a[0] + b[0], a[1] + b[1])


def compute_average(sum_count: tuple[int, int]) -> float:
    """
    Compute average from (sum, count) tuple.

    Args:
        sum_count: Tuple of (total_sum, count)

    Returns:
        The average value (sum / count)
    """
    total, count = sum_count
    return total / count


def add_offset(value: int, offset: int = 100) -> int:
    """
    Add a fixed offset to the value.

    Args:
        value: Original integer value
        offset: Amount to add (default 100)

    Returns:
        Value plus offset
    """
    return value + offset


def demonstrate_basic_transforms(sc: SparkContext) -> None:
    """
    Demonstrate basic value transformations with mapValues.

    Shows doubling, squaring, and string formatting of values
    while preserving keys.

    Args:
        sc: SparkContext for creating RDDs
    """
    print("--- Example 1: Basic value transformations ---\n")

    pairs: list[tuple[str, int]] = [
        ("a", 1),
        ("b", 2),
        ("c", 3),
        ("a", 4),
        ("b", 5),
    ]
    print(f"Input pairs: {pairs}\n")

    pairs_rdd: RDD[tuple[str, int]] = sc.parallelize(pairs)

    # Double all values - keys unchanged
    doubled_rdd: RDD[tuple[str, int]] = pairs_rdd.mapValues(double_value)
    print(f"mapValues(double): {doubled_rdd.collect()}")

    # Square all values - keys unchanged
    squared_rdd: RDD[tuple[str, int]] = pairs_rdd.mapValues(square_value)
    print(f"mapValues(square): {squared_rdd.collect()}")

    # Convert values to formatted strings - keys unchanged
    formatted_rdd: RDD[tuple[str, str]] = pairs_rdd.mapValues(format_value)
    print(f"mapValues(format): {formatted_rdd.collect()}")


def demonstrate_averaging_pattern(sc: SparkContext) -> None:
    """
    Demonstrate the reduceByKey + mapValues pattern for computing averages.

    This is a common pattern:
    1. Start with (key, (value, 1)) pairs
    2. reduceByKey to sum values and counts
    3. mapValues to compute average from (sum, count)

    Args:
        sc: SparkContext for creating RDDs
    """
    print("\n--- Example 2: Computing averages with reduceByKey + mapValues ---\n")

    score_pairs: list[tuple[str, tuple[int, int]]] = [
        ("alice", (85, 1)),
        ("bob", (90, 1)),
        ("alice", (92, 1)),
        ("bob", (88, 1)),
    ]
    print(f"Score pairs (name, (score, count)): {score_pairs}")

    score_rdd: RDD[tuple[str, tuple[int, int]]] = sc.parallelize(score_pairs)

    # Step 1: reduceByKey to sum scores and counts
    totals_rdd: RDD[tuple[str, tuple[int, int]]] = score_rdd.reduceByKey(add_score_tuples)
    print("\nAfter reduceByKey (sum scores and counts):")
    print(f"  {totals_rdd.collect()}")

    # Step 2: mapValues to compute average from (sum, count)
    averages_rdd: RDD[tuple[str, float]] = totals_rdd.mapValues(compute_average)
    print("\nAfter mapValues (compute average):")
    print(f"  {averages_rdd.collect()}")


def demonstrate_map_vs_mapvalues(sc: SparkContext) -> None:
    """
    Compare map() and mapValues() for value-only transformations.

    Shows why mapValues() is cleaner and preferred when only
    modifying values.

    Args:
        sc: SparkContext for creating RDDs
    """
    print("\n--- Example 3: Comparison - map() vs mapValues() ---\n")

    data: list[tuple[str, int]] = [("x", 10), ("y", 20)]
    data_rdd: RDD[tuple[str, int]] = sc.parallelize(data)

    print(f"Input: {data}\n")

    # Using map() - must destructure and reconstruct the tuple
    # Less readable, doesn't preserve partitioning
    with_map: RDD[tuple[str, int]] = data_rdd.map(lambda kv: (kv[0], add_offset(kv[1])))
    print(f"Using map(lambda kv: (kv[0], kv[1] + 100)): {with_map.collect()}")

    # Using mapValues() - cleaner, preserves partitioning
    with_mapvalues: RDD[tuple[str, int]] = data_rdd.mapValues(lambda v: add_offset(v))
    print(f"Using mapValues(lambda v: v + 100):        {with_mapvalues.collect()}")

    print("\nmapValues() advantages:")
    print("  1. Cleaner syntax - no tuple destructuring needed")
    print("  2. Preserves partitioning - important for shuffles")
    print("  3. Intent is clear - only values are being transformed")


def main() -> None:
    """
    Main entry point demonstrating mapValues transformation.

    Runs three examples:
    1. Basic value transformations (double, square, format)
    2. Average computation pattern (reduceByKey + mapValues)
    3. Comparison between map() and mapValues()
    """
    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print("=== mapValues() Transformation Demo ===\n")

    # Run all demonstrations
    demonstrate_basic_transforms(sc)
    demonstrate_averaging_pattern(sc)
    demonstrate_map_vs_mapvalues(sc)

    spark.stop()


if __name__ == "__main__":
    main()

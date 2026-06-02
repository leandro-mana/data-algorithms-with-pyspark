"""
Chapter 3: mapPartitions() Transformation

Demonstrates mapPartitions() which processes an entire partition at once,
enabling efficient batch operations and local aggregation.

Algorithm:
    1. Spark distributes data across partitions
    2. mapPartitions() calls your function once per partition
    3. Function receives iterator of all elements in partition
    4. Function yields results (can be any number of outputs)
    5. Results from all partitions are combined into output RDD

Trade-offs:
    - More efficient than map() when setup/teardown is expensive
    - Enables local aggregation (InMapper Combiner pattern)
    - Reduces shuffle data by aggregating before network transfer
    - Must process all elements before yielding (memory consideration)
    - Ideal for: database connections, ML model loading, batch API calls
"""

import sys
from collections.abc import Iterable, Iterator
from pathlib import Path

from src.common.spark_session import create_spark_session

# Default input file for sample data
DEFAULT_INPUT = Path(__file__).parent.parent / "data" / "sample_numbers.txt"


def count_negative_zero_positive(
    partition: Iterable[str],
) -> Iterator[tuple[int, int, int]]:
    """
    Count negative, zero, and positive numbers in a partition.

    This demonstrates local aggregation within a partition - we process
    all elements and emit a single summary tuple, reducing shuffle data.

    Args:
        partition: Iterator of number strings from this partition

    Yields:
        Single tuple (negative_count, zero_count, positive_count)
    """
    negative = 0
    zero = 0
    positive = 0

    for num_str in partition:
        try:
            num = int(num_str.strip())
            if num < 0:
                negative += 1
            elif num > 0:
                positive += 1
            else:
                zero += 1
        except ValueError:
            continue  # Skip non-numeric lines

    yield (negative, zero, positive)


def compute_statistics(
    partition: Iterable[str],
) -> Iterator[tuple[int | None, int | None, int, int]]:
    """
    Compute min, max, sum, count for numbers in a partition.

    Another example of per-partition aggregation. Instead of emitting
    each number, we compute summary statistics locally.

    Args:
        partition: Iterator of number strings from this partition

    Yields:
        Single tuple (min, max, sum, count) - None values if partition is empty
    """
    numbers: list[int] = []
    for num_str in partition:
        try:
            numbers.append(int(num_str.strip()))
        except ValueError:
            continue

    if numbers:
        yield (min(numbers), max(numbers), sum(numbers), len(numbers))
    else:
        yield (None, None, 0, 0)


def create_sample_numbers() -> list[str]:
    """
    Create sample number data for demonstration.

    Returns:
        List of number strings including negative, zero, and positive values
    """
    return ["-5", "0", "10", "-3", "7", "0", "15", "-1", "0", "20", "8", "-2"]


def aggregate_counts(a: tuple[int, int, int], b: tuple[int, int, int]) -> tuple[int, int, int]:
    """
    Combine two (negative, zero, positive) count tuples.

    Used to aggregate partition-level counts into global totals.

    Args:
        a: First count tuple (negative, zero, positive)
        b: Second count tuple (negative, zero, positive)

    Returns:
        Combined count tuple
    """
    return (a[0] + b[0], a[1] + b[1], a[2] + b[2])


def print_partition_contents(partitions: list[list[str]]) -> None:
    """
    Print the contents of each partition for debugging.

    Args:
        partitions: List of partition contents from rdd.glom().collect()
    """
    print("--- Partition contents ---")
    for i, partition in enumerate(partitions):
        print(f"  Partition {i}: {partition}")


def print_count_results(results: list[tuple[int, int, int]], totals: tuple[int, int, int]) -> None:
    """
    Print the negative/zero/positive count results.

    Args:
        results: Per-partition count tuples
        totals: Aggregated totals across all partitions
    """
    print("\n--- Count Negative/Zero/Positive per partition ---")
    for i, (n, z, p) in enumerate(results):
        print(f"  Partition {i}: negative={n}, zero={z}, positive={p}")
    print(f"\n  Total: negative={totals[0]}, zero={totals[1]}, positive={totals[2]}")


def print_statistics_results(
    results: list[tuple[int | None, int | None, int, int]],
) -> None:
    """
    Print the statistics results per partition.

    Args:
        results: Per-partition statistics tuples (min, max, sum, count)
    """
    print("\n--- Statistics per partition ---")
    for i, (mn, mx, sm, ct) in enumerate(results):
        print(f"  Partition {i}: min={mn}, max={mx}, sum={sm}, count={ct}")


def main() -> None:
    """
    Main entry point demonstrating mapPartitions transformation.

    Demonstrates two use cases:
    1. Counting negative/zero/positive numbers per partition
    2. Computing statistics (min, max, sum, count) per partition

    Both examples show how mapPartitions enables efficient local aggregation.
    """
    input_path = sys.argv[1] if len(sys.argv) >= 2 else str(DEFAULT_INPUT)

    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print("=== mapPartitions() Transformation Demo ===\n")

    # Load data from file or use sample data if file doesn't exist
    input_file = Path(input_path)
    if input_file.exists():
        rdd = sc.textFile(str(input_file), minPartitions=3)
        print(f"Input file: {input_path}")
    else:
        sample_numbers = create_sample_numbers()
        rdd = sc.parallelize(sample_numbers, numSlices=3)
        print(f"Sample data: {sample_numbers}")

    print(f"Number of partitions: {rdd.getNumPartitions()}\n")

    # Show partition contents for understanding data distribution
    partitions = rdd.glom().collect()
    print_partition_contents(partitions)

    # Example 1: Count negative, zero, positive per partition
    # Each partition emits ONE tuple instead of N tuples
    nzp_rdd = rdd.mapPartitions(count_negative_zero_positive)
    nzp_results = nzp_rdd.collect()

    # Aggregate across all partitions
    total_nzp = nzp_rdd.reduce(aggregate_counts)
    print_count_results(nzp_results, total_nzp)

    # Example 2: Compute statistics per partition
    # Again, each partition emits ONE tuple with summary stats
    stats_rdd = rdd.mapPartitions(compute_statistics)
    stats_results = stats_rdd.collect()
    print_statistics_results(stats_results)

    spark.stop()


if __name__ == "__main__":
    main()

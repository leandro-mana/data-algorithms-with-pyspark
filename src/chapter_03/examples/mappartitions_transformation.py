"""
Chapter 3: mapPartitions() Transformation

Demonstrates mapPartitions() which processes an entire partition at once,
enabling efficient batch operations and local aggregation.
"""

import sys
from collections.abc import Iterable, Iterator

from src.common.spark_session import create_spark_session


def count_negative_zero_positive(
    partition: Iterable[str],
) -> Iterator[tuple[int, int, int]]:
    """
    Count negative, zero, and positive numbers in a partition.

    This demonstrates local aggregation within a partition - we process
    all elements and emit a single summary tuple.

    Args:
        partition: Iterator of number strings

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

    Another example of per-partition aggregation.

    Args:
        partition: Iterator of number strings

    Yields:
        Single tuple (min, max, sum, count)
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
    """Create sample number data for demonstration."""
    return ["-5", "0", "10", "-3", "7", "0", "15", "-1", "0", "20", "8", "-2"]


def main() -> None:
    """Main entry point demonstrating mapPartitions transformation."""
    input_path = sys.argv[1] if len(sys.argv) >= 2 else None

    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print("=== mapPartitions() Transformation Demo ===\n")

    if input_path:
        rdd = sc.textFile(input_path, minPartitions=3)
        print(f"Input file: {input_path}")
    else:
        sample_numbers = create_sample_numbers()
        rdd = sc.parallelize(sample_numbers, numSlices=3)
        print(f"Sample data: {sample_numbers}")

    print(f"Number of partitions: {rdd.getNumPartitions()}\n")

    # Show partition contents
    print("--- Partition contents ---")
    partitions = rdd.glom().collect()
    for i, partition in enumerate(partitions):
        print(f"  Partition {i}: {partition}")

    # Example 1: Count negative, zero, positive
    print("\n--- Count Negative/Zero/Positive per partition ---")
    nzp_rdd = rdd.mapPartitions(count_negative_zero_positive)
    nzp_results = nzp_rdd.collect()
    for i, (n, z, p) in enumerate(nzp_results):
        print(f"  Partition {i}: negative={n}, zero={z}, positive={p}")

    # Aggregate across all partitions
    total_nzp = nzp_rdd.reduce(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))
    print(f"\n  Total: negative={total_nzp[0]}, zero={total_nzp[1]}, positive={total_nzp[2]}")

    # Example 2: Compute statistics per partition
    print("\n--- Statistics per partition ---")
    stats_rdd = rdd.mapPartitions(compute_statistics)
    stats_results = stats_rdd.collect()
    for i, (mn, mx, sm, ct) in enumerate(stats_results):
        print(f"  Partition {i}: min={mn}, max={mx}, sum={sm}, count={ct}")

    spark.stop()


if __name__ == "__main__":
    main()

"""
Chapter 5: Partition Basics — RDD Partition Management

Demonstrates how Spark partitions data and the key APIs for
inspecting and managing partitions:
- getNumPartitions() — check partition count
- glom() — view elements per partition
- repartition() — increase or decrease partitions (full shuffle)
- coalesce() — decrease partitions without shuffle
- mapPartitions() — process entire partitions for efficiency
"""

from collections.abc import Iterable, Iterator

from src.common.spark_session import create_spark_session

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

NUMBERS: list[int] = list(range(1, 13))  # [1, 2, 3, ..., 12]


# ---------------------------------------------------------------------------
# Partition-level processing functions
# ---------------------------------------------------------------------------


def partition_sum(iterator: Iterable[int]) -> Iterator[tuple[int, int]]:
    """Sum elements within a partition, yielding (sum, count).

    This is the mapPartitions pattern — process a batch of elements
    and emit aggregated results, reducing shuffle data.
    """
    total = 0
    count = 0
    for num in iterator:
        total += num
        count += 1
    if count > 0:
        yield (total, count)


def partition_stats(iterator: Iterable[int]) -> Iterator[dict[str, int | float]]:
    """Compute basic statistics per partition.

    Demonstrates that mapPartitions can yield richer structures
    than simple values.
    """
    values = list(iterator)
    if values:
        yield {
            "count": len(values),
            "sum": sum(values),
            "min": min(values),
            "max": max(values),
            "mean": round(sum(values) / len(values), 2),
        }


# ---------------------------------------------------------------------------
# Display helper
# ---------------------------------------------------------------------------


def show_partitions(rdd, label: str) -> None:
    """Display partition contents using glom()."""
    partitions = rdd.glom().collect()
    print(f"\n{label}")
    print(f"  Partitions: {rdd.getNumPartitions()}")
    for i, partition in enumerate(partitions):
        print(f"  Partition {i}: {partition}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Demonstrate RDD partition management."""
    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print("=== Partition Basics: RDD Partition Management ===\n")
    print(f"Data: {NUMBERS}")

    # --- 1. Default partitioning ---
    rdd_default = sc.parallelize(NUMBERS)
    show_partitions(rdd_default, "1. Default partitioning (Spark decides):")

    # --- 2. Explicit partitioning at creation ---
    rdd_3 = sc.parallelize(NUMBERS, 3)
    show_partitions(rdd_3, "2. Explicit 3 partitions at creation:")

    # --- 3. repartition() — increase partitions (triggers shuffle) ---
    rdd_6 = rdd_3.repartition(6)
    show_partitions(rdd_6, "3. repartition(6) — increased from 3 (full shuffle):")

    # --- 4. coalesce() — decrease partitions (no shuffle) ---
    rdd_2 = rdd_3.coalesce(2)
    show_partitions(rdd_2, "4. coalesce(2) — decreased from 3 (no shuffle):")

    # --- 5. mapPartitions() — partition-level aggregation ---
    rdd_3 = sc.parallelize(NUMBERS, 3)
    print("\n5. mapPartitions() — sum per partition:")
    sums = rdd_3.mapPartitions(partition_sum).collect()
    for total, count in sums:
        print(f"  Partition sum={total}, count={count}")
    print(f"  Grand total: {sum(t for t, _ in sums)}")

    # --- 6. mapPartitions() — richer statistics ---
    print("\n6. mapPartitions() — statistics per partition:")
    stats = rdd_3.mapPartitions(partition_stats).collect()
    for i, stat in enumerate(stats):
        print(f"  Partition {i}: {stat}")

    # --- 7. Partition count impact on parallelism ---
    print("\n7. Partition count impact on parallelism:")
    for n_parts in [1, 4, 12]:
        rdd = sc.parallelize(NUMBERS, n_parts)
        partitions = rdd.glom().collect()
        sizes = [len(p) for p in partitions]
        print(f"  {n_parts:2d} partitions → sizes: {sizes}")

    spark.stop()


if __name__ == "__main__":
    main()

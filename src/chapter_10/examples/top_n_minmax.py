"""
Chapter 10: Top-N and MinMax Design Patterns

Two partition-level summarization patterns using mapPartitions():

Top-N:
  1. Each partition finds its LOCAL top-N using a bounded sorted structure
  2. A single reducer merges all local top-N lists into the GLOBAL top-N
  Result: only N pairs emitted per partition — minimal shuffle

MinMax:
  1. Each partition emits (local_min, local_max, local_count)
  2. Driver collects all partition summaries and computes global min/max/count
  Result: exactly 1 triple per partition — O(partitions) data to process

Both patterns work because top-N and min/max are associative operations.
"""

import heapq
import sys
from typing import NamedTuple

from pyspark.rdd import RDD

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session

# ---------------------------------------------------------------------------
# Top-N pattern
# ---------------------------------------------------------------------------


class TopNResult(NamedTuple):
    url: str
    frequency: int


def top_n_per_partition(n: int):
    """Return a mapPartitions function that finds local top-N.

    Uses a min-heap of size N — O(partition_size * log N) per partition.
    heapq maintains smallest at index 0, so we pop the smallest when full.
    """

    def _find_top_n(partition) -> list[tuple[int, str]]:
        heap: list[tuple[int, str]] = []
        for url, freq in partition:
            if len(heap) < n:
                heapq.heappush(heap, (freq, url))
            elif freq > heap[0][0]:
                heapq.heapreplace(heap, (freq, url))
        return heap

    return _find_top_n


def find_top_n(rdd: RDD, n: int) -> list[TopNResult]:
    """Find global top-N using mapPartitions.

    Step 1: each partition emits its local top-N (at most N pairs)
    Step 2: collect all local top-N lists
    Step 3: merge into a single global top-N
    """
    local_tops = rdd.mapPartitions(top_n_per_partition(n))
    all_candidates = local_tops.collect()

    # Final merge — same heap logic on the collected candidates
    heap: list[tuple[int, str]] = []
    for freq, url in all_candidates:
        if len(heap) < n:
            heapq.heappush(heap, (freq, url))
        elif freq > heap[0][0]:
            heapq.heapreplace(heap, (freq, url))

    return [TopNResult(url, freq) for freq, url in sorted(heap, reverse=True)]


def find_bottom_n(rdd: RDD, n: int) -> list[TopNResult]:
    """Find global bottom-N — same pattern but keep smallest values.

    Uses a max-heap (negate values) to pop the largest.
    """

    def _find_bottom_n(partition) -> list[tuple[int, str]]:
        heap: list[tuple[int, str]] = []
        for url, freq in partition:
            neg = -freq
            if len(heap) < n:
                heapq.heappush(heap, (neg, url))
            elif neg > heap[0][0]:
                heapq.heapreplace(heap, (neg, url))
        return heap

    local_bottoms = rdd.mapPartitions(_find_bottom_n)
    all_candidates = local_bottoms.collect()

    heap: list[tuple[int, str]] = []
    for neg, url in all_candidates:
        if len(heap) < n:
            heapq.heappush(heap, (neg, url))
        elif neg > heap[0][0]:
            heapq.heapreplace(heap, (neg, url))

    return [TopNResult(url, -neg) for neg, url in sorted(heap)]


# ---------------------------------------------------------------------------
# MinMax pattern
# ---------------------------------------------------------------------------


class MinMaxResult(NamedTuple):
    min_val: int
    max_val: int
    total_count: int


def minmax_per_partition(partition) -> list[tuple[int, int, int]]:
    """Find (min, max, count) for a single partition.

    Handles empty partitions gracefully by returning an empty list.
    """
    local_min: int = 0
    local_max: int = 0
    local_count = 0
    first_time = True

    for record in partition:
        numbers = [int(n) for n in record.split(",")]
        rec_min = min(numbers)
        rec_max = max(numbers)

        if first_time:
            local_min = rec_min
            local_max = rec_max
            first_time = False
        else:
            local_min = min(local_min, rec_min)
            local_max = max(local_max, rec_max)
        local_count += len(numbers)

    if not first_time:
        return [(local_min, local_max, local_count)]
    return []  # empty partition


def find_minmax(rdd: RDD) -> MinMaxResult:
    """Find global (min, max, count) using mapPartitions.

    Each partition emits at most 1 triple → O(num_partitions) merge.
    """
    partition_results = rdd.mapPartitions(minmax_per_partition).collect()

    global_min = partition_results[0][0]
    global_max = partition_results[0][1]
    global_count = partition_results[0][2]

    for local_min, local_max, local_count in partition_results[1:]:
        global_min = min(global_min, local_min)
        global_max = max(global_max, local_max)
        global_count += local_count

    return MinMaxResult(min_val=global_min, max_val=global_max, total_count=global_count)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def parse_url_frequency(line: str) -> tuple[str, int]:
    """Parse 'url,frequency' into (url, frequency)."""
    tokens = line.split(",")
    return (tokens[0], int(tokens[1]))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Demonstrate Top-N and MinMax design patterns."""
    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    url_path = str(get_chapter_data_path("chapter_10", "url_frequencies.csv"))
    num_path = str(get_chapter_data_path("chapter_10", "numbers.csv"))

    if len(sys.argv) > 1:
        url_path = sys.argv[1]

    print("=" * 60)
    print("Chapter 10: Top-N and MinMax Design Patterns")
    print("=" * 60)

    # --- Top-N ---
    print("\n--- Top-N Pattern (mapPartitions) ---\n")

    raw_urls = sc.textFile(url_path)
    header = raw_urls.first()
    url_rdd = raw_urls.filter(lambda line: line != header).map(parse_url_frequency)

    n = 5
    print(f"All URLs ({url_rdd.count()} total):")
    for url, freq in sorted(url_rdd.collect(), key=lambda x: -x[1]):
        print(f"  {url:40s} freq={freq}")

    print(f"\nTop-{n}:")
    for result in find_top_n(url_rdd, n):
        print(f"  {result.url:40s} freq={result.frequency}")

    print(f"\nBottom-{n}:")
    for result in find_bottom_n(url_rdd, n):
        print(f"  {result.url:40s} freq={result.frequency}")

    # Also show takeOrdered (Spark's built-in, simpler but less scalable)
    print(f"\nTop-{n} via takeOrdered (built-in):")
    for url, freq in url_rdd.takeOrdered(n, key=lambda x: -x[1]):  # type: ignore[type-var]
        print(f"  {url:40s} freq={freq}")

    # --- MinMax ---
    print("\n--- MinMax Pattern (mapPartitions) ---\n")

    numbers_rdd = sc.textFile(num_path)
    print(f"Input: {num_path}")
    print(f"Records: {numbers_rdd.count()}\n")

    mm_result = find_minmax(numbers_rdd)
    print(f"  Minimum: {mm_result.min_val}")
    print(f"  Maximum: {mm_result.max_val}")
    print(f"  Count:   {mm_result.total_count}")

    print()
    spark.stop()


if __name__ == "__main__":
    main()

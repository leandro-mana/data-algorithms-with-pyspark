"""
Chapter 10: In-Mapper Combining — Character Frequency

Three progressively more efficient approaches to counting character
frequencies in a text corpus:

  1. Basic MapReduce  — emit (char, 1) per character (most shuffle)
  2. Per-record        — local dict per record, emit unique chars (less shuffle)
  3. Per-partition      — local dict per partition via mapPartitions (least shuffle)

The in-mapper combining pattern reduces network traffic by aggregating
locally before the shuffle. Approach 3 is the most efficient because
it creates a single dict per partition instead of per record.
"""

import sys
from collections import defaultdict

from pyspark.rdd import RDD

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session

# ---------------------------------------------------------------------------
# Approach 1: Basic MapReduce — emit (char, 1) for every character
# ---------------------------------------------------------------------------


def basic_mapper(record: str) -> list[tuple[str, int]]:
    """Emit (char, 1) for every character in every word.

    Problem: generates a huge number of pairs for large datasets.
    """
    return [(c, 1) for word in record.lower().split() for c in word]


def char_freq_basic(rdd: RDD) -> RDD:
    """Basic MapReduce: flatMap + reduceByKey."""
    return rdd.flatMap(basic_mapper).reduceByKey(lambda a, b: a + b)


# ---------------------------------------------------------------------------
# Approach 2: In-mapper combining per record — aggregate locally per record
# ---------------------------------------------------------------------------


def per_record_combiner(record: str) -> list[tuple[str, int]]:
    """Aggregate character frequencies within a single record.

    Emits fewer pairs than basic: one per unique character per record.
    """
    freq: dict[str, int] = defaultdict(int)
    for word in record.lower().split():
        for c in word:
            freq[c] += 1
    return list(freq.items())


def char_freq_per_record(rdd: RDD) -> RDD:
    """Per-record combining: local dict per record + reduceByKey."""
    return rdd.flatMap(per_record_combiner).reduceByKey(lambda a, b: a + b)


# ---------------------------------------------------------------------------
# Approach 3: In-mapper combining per partition — aggregate per partition
# ---------------------------------------------------------------------------


def per_partition_combiner(partition) -> list[tuple[str, int]]:
    """Aggregate character frequencies across an entire partition.

    Creates ONE dict for potentially millions of records.
    This is the most efficient approach — fewest pairs emitted.
    """
    freq: dict[str, int] = defaultdict(int)
    for record in partition:
        for word in record.lower().split():
            for c in word:
                freq[c] += 1
    return list(freq.items())


def char_freq_per_partition(rdd: RDD) -> RDD:
    """Per-partition combining: mapPartitions + reduceByKey."""
    return rdd.mapPartitions(per_partition_combiner).reduceByKey(lambda a, b: a + b)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Compare three in-mapper combining approaches."""
    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    input_path = str(get_chapter_data_path("chapter_10", "corpus.txt"))
    if len(sys.argv) > 1:
        input_path = sys.argv[1]

    rdd = sc.textFile(input_path)

    print("=" * 60)
    print("Chapter 10: In-Mapper Combining — Character Frequency")
    print("=" * 60)
    print(f"\nInput: {input_path}")
    print(f"Records: {rdd.count()}\n")

    approaches = [
        ("Basic MapReduce (char, 1)", char_freq_basic),
        ("Per-record combining", char_freq_per_record),
        ("Per-partition combining", char_freq_per_partition),
    ]

    for name, func in approaches:
        result = sorted(func(rdd).collect())
        total_chars = sum(count for _, count in result)
        print(f"--- {name} ---")
        print(f"  Unique characters: {len(result)}, Total count: {total_chars}")
        for char, count in result:
            print(f"    '{char}': {count}")
        print()

    spark.stop()


if __name__ == "__main__":
    main()

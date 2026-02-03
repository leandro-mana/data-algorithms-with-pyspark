"""
Chapter 2: DNA Base Count - Using mapPartitions (InMapper Combiner)

This version uses mapPartitions() for local aggregation within each partition
before the shuffle, reducing network traffic (InMapper Combiner pattern).
"""

import sys
from collections import Counter
from collections.abc import Iterable, Iterator

from src.common.spark_session import create_spark_session

# Valid DNA bases
VALID_BASES = frozenset("ATCGN")
DNA_BASES = ["A", "T", "C", "G", "N"]


def count_bases_in_partition(partition: Iterable[str]) -> Iterator[tuple[str, int]]:
    """
    Count bases within a single partition (InMapper Combiner).

    This approach aggregates locally before emitting, reducing shuffle.

    Args:
        partition: Iterator of FASTA lines in this partition

    Yields:
        (base, count) tuples for this partition
    """
    local_counts: Counter[str] = Counter()

    for record in partition:
        if record.startswith(">"):
            local_counts["z"] += 1
        else:
            for base in record.upper():
                if base in VALID_BASES:
                    local_counts[base] += 1

    for base, count in local_counts.items():
        yield (base, count)


def create_sample_fasta() -> list[str]:
    """Create sample FASTA data for demonstration."""
    return [
        ">sequence1",
        "ATCGATCGATCG",
        "GCTAGCTAGCTA",
        ">sequence2",
        "AAATTTTCCCCGGGG",
        "NNNNATCGATCG",
    ]


def print_results(results: dict[str, int]) -> None:
    """Print the base count results."""
    print("\n--- Results ---")
    print(f"Total FASTA records: {results.get('z', 0)}")
    print("\nBase frequencies:")
    for base in DNA_BASES:
        count = results.get(base, 0)
        print(f"  {base}: {count}")

    total_bases = sum(results.get(b, 0) for b in DNA_BASES)
    print(f"\nTotal bases: {total_bases}")


def main() -> None:
    """Main entry point for DNA base counting using mapPartitions."""
    input_path = sys.argv[1] if len(sys.argv) >= 2 else None

    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print("=== DNA Base Count (mapPartitions - InMapper Combiner) ===\n")

    if input_path:
        records_rdd = sc.textFile(input_path)
        print(f"Input file: {input_path}")
    else:
        sample_fasta = create_sample_fasta()
        records_rdd = sc.parallelize(sample_fasta, numSlices=2)
        print("Using sample FASTA data (2 partitions)")

    print(f"Number of partitions: {records_rdd.getNumPartitions()}")

    # Use mapPartitions for local aggregation, then reduceByKey for global
    base_counts = records_rdd.mapPartitions(count_bases_in_partition).reduceByKey(
        lambda a, b: a + b
    )

    results = dict(base_counts.collect())
    print_results(results)

    spark.stop()


if __name__ == "__main__":
    main()

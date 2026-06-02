"""
Chapter 2: DNA Base Count - Version 3 (mapPartitions - Partition-level InMapper Combiner)

Counts the frequency of DNA bases (A, T, C, G, N) in FASTA format files.
This version uses mapPartitions() for partition-level aggregation.
Note: N is included to count ambiguous bases, and 'z' is used to count total FASTA records.

Algorithm:
    1. mapPartitions: Process entire partition, build single hashmap, emit aggregated pairs
    2. reduceByKey: Sum all counts per base

Trade-offs:
    - Most efficient: only emits one set of pairs per partition
    - Minimal shuffle data (at most 6 pairs per partition: A, T, C, G, N, z)
    - Processes all records in partition before emitting
    - Best choice for large datasets where shuffle is the bottleneck
"""

import sys
from collections import Counter
from collections.abc import Iterable, Iterator
from pathlib import Path

from src.common.spark_session import create_spark_session

# Valid DNA bases to count
VALID_BASES = frozenset("ATCGN")
DNA_BASES = ["A", "T", "C", "G", "N"]

# Default input file
DEFAULT_INPUT = Path(__file__).parent.parent / "data" / "sample.fasta"


def process_fasta_partition(partition: Iterable[str]) -> Iterator[tuple[str, int]]:
    """
    Process an entire partition using mapPartitions (InMapper Combiner).

    Aggregates all base counts for the entire partition into a single
    hashmap before emitting, minimizing shuffle data.

    Args:
        partition: Iterator of FASTA lines in this partition

    Yields:
        (base, count) tuples - one per unique base in the entire partition
    """
    # Single hashmap for the entire partition
    partition_counts: Counter[str] = Counter()

    for record in partition:
        if record.startswith(">"):
            partition_counts["z"] += 1
        else:
            for base in record.upper():
                if base in VALID_BASES:
                    partition_counts[base] += 1

    # Emit aggregated counts for this partition
    for base, count in partition_counts.items():
        yield (base, count)


def print_results(results: dict[str, int], num_partitions: int) -> None:
    """Print the base count results in a formatted way."""
    print("\n--- Results ---")
    print(f"Number of partitions processed: {num_partitions}")
    print(f"Total FASTA records: {results.get('z', 0)}")
    print("\nBase frequencies:")

    for base in DNA_BASES:
        count = results.get(base, 0)
        print(f"  {base}: {count}")

    total_bases = sum(results.get(b, 0) for b in DNA_BASES)
    print(f"\nTotal bases: {total_bases}")


def main() -> None:
    """Main entry point for DNA base counting using mapPartitions."""
    input_path = sys.argv[1] if len(sys.argv) >= 2 else str(DEFAULT_INPUT)

    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print("=== DNA Base Count - Version 3 (mapPartitions) ===\n")
    print(f"Input file: {input_path}")

    # Read FASTA file
    records_rdd = sc.textFile(input_path)
    num_partitions = records_rdd.getNumPartitions()
    print(f"Total lines: {records_rdd.count()}")
    print(f"Number of partitions: {num_partitions}")

    # Step 1: mapPartitions - aggregate entire partition into hashmap
    # Step 2: reduceByKey - sum all partition counts per base
    base_counts = records_rdd.mapPartitions(process_fasta_partition).reduceByKey(lambda a, b: a + b)

    results = dict(base_counts.collect())
    print_results(results, num_partitions)

    spark.stop()


if __name__ == "__main__":
    main()

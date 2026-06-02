"""
Chapter 2: DNA Base Count - Version 2 (InMapper Combiner per Record)

Counts the frequency of DNA bases (A, T, C, G, N) in FASTA format files.
This version uses a hashmap to aggregate counts within each record before emitting.
Note: N is included to count ambiguous bases, and 'z' is used to count total FASTA records.

Algorithm:
    1. flatMap with local aggregation: Build hashmap per record, emit aggregated pairs
    2. reduceByKey: Sum all counts per base

Trade-offs:
    - Reduces intermediate key-value pairs (one per unique base per record)
    - Less shuffle data than Version 1
    - Slightly more memory per record (hashmap overhead)
    - Still creates hashmap per record (partition-level would be more efficient)
"""

import sys
from collections import Counter
from pathlib import Path

from src.common.spark_session import create_spark_session

# Valid DNA bases to count
VALID_BASES = frozenset("ATCGN")
DNA_BASES = ["A", "T", "C", "G", "N"]

# Default input file
DEFAULT_INPUT = Path(__file__).parent.parent / "data" / "sample.fasta"


def process_fasta_as_hashmap(record: str) -> list[tuple[str, int]]:
    """
    Process a FASTA record using local hashmap aggregation (InMapper Combiner).

    Instead of emitting (base, 1) for each character, this aggregates
    counts within the record first, then emits (base, count) pairs.

    Args:
        record: A line from a FASTA file

    Returns:
        List of (base, count) tuples - one per unique base in record
    """
    if record.startswith(">"):
        return [("z", 1)]

    # Local aggregation using Counter (hashmap)
    local_counts: Counter[str] = Counter()

    for base in record.upper():
        if base in VALID_BASES:
            local_counts[base] += 1

    # Emit aggregated counts
    return list(local_counts.items())


def print_results(results: dict[str, int]) -> None:
    """Print the base count results in a formatted way."""
    print("\n--- Results ---")
    print(f"Total FASTA records: {results.get('z', 0)}")
    print("\nBase frequencies:")

    for base in DNA_BASES:
        count = results.get(base, 0)
        print(f"  {base}: {count}")

    total_bases = sum(results.get(b, 0) for b in DNA_BASES)
    print(f"\nTotal bases: {total_bases}")


def main() -> None:
    """Main entry point for DNA base counting using InMapper Combiner per record."""
    input_path = sys.argv[1] if len(sys.argv) >= 2 else str(DEFAULT_INPUT)

    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print("=== DNA Base Count - Version 2 (InMapper Combiner per Record) ===\n")
    print(f"Input file: {input_path}")

    # Read FASTA file
    records_rdd = sc.textFile(input_path)
    print(f"Total lines: {records_rdd.count()}")

    # Step 1: flatMap with local aggregation per record
    # Step 2: reduceByKey - sum all counts per base
    base_counts = records_rdd.flatMap(process_fasta_as_hashmap).reduceByKey(lambda a, b: a + b)

    results = dict(base_counts.collect())
    print_results(results)

    spark.stop()


if __name__ == "__main__":
    main()

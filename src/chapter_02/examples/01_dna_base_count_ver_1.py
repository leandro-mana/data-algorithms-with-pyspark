"""
Chapter 2: DNA Base Count - Version 1 (Basic MapReduce)

Counts the frequency of DNA bases (A, T, C, G, N) in FASTA format files.
This is the basic version using flatMap and reduceByKey.
Note: N is included to count ambiguous bases, and 'z' is used to count total FASTA records.

Algorithm:
    1. flatMap: Each character emits (base, 1)
    2. reduceByKey: Sum all counts per base

Trade-offs:
    - Simple and easy to understand
    - Emits many intermediate key-value pairs (one per character)
    - More shuffle data compared to InMapper Combiner patterns
"""

import sys
from pathlib import Path

from src.common.spark_session import create_spark_session

# Valid DNA bases to count
VALID_BASES = frozenset("ATCGN")
DNA_BASES = ["A", "T", "C", "G", "N"]

# Default input file
DEFAULT_INPUT = Path(__file__).parent.parent / "data" / "sample.fasta"


def process_fasta_record(record: str) -> list[tuple[str, int]]:
    """
    Process a single FASTA record line.

    - Header lines (starting with '>') are counted as 'z' (record count)
    - Sequence lines: each base emits (base, 1)

    Args:
        record: A line from a FASTA file

    Returns:
        List of (base, 1) tuples for counting
    """
    if record.startswith(">"):
        return [("z", 1)]

    return [(base.upper(), 1) for base in record if base.upper() in VALID_BASES]


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
    """Main entry point for DNA base counting using basic MapReduce."""
    input_path = sys.argv[1] if len(sys.argv) >= 2 else str(DEFAULT_INPUT)

    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print("=== DNA Base Count - Version 1 (Basic MapReduce) ===\n")
    print(f"Input file: {input_path}")

    # Read FASTA file
    records_rdd = sc.textFile(input_path)
    print(f"Total lines: {records_rdd.count()}")

    # Step 1: flatMap - emit (base, 1) for each character
    # Step 2: reduceByKey - sum all counts per base
    base_counts = records_rdd.flatMap(process_fasta_record).reduceByKey(lambda a, b: a + b)

    results = dict(base_counts.collect())
    print_results(results)

    spark.stop()


if __name__ == "__main__":
    main()

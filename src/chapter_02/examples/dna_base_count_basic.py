"""
Chapter 2: DNA Base Count - Basic MapReduce

Counts the frequency of DNA bases (A, T, C, G) in FASTA format files.
This is the basic version using flatMap and reduceByKey.
"""

import sys

from src.common.spark_session import create_spark_session

# Valid DNA bases
VALID_BASES = frozenset("ATCGN")
DNA_BASES = ["A", "T", "C", "G", "N"]


def process_fasta_record(record: str) -> list[tuple[str, int]]:
    """
    Process a single FASTA record line.

    - Header lines (starting with '>') are counted as 'z' (record count)
    - Sequence lines are split into individual bases

    Args:
        record: A line from a FASTA file

    Returns:
        List of (base, 1) tuples for counting
    """
    if record.startswith(">"):
        return [("z", 1)]
    return [(base.upper(), 1) for base in record if base.upper() in VALID_BASES]


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
    """Main entry point for DNA base counting using basic MapReduce."""
    input_path = sys.argv[1] if len(sys.argv) >= 2 else None

    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print("=== DNA Base Count (Basic MapReduce) ===\n")

    if input_path:
        records_rdd = sc.textFile(input_path)
        print(f"Input file: {input_path}")
    else:
        sample_fasta = create_sample_fasta()
        records_rdd = sc.parallelize(sample_fasta)
        print("Using sample FASTA data:")
        for line in sample_fasta:
            print(f"  {line}")

    print(f"\nTotal lines: {records_rdd.count()}")

    # MapReduce: flatMap to emit (base, 1), then reduceByKey to sum
    base_counts = records_rdd.flatMap(process_fasta_record).reduceByKey(lambda a, b: a + b)

    results = dict(base_counts.collect())
    print_results(results)

    spark.stop()


if __name__ == "__main__":
    main()

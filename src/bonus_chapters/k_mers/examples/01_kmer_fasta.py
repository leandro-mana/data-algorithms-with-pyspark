"""
Bonus Chapter: K-mers - Example 1 (FASTA input)

K-mer counting on DNA sequences in FASTA format. A k-mer is every
contiguous substring of length K in a sequence. This example demonstrates
two progressively more efficient approaches, mirroring the three-version
pattern from Chapter 2 (DNA Base Count).

Algorithm:
    1. Read FASTA file, filter header lines (starting with '>')
    2. Generate (kmer, 1) pairs using a sliding window of width K
    3. Count frequencies with reduceByKey
    4. Return Top-N most frequent k-mers

Approach 1 — flatMap per sequence:
    Each sequence line emits L-K+1 pairs (one per kmer position).

Approach 2 — mapPartitions (partition-level in-mapper combiner):
    Each partition emits at most 4^K pairs regardless of partition size.
    Connects to: Chapter 2 (mapPartitions), Chapter 10 (in-mapper combining).
"""

import sys
from collections import defaultdict
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import NamedTuple

from src.common.spark_session import create_spark_session

DEFAULT_K = 4
DEFAULT_TOP_N = 10
DEFAULT_INPUT = Path(__file__).parent.parent / "data" / "sample.fasta"


class KmerCount(NamedTuple):
    kmer: str
    frequency: int


def is_dna_sequence(line: str) -> bool:
    """Return True for sequence lines; False for FASTA header lines ('>...')."""
    stripped = line.strip()
    return bool(stripped) and not stripped.startswith(">")


def generate_kmers(sequence: str, k: int) -> list[tuple[str, int]]:
    """Emit (kmer, 1) for every k-length window across the sequence."""
    seq = sequence.strip().upper()
    if len(seq) < k:
        return []
    return [(seq[i : i + k], 1) for i in range(len(seq) - k + 1)]


def kmers_per_partition(k: int) -> Callable[[Iterable[str]], Iterable[tuple[str, int]]]:
    """
    Return a mapPartitions function that aggregates k-mers locally.

    Each partition emits at most 4^K (kmer, count) pairs instead of one
    pair per kmer occurrence — minimising shuffle volume.
    """

    def _aggregate(sequences: Iterable[str]) -> Iterable[tuple[str, int]]:
        local: dict[str, int] = defaultdict(int)
        for seq in sequences:
            stripped = seq.strip().upper()
            for i in range(len(stripped) - k + 1):
                local[stripped[i : i + k]] += 1
        return local.items()

    return _aggregate


def print_top_kmers(label: str, top_kmers: list[KmerCount]) -> None:
    print(f"\n--- {label} ---")
    for entry in top_kmers:
        print(f"  {entry.kmer}: {entry.frequency}")


def main() -> None:
    input_path = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_INPUT)
    k = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_K
    top_n = int(sys.argv[3]) if len(sys.argv) > 3 else DEFAULT_TOP_N

    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print(f"=== K-mer Counting (FASTA) | K={k}, Top-N={top_n} ===\n")
    print(f"Input: {input_path}")

    lines = sc.textFile(input_path)
    sequences = lines.filter(is_dna_sequence)

    total_sequences = sequences.count()
    print(f"Sequence lines: {total_sequences}")

    # --- Approach 1: flatMap per sequence ---
    # Each sequence line emits one pair per k-mer position: O(L-K+1) pairs per line
    frequencies_v1 = sequences.flatMap(lambda seq: generate_kmers(seq, k)).reduceByKey(
        lambda a, b: a + b
    )

    top_kmers_v1 = [
        KmerCount(kmer, freq)
        for kmer, freq in frequencies_v1.takeOrdered(top_n, key=lambda x: -x[1])  # type: ignore[type-var]
    ]
    print_top_kmers(f"Top-{top_n} {k}-mers (Approach 1: flatMap)", top_kmers_v1)

    # --- Approach 2: mapPartitions (partition-level in-mapper combiner) ---
    # Each partition emits at most 4^K pairs regardless of how many sequences it holds.
    # Same result as Approach 1, far less shuffle data for large inputs.
    frequencies_v2 = sequences.mapPartitions(kmers_per_partition(k)).reduceByKey(lambda a, b: a + b)

    top_kmers_v2 = [
        KmerCount(kmer, freq)
        for kmer, freq in frequencies_v2.takeOrdered(top_n, key=lambda x: -x[1])  # type: ignore[type-var]
    ]
    print_top_kmers(f"Top-{top_n} {k}-mers (Approach 2: mapPartitions)", top_kmers_v2)

    spark.stop()


if __name__ == "__main__":
    main()

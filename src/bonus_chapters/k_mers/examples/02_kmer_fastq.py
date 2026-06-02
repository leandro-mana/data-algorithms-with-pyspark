"""
Bonus Chapter: K-mers - Example 2 (FASTQ input)

K-mer counting on DNA sequences in FASTQ format. FASTQ extends FASTA by
including a per-base quality score for each read, produced by sequencers
to indicate base call confidence.

FASTQ record structure (4 lines per read):
    Line 0: '@<name>'          — read identifier
    Line 1: '<sequence>'       — DNA bases (A, T, C, G, N)
    Line 2: '+'                — separator (sometimes repeats the name)
    Line 3: '<quality>'        — Phred+33 ASCII-encoded quality scores

Phred quality score:
    Q = -10 * log10(P)   where P = probability of wrong base call
    Q20 → 1% error rate  (commonly used minimum threshold)
    Q30 → 0.1% error rate

    ASCII encoding: score = ord(char) - 33
    'I' → ord(73) - 33 = 40  (excellent)
    'H' → ord(72) - 33 = 39  (excellent)
    '!' → ord(33) - 33 = 0   (unusable)

Algorithm:
    1. zipWithIndex to label each line by position in file
    2. Extract sequence lines (index % 4 == 1) and quality lines (index % 4 == 3)
    3. Join by record index, filter reads below minimum quality
    4. Count k-mers with flatMap → reduceByKey (same as FASTA example)
"""

import sys
from pathlib import Path
from typing import NamedTuple

from src.common.spark_session import create_spark_session

DEFAULT_K = 4
DEFAULT_TOP_N = 10
DEFAULT_MIN_QUALITY = 20
DEFAULT_INPUT = Path(__file__).parent.parent / "data" / "sample.fastq"


class KmerCount(NamedTuple):
    kmer: str
    frequency: int


def phred_score(char: str) -> int:
    """Convert a Phred+33 ASCII character to its quality score."""
    return ord(char) - 33


def meets_quality(sequence: str, quality: str, min_q: int) -> bool:
    """Return True if every base in the read meets the minimum Phred quality."""
    if len(sequence) != len(quality):
        return False
    return all(phred_score(c) >= min_q for c in quality)


def generate_kmers(sequence: str, k: int) -> list[tuple[str, int]]:
    """Emit (kmer, 1) for every k-length window across the sequence."""
    seq = sequence.strip().upper()
    if len(seq) < k:
        return []
    return [(seq[i : i + k], 1) for i in range(len(seq) - k + 1)]


def main() -> None:
    input_path = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_INPUT)
    k = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_K
    top_n = int(sys.argv[3]) if len(sys.argv) > 3 else DEFAULT_TOP_N
    min_quality = int(sys.argv[4]) if len(sys.argv) > 4 else DEFAULT_MIN_QUALITY

    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print(f"=== K-mer Counting (FASTQ) | K={k}, Top-N={top_n}, Min Q={min_quality} ===\n")
    print(f"Input: {input_path}")

    # zipWithIndex gives (line, global_index) — stable across partitions
    indexed = sc.textFile(input_path).zipWithIndex()

    # Sequence lines are at position 1 within each 4-line record
    # record_index = global_index // 4 is used as join key
    sequences = indexed.filter(lambda li: li[1] % 4 == 1).map(
        lambda li: (li[1] // 4, li[0].strip().upper())
    )

    # Quality lines are at position 3 within each 4-line record
    qualities = indexed.filter(lambda li: li[1] % 4 == 3).map(
        lambda li: (li[1] // 4, li[0].strip())
    )

    total_reads = sequences.count()
    print(f"Total reads: {total_reads}")

    # Join on record index → (record_idx, (sequence, quality))
    # Filter reads that don't meet minimum quality threshold
    high_quality_seqs = (
        sequences.join(qualities)
        .values()
        .filter(lambda sq: meets_quality(sq[0], sq[1], min_quality))
        .map(lambda sq: sq[0])
    )

    passing_reads = high_quality_seqs.count()
    print(f"Reads passing Q{min_quality} filter: {passing_reads}/{total_reads}")

    # Count k-mers across quality-filtered sequences
    frequencies = high_quality_seqs.flatMap(lambda seq: generate_kmers(seq, k)).reduceByKey(
        lambda a, b: a + b
    )

    top_kmers = [
        KmerCount(kmer, freq)
        for kmer, freq in frequencies.takeOrdered(top_n, key=lambda x: -x[1])  # type: ignore[type-var]
    ]

    print(f"\n--- Top-{top_n} {k}-mers (Q{min_quality}+ reads only) ---")
    for entry in top_kmers:
        print(f"  {entry.kmer}: {entry.frequency}")

    spark.stop()


if __name__ == "__main__":
    main()

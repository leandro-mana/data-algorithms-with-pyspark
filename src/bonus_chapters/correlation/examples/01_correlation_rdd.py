"""
Bonus Chapter: Correlation - Example 1 (RDD)

All-versus-all Pearson correlation between genes across patients.
Given measurements of N genes across M patients, compute the correlation
between every pair of genes (g_i, g_j) where i < j to avoid duplicates.

Input format: CSV with columns gene_id, patient_id, value

Algorithm:
    1. Parse records into (gene_id, (patient_id, value)) pairs
    2. Group by gene_id → (gene_id, [(patient_id, value), ...])
    3. Cartesian product of all gene pairs
    4. For each pair, align values by patient and compute Pearson r
    5. Filter to i < j (avoid duplicates), sort by |r| descending

Pearson r formula (computed without external libraries):
    r = Σ((x - x̄)(y - ȳ)) / (σx * σy)
"""

import sys
from pathlib import Path
from typing import NamedTuple

from src.common.spark_session import create_spark_session

DEFAULT_INPUT = Path(__file__).parent.parent / "data" / "sample_genes.csv"


class GeneRecord(NamedTuple):
    gene_id: str
    patient_id: str
    value: float


class Correlation(NamedTuple):
    gene_a: str
    gene_b: str
    pearson_r: float


def parse_record(line: str) -> GeneRecord | None:
    """Parse a CSV record, return None for the header or malformed lines."""
    parts = line.strip().split(",")
    if len(parts) != 3 or parts[0] == "gene_id":
        return None
    try:
        return GeneRecord(parts[0], parts[1], float(parts[2]))
    except ValueError:
        return None


def pearson_r(xs: list[float], ys: list[float]) -> float:
    """Compute Pearson correlation coefficient between two value lists."""
    n = len(xs)
    if n < 2:
        return 0.0
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    numerator: float = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    denom_x: float = sum((x - mean_x) ** 2 for x in xs) ** 0.5
    denom_y: float = sum((y - mean_y) ** 2 for y in ys) ** 0.5
    if denom_x == 0 or denom_y == 0:
        return 0.0
    return numerator / (denom_x * denom_y)


def compute_correlation(
    pair: tuple[
        tuple[str, dict[str, float]],
        tuple[str, dict[str, float]],
    ],
) -> Correlation | None:
    """
    Compute Pearson r for a gene pair.

    Aligns measurements by patient_id, skipping patients with missing data
    in either gene. Returns None if fewer than 2 common patients exist.
    """
    (gene_a, values_a), (gene_b, values_b) = pair

    # Only use patients present in both genes
    common_patients = sorted(set(values_a) & set(values_b))
    if len(common_patients) < 2:
        return None

    xs = [values_a[p] for p in common_patients]
    ys = [values_b[p] for p in common_patients]

    return Correlation(gene_a, gene_b, round(pearson_r(xs, ys), 4))


def main() -> None:
    input_path = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_INPUT)

    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print("=== All-vs-All Correlation (RDD) ===\n")
    print(f"Input: {input_path}")

    # Parse records, drop header and malformed lines
    records = sc.textFile(input_path).map(parse_record).filter(lambda r: r is not None)

    # Group measurements by gene: (gene_id, {patient_id: value})
    gene_vectors = (
        records.map(lambda r: (r.gene_id, (r.patient_id, r.value)))  # type: ignore[union-attr]
        .groupByKey()
        .mapValues(lambda pairs: dict(pairs))
    )

    gene_count = gene_vectors.count()
    print(f"Genes: {gene_count}")
    print(f"Gene pairs to correlate: {gene_count * (gene_count - 1) // 2}")

    # Cartesian product — all (gene_a, gene_b) pairs
    # Filter gene_a < gene_b to avoid duplicate pairs and self-correlation
    all_pairs = gene_vectors.cartesian(gene_vectors).filter(lambda pair: pair[0][0] < pair[1][0])

    # Compute Pearson r for each pair
    correlations = (
        all_pairs.map(compute_correlation)
        .filter(lambda c: c is not None)
        .sortBy(lambda c: -abs(c.pearson_r))  # type: ignore[union-attr]
    )

    results = correlations.collect()

    print("\n--- Gene pair correlations (sorted by |r|) ---")
    print(f"  {'Gene A':8s}  {'Gene B':8s}  {'Pearson r':>10s}  Interpretation")
    print(f"  {'-'*8}  {'-'*8}  {'-'*10}  {'-'*20}")
    for corr in results:
        r = corr.pearson_r  # type: ignore[union-attr]
        interpretation = (
            "strong positive"
            if r > 0.8
            else "strong negative"
            if r < -0.8
            else "moderate positive"
            if r > 0.5
            else "moderate negative"
            if r < -0.5
            else "weak"
        )
        print(f"  {corr.gene_a:8s}  {corr.gene_b:8s}  {r:>10.4f}  {interpretation}")  # type: ignore[union-attr]

    spark.stop()


if __name__ == "__main__":
    main()

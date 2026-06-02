"""
Bonus Chapter: Correlation - Example 2 (DataFrame)

Same all-versus-all Pearson correlation using the DataFrame API.
The key step is pivoting the data so each gene becomes a column,
then using Spark's built-in df.stat.corr() for pairwise correlation.

Algorithm:
    1. Read CSV into a DataFrame with schema (gene_id, patient_id, value)
    2. Pivot: rows = patients, columns = genes, values = measurements
    3. For every pair of gene columns, call df.stat.corr(col_a, col_b)
    4. Build a readable correlation matrix

This approach is concise for a moderate number of genes. For hundreds
of genes, the RDD cartesian approach in Example 1 scales better.
"""

import sys
from itertools import combinations
from pathlib import Path
from typing import NamedTuple

import pyspark.sql.functions as F

from src.common.spark_session import create_spark_session

DEFAULT_INPUT = Path(__file__).parent.parent / "data" / "sample_genes.csv"


class Correlation(NamedTuple):
    gene_a: str
    gene_b: str
    pearson_r: float


def main() -> None:
    input_path = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_INPUT)

    spark = create_spark_session(__file__)

    print("=== All-vs-All Correlation (DataFrame) ===\n")
    print(f"Input: {input_path}")

    # Read with explicit schema
    raw_df = spark.read.csv(input_path, header=True, inferSchema=True)
    raw_df.printSchema()

    # Pivot: one row per patient, one column per gene
    # Values are average measurements per (gene, patient) combination
    pivoted_df = (
        raw_df.groupBy("patient_id").pivot("gene_id").agg(F.avg("value")).orderBy("patient_id")
    )

    print("\n--- Pivoted gene expression matrix ---")
    pivoted_df.show(truncate=False)

    # Identify gene columns (everything except patient_id)
    gene_cols = [c for c in pivoted_df.columns if c != "patient_id"]
    print(f"Genes: {gene_cols}")
    print(f"Gene pairs to correlate: {len(gene_cols) * (len(gene_cols) - 1) // 2}\n")

    # Compute pairwise correlations using df.stat.corr()
    correlations: list[Correlation] = []
    for gene_a, gene_b in combinations(gene_cols, 2):
        r = pivoted_df.stat.corr(gene_a, gene_b)
        correlations.append(Correlation(gene_a, gene_b, round(r, 4)))

    # Sort by absolute correlation descending
    correlations.sort(key=lambda c: -abs(c.pearson_r))

    print("--- Gene pair correlations (sorted by |r|) ---")
    print(f"  {'Gene A':8s}  {'Gene B':8s}  {'Pearson r':>10s}  Interpretation")
    print(f"  {'-'*8}  {'-'*8}  {'-'*10}  {'-'*20}")
    for corr in correlations:
        r = corr.pearson_r
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
        print(f"  {corr.gene_a:8s}  {corr.gene_b:8s}  {r:>10.4f}  {interpretation}")

    spark.stop()


if __name__ == "__main__":
    main()

"""
Chapter 8: Rank Product — Ranking Items Across Multiple Studies

The rank product algorithm ranks items (e.g., genes) across multiple
ranked lists by computing the geometric mean of their ranks. Originally
developed for detecting differentially expressed genes in replicated
microarray experiments, it's now used broadly in bioinformatics and ML.

Algorithm steps:
1. For each study, compute the mean value per item (gene)
2. Sort items by value (descending) and assign ranks (1 = highest)
3. Compute the rank product: RP(g) = (r1 * r2 * ... * rk)^(1/k)

A smaller rank product means the item consistently ranks near the top
across all studies.

Key PySpark patterns:
- combineByKey() for mean computation (sum, count) per partition
- zipWithIndex() for rank assignment after sorting
- union() + combineByKey() for cross-study aggregation
"""

from typing import NamedTuple

from pyspark import RDD
from pyspark.sql import SparkSession

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session


class RankProductResult(NamedTuple):
    """Result for a single gene's rank product computation."""

    gene_id: str
    rank_product: float
    num_studies: int


# ---------------------------------------------------------------------------
# Step 1: Compute mean value per gene for a single study
# ---------------------------------------------------------------------------


def parse_gene_record(line: str) -> tuple[str, float]:
    """Parse a CSV line into (gene_id, value)."""
    parts = line.split(",")
    return (parts[0], float(parts[1]))


def compute_mean_per_gene(rdd: RDD) -> RDD:
    """Compute the mean value per gene using combineByKey.

    combineByKey uses (sum, count) accumulators that merge across
    partitions — more efficient than groupByKey for large datasets.

    Returns: RDD[(gene_id, mean_value)]
    """
    combined = rdd.combineByKey(
        lambda v: (v, 1),  # createCombiner: first value → (sum, count)
        lambda acc, v: (acc[0] + v, acc[1] + 1),  # mergeValue: add to accumulator
        lambda a, b: (a[0] + b[0], a[1] + b[1]),  # mergeCombiners: combine partitions
    )
    return combined.mapValues(lambda sc: sc[0] / sc[1])


# ---------------------------------------------------------------------------
# Step 2: Sort by value and assign ranks
# ---------------------------------------------------------------------------


def assign_ranks(means_rdd: RDD) -> RDD:
    """Assign ranks based on descending value (1 = highest value).

    Uses zipWithIndex() which assigns sequential indices starting from 0.
    We add 1 to convert to 1-based ranking.

    Requires coalescing to 1 partition so zipWithIndex produces a
    globally meaningful ordering (not per-partition).

    Returns: RDD[(gene_id, rank)]
    """
    # Swap to (abs_value, gene_id) for sorting by value
    swapped = means_rdd.map(lambda kv: (abs(kv[1]), kv[0]))
    # Sort descending, single partition for global ordering
    sorted_rdd: RDD = swapped.sortByKey(ascending=False, numPartitions=1)
    # Zip with index and convert to (gene_id, rank)
    indexed = sorted_rdd.zipWithIndex()
    return indexed.map(lambda vi: (vi[0][1], vi[1] + 1))


# ---------------------------------------------------------------------------
# Step 3: Compute rank product across all studies
# ---------------------------------------------------------------------------


def compute_rank_product(ranked_rdds: list[RDD], spark: SparkSession) -> RDD:
    """Compute the rank product for each gene across all studies.

    Unions all per-study rank RDDs, then uses combineByKey to
    multiply ranks and count studies per gene.

    RP(gene) = (r1 * r2 * ... * rk) ^ (1/k)

    Returns: RDD[(gene_id, RankProductResult)]
    """
    # Union all study ranks into a single RDD
    union_rdd = spark.sparkContext.union(ranked_rdds)

    # combineByKey: accumulate (product_of_ranks, count_of_studies)
    combined = union_rdd.combineByKey(
        lambda rank: (rank, 1),  # createCombiner
        lambda acc, rank: (acc[0] * rank, acc[1] + 1),  # mergeValue: multiply ranks
        lambda a, b: (a[0] * b[0], a[1] + b[1]),  # mergeCombiners
    )

    # Compute geometric mean: (product)^(1/k)
    return combined.mapValues(lambda pc: RankProductResult("", pow(pc[0], 1.0 / pc[1]), pc[1]))


def format_results(rdd: RDD) -> list[RankProductResult]:
    """Collect and format rank product results."""
    return [
        RankProductResult(
            gene_id=gene_id, rank_product=result.rank_product, num_studies=result.num_studies
        )
        for gene_id, result in sorted(rdd.collect(), key=lambda x: x[1].rank_product)
    ]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Compute rank product across three gene studies."""
    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    # Load study data (skip CSV headers)
    study_paths = [str(get_chapter_data_path("chapter_08", f"study_{i}.csv")) for i in range(1, 4)]

    print("=== Rank Product: Ranking Genes Across Multiple Studies ===\n")

    # Process each study: parse → compute means → assign ranks
    all_ranks: list[RDD] = []
    for i, path in enumerate(study_paths, 1):
        raw = sc.textFile(path).filter(lambda line: not line.startswith("gene_id"))
        genes = raw.map(parse_gene_record)
        means = compute_mean_per_gene(genes)
        ranks = assign_ranks(means)

        print(f"--- Study {i}: {path.split('/')[-1]} ---")
        print(f"  Means: {sorted(means.collect())}")
        print(f"  Ranks: {sorted(ranks.collect())}")
        print()

        all_ranks.append(ranks)

    # Compute rank product across all studies
    rp_rdd = compute_rank_product(all_ranks, spark)
    results = format_results(rp_rdd)

    print("--- Rank Product Results (lower = more consistently top-ranked) ---")
    print(f"{'Gene':<8} {'Rank Product':<16} {'Studies'}")
    print("-" * 40)
    for r in results:
        print(f"{r.gene_id:<8} {r.rank_product:<16.4f} {r.num_studies}")

    print(f"\nTotal genes ranked: {len(results)}")

    spark.stop()


if __name__ == "__main__":
    main()

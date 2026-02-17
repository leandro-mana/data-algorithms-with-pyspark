"""
Chapter 10: Binning and Sorting Design Patterns

Binning:
  Organize data into categories (bins) to enable faster queries.
  Instead of searching the entire dataset, search only the relevant bin.

  This example uses genomics variant data with two-level binning:
    Level 1: bin by chromosome (chr3, chr5, chr10, chr22, ...)
    Level 2: bin by modulo of start position (0-100)

  Physical binning via partitionBy() creates directory structures
  that Spark can prune at read time — only relevant partitions are loaded.

Sorting:
  Demonstrates Spark's sorting APIs for RDDs and DataFrames.
"""

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

from src.common.data_loader import PROJECT_ROOT, get_chapter_data_path
from src.common.spark_session import create_spark_session

NUM_BINS = 101  # prime number for modulo binning


# ---------------------------------------------------------------------------
# Binning functions (used as UDFs)
# ---------------------------------------------------------------------------


def extract_chromosome(variant_key: str) -> str:
    """Extract chromosome ID from a variant key.

    Input:  '5:163697197:163697197:T'
    Output: 'chr5'
    """
    return "chr" + variant_key.split(":")[0]


def compute_modulo(variant_key: str) -> int:
    """Compute start_position % NUM_BINS for second-level binning.

    Input:  '5:163697197:163697197:T'
    Output: 163697197 % 101 = 33
    """
    start_position = int(variant_key.split(":")[1])
    return start_position % NUM_BINS


# ---------------------------------------------------------------------------
# Binning demonstration
# ---------------------------------------------------------------------------


def apply_binning(df: DataFrame, spark: SparkSession) -> DataFrame:
    """Apply two-level binning to a variant DataFrame.

    Level 1: CHR_ID from variant_key (e.g., 'chr5')
    Level 2: MODULO from start_position % 101
    """
    extract_chr_udf = F.udf(extract_chromosome, StringType())
    compute_mod_udf = F.udf(compute_modulo, IntegerType())

    return df.withColumn("CHR_ID", extract_chr_udf(F.col("variant_key"))).withColumn(
        "MODULO", compute_mod_udf(F.col("variant_key"))
    )


# ---------------------------------------------------------------------------
# Sorting demonstration
# ---------------------------------------------------------------------------


def demonstrate_sorting(spark: SparkSession) -> None:
    """Show Spark's sorting APIs on RDDs and DataFrames."""
    sc = spark.sparkContext

    print("--- Sorting APIs ---\n")

    # RDD sorting
    pairs = [("spark", 9500), ("python", 8200), ("java", 12000), ("scala", 6800), ("sql", 11500)]
    rdd = sc.parallelize(pairs)

    print("  RDD.sortByKey (ascending):")
    for k, v in rdd.sortByKey(ascending=True).collect():  # type: ignore[call-overload]
        print(f"    {k:10s} {v}")

    print("\n  RDD.sortBy value (descending):")
    for k, v in rdd.sortBy(lambda x: -x[1]).collect():  # type: ignore[type-var]
        print(f"    {k:10s} {v}")

    # DataFrame sorting
    df = spark.createDataFrame(pairs, ["language", "popularity"])

    print("\n  DataFrame.sort (ascending by language):")
    df.sort("language").show(truncate=False)

    print("  DataFrame.sort (descending by popularity):")
    df.sort(F.col("popularity").desc()).show(truncate=False)

    print("  DataFrame.orderBy (same as sort, SQL-friendly alias):")
    df.orderBy(F.col("popularity").desc()).show(truncate=False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Demonstrate binning and sorting design patterns."""
    spark: SparkSession = create_spark_session(__file__)

    variants_path = str(get_chapter_data_path("chapter_10", "variants.csv"))
    if len(sys.argv) > 1:
        variants_path = sys.argv[1]

    print("=" * 60)
    print("Chapter 10: Binning and Sorting Design Patterns")
    print("=" * 60)

    # --- Binning ---
    print("\n--- Binning Pattern (genomics variant data) ---\n")

    df = spark.read.csv(variants_path, header=True, inferSchema=True)
    print("Original data:")
    df.show(truncate=False)

    binned = apply_binning(df, spark)
    print("After two-level binning (CHR_ID + MODULO):")
    binned.show(truncate=False)

    # Show bin distribution
    print("Bin distribution by chromosome:")
    binned.groupBy("CHR_ID").count().orderBy("CHR_ID").show(truncate=False)

    print("Bin distribution by chromosome + modulo:")
    binned.groupBy("CHR_ID", "MODULO").count().orderBy("CHR_ID", "MODULO").show(truncate=False)

    # Physical binning with partitionBy
    output_path = str(PROJECT_ROOT / ".output" / "binned_variants")
    binned.write.mode("overwrite").partitionBy("CHR_ID", "MODULO").parquet(output_path)
    print(f"Partitioned Parquet written to: {output_path}")
    print("  Directory structure enables partition pruning at read time.")
    print("  e.g., reading CHR_ID=chr5 only loads chr5 partitions.\n")

    # --- Sorting ---
    demonstrate_sorting(spark)

    spark.stop()


if __name__ == "__main__":
    main()

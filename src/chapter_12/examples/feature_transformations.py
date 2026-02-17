"""
Chapter 12: Numeric Feature Transformations

Demonstrates common feature engineering techniques for numeric data:

  1. MinMaxScaler    — scale features to [0, 1] range
  2. Normalizer      — scale each row to unit norm (L1 or L2)
  3. Bucketizer      — bin continuous values into user-defined buckets
  4. QuantileDiscretizer — auto-bin by quantiles
  5. Log transform   — compress skewed distributions

All transformations use Spark ML's Pipeline API with VectorAssembler.
"""

import sys

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Bucketizer,
    MinMaxScaler,
    Normalizer,
    QuantileDiscretizer,
    VectorAssembler,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session

# ---------------------------------------------------------------------------
# MinMax scaling
# ---------------------------------------------------------------------------


def demonstrate_minmax(spark: SparkSession, df) -> None:
    """Scale numeric features to [0, 1] using MinMaxScaler.

    Formula: (x - x_min) / (x_max - x_min)
    """
    print("--- MinMaxScaler (scale to [0, 1]) ---\n")

    print("Before scaling:")
    df.show(truncate=False)

    # VectorAssembler + MinMaxScaler per feature column
    unlist = F.udf(lambda x: round(float(list(x)[0]), 4), DoubleType())

    scaled_df = df
    for col_name in ["revenue", "num_of_days"]:
        assembler = VectorAssembler(inputCols=[col_name], outputCol=f"{col_name}_vec")
        scaler = MinMaxScaler(inputCol=f"{col_name}_vec", outputCol=f"{col_name}_scaled_vec")
        pipeline = Pipeline(stages=[assembler, scaler])
        scaled_df = (
            pipeline.fit(scaled_df)
            .transform(scaled_df)
            .withColumn(f"{col_name}_scaled", unlist(F.col(f"{col_name}_scaled_vec")))
            .drop(f"{col_name}_vec", f"{col_name}_scaled_vec")
        )

    print("After MinMax scaling:")
    scaled_df.show(truncate=False)


# ---------------------------------------------------------------------------
# Normalizer (L1 and L2 norms)
# ---------------------------------------------------------------------------


def demonstrate_normalizer(spark: SparkSession, df) -> None:
    """Normalize feature vectors to unit norm.

    L1 norm: sum of absolute values = 1
    L2 norm: sum of squares = 1 (Euclidean)
    """
    print("--- Normalizer (unit norm) ---\n")

    assembler = VectorAssembler(
        inputCols=["revenue", "num_of_days", "age"],
        outputCol="features",
    )
    assembled = assembler.transform(df)

    l1_normalizer = Normalizer(inputCol="features", outputCol="features_L1", p=1.0)
    l2_normalizer = Normalizer(inputCol="features", outputCol="features_L2", p=2.0)

    result = l2_normalizer.transform(l1_normalizer.transform(assembled))

    print("Features with L1 and L2 normalized vectors:")
    result.select("user_id", "features", "features_L1", "features_L2").show(truncate=False)


# ---------------------------------------------------------------------------
# Bucketizer and QuantileDiscretizer
# ---------------------------------------------------------------------------


def demonstrate_bucketing(spark: SparkSession, df) -> None:
    """Bin continuous features into discrete buckets.

    Bucketizer: user-defined boundaries
    QuantileDiscretizer: auto-determined by data quantiles
    """
    print("--- Bucketizer (user-defined bins) ---\n")

    # Bucketize age into groups
    age_buckets = Bucketizer(
        splits=[0, 30, 40, 50, float("inf")],
        inputCol="age",
        outputCol="age_bucket",
    )
    bucketed = age_buckets.transform(df)
    print("Age buckets (0-30, 30-40, 40-50, 50+):")
    bucketed.select("user_id", "age", "age_bucket").show(truncate=False)

    print("--- QuantileDiscretizer (auto-binning by quantiles) ---\n")

    quantile = QuantileDiscretizer(
        numBuckets=3,
        inputCol="revenue",
        outputCol="revenue_bucket",
        handleInvalid="skip",
    )
    quantiled = quantile.fit(df).transform(df)
    print("Revenue quantile bins (3 buckets):")
    quantiled.select("user_id", "revenue", "revenue_bucket").show(truncate=False)


# ---------------------------------------------------------------------------
# Log transformation
# ---------------------------------------------------------------------------


def demonstrate_log_transform(spark: SparkSession, df) -> None:
    """Apply logarithmic transformation to compress skewed distributions.

    log(x) compresses large values and expands small ones,
    making the distribution more normal-like.
    """
    print("--- Log Transformation ---\n")

    log_df = df.withColumn("log_revenue", F.log("revenue")).withColumn(
        "log10_revenue", F.log10("revenue")
    )

    print("Revenue with log transformations:")
    log_df.select("user_id", "revenue", "log_revenue", "log10_revenue").show(truncate=False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Demonstrate numeric feature transformations."""
    spark: SparkSession = create_spark_session(__file__)

    input_path = str(get_chapter_data_path("chapter_12", "customers.csv"))
    if len(sys.argv) > 1:
        input_path = sys.argv[1]

    print("=" * 60)
    print("Chapter 12: Numeric Feature Transformations")
    print("=" * 60)

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    demonstrate_minmax(spark, df)
    demonstrate_normalizer(spark, df)
    demonstrate_bucketing(spark, df)
    demonstrate_log_transform(spark, df)

    spark.stop()


if __name__ == "__main__":
    main()

"""
Chapter 12: Categorical Feature Encoding

Demonstrates techniques for converting categorical features into
numeric representations that ML algorithms can consume:

  1. StringIndexer   — map string labels to numeric indices
  2. OneHotEncoder   — convert indices to sparse binary vectors
  3. VectorAssembler — combine features into a single vector
  4. Full pipeline   — StringIndexer → OneHotEncoder → VectorAssembler
"""

import sys

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import SparkSession

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session

# ---------------------------------------------------------------------------
# StringIndexer — string labels to numeric indices
# ---------------------------------------------------------------------------


def demonstrate_string_indexer(spark: SparkSession, df) -> None:
    """Convert categorical string columns to numeric indices.

    Default ordering: by label frequency (most frequent = 0).
    """
    print("--- StringIndexer ---\n")

    print("Original data:")
    df.show(truncate=False)

    indexer = StringIndexer(inputCol="safety_level", outputCol="safety_index")
    indexed = indexer.fit(df).transform(df)

    print("After StringIndexer (safety_level → safety_index):")
    indexed.select("id", "safety_level", "safety_index").show(truncate=False)

    # Multi-column indexing via Pipeline
    indexers = [
        StringIndexer(inputCol=col, outputCol=f"{col}_index")
        for col in ["safety_level", "engine_type", "brand"]
    ]
    pipeline = Pipeline(stages=indexers)  # type: ignore[arg-type]
    multi_indexed = pipeline.fit(df).transform(df)

    print("Multi-column StringIndexer:")
    multi_indexed.select(
        "id",
        "brand",
        "brand_index",
        "safety_level",
        "safety_level_index",
        "engine_type",
        "engine_type_index",
    ).show(truncate=False)


# ---------------------------------------------------------------------------
# OneHotEncoder — sparse binary vectors
# ---------------------------------------------------------------------------


def demonstrate_one_hot_encoding(spark: SparkSession, df) -> None:
    """Convert indexed categories to sparse binary vectors.

    Pipeline: StringIndexer → OneHotEncoder
    Each category gets a vector with a single 1 in its position.
    """
    print("--- OneHotEncoder ---\n")

    # Full pipeline: index then one-hot encode
    safety_indexer = StringIndexer(inputCol="safety_level", outputCol="safety_index")
    engine_indexer = StringIndexer(inputCol="engine_type", outputCol="engine_index")

    safety_encoder = OneHotEncoder(inputCol="safety_index", outputCol="safety_vec")
    engine_encoder = OneHotEncoder(inputCol="engine_index", outputCol="engine_vec")

    pipeline = Pipeline(stages=[safety_indexer, engine_indexer, safety_encoder, engine_encoder])
    encoded = pipeline.fit(df).transform(df)

    print("OneHotEncoded features:")
    encoded.select(
        "id",
        "safety_level",
        "safety_index",
        "safety_vec",
        "engine_type",
        "engine_index",
        "engine_vec",
    ).show(truncate=False)


# ---------------------------------------------------------------------------
# Full pipeline: categorical + numeric → feature vector
# ---------------------------------------------------------------------------


def demonstrate_full_pipeline(spark: SparkSession, df) -> None:
    """Complete feature engineering pipeline:
    1. StringIndexer for categorical columns
    2. OneHotEncoder for indexed columns
    3. VectorAssembler to combine all features into one vector
    """
    print("--- Full Pipeline: StringIndexer → OneHotEncoder → VectorAssembler ---\n")

    # Stage 1: Index categorical columns
    safety_indexer = StringIndexer(inputCol="safety_level", outputCol="safety_index")
    engine_indexer = StringIndexer(inputCol="engine_type", outputCol="engine_index")

    # Stage 2: One-hot encode indexed columns
    safety_encoder = OneHotEncoder(inputCol="safety_index", outputCol="safety_vec")
    engine_encoder = OneHotEncoder(inputCol="engine_index", outputCol="engine_vec")

    # Stage 3: Assemble all features into a single vector
    assembler = VectorAssembler(
        inputCols=["price", "safety_vec", "engine_vec"],
        outputCol="features",
    )

    pipeline = Pipeline(
        stages=[safety_indexer, engine_indexer, safety_encoder, engine_encoder, assembler]
    )

    result = pipeline.fit(df).transform(df)

    print("Feature vectors (price + safety one-hot + engine one-hot):")
    result.select("id", "brand", "price", "safety_level", "engine_type", "features").show(
        truncate=False
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Demonstrate categorical feature encoding techniques."""
    spark: SparkSession = create_spark_session(__file__)

    input_path = str(get_chapter_data_path("chapter_12", "cars.csv"))
    if len(sys.argv) > 1:
        input_path = sys.argv[1]

    print("=" * 60)
    print("Chapter 12: Categorical Feature Encoding")
    print("=" * 60)

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    demonstrate_string_indexer(spark, df)
    demonstrate_one_hot_encoding(spark, df)
    demonstrate_full_pipeline(spark, df)

    spark.stop()


if __name__ == "__main__":
    main()

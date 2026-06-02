"""
Chapter 12: Text Feature Engineering — TF-IDF

Demonstrates text feature extraction using TF-IDF (Term Frequency —
Inverse Document Frequency), the standard technique for converting
text documents into numeric feature vectors.

Two approaches:
  1. HashingTF + IDF    — fixed-size vectors via feature hashing
  2. CountVectorizer    — vocabulary-based vectors from training data

TF-IDF measures word importance: words frequent in one document but
rare across the corpus get high weights. Common words (stopwords)
get low weights.
"""

import sys

from pyspark.ml import Pipeline
from pyspark.ml.feature import IDF, CountVectorizer, HashingTF, Tokenizer
from pyspark.sql import SparkSession

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session

# ---------------------------------------------------------------------------
# TF-IDF with HashingTF
# ---------------------------------------------------------------------------


def demonstrate_hashing_tfidf(spark: SparkSession, df) -> None:
    """TF-IDF using HashingTF for fixed-size term frequency vectors.

    Pipeline: Tokenizer → HashingTF → IDF
    HashingTF uses the hashing trick to map terms to indices.
    """
    print("--- TF-IDF with HashingTF ---\n")

    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashing_tf = HashingTF(inputCol="words", outputCol="raw_features", numFeatures=128)
    idf = IDF(inputCol="raw_features", outputCol="tfidf_features")

    pipeline = Pipeline(stages=[tokenizer, hashing_tf, idf])
    model = pipeline.fit(df)
    result = model.transform(df)

    print("Tokenized words:")
    result.select("label", "words").show(truncate=False)

    print("TF-IDF feature vectors (HashingTF, 128 dimensions):")
    result.select("label", "tfidf_features").show(truncate=False)


# ---------------------------------------------------------------------------
# TF-IDF with CountVectorizer
# ---------------------------------------------------------------------------


def demonstrate_count_tfidf(spark: SparkSession, df) -> None:
    """TF-IDF using CountVectorizer for vocabulary-based vectors.

    Pipeline: Tokenizer → CountVectorizer → IDF
    CountVectorizer builds a vocabulary from the training corpus.
    """
    print("--- TF-IDF with CountVectorizer ---\n")

    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    count_vec = CountVectorizer(inputCol="words", outputCol="raw_features")
    idf = IDF(inputCol="raw_features", outputCol="tfidf_features")

    pipeline = Pipeline(stages=[tokenizer, count_vec, idf])
    model = pipeline.fit(df)
    result = model.transform(df)

    # Show the vocabulary
    cv_model = model.stages[1]
    print(f"Vocabulary ({len(cv_model.vocabulary)} terms):")  # type: ignore[attr-defined]
    vocab = cv_model.vocabulary  # type: ignore[attr-defined]
    for i, term in enumerate(vocab[:20]):  # show first 20
        print(f"  [{i}] {term}")

    print(f"\nTF-IDF feature vectors (CountVectorizer, {len(vocab)} dimensions):")
    result.select("label", "tfidf_features").show(truncate=False)


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def compare_approaches(spark: SparkSession, df) -> None:
    """Compare HashingTF vs CountVectorizer approaches."""
    print("--- Comparison: HashingTF vs CountVectorizer ---\n")

    print("| Property              | HashingTF           | CountVectorizer     |")
    print("| --------------------- | ------------------- | ------------------- |")
    print("| Vector size           | Fixed (numFeatures) | Vocabulary size     |")
    print("| Vocabulary needed     | No                  | Yes (built at fit)  |")
    print("| Hash collisions       | Possible            | None                |")
    print("| Inverse mapping       | Not possible        | Term ↔ index        |")
    print("| Best for              | Large, streaming    | Moderate, offline   |")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Demonstrate text feature engineering with TF-IDF."""
    spark: SparkSession = create_spark_session(__file__)

    input_path = str(get_chapter_data_path("chapter_12", "documents.csv"))
    if len(sys.argv) > 1:
        input_path = sys.argv[1]

    print("=" * 60)
    print("Chapter 12: Text Feature Engineering — TF-IDF")
    print("=" * 60)

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    print("\nInput documents:")
    df.show(truncate=False)

    demonstrate_hashing_tfidf(spark, df)
    demonstrate_count_tfidf(spark, df)
    compare_approaches(spark, df)

    spark.stop()


if __name__ == "__main__":
    main()

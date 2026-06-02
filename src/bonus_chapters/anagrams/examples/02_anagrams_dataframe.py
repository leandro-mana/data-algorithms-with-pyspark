"""
Bonus Chapter: Anagrams - Example 2 (DataFrame)

Same anagram grouping problem solved with the DataFrame API. The key
insight is the same — sort the letters of each word to get a canonical
key — but expressed as a UDF and SQL-style aggregation.

The DataFrame approach adds a word frequency column, showing how many
times each word appeared in the input (not just whether it exists).
"""

import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

from src.common.spark_session import create_spark_session

DEFAULT_INPUT = Path(__file__).parent.parent / "data" / "sample_text.txt"


def anagram_key(word: str) -> str:
    """Canonical key: letters sorted alphabetically."""
    return "".join(sorted(word)) if word else ""


def main() -> None:
    input_path = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_INPUT)

    spark = create_spark_session(__file__)

    print("=== Anagram Grouping (DataFrame) ===\n")
    print(f"Input: {input_path}")

    # Register UDF for anagram key computation
    anagram_key_udf = F.udf(anagram_key, StringType())

    # Tokenise: read lines → explode words → lowercase → alpha-only
    lines_df = spark.read.text(input_path)
    words_df = (
        lines_df.select(F.explode(F.split(F.col("value"), r"\s+")).alias("raw_word"))
        .withColumn("word", F.lower(F.col("raw_word")))
        .filter(F.col("word").rlike(r"^[a-z]+$"))
        .drop("raw_word")
    )

    # Compute anagram key and word frequency
    keyed_df = (
        words_df.withColumn("anagram_key", anagram_key_udf(F.col("word")))
        .groupBy("anagram_key", "word")
        .agg(F.count("*").alias("frequency"))
    )

    # Group by anagram key → collect distinct words into a list
    # Filter to groups with more than one distinct word
    groups_df = (
        keyed_df.groupBy("anagram_key")
        .agg(
            F.collect_set("word").alias("words"),
            F.sum("frequency").alias("total_occurrences"),
        )
        .filter(F.size(F.col("words")) > 1)
        .orderBy(F.col("total_occurrences").desc())
    )

    print("\n--- Anagram groups (DataFrame) ---")
    groups_df.show(truncate=False)

    # SQL variant
    keyed_df.createOrReplaceTempView("word_keys")
    spark.sql("""
        SELECT anagram_key,
               collect_set(word) AS words,
               sum(frequency)    AS total_occurrences
        FROM word_keys
        GROUP BY anagram_key
        HAVING size(collect_set(word)) > 1
        ORDER BY total_occurrences DESC
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

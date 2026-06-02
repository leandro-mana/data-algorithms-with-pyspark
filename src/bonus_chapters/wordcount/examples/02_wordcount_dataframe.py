"""
Bonus Chapter: Word Count - Example 2 (DataFrame)

The same word count problem solved with the DataFrame API. The Catalyst
optimizer rewrites the plan — no manual partition tuning required. Compared
to the RDD approach, the code is more concise and benefits from automatic
predicate pushdown and code generation.

Algorithm:
    1. Read text file into a single-column DataFrame
    2. split() the line column into an array of words
    3. explode() the array so each word becomes its own row
    4. groupBy + count to aggregate frequencies
    5. orderBy to rank by frequency descending
"""

import sys
from pathlib import Path

import pyspark.sql.functions as F

from src.common.spark_session import create_spark_session

DEFAULT_INPUT = Path(__file__).parent.parent / "data" / "sample_text.txt"
DEFAULT_TOP_N = 10
MIN_WORD_LENGTH = 4


def main() -> None:
    input_path = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_INPUT)
    top_n = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_TOP_N

    spark = create_spark_session(__file__)

    print(f"=== Word Count (DataFrame) | Top-{top_n} ===\n")
    print(f"Input: {input_path}")

    # Read: one row per line
    lines_df = spark.read.text(input_path)

    # Tokenise: split on whitespace → array, then explode into one word per row
    # regexp_replace removes punctuation before splitting
    words_df = lines_df.select(
        F.explode(F.split(F.regexp_replace(F.col("value"), r"[.,!?;:\"'()]", ""), r"\s+")).alias(
            "word"
        )
    ).filter(F.col("word") != "")

    # Normalise to lowercase
    words_df = words_df.withColumn("word", F.lower(F.col("word")))

    print(f"Total word tokens: {words_df.count()}")

    # --- Standard word count ---
    freq_df = words_df.groupBy("word").count().orderBy(F.col("count").desc())

    print(f"\n--- Top-{top_n} words ---")
    freq_df.show(top_n, truncate=False)

    # --- Filter variant: words of minimum length ---
    freq_long_df = (
        words_df.filter(F.length(F.col("word")) >= MIN_WORD_LENGTH)
        .groupBy("word")
        .count()
        .orderBy(F.col("count").desc())
    )

    print(f"--- Top-{top_n} words of length >= {MIN_WORD_LENGTH} ---")
    freq_long_df.show(top_n, truncate=False)

    # --- SQL variant: register as temp view and query with SQL ---
    words_df.createOrReplaceTempView("words")
    sql_result = spark.sql(f"""
        SELECT word, COUNT(*) AS frequency
        FROM words
        GROUP BY word
        ORDER BY frequency DESC
        LIMIT {top_n}
    """)

    print(f"--- Top-{top_n} words (SQL variant) ---")
    sql_result.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

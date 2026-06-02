"""
Bonus Chapter: Word Count - Example 1 (RDD)

The classic MapReduce "hello world" problem in PySpark. Three RDD-based
approaches are compared side-by-side, demonstrating the same efficiency
trade-offs covered in Chapter 4 (Reductions).

Algorithm:
    1. Read text file as lines
    2. flatMap each line into individual words (tokenise)
    3. Aggregate word frequencies by key

Approach 1 — reduceByKey:    pre-aggregates per partition before shuffle
Approach 2 — groupByKey:     ships all values before aggregating (less efficient)
Approach 3 — combineByKey:   most general, same efficiency as reduceByKey here

Bonus — filter variant: count only words meeting a minimum length threshold.
"""

import sys
from pathlib import Path
from typing import NamedTuple

from src.common.spark_session import create_spark_session

DEFAULT_INPUT = Path(__file__).parent.parent / "data" / "sample_text.txt"
DEFAULT_TOP_N = 10
MIN_WORD_LENGTH = 4


class WordCount(NamedTuple):
    word: str
    frequency: int


def tokenise(line: str) -> list[str]:
    """Split a line into lowercase words, stripping punctuation."""
    return [w.strip(".,!?;:\"'()").lower() for w in line.split() if w.strip(".,!?;:\"'()")]


def is_long_word(word: str, min_length: int) -> bool:
    """Return True if word meets minimum length threshold."""
    return len(word) >= min_length


def print_top(label: str, top: list[WordCount]) -> None:
    print(f"\n--- {label} ---")
    for entry in top:
        print(f"  {entry.word:20s} {entry.frequency}")


def main() -> None:
    input_path = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_INPUT)
    top_n = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_TOP_N

    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print(f"=== Word Count (RDD) | Top-{top_n} ===\n")
    print(f"Input: {input_path}")

    lines = sc.textFile(input_path)
    words = lines.flatMap(tokenise)
    print(f"Total word tokens: {words.count()}")

    # --- Approach 1: reduceByKey ---
    # Combines per-partition before shuffle — O(unique_words) shuffle volume
    freq_reduce = words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)

    top_reduce = [
        WordCount(w, c)
        for w, c in freq_reduce.takeOrdered(top_n, key=lambda x: -x[1])  # type: ignore[type-var]
    ]
    print_top(f"Top-{top_n} words (Approach 1: reduceByKey)", top_reduce)

    # --- Approach 2: groupByKey ---
    # All values shuffled to reducer — higher network cost for large inputs
    freq_group = words.map(lambda w: (w, 1)).groupByKey().mapValues(sum)

    top_group = [
        WordCount(w, c)
        for w, c in freq_group.takeOrdered(top_n, key=lambda x: -x[1])  # type: ignore[type-var]
    ]
    print_top(f"Top-{top_n} words (Approach 2: groupByKey)", top_group)

    # --- Approach 3: combineByKey ---
    # Most general reducer: identical result to reduceByKey with same efficiency
    freq_combine = words.map(lambda w: (w, 1)).combineByKey(
        lambda v: v,  # createCombiner: first value is the count
        lambda acc, v: acc + v,  # mergeValue: add 1 to running total
        lambda a, b: a + b,  # mergeCombiners: merge partition totals
    )

    top_combine = [WordCount(w, c) for w, c in freq_combine.takeOrdered(top_n, key=lambda x: -x[1])]
    print_top(f"Top-{top_n} words (Approach 3: combineByKey)", top_combine)

    # --- Bonus: filter variant — only count long words ---
    freq_long = (
        words.filter(lambda w: is_long_word(w, MIN_WORD_LENGTH))
        .map(lambda w: (w, 1))
        .reduceByKey(lambda a, b: a + b)
    )

    top_long = [
        WordCount(w, c)
        for w, c in freq_long.takeOrdered(top_n, key=lambda x: -x[1])  # type: ignore[type-var]
    ]
    print_top(f"Top-{top_n} words of length >= {MIN_WORD_LENGTH} (filter variant)", top_long)

    spark.stop()


if __name__ == "__main__":
    main()

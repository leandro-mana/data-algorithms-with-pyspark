"""
Bonus Chapter: Anagrams - Example 1 (RDD)

Groups words that are anagrams of each other. Two words are anagrams
if they contain the same letters in any order: "listen" and "silent"
both sort to "eilnst", so they share the same canonical key.

Algorithm:
    1. Tokenise text into lowercase words
    2. Map each word to (sorted_letters, word) — the sorted letters are the anagram key
    3. Group all words by their anagram key
    4. Keep only groups with more than one unique word (true anagrams)

Two approaches compared:
    groupByKey  — collects all words per key, then deduplicates
    reduceByKey — merges sorted word lists per key using set union
"""

import sys
from pathlib import Path
from typing import NamedTuple

from src.common.spark_session import create_spark_session

DEFAULT_INPUT = Path(__file__).parent.parent / "data" / "sample_text.txt"


class AnagramGroup(NamedTuple):
    key: str
    words: list[str]


def tokenise(line: str) -> list[str]:
    """Split a line into lowercase alpha-only words."""
    return [w.lower() for w in line.split() if w.isalpha()]


def anagram_key(word: str) -> str:
    """Canonical key: letters sorted alphabetically."""
    return "".join(sorted(word))


def print_groups(label: str, groups: list[AnagramGroup]) -> None:
    print(f"\n--- {label} ---")
    for group in sorted(groups, key=lambda g: -len(g.words)):
        print(f"  {group.key:12s} → {sorted(group.words)}")


def main() -> None:
    input_path = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_INPUT)

    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print("=== Anagram Grouping (RDD) ===\n")
    print(f"Input: {input_path}")

    lines = sc.textFile(input_path)
    words = lines.flatMap(tokenise)

    # --- Approach 1: groupByKey ---
    # Map each word to (sorted_letters, word), group, deduplicate, filter singletons
    anagram_pairs = words.map(lambda w: (anagram_key(w), w))

    groups_v1 = (
        anagram_pairs.groupByKey()
        .mapValues(lambda ws: sorted(set(ws)))
        .filter(lambda kv: len(kv[1]) > 1)
    )

    result_v1 = [AnagramGroup(k, ws) for k, ws in groups_v1.collect()]
    print_groups("Anagram groups (Approach 1: groupByKey)", result_v1)

    # --- Approach 2: reduceByKey ---
    # Map each word to (sorted_letters, [word]), reduce by merging sorted lists
    # reduceByKey does local combining — less data shuffled than groupByKey
    anagram_sets = words.map(lambda w: (anagram_key(w), [w]))

    groups_v2 = (
        anagram_sets.reduceByKey(lambda a, b: list(set(a) | set(b)))
        .mapValues(sorted)
        .filter(lambda kv: len(kv[1]) > 1)
    )

    result_v2 = [AnagramGroup(k, ws) for k, ws in groups_v2.collect()]
    print_groups("Anagram groups (Approach 2: reduceByKey)", result_v2)

    spark.stop()


if __name__ == "__main__":
    main()

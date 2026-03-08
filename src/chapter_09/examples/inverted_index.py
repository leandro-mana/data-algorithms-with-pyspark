"""
Chapter 9: Inverted Index Design Pattern

Builds an inverted index from a set of documents: a mapping from
each word to the list of documents it appears in, with frequency counts.

Pipeline:
  1. wholeTextFiles() — read each file as (path, content)
  2. flatMap          — emit ((word, document), 1) pairs
  3. reduceByKey      — sum frequencies per (word, document)
  4. map + groupByKey — restructure into (word, [(doc, freq), ...])

The inverted index is the core data structure behind search engines:
given a query word, instantly look up which documents contain it.
"""

import sys
from pathlib import Path

from pyspark.rdd import RDD

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session

# ---------------------------------------------------------------------------
# Step 1: Extract document name from full path
# ---------------------------------------------------------------------------


def get_document_name(path: str) -> str:
    """Extract filename from a full file:// URI or path."""
    return Path(path).name


# ---------------------------------------------------------------------------
# Step 2: Create (word, document) pairs from (document, content)
# ---------------------------------------------------------------------------

STOPWORDS = {"a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "of", "is"}


def create_word_doc_pairs(doc_tuple: tuple[str, str]) -> list[tuple[tuple[str, str], int]]:
    """Emit ((word, document), 1) for each word in the document.

    Filters out stopwords and empty tokens.
    """
    document, content = doc_tuple
    pairs: list[tuple[tuple[str, str], int]] = []
    for line in content.strip().splitlines():
        for word in line.lower().split():
            cleaned = word.strip()
            if cleaned and cleaned not in STOPWORDS:
                pairs.append(((cleaned, document), 1))
    return pairs


# ---------------------------------------------------------------------------
# Step 3-4: Build the inverted index
# ---------------------------------------------------------------------------


def build_inverted_index(rdd: RDD) -> RDD:
    """Build inverted index from (path, content) RDD.

    Returns: RDD[(word, [(doc1, freq1), (doc2, freq2), ...])]
    """
    # Step 2: rename paths to filenames, then flatMap into ((word, doc), 1)
    named = rdd.map(lambda x: (get_document_name(x[0]), x[1]))
    word_doc_pairs = named.flatMap(create_word_doc_pairs)

    # Step 3: sum frequencies per (word, document)
    frequencies = word_doc_pairs.reduceByKey(lambda a, b: a + b)

    # Step 4: restructure — ((word, doc), freq) → (word, (doc, freq))
    word_postings = frequencies.map(lambda v: (v[0][0], (v[0][1], v[1])))

    # Group postings by word
    return word_postings.groupByKey().mapValues(
        lambda postings: sorted(postings, key=lambda p: (-p[1], p[0]))
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Build and display an inverted index from document files."""
    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    docs_path = str(get_chapter_data_path("chapter_09", "documents"))
    if len(sys.argv) > 1:
        docs_path = sys.argv[1]

    print("=" * 60)
    print("Chapter 9: Inverted Index Design Pattern")
    print("=" * 60)

    # Step 1: read all documents as (path, content) pairs
    docs_rdd = sc.wholeTextFiles(docs_path)

    print(f"\nInput directory: {docs_path}")
    print(f"Documents found: {docs_rdd.count()}\n")

    print("--- Document Contents ---")
    for path, content in docs_rdd.collect():
        name = get_document_name(path)
        words = content.strip().split()
        print(f"  {name}: {len(words)} words")

    # Build the inverted index
    index = build_inverted_index(docs_rdd)

    print("\n--- Inverted Index ---")
    for word, postings in sorted(index.collect()):
        posting_str = ", ".join(f"{doc}:{freq}" for doc, freq in postings)
        print(f"  {word:12s} → [{posting_str}]")

    # Demonstrate lookup
    print("\n--- Search Demo ---")
    search_terms = ["fox", "bear", "honey", "jumped"]
    index_dict = dict(index.collect())
    for term in search_terms:
        if term in index_dict:
            docs = [doc for doc, _ in index_dict[term]]
            print(f'  Search "{term}": found in {docs}')
        else:
            print(f'  Search "{term}": not found')

    print()
    spark.stop()


if __name__ == "__main__":
    main()

"""
Chapter 3: map() vs flatMap() Transformations

Demonstrates the key difference between map() and flatMap():
- map(): 1-to-1 transformation, preserves structure
- flatMap(): 1-to-many transformation, flattens results
"""

from src.common.spark_session import create_spark_session


def main() -> None:
    """Main entry point demonstrating map vs flatMap transformations."""
    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print("=== map() vs flatMap() Comparison ===\n")

    # Example 1: List of arrays
    list_of_arrays: list[list[str]] = [
        ["item_11", "item_12", "item_13"],
        ["item_21", "item_22"],
        [],  # Empty array
        ["item_31", "item_32", "item_33", "item_34"],
        [],  # Empty array
    ]

    print(f"Input: {list_of_arrays}")
    print(f"Input length: {len(list_of_arrays)}\n")

    rdd = sc.parallelize(list_of_arrays)

    # map() - preserves structure (1-to-1)
    mapped = rdd.map(lambda x: x)
    print(f"After map(): count = {mapped.count()}")
    print(f"After map(): {mapped.collect()}\n")

    # flatMap() - flattens structure (1-to-many)
    flat_mapped = rdd.flatMap(lambda x: x)
    print(f"After flatMap(): count = {flat_mapped.count()}")
    print(f"After flatMap(): {flat_mapped.collect()}\n")

    # Example 2: Sentences to words
    print("--- Example 2: Splitting sentences into words ---\n")

    sentences = ["hello world", "spark is awesome", "pyspark flatmap demo"]
    sentences_rdd = sc.parallelize(sentences)

    print(f"Sentences: {sentences}")

    # map() keeps each result as a list
    words_mapped = sentences_rdd.map(lambda s: s.split())
    print(f"\nmap(split): {words_mapped.collect()}")
    print("  (Each sentence becomes a list of words)")

    # flatMap() flattens into individual words
    words_flat = sentences_rdd.flatMap(lambda s: s.split())
    print(f"\nflatMap(split): {words_flat.collect()}")
    print("  (All words in a single flat list)")

    # Example 3: Generating multiple outputs per input
    print("\n--- Example 3: Generating ranges ---\n")

    numbers = sc.parallelize([1, 2, 3])
    print("Input: [1, 2, 3]")

    # map() - each number becomes a list
    ranges_mapped = numbers.map(lambda n: list(range(n)))
    print(f"\nmap(range): {ranges_mapped.collect()}")

    # flatMap() - ranges are flattened
    ranges_flat = numbers.flatMap(lambda n: range(n))
    print(f"flatMap(range): {ranges_flat.collect()}")

    spark.stop()


if __name__ == "__main__":
    main()

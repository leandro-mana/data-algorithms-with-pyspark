"""
Chapter 3: map() vs flatMap() Transformations

Demonstrates the key difference between map() and flatMap() transformations:
- map(): 1-to-1 transformation that preserves RDD structure
- flatMap(): 1-to-many transformation that flattens results

Algorithm:
    map():
        1. Apply function to each element
        2. Each input element produces exactly one output element
        3. Output RDD has same number of elements as input

    flatMap():
        1. Apply function to each element (function returns iterable)
        2. Flatten all iterables into single sequence
        3. Output RDD may have different number of elements

Trade-offs:
    - map() is simpler when you need 1-to-1 transformations
    - flatMap() is essential for operations that produce variable outputs
    - flatMap() filters empty results automatically (empty iterables contribute nothing)
    - Use flatMap() for tokenization, parsing, or generating multiple outputs per input
"""

from pyspark import RDD, SparkContext

from src.common.spark_session import create_spark_session


def identity(element: list[str]) -> list[str]:
    """
    Identity function that returns the element unchanged.

    Used with map() to demonstrate that structure is preserved.

    Args:
        element: A list of strings

    Returns:
        The same list unchanged
    """
    return element


def flatten(element: list[str]) -> list[str]:
    """
    Return the element for flattening by flatMap.

    When used with flatMap(), the list elements become individual RDD elements.

    Args:
        element: A list of strings to flatten

    Returns:
        The same list (flatMap will iterate over it)
    """
    return element


def split_sentence(sentence: str) -> list[str]:
    """
    Split a sentence into words.

    Args:
        sentence: A string containing space-separated words

    Returns:
        List of individual words
    """
    return sentence.split()


def generate_range(n: int) -> range:
    """
    Generate a range of numbers from 0 to n-1.

    Args:
        n: The upper bound (exclusive) for the range

    Returns:
        A range object from 0 to n-1
    """
    return range(n)


def demonstrate_array_flattening(sc: SparkContext) -> None:
    """
    Demonstrate map vs flatMap with nested arrays.

    Shows how map() preserves structure while flatMap() flattens nested lists.

    Args:
        sc: SparkContext for creating RDDs
    """
    print("--- Example 1: Flattening nested arrays ---\n")

    nested_arrays: list[list[str]] = [
        ["item_11", "item_12", "item_13"],
        ["item_21", "item_22"],
        [],  # Empty array - filtered out by flatMap
        ["item_31", "item_32", "item_33", "item_34"],
        [],  # Empty array - filtered out by flatMap
    ]

    print(f"Input: {nested_arrays}")
    print(f"Input length: {len(nested_arrays)}\n")

    rdd: RDD[list[str]] = sc.parallelize(nested_arrays)

    # map() - preserves structure (1-to-1)
    # Each list remains as a single element
    mapped_rdd: RDD[list[str]] = rdd.map(identity)
    print(f"After map(): count = {mapped_rdd.count()}")
    print(f"After map(): {mapped_rdd.collect()}")
    print("  (Each list is one element - structure preserved)\n")

    # flatMap() - flattens structure (1-to-many)
    # Lists are unpacked, empty lists contribute nothing
    flat_mapped_rdd: RDD[str] = rdd.flatMap(flatten)
    print(f"After flatMap(): count = {flat_mapped_rdd.count()}")
    print(f"After flatMap(): {flat_mapped_rdd.collect()}")
    print("  (All items flattened into single list, empty arrays removed)\n")


def demonstrate_sentence_tokenization(sc: SparkContext) -> None:
    """
    Demonstrate map vs flatMap for text tokenization.

    A common use case: splitting sentences into individual words.

    Args:
        sc: SparkContext for creating RDDs
    """
    print("--- Example 2: Splitting sentences into words ---\n")

    sentences: list[str] = [
        "hello world",
        "spark is awesome",
        "pyspark flatmap demo",
    ]
    sentences_rdd: RDD[str] = sc.parallelize(sentences)

    print(f"Sentences: {sentences}")

    # map() keeps each split result as a list
    words_as_lists: RDD[list[str]] = sentences_rdd.map(split_sentence)
    print(f"\nmap(split): {words_as_lists.collect()}")
    print("  (Each sentence becomes a list of words - nested structure)")

    # flatMap() flattens into individual words
    words_flat: RDD[str] = sentences_rdd.flatMap(split_sentence)
    print(f"\nflatMap(split): {words_flat.collect()}")
    print("  (All words in a single flat list - ready for word count)")


def demonstrate_range_generation(sc: SparkContext) -> None:
    """
    Demonstrate map vs flatMap for generating multiple outputs.

    Shows how flatMap() handles variable-length outputs per input.

    Args:
        sc: SparkContext for creating RDDs
    """
    print("\n--- Example 3: Generating ranges ---\n")

    numbers: list[int] = [1, 2, 3]
    numbers_rdd: RDD[int] = sc.parallelize(numbers)
    print(f"Input: {numbers}")

    # map() - each number becomes a list
    ranges_as_lists: RDD[list[int]] = numbers_rdd.map(lambda n: list(generate_range(n)))
    print(f"\nmap(range): {ranges_as_lists.collect()}")
    print("  (Each number produces a list)")

    # flatMap() - ranges are flattened into individual elements
    ranges_flat: RDD[int] = numbers_rdd.flatMap(generate_range)
    print(f"flatMap(range): {ranges_flat.collect()}")
    print("  (All range elements combined: 0 + 0,1 + 0,1,2)")


def main() -> None:
    """
    Main entry point demonstrating map vs flatMap transformations.

    Runs three examples showing the fundamental difference:
    1. Flattening nested arrays
    2. Tokenizing sentences into words
    3. Generating variable-length outputs
    """
    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    print("=== map() vs flatMap() Comparison ===\n")

    # Run all demonstrations
    demonstrate_array_flattening(sc)
    demonstrate_sentence_tokenization(sc)
    demonstrate_range_generation(sc)

    spark.stop()


if __name__ == "__main__":
    main()

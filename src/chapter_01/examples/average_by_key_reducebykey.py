"""
Chapter 1: Average by Key using reduceByKey()

Demonstrates calculating averages per key using the reduceByKey() transformation.
This is a common pattern: transform values to (sum, count) tuples, then reduce.
"""

from typing import NamedTuple

from src.common.data_loader import get_chapter_data_path, load_csv_as_tuples
from src.common.spark_session import create_spark_session


class PersonRecord(NamedTuple):
    """A record representing a person with name, city, and score."""

    name: str
    city: str
    score: int


def load_people_data() -> list[PersonRecord]:
    """Load people data from CSV file."""
    csv_path = get_chapter_data_path("chapter_01", "people.csv")
    return load_csv_as_tuples(
        csv_path,
        lambda name, city, value: PersonRecord(name, city, int(value)),
    )


def to_sum_count(record: PersonRecord) -> tuple[str, tuple[int, int]]:
    """
    Convert a PersonRecord to (name, (score, 1)) for averaging.

    Args:
        record: A PersonRecord with name, city, and score

    Returns:
        Tuple of (name, (score, 1)) for use in reduceByKey
    """
    return (record.name, (record.score, 1))


def add_pairs(a: tuple[int, int], b: tuple[int, int]) -> tuple[int, int]:
    """
    Add two (sum, count) pairs together.

    Args:
        a: First (sum, count) tuple
        b: Second (sum, count) tuple

    Returns:
        Combined (sum, count) tuple
    """
    return (a[0] + b[0], a[1] + b[1])


def compute_average(sum_count: tuple[int, int]) -> float:
    """
    Compute average from (sum, count) tuple.

    Args:
        sum_count: Tuple of (total_sum, count)

    Returns:
        The average value
    """
    total, count = sum_count
    return total / count


def main() -> None:
    """Main entry point demonstrating average by key using reduceByKey."""
    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    data = load_people_data()

    print("=== Average by Key using reduceByKey() ===\n")
    print(f"Input data: {data}\n")

    rdd = sc.parallelize(data)

    # Step 1: Transform to (key, (value, 1))
    sum_count_rdd = rdd.map(to_sum_count)
    print("After map to (key, (value, 1)):")
    print(f"  {sum_count_rdd.collect()}\n")

    # Step 2: Reduce by key to get (key, (total_sum, total_count))
    totals_rdd = sum_count_rdd.reduceByKey(add_pairs)
    print("After reduceByKey (sum totals and counts):")
    print(f"  {totals_rdd.collect()}\n")

    # Step 3: Compute averages using mapValues
    averages_rdd = totals_rdd.mapValues(compute_average)
    print("Final averages:")
    for name, avg in sorted(averages_rdd.collect()):
        print(f"  {name}: {avg:.2f}")

    spark.stop()


if __name__ == "__main__":
    main()

"""
Chapter 1: RDD map() Transformation

Demonstrates the map() transformation which applies a function to each element
of an RDD, producing a new RDD with the same number of elements (1-to-1).
"""

from typing import NamedTuple

from src.common.data_loader import get_chapter_data_path, load_csv_as_tuples
from src.common.spark_session import create_spark_session


class PersonRecord(NamedTuple):
    """A record representing a person with name, city, and age."""

    name: str
    city: str
    age: int


def load_people_data() -> list[PersonRecord]:
    """Load people data from CSV file."""
    csv_path = get_chapter_data_path("chapter_01", "people.csv")
    return load_csv_as_tuples(
        csv_path,
        lambda name, city, value: PersonRecord(name, city, int(value)),
    )


def create_name_age_pair(record: PersonRecord) -> tuple[str, int]:
    """
    Extract name and age from a PersonRecord.

    Args:
        record: A PersonRecord with name, city, and age

    Returns:
        Tuple of (name, age)
    """
    return (record.name, record.age)


def create_city_pair(record: PersonRecord) -> tuple[str, PersonRecord]:
    """
    Create a key-value pair with city as key.

    Args:
        record: A PersonRecord

    Returns:
        Tuple of (city, record)
    """
    return (record.city, record)


def main() -> None:
    """Main entry point demonstrating RDD map transformations."""
    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    data = load_people_data()

    print("=== RDD map() Transformation Demo ===\n")
    print(f"Input data: {data}\n")

    # Create RDD from collection
    rdd = sc.parallelize(data)
    print(f"Original RDD count: {rdd.count()}")

    # Apply map transformation to extract name-age pairs
    name_age_rdd = rdd.map(create_name_age_pair)
    print("\nAfter map(create_name_age_pair):")
    print(f"  Count: {name_age_rdd.count()}")
    print(f"  Data: {name_age_rdd.collect()}")

    # Apply mapValues to increment ages by 5
    incremented_rdd = name_age_rdd.mapValues(lambda age: age + 5)
    print("\nAfter mapValues(age + 5):")
    print(f"  Data: {incremented_rdd.collect()}")

    # Apply map with city as key
    city_keyed_rdd = rdd.map(create_city_pair)
    print("\nAfter map(create_city_pair):")
    print(f"  Data: {city_keyed_rdd.collect()}")

    spark.stop()


if __name__ == "__main__":
    main()

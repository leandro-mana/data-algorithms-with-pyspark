"""
Chapter 1: DataFrame Basics

Demonstrates basic DataFrame operations: creation, filtering, and simple queries.
"""

from typing import NamedTuple

from pyspark.sql.functions import col

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


def main() -> None:
    """Main entry point demonstrating DataFrame basics."""
    spark = create_spark_session(__file__)

    data = load_people_data()

    print("=== DataFrame Basics Demo ===\n")

    # Create DataFrame with schema
    df = spark.createDataFrame(data, ["name", "city", "age"])

    print("Original DataFrame:")
    df.show()
    df.printSchema()

    # Filter: age > 50
    print("\nFiltered: age > 50")
    df_over_50 = df.filter(col("age") > 50)
    df_over_50.show()

    # Filter: city contains 'me'
    print("\nFiltered: city contains 'me'")
    df_city_me = df.filter(col("city").contains("me"))
    df_city_me.show()

    # Select specific columns
    print("\nSelect name and age only:")
    df.select("name", "age").show()

    # Add a new column
    print("\nAdd 'age_in_10_years' column:")
    df.withColumn("age_in_10_years", col("age") + 10).show()

    # Group by and aggregate
    print("\nAverage age by name:")
    df.groupBy("name").avg("age").show()

    spark.stop()


if __name__ == "__main__":
    main()

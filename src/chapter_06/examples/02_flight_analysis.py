"""
Chapter 6: Flight Analysis — Real-World Graph Queries

Demonstrates practical graph analysis on airline data using pure
PySpark DataFrames. Airports are vertices, flights are edges.

Queries answered:
- Most connected airports (degree analysis)
- Longest/shortest routes
- Average departure delays by route and hour
- Connecting flights between airports with no direct route
- Most frequent flight routes

Key insight: real-world graph analysis often reduces to DataFrame
joins, groupBy, and aggregation — no specialized graph library needed.
"""

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import round as spark_round

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_airports(spark: SparkSession, path: str) -> DataFrame:
    """Load airport vertices from CSV (id, city, state)."""
    return spark.read.option("header", "true").csv(path)


def load_flights(spark: SparkSession, path: str) -> DataFrame:
    """Load flight edges from CSV (src, dst, carrier, distance, dep_delay)."""
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)


# ---------------------------------------------------------------------------
# Graph metrics
# ---------------------------------------------------------------------------


def airport_degrees(flights: DataFrame) -> DataFrame:
    """Compute total degree (incoming + outgoing flights) per airport."""
    outgoing = flights.groupBy("src").agg(count("*").alias("out_flights"))
    incoming = flights.groupBy("dst").agg(count("*").alias("in_flights"))
    return (
        outgoing.withColumnRenamed("src", "airport")
        .join(incoming.withColumnRenamed("dst", "airport"), on="airport", how="outer")
        .fillna(0)
        .withColumn("total_flights", col("out_flights") + col("in_flights"))
        .orderBy(col("total_flights").desc())
    )


def longest_routes(flights: DataFrame) -> DataFrame:
    """Find routes with the longest distance."""
    return (
        flights.groupBy("src", "dst")
        .agg(spark_max("distance").alias("max_distance"))
        .orderBy(col("max_distance").desc())
    )


def avg_delay_by_route(flights: DataFrame) -> DataFrame:
    """Compute average departure delay per route."""
    return (
        flights.groupBy("src", "dst")
        .agg(
            spark_round(avg("dep_delay"), 2).alias("avg_delay"),
            count("*").alias("flights"),
        )
        .orderBy(col("avg_delay").desc())
    )


def most_frequent_routes(flights: DataFrame) -> DataFrame:
    """Find the most frequent flight routes."""
    return (
        flights.groupBy("src", "dst")
        .agg(count("*").alias("flight_count"))
        .orderBy(col("flight_count").desc())
    )


# ---------------------------------------------------------------------------
# Path finding — connecting flights via self-join
# ---------------------------------------------------------------------------


def find_connecting_flights(
    flights: DataFrame,
    origin: str,
    destination: str,
) -> DataFrame:
    """Find 2-hop connecting flights between airports with no direct route.

    Joins flights with themselves: first leg (origin -> hub) + second leg
    (hub -> destination). This is equivalent to the GraphFrames motif:
      (a)-[]->(b); (b)-[]->(c)
    with filters on a and c.
    """
    direct = flights.filter((col("src") == origin) & (col("dst") == destination))

    if direct.count() > 0:
        return direct.select(
            col("src").alias("origin"),
            col("dst").alias("destination"),
            col("carrier"),
            col("distance"),
        )

    # No direct route — find connecting flights
    return (
        flights.alias("leg1")
        .join(flights.alias("leg2"), col("leg1.dst") == col("leg2.src"))
        .filter((col("leg1.src") == origin) & (col("leg2.dst") == destination))
        .select(
            col("leg1.src").alias("origin"),
            col("leg1.dst").alias("hub"),
            col("leg2.dst").alias("destination"),
            col("leg1.carrier").alias("carrier_1"),
            col("leg2.carrier").alias("carrier_2"),
            (col("leg1.distance") + col("leg2.distance")).alias("total_distance"),
        )
        .distinct()
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Analyze flight data as a graph using pure DataFrames."""
    spark = create_spark_session(__file__)

    airports_path = str(get_chapter_data_path("chapter_06", "airports.csv"))
    flights_path = str(get_chapter_data_path("chapter_06", "flights.csv"))
    if len(sys.argv) > 2:
        airports_path = sys.argv[1]
        flights_path = sys.argv[2]

    airports = load_airports(spark, airports_path)
    flights = load_flights(spark, flights_path)

    print("=== Flight Analysis: Graph Queries with DataFrames ===\n")
    print(f"Airports: {airports.count()}")
    print(f"Flights: {flights.count()}\n")

    # --- Most connected airports ---
    print("--- Most Connected Airports (by total flights) ---")
    airport_degrees(flights).show(5, truncate=False)

    # --- Longest routes ---
    print("--- Longest Flight Routes ---")
    longest_routes(flights).show(5, truncate=False)

    # --- Average delays by route ---
    print("--- Highest Average Departure Delays (by route) ---")
    avg_delay_by_route(flights).show(5, truncate=False)

    # --- Most frequent routes ---
    print("--- Most Frequent Routes ---")
    most_frequent_routes(flights).show(5, truncate=False)

    # --- Connecting flights (no direct route) ---
    print("--- Connecting Flights: LAX → LGA (no direct route) ---")
    connections = find_connecting_flights(flights, "LAX", "LGA")
    connections.show(truncate=False)
    print(f"Connecting options found: {connections.count()}")

    print("\n--- Connecting Flights: CLT → BOS (no direct route) ---")
    connections2 = find_connecting_flights(flights, "CLT", "BOS")
    connections2.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

"""
Chapter 6: Graph Basics — Building and Querying Graphs with DataFrames

Demonstrates core graph concepts using pure PySpark DataFrames
(no external GraphFrames library required):
- Building a graph from vertices and edges DataFrames
- Computing in-degree, out-degree, and total degree
- Finding bidirectional relationships
- Finding 2-hop paths (friends of friends)
- Finding triangles via triple self-join

Key insight: many graph operations are just DataFrame joins and aggregations.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session

# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------


def create_vertices(names: list[str], spark: SparkSession) -> DataFrame:
    """Create a vertices DataFrame from a list of names.

    The 'id' column uniquely identifies each vertex — same convention
    as the GraphFrames API.
    """
    return spark.createDataFrame([(name,) for name in names], ["id"])


def load_edges(spark: SparkSession, path: str) -> DataFrame:
    """Load edges from CSV with columns: src, dst, relationship."""
    return spark.read.option("header", "true").csv(path)


# ---------------------------------------------------------------------------
# Graph metrics — degree computations
# ---------------------------------------------------------------------------


def compute_in_degrees(edges: DataFrame) -> DataFrame:
    """Count incoming edges per vertex (how many point TO this vertex)."""
    return edges.groupBy("dst").agg(count("*").alias("in_degree")).withColumnRenamed("dst", "id")


def compute_out_degrees(edges: DataFrame) -> DataFrame:
    """Count outgoing edges per vertex (how many point FROM this vertex)."""
    return edges.groupBy("src").agg(count("*").alias("out_degree")).withColumnRenamed("src", "id")


def compute_total_degrees(in_deg: DataFrame, out_deg: DataFrame) -> DataFrame:
    """Combine in-degree and out-degree into total degree per vertex."""
    return (
        in_deg.join(out_deg, on="id", how="outer")
        .fillna(0)
        .withColumn("total_degree", col("in_degree") + col("out_degree"))
        .orderBy(col("total_degree").desc())
    )


# ---------------------------------------------------------------------------
# Graph queries
# ---------------------------------------------------------------------------


def find_bidirectional(edges: DataFrame) -> DataFrame:
    """Find pairs with edges in BOTH directions (mutual relationships).

    Self-join edges on reversed src/dst to find (a->b) AND (b->a).
    """
    return (
        edges.alias("e1")
        .join(
            edges.alias("e2"),
            (col("e1.src") == col("e2.dst")) & (col("e1.dst") == col("e2.src")),
        )
        .filter(col("e1.src") < col("e1.dst"))  # deduplicate pairs
        .select(
            col("e1.src").alias("person_a"),
            col("e1.dst").alias("person_b"),
            col("e1.relationship").alias("a_to_b"),
            col("e2.relationship").alias("b_to_a"),
        )
    )


def find_friends_of_friends(edges: DataFrame) -> DataFrame:
    """Find 2-hop paths: a -> b -> c where a != c.

    This is the foundation for friend recommendations:
    'people you may know' = friends of friends who aren't already connected.
    """
    return (
        edges.alias("e1")
        .join(edges.alias("e2"), col("e1.dst") == col("e2.src"))
        .filter(col("e1.src") != col("e2.dst"))
        .select(
            col("e1.src").alias("person"),
            col("e1.dst").alias("through"),
            col("e2.dst").alias("suggestion"),
        )
        .distinct()
    )


def find_triangles(edges: DataFrame) -> DataFrame:
    """Find all unique triangles in the graph via triple self-join.

    A triangle is three vertices {a, b, c} where:
      a -> b, b -> c, and c -> a all exist.

    Filter a.id < b.id < c.id to get each triangle exactly once.
    """
    return (
        edges.alias("e1")
        .join(edges.alias("e2"), col("e1.dst") == col("e2.src"))
        .join(
            edges.alias("e3"), (col("e2.dst") == col("e3.src")) & (col("e3.dst") == col("e1.src"))
        )
        .filter(col("e1.src") < col("e1.dst"))
        .filter(col("e2.src") < col("e2.dst"))
        .select(
            col("e1.src").alias("vertex_a"),
            col("e1.dst").alias("vertex_b"),
            col("e2.dst").alias("vertex_c"),
        )
        .distinct()
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Build a social network graph and run queries with pure DataFrames."""
    spark = create_spark_session(__file__)

    edges_path = str(get_chapter_data_path("chapter_06", "social_network.csv"))
    edges = load_edges(spark, edges_path)

    # Extract unique vertex names from edges
    vertex_names = sorted(
        {row.src for row in edges.select("src").collect()}
        | {row.dst for row in edges.select("dst").collect()}
    )
    vertices = create_vertices(vertex_names, spark)

    print("=== Graph Basics: Building and Querying Graphs ===\n")

    print("Vertices:")
    vertices.show(truncate=False)

    print("Edges:")
    edges.show(truncate=False)

    # --- Degree metrics ---
    in_deg = compute_in_degrees(edges)
    out_deg = compute_out_degrees(edges)
    total_deg = compute_total_degrees(in_deg, out_deg)

    print("--- Degree Metrics ---")
    print("In-degree (incoming connections):")
    in_deg.orderBy(col("in_degree").desc()).show(truncate=False)

    print("Out-degree (outgoing connections):")
    out_deg.orderBy(col("out_degree").desc()).show(truncate=False)

    print("Total degree (in + out):")
    total_deg.show(truncate=False)

    # --- Bidirectional relationships ---
    print("--- Bidirectional Relationships ---")
    bidir = find_bidirectional(edges)
    bidir.show(truncate=False)

    # --- Friends of friends ---
    print("--- Friends of Friends (2-hop paths) ---")
    fof = find_friends_of_friends(edges)
    fof.orderBy("person", "suggestion").show(truncate=False)

    # --- Triangles ---
    print("--- Triangles (unique) ---")
    triangles = find_triangles(edges)
    triangles.show(truncate=False)
    print(f"Unique triangles found: {triangles.count()}")

    spark.stop()


if __name__ == "__main__":
    main()

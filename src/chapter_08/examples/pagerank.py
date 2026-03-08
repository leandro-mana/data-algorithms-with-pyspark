"""
Chapter 8: PageRank — Iterative Node Importance in Graphs

PageRank measures the importance of each node in a directed graph.
Originally developed by Google to rank web pages, it's based on the
idea that a node is important if other important nodes point to it.

The formula:
  PR(A) = (1 - d) + d × Σ(PR(Ti) / L(Ti))

Where:
  d = damping factor (typically 0.85)
  PR(Ti) = PageRank of page Ti that links to A
  L(Ti) = number of outbound links from Ti

The algorithm is iterative and converges regardless of initial values.
After enough iterations, ranks stabilize (convergence).

Key PySpark patterns:
- groupByKey() to build adjacency list from edge pairs
- join() + flatMap() to propagate rank contributions
- reduceByKey() to sum incoming contributions per node
- mapValues() to apply damping factor
"""

from pyspark import RDD

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session

DAMPING_FACTOR = 0.85


# ---------------------------------------------------------------------------
# Graph loading
# ---------------------------------------------------------------------------


def parse_edge(line: str) -> tuple[str, str]:
    """Parse an edge line 'src,dst' into a (source, destination) tuple."""
    parts = line.strip().split(",")
    return (parts[0], parts[1])


def build_adjacency_list(edges_rdd: RDD) -> RDD:
    """Build adjacency list: RDD[(node, [neighbor1, neighbor2, ...])].

    Uses distinct() to remove duplicate edges, then groupByKey()
    to collect all neighbors per source node. The result is cached
    because it's reused in every iteration.
    """
    return edges_rdd.distinct().groupByKey().mapValues(list).cache()


# ---------------------------------------------------------------------------
# PageRank iteration
# ---------------------------------------------------------------------------


def compute_contributions(
    node_data: tuple[str, tuple[list[str], float]],
) -> list[tuple[str, float]]:
    """Compute rank contributions from a node to its neighbors.

    Each neighbor gets an equal share: rank / num_neighbors.
    This is the 'distribute rank' step of PageRank.
    """
    neighbors, rank = node_data[1]
    num_neighbors = len(neighbors)
    return [(neighbor, rank / num_neighbors) for neighbor in neighbors]


def apply_damping(rank: float) -> float:
    """Apply the damping factor to a raw rank sum.

    PR(node) = (1 - d) + d × sum_of_contributions
    The (1-d) term represents the probability of a random jump.
    """
    return (1 - DAMPING_FACTOR) + DAMPING_FACTOR * rank


def run_pagerank(links: RDD, num_iterations: int) -> RDD:
    """Run the PageRank algorithm for a fixed number of iterations.

    Returns: RDD[(node, pagerank_value)]
    """
    # Initialize all nodes with rank 1.0
    ranks = links.mapValues(lambda _: 1.0)

    for i in range(num_iterations):
        # Join links with current ranks, then compute contributions
        contributions = links.join(ranks).flatMap(compute_contributions)

        # Sum contributions per node and apply damping
        ranks = contributions.reduceByKey(lambda a, b: a + b).mapValues(apply_damping)

    return ranks


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Run PageRank on a small web graph."""
    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    graph_path = str(get_chapter_data_path("chapter_08", "web_graph.txt"))
    edges_rdd = sc.textFile(graph_path).map(parse_edge)
    links = build_adjacency_list(edges_rdd)

    print("=== PageRank: Iterative Node Importance ===\n")

    # Show the graph structure
    print("--- Graph (adjacency list) ---")
    for node, neighbors in sorted(links.collect()):
        print(f"  {node} → {neighbors}")

    # Run PageRank for different iteration counts to show convergence
    for iterations in [1, 5, 10, 20]:
        ranks = run_pagerank(links, iterations)
        sorted_ranks = sorted(ranks.collect(), key=lambda x: -x[1])

        print(f"\n--- PageRank after {iterations} iteration(s) ---")
        total = 0.0
        for node, rank in sorted_ranks:
            print(f"  {node}: {rank:.6f}")
            total += rank
        print(f"  Sum: {total:.6f}")

    # Final results with analysis
    print("\n--- Final Analysis (20 iterations) ---")
    final_ranks = run_pagerank(links, 20)
    sorted_final = sorted(final_ranks.collect(), key=lambda x: -x[1])
    most_important = sorted_final[0]
    print(f"Most important node: {most_important[0]} (PR = {most_important[1]:.6f})")
    print(
        f"This makes sense — node {most_important[0]} has the most incoming edges,\n"
        "making it the most 'endorsed' by other nodes in the graph."
    )

    spark.stop()


if __name__ == "__main__":
    main()

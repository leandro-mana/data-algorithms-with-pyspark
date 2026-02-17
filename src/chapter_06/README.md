# Chapter 6: Graph Algorithms

This chapter introduces graph-based data structures and demonstrates how to build, query, and analyze graphs using PySpark DataFrames. Graphs model relationships between entities — social networks, flight routes, gene interactions — where vertices represent entities and edges represent connections. All examples use **pure DataFrames** (joins, groupBy, aggregations) without external graph libraries.

## Examples

| Example | Description | Key Concepts |
| --- | --- | --- |
| `graph_basics.py` | Build a social network graph, compute degrees, find paths and triangles | Vertices/edges as DataFrames, self-joins, degree metrics |
| `flight_analysis.py` | Real-world graph queries on airline data (airports + flights) | Route analysis, connecting flights via 2-hop joins |

## Running Examples

```bash
# Run graph basics (social network analysis)
make run-spark CHAPTER=chapter_06 EXAMPLE=graph_basics

# Run flight analysis with sample data
make run-spark CHAPTER=chapter_06 EXAMPLE=flight_analysis

# Run flight analysis with custom data files
make run-spark CHAPTER=chapter_06 EXAMPLE=flight_analysis \
    ARGS="/path/to/airports.csv /path/to/flights.csv"
```

## Key Concepts

### Graphs: Vertices and Edges

A graph `G = (V, E)` consists of:
- **Vertices (V)** — entities (people, airports, genes)
- **Edges (E)** — connections between vertices (friendships, flights, interactions)

```python
# Vertices DataFrame — 'id' column identifies each vertex
vertices = spark.createDataFrame(
    [("Alice", 34), ("Bob", 36), ("Charlie", 30)],
    ["id", "age"]
)

# Edges DataFrame — 'src' and 'dst' columns define connections
edges = spark.createDataFrame(
    [("Alice", "Bob", "friend"), ("Bob", "Charlie", "follow")],
    ["src", "dst", "relationship"]
)
```

| Graph Type | Description | Example |
| --- | --- | --- |
| **Directed** | Edges have direction (a → b) | Twitter follows, airline routes |
| **Undirected** | Edges are bidirectional (a ↔ b) | Facebook friendships |

To convert directed → undirected: for every edge `(a, b)`, add edge `(b, a)`.

### Degree Metrics

The **degree** of a vertex is the number of edges connected to it:

```python
# In-degree: how many edges point TO this vertex
in_degrees = edges.groupBy("dst") \
    .count() \
    .withColumnRenamed("count", "in_degree")

# Out-degree: how many edges point FROM this vertex
out_degrees = edges.groupBy("src") \
    .count() \
    .withColumnRenamed("count", "out_degree")
```

| Metric | Meaning | Use Case |
| --- | --- | --- |
| In-degree | Incoming connections | Popularity, influence |
| Out-degree | Outgoing connections | Activity, reach |
| Total degree | In + Out | Overall connectivity |

### Finding Paths with Self-Joins

Graph traversal is just **DataFrame self-joins**. A 2-hop path (a → b → c) is found by joining edges with themselves:

```python
# Find friends-of-friends (2-hop paths)
friends_of_friends = (
    edges.alias("e1")
    .join(edges.alias("e2"), col("e1.dst") == col("e2.src"))
    .filter(col("e1.src") != col("e2.dst"))
    .select(
        col("e1.src").alias("person"),
        col("e1.dst").alias("through"),
        col("e2.dst").alias("suggestion"),
    )
)
```

This is the foundation for **friend recommendations**: suggest people who are 2 hops away but not directly connected.

### Finding Triangles

A **triangle** is three vertices `{a, b, c}` where all three are connected: `a→b`, `b→c`, `c→a`. Found via triple self-join:

```python
triangles = (
    edges.alias("e1")
    .join(edges.alias("e2"), col("e1.dst") == col("e2.src"))
    .join(edges.alias("e3"),
          (col("e2.dst") == col("e3.src")) & (col("e3.dst") == col("e1.src")))
    .filter(col("e1.src") < col("e1.dst"))   # deduplicate
    .filter(col("e2.src") < col("e2.dst"))
    .select("e1.src", "e1.dst", "e2.dst")
    .distinct()
)
```

**Why deduplicate?** A triangle `{a, b, c}` can be traversed 6 ways (3 starting vertices × 2 directions). The filters `a < b` and `b < c` ensure each triangle appears exactly once.

Triangle counting is used in:
- Social network cohesion analysis
- Clustering coefficient computation
- Community detection

### Bidirectional Relationships

Find pairs with edges in **both** directions (mutual follows, reciprocal friendships):

```python
bidirectional = (
    edges.alias("e1")
    .join(edges.alias("e2"),
          (col("e1.src") == col("e2.dst")) & (col("e1.dst") == col("e2.src")))
    .filter(col("e1.src") < col("e1.dst"))  # deduplicate
)
```

### Connecting Flights (No Direct Route)

A practical 2-hop path query: find connections between airports when no direct flight exists:

```python
# leg1: origin → hub, leg2: hub → destination
connections = (
    flights.alias("leg1")
    .join(flights.alias("leg2"), col("leg1.dst") == col("leg2.src"))
    .filter((col("leg1.src") == "LAX") & (col("leg2.dst") == "LGA"))
    .select("leg1.src", "leg1.dst", "leg2.dst",
            (col("leg1.distance") + col("leg2.distance")).alias("total_dist"))
)
```

## GraphFrames vs Pure DataFrames

The book uses the [GraphFrames](https://graphframes.github.io/) external library, which provides built-in algorithms (PageRank, connected components, motif finding). Our examples demonstrate that many graph operations can be done with pure DataFrames:

| Operation | GraphFrames | Pure DataFrames |
| --- | --- | --- |
| Build graph | `GraphFrame(v, e)` | Two DataFrames with convention |
| In/Out degree | `graph.inDegrees` | `groupBy("dst").count()` |
| Motif finding | `graph.find("(a)-[]->(b)")` | Self-join with filters |
| Triangle count | `graph.triangleCount()` | Triple self-join |
| Bidirectional | `graph.find("(a)-[]->(b); (b)-[]->(a)")` | Self-join on reversed src/dst |
| PageRank | `graph.pageRank()` | Iterative join (more complex) |
| Connected components | `graph.connectedComponents()` | BFS/union-find (more complex) |

**Trade-off**: GraphFrames provides optimized implementations of complex algorithms (PageRank, connected components) as one-liners. Pure DataFrames give you full control and zero external dependencies, but complex algorithms require more code.

## Performance Considerations

| Operation | Complexity | Notes |
| --- | --- | --- |
| Degree computation | O(E) | Single pass over edges |
| 2-hop path (self-join) | O(E²) worst case | Filter early to reduce join size |
| Triangle finding | O(E³) worst case | Deduplicate filters reduce output |
| Bidirectional check | O(E²) worst case | Symmetry filter halves output |

**Tips for large graphs:**
- Always **filter before joining** — reduce edge DataFrame size first
- Use **broadcast joins** for small vertex DataFrames
- Consider **repartitioning** edges by `src` before repeated self-joins
- For complex algorithms (PageRank, connected components), GraphFrames' optimized implementations may be worth the dependency

## Additional Resources

- [GraphFrames User Guide](https://graphframes.github.io/graphframes/docs/_site/user-guide.html)
- [Stanford SNAP Datasets](https://snap.stanford.edu/data/) (real-world graph data)
- [PySpark DataFrame Joins](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)
- [Graph Theory — Wikipedia](https://en.wikipedia.org/wiki/Graph_theory)

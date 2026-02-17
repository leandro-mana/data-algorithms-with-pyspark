# Chapter 8: Ranking Algorithms

This chapter covers two ranking algorithms: **Rank Product** (used in bioinformatics and machine learning) and **PageRank** (the algorithm that powered Google's search engine). Both demonstrate iterative computation patterns in PySpark using RDDs.

## Examples

| Example | Description | Key Concepts |
| --- | --- | --- |
| `rank_product.py` | Rank genes across multiple studies using geometric mean of ranks | `combineByKey()`, `zipWithIndex()`, `union()`, NamedTuples |
| `pagerank.py` | Compute node importance in a directed graph via iterative rank propagation | `groupByKey()`, `join()`, `flatMap()`, `reduceByKey()`, damping factor |

## Running Examples

```bash
# Run rank product (3 gene studies)
make run-spark CHAPTER=chapter_08 EXAMPLE=rank_product

# Run PageRank (web graph)
make run-spark CHAPTER=chapter_08 EXAMPLE=pagerank
```

## Key Concepts

### Rank Product

The rank product algorithm ranks items across multiple ranked lists by computing the **geometric mean** of their ranks. It was originally developed for detecting differentially expressed genes in microarray experiments.

**Algorithm:**

1. For each study, compute the mean value per gene
2. Sort genes by value (descending) and assign ranks (1 = highest)
3. Compute the rank product: `RP(g) = (r1 × r2 × ... × rk) ^ (1/k)`

```python
# Step 1: Mean per gene using combineByKey (sum, count)
combined = rdd.combineByKey(
    lambda v: (v, 1),                        # createCombiner
    lambda acc, v: (acc[0] + v, acc[1] + 1), # mergeValue
    lambda a, b: (a[0] + b[0], a[1] + b[1]) # mergeCombiners
)
means = combined.mapValues(lambda sc: sc[0] / sc[1])

# Step 2: Sort and assign ranks using zipWithIndex
sorted_rdd = swapped.sortByKey(ascending=False, numPartitions=1)
ranked = sorted_rdd.zipWithIndex().map(lambda vi: (vi[0][1], vi[1] + 1))

# Step 3: Geometric mean across studies
# RP(gene) = (product_of_ranks) ^ (1/num_studies)
```

**Example with 3 studies:**

| Gene | Study 1 (rank) | Study 2 (rank) | Study 3 (rank) | Rank Product |
| --- | --- | --- | --- | --- |
| G1 | 3 | 1 | 2 | (3×1×2)^(1/3) = 1.82 |
| G2 | 2 | 2 | 1 | (2×2×1)^(1/3) = 1.59 |
| G3 | 4 | 4 | — | (4×4)^(1/2) = 4.00 |
| G4 | 1 | 3 | — | (1×3)^(1/2) = 1.73 |

A **smaller** rank product means the item consistently ranks near the top.

### PageRank

PageRank measures node importance in a directed graph. The core idea: **a node is important if other important nodes point to it**.

**The Formula:**

```
PR(A) = (1 - d) + d × Σ(PR(Ti) / L(Ti))
```

Where:
- `d` = damping factor (typically 0.85)
- `PR(Ti)` = PageRank of page Ti that links to A
- `L(Ti)` = number of outbound links from Ti
- `(1 - d)` = probability of a random jump (not following any link)

**The Algorithm (iterative):**

```python
# Initialize all nodes with rank 1.0
ranks = links.mapValues(lambda _: 1.0)

for i in range(num_iterations):
    # Each node distributes its rank equally to neighbors
    contributions = links.join(ranks).flatMap(compute_contributions)

    # Sum contributions and apply damping
    ranks = (
        contributions
        .reduceByKey(lambda a, b: a + b)
        .mapValues(lambda rank: (1 - d) + d * rank)
    )
```

**Convergence:** PageRank converges to the same values regardless of initial ranks. After ~20 iterations, values typically stabilize.

**Why the damping factor?** It models a "random surfer" who sometimes stops following links and jumps to a random page. With d=0.85, there's a 15% chance of a random jump at each step.

### combineByKey vs groupByKey for Rank Product

The book presents both solutions. `combineByKey()` is more efficient:

| Approach | How it works | Efficiency |
| --- | --- | --- |
| `groupByKey()` | Ships ALL values to one node, then reduces | All data shuffled |
| `combineByKey()` | Combines locally per partition FIRST, then merges | Less shuffle |

```python
# groupByKey — all values go to one node
grouped = rdd.groupByKey()  # RDD[(key, Iterable[value])]
result = grouped.mapValues(lambda vals: reduce(multiply, vals))

# combineByKey — local reduction first (preferred)
combined = rdd.combineByKey(
    lambda v: (v, 1),                        # create local accumulator
    lambda acc, v: (acc[0] * v, acc[1] + 1), # merge value into accumulator
    lambda a, b: (a[0] * b[0], a[1] + b[1]) # merge accumulators across partitions
)
```

## Performance Considerations

| Operation | Complexity | Notes |
| --- | --- | --- |
| Rank product (per study) | O(N log N) | Dominated by sort for rank assignment |
| Rank product (cross-study) | O(K × N) | K studies, N genes — union + combineByKey |
| PageRank (per iteration) | O(V + E) | V vertices, E edges — join + reduce |
| PageRank (total) | O(I × (V + E)) | I iterations, typically 20-30 |

**Tips:**
- `cache()` the links RDD in PageRank — it's reused every iteration
- Use `combineByKey()` over `groupByKey()` for rank product
- `sortByKey(numPartitions=1)` is required for globally meaningful `zipWithIndex()`
- PageRank converges faster on dense graphs

## Additional Resources

- [Rank Product — Wikipedia](https://en.wikipedia.org/wiki/Rank_product)
- [PageRank — Wikipedia](https://en.wikipedia.org/wiki/PageRank)
- [Understanding Google PageRank (Ian Rogers)](https://www.cs.princeton.edu/~chazelle/courses/BIB/pagerank.htm)
- [The PageRank Citation Ranking (Original Paper)](http://ilpubs.stanford.edu:8090/422/)

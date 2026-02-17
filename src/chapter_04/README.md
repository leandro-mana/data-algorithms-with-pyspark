# Chapter 4: Reductions in Spark

This chapter focuses on reduction transformations for (key, value) pair RDDs. Four approaches — `reduceByKey()`, `groupByKey()`, `aggregateByKey()`, and `combineByKey()` — are compared using a warmup problem and a real-world movie ratings problem. The critical concept of **monoids** is introduced as a design principle for writing correct distributed reducers.

## Examples

| Example | Description | Key Concepts |
| --- | --- | --- |
| `reduction_warmup.py` | Sum per key using all 4 reduction approaches side-by-side | reduceByKey, groupByKey, aggregateByKey, combineByKey |
| `movie_avg_rating.py` | Average movie rating per user using the (sum, count) monoid pattern | Monoids, (sum, count) pattern, wrong vs correct mean |

## Running Examples

```bash
# Run reduction warmup (all 4 approaches compared)
make run CHAPTER=chapter_04 EXAMPLE=reduction_warmup

# Run movie average rating with sample data
make run-spark CHAPTER=chapter_04 EXAMPLE=movie_avg_rating

# Run with a custom ratings file (MovieLens format)
make run-spark CHAPTER=chapter_04 EXAMPLE=movie_avg_rating ARGS="/path/to/ratings.csv"
```

## Key Concepts

### Reduction Transformations

All reduction transformations operate on (key, value) pair RDDs. They reduce all values for each unique key into a single result:

```
{ (K, V1), (K, V2), ..., (K, Vn) } → (K, R)
where R = f(V1, V2, ..., Vn)
```

| Transformation | Source → Target | Description |
| --- | --- | --- |
| `reduceByKey()` | `RDD[(K, V)] → RDD[(K, V)]` | Merges values using associative + commutative function. **V must equal V** |
| `groupByKey()` | `RDD[(K, V)] → RDD[(K, [V])]` | Groups all values per key into a list. **Expensive shuffle** |
| `aggregateByKey()` | `RDD[(K, V)] → RDD[(K, C)]` | Uses zero_value + seq_func + comb_func. **C can differ from V** |
| `combineByKey()` | `RDD[(K, V)] → RDD[(K, C)]` | Most general: create_combiner + merge_value + merge_combiners. **C can differ from V** |

### Monoids — Why They Matter

A monoid is an algebraic structure `M = (T, f, Zero)` where:

- `f` is a **binary operation**: `f: (T, T) → T`
- `Zero` is a **neutral element**: `f(Zero, a) = a` and `f(a, Zero) = a`
- `f` is **associative**: `f(f(a, b), c) = f(a, f(b, c))`

**Why this matters for Spark**: Reductions execute per-partition in parallel. If your reducer function is NOT a monoid, it will produce **incorrect results** in a distributed environment.

| Operation | Monoid? | Why |
| --- | --- | --- |
| Addition (`+`) | Yes | `(a + b) + c = a + (b + c)`, zero = 0 |
| Multiplication (`*`) | Yes | `(a * b) * c = a * (b * c)`, zero = 1 |
| String concat (`+`) | Yes | `(a + b) + c = a + (b + c)`, zero = `""` |
| Mean/Average | **No** | `mean(mean(a,b), c) != mean(a, mean(b,c))` |
| Subtraction (`-`) | **No** | `(a - b) - c != a - (b - c)` |
| Median | **No** | `median(median(a,b), c) != median(a, median(b,c))` |

### The (sum, count) Monoid Pattern

The mean/average function is NOT a monoid, but we can make it work by converting to a monoidal data structure:

```python
# WRONG — mean of means != mean of all values
rdd.reduceByKey(lambda x, y: (x + y) / 2)  # INCORRECT!

# RIGHT — use (sum, count) pairs, which ARE monoidal
rdd.mapValues(lambda v: (v, 1)) \
   .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
   .mapValues(lambda sc: sc[0] / sc[1])
```

**Proof**: For three partitions with values `(1,2,3)`, `(4,5)`, `(6)`:

| Approach | Partition results | Final | Correct? |
| --- | --- | --- | --- |
| `mean()` directly | `2.25, 4.5, 6` | `4.6875` | **No** (correct = 3.5) |
| `(sum, count)` | `(6,3), (9,2), (6,1)` | `(21,6) → 3.5` | **Yes** |

### reduceByKey() vs groupByKey()

```python
# These produce the SAME result:
rdd.groupByKey().mapValues(lambda values: sum(values))
rdd.reduceByKey(lambda x, y: x + y)
```

But their shuffle behavior is very different:

| Aspect | reduceByKey() | groupByKey() |
| --- | --- | --- |
| **Pre-shuffle** | Combines locally per partition | No local combining |
| **Shuffle volume** | One value per key per partition | ALL values sent over network |
| **Memory risk** | Low | OOM if many values per key |
| **Post-shuffle** | Result is ready | Requires `mapValues()` step |

**Rule of thumb**: Always prefer `reduceByKey()` over `groupByKey()` for aggregations. Use `groupByKey()` only when you need access to ALL values at once (e.g., median).

### combineByKey() — The Most Powerful Reducer

`combineByKey()` is the most general reduction. It requires three functions:

```python
# V = input value type, C = combined (output) type
rdd.combineByKey(
    lambda v: (v, 1),                       # create_combiner: V → C
    lambda c, v: (c[0] + v, c[1] + 1),      # merge_value: (C, V) → C
    lambda c1, c2: (c1[0] + c2[0], c1[1] + c2[1]),  # merge_combiners: (C, C) → C
)
```

| Function | Purpose | Scope |
| --- | --- | --- |
| `create_combiner` | Initialize C from first V in partition | Per key, per partition |
| `merge_value` | Add a V into existing C | Within a partition |
| `merge_combiners` | Merge two Cs | Across partitions |

### aggregateByKey() — Zero-Value Initialization

Similar to `combineByKey()` but uses an explicit zero value instead of `create_combiner`:

```python
rdd.aggregateByKey(
    (0.0, 0),                              # zero_value: initial C per partition
    lambda c, v: (c[0] + v, c[1] + 1),     # seq_func: (C, V) → C
    lambda c1, c2: (c1[0] + c2[0], c1[1] + c2[1]),  # comb_func: (C, C) → C
)
```

> **Note**: The zero value is applied **per key, per partition**. If key X exists in N partitions, the zero value is applied N times.

## Performance Comparison

| Transformation | Local combining | Shuffle volume | Output type flexibility | Complexity |
| --- | --- | --- | --- | --- |
| `reduceByKey()` | Yes | Low | V must equal V | Simple |
| `groupByKey()` | No | High | Groups to `[V]` | Simple |
| `aggregateByKey()` | Yes | Low | C can differ from V | Medium |
| `combineByKey()` | Yes | Low | C can differ from V | Medium |

For large datasets, `reduceByKey()` and `combineByKey()` significantly outperform `groupByKey()` due to local combining before the shuffle step.

## Additional Resources

- [Spark RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [PySpark RDD API — reduceByKey](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.reduceByKey.html)
- [MovieLens Dataset](https://grouplens.org/datasets/movielens/) (for testing with real data)
- [Monoidify! Monoids as a Design Principle for Efficient MapReduce Algorithms](https://arxiv.org/abs/1304.7544) by Jimmy Lin

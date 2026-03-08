# Chapter 9: Classic Data Design Patterns

This chapter presents the fundamental data design patterns that underpin virtually every big data pipeline. These six patterns are composable building blocks — most real-world Spark jobs are combinations of them. The chapter also introduces the **Inverted Index**, the core data structure behind search engines.

## Examples

| Example | Description | Key Concepts |
| --- | --- | --- |
| `design_patterns.py` | Six classic patterns on employee + movie data | map, filter, combineByKey, join, mapPartitions, monoid correctness |
| `inverted_index.py` | Build a word→document index from text files | wholeTextFiles, flatMap, reduceByKey, groupByKey |

## Running Examples

```bash
# Run all six classic design patterns
make run-spark CHAPTER=chapter_09 EXAMPLE=design_patterns

# Run inverted index
make run-spark CHAPTER=chapter_09 EXAMPLE=inverted_index
```

## Key Concepts

### The Six Classic Patterns

Every Spark job is a composition of these fundamental patterns:

| # | Pattern | Spark API | Use Case |
| --- | --- | --- | --- |
| 1 | Input-Map-Output | `map()`, `flatMap()`, `withColumn()` | Transform, normalize, reformat records |
| 2 | Input-Filter-Output | `filter()`, `where()` | Keep/discard records by predicate |
| 3 | Input-Map-Reduce-Output | `reduceByKey()`, `combineByKey()`, `groupBy().agg()` | Aggregate values by key |
| 4 | Input-Multiple-Maps-Reduce | `join()`, `groupByKey()` | Combine two datasets (reduce-side join) |
| 5 | Input-Map-Combiner-Reduce | `reduceByKey()` with monoid values | Combiner-safe aggregation |
| 6 | Input-MapPartitions-Reduce | `mapPartitions()`, `reduceByKey()` | Partition-level summarization |

### Pattern 1: Input-Map-Output

The simplest pattern — transform each record independently with no aggregation:

```python
# RDD: normalize a gender field with varied representations
def normalize_gender(raw: str) -> str:
    lowered = raw.strip().lower()
    if lowered in {"0", "f", "female"}: return "female"
    if lowered in {"1", "m", "male"}:   return "male"
    return "unknown"

normalized = rdd.map(lambda rec: parse_and_normalize(rec))

# DataFrame equivalent: add computed columns
df.withColumn("total_pay", F.col("weekly_pay") + F.col("overtime") * 20)
```

**flatMap variant**: when one input produces zero or more outputs (e.g., tokenizing text into words, filtering inline):

```python
# flatMap = map + filter in one step
# Return [] to drop, return [x, y] to emit multiple
bigrams = rdd.flatMap(extract_bigrams)

# DataFrame equivalent: explode() for array columns
df.select("name", explode("skills").alias("skill"))
```

### Pattern 2: Input-Filter-Output

Keep records satisfying a Boolean predicate:

```python
# RDD
high_earners = employees.filter(lambda e: e.salary >= 60000)

# DataFrame — filter() and where() are identical
df.filter(F.col("salary") >= 60000)
df.where(F.col("salary") >= 60000)       # same result

# Multiple conditions — wrap each in parentheses
df.filter((F.col("salary") > 400) & (F.col("overtime") > 5))
```

### Pattern 3: Input-Map-Reduce-Output

The classic MapReduce aggregation — map to `(key, value)` pairs, then reduce:

```python
# Average salary by age group using combineByKey
age_salary = employees.map(lambda e: (age_to_group(e.age), e.salary))

avg_by_group = age_salary.combineByKey(
    lambda v: (v, 1),                        # create combiner
    lambda c, v: (c[0] + v, c[1] + 1),       # merge value
    lambda c1, c2: (c1[0] + c2[0], c1[1] + c2[1]),  # merge combiners
).mapValues(lambda sc: sc[0] / sc[1])

# DataFrame equivalent
df.groupBy("age_group").agg(F.avg("salary"))
```

### Pattern 4: Input-Multiple-Maps-Reduce-Output (Reduce-Side Join)

Join two datasets on a common key, then aggregate:

```python
# movies_rdd: (movie_id, movie_name)
# ratings_rdd: (movie_id, rating)

joined = ratings_rdd.join(movies_rdd)
# → (movie_id, (rating, movie_name))

avg_ratings = (
    joined.groupByKey()
    .mapValues(lambda vals: sum(v[0] for v in vals) / len(list(vals)))
)

# DataFrame equivalent — much simpler
ratings_df.join(movies_df, "movie_id").groupBy("movie_id", "movie_name").avg("rating")
```

### Pattern 5: Combiner Correctness — The Monoid Requirement

For `reduceByKey()` to produce correct results across partitions, the reduction function **must be associative and commutative**. The classic pitfall: averaging.

```python
# WRONG — mean is NOT associative
# AVG(AVG(20, 30), AVG(40, 50, 60)) = 38.75  ≠  40.0
rdd.reduceByKey(lambda x, y: (x + y) / 2)

# CORRECT — (sum, count) IS a monoid
rdd.mapValues(lambda v: (v, 1))                          \
   .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))     \
   .mapValues(lambda sc: sc[0] / sc[1])
```

| Operation | Associative? | Combiner-Safe? |
| --- | --- | --- |
| `sum(a, b)` | Yes | Yes |
| `min(a, b)` / `max(a, b)` | Yes | Yes |
| `mean(a, b)` | **No** | **No** — use (sum, count) |
| `(sum, count)` merge | Yes | Yes |

### Pattern 6: Input-MapPartitions-Reduce-Output

Process entire partitions at once instead of individual elements. Ideal when:
- You need **heavyweight initialization** (DB connections, ML models)
- You want to **minimize shuffle** by emitting compact summaries
- You have a **small key space** (e.g., 3 genders across billions of records)

```python
def summarize_partition(partition):
    """Process entire partition, emit compact summary."""
    summary = {}
    for emp in partition:
        prev = summary.get(emp.gender, (0, 0))
        summary[emp.gender] = (prev[0] + emp.salary, prev[1] + 1)
    return list(summary.items())

# Each partition emits at most 3 pairs instead of millions
partition_summaries = rdd.mapPartitions(summarize_partition)
totals = partition_summaries.reduceByKey(
    lambda a, b: (a[0]+b[0], a[1]+b[1])
)
```

### Inverted Index

A mapping from **content → location(s)**. The core data structure behind every search engine.

```python
# Pipeline: wholeTextFiles → flatMap → reduceByKey → groupByKey
docs = sc.wholeTextFiles(docs_path)          # (path, content) pairs
pairs = docs.flatMap(emit_word_doc_pairs)    # ((word, doc), 1)
freqs = pairs.reduceByKey(lambda a, b: a+b)  # ((word, doc), count)
index = freqs.map(restructure).groupByKey()  # (word, [(doc, count), ...])
```

**Example output:**
```
fox     → [doc1.txt:3, doc2.txt:3, doc3.txt:1]
bear    → [doc2.txt:2, doc3.txt:1]
jumped  → [doc1.txt:4, doc2.txt:2, doc3.txt:1]
```

| Pros | Cons |
| --- | --- |
| Fast full-text search (O(1) lookup) | Storage overhead for index |
| Easy to parallelize construction | High maintenance cost for updates |
| Scales to billions of documents | Insert/delete requires reindexing |

## MapReduce vs Spark

The book draws a clear parallel between MapReduce and Spark:

| MapReduce Component | Spark Equivalent |
| --- | --- |
| `map()` | `map()`, `flatMap()`, `filter()` |
| `combine()` (local reducer) | Automatic in `reduceByKey()`, `combineByKey()` |
| `reduce()` | `reduceByKey()`, `groupByKey()`, `aggregateByKey()` |
| Driver | SparkSession / main() |

Spark is a **superset** of MapReduce — same patterns, richer API, in-memory execution.

## Performance Considerations

| Pattern | Tip |
| --- | --- |
| Map-Output | Prefer `flatMap()` over `map()` + `filter()` when doing both |
| Filter-Output | Filter early to reduce data volume for downstream stages |
| Map-Reduce | Use `combineByKey()` / `reduceByKey()` over `groupByKey()` for aggregation |
| Combiner-Reduce | Ensure reducer function is associative + commutative (monoid) |
| MapPartitions | Use for heavyweight init (DB connections) and compact summaries |
| Inverted Index | Use `reduceByKey()` before `groupByKey()` to pre-aggregate counts |
| DataFrame | Prefer DataFrame API for joins — Catalyst optimizer handles execution |

## Additional Resources

- [MapReduce: Simplified Data Processing on Large Clusters](https://research.google/pubs/pub62/) — Dean & Ghemawat
- [Monoidify! Monoids as a Design Principle for Efficient MapReduce Algorithms](https://arxiv.org/abs/1304.7544) — Jimmy Lin
- [PySpark RDD API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.html)
- [PySpark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html)

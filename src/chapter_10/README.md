# Chapter 10: Practical Data Design Patterns

This chapter introduces practical data design patterns used in production big data pipelines. It builds on the classic patterns from Chapter 9 with optimization-focused techniques: **in-mapper combining** to minimize shuffle, **Top-N** and **MinMax** for partition-level summarization, **monoids** for combiner correctness, and **binning** for data organization.

## Examples

| Example | Description | Key Concepts |
| --- | --- | --- |
| `in_mapper_combining.py` | Character frequency: 3 approaches (basic, per-record, per-partition) | flatMap, defaultdict, mapPartitions, local aggregation |
| `top_n_minmax.py` | Top-N with bounded heaps + MinMax via mapPartitions | heapq, mapPartitions, partition-level summarization |
| `binning_and_sorting.py` | Multi-level binning with UDFs + sorting APIs | UDF, partitionBy, sortByKey, sort, orderBy |

## Running Examples

```bash
# In-mapper combining (character frequency)
make run-spark CHAPTER=chapter_10 EXAMPLE=in_mapper_combining

# Top-N and MinMax
make run-spark CHAPTER=chapter_10 EXAMPLE=top_n_minmax

# Binning and sorting
make run-spark CHAPTER=chapter_10 EXAMPLE=binning_and_sorting
```

## Key Concepts

### In-Mapper Combining

Three progressively more efficient approaches to the same problem (character counting):

| Approach | Emits | Shuffle Volume | When to Use |
| --- | --- | --- | --- |
| Basic `flatMap` | `(char, 1)` per character | Huge — one pair per char | Never in production |
| Per-record dict | `(char, freq)` per unique char per record | Moderate | Small to medium datasets |
| Per-partition `mapPartitions` | `(char, freq)` per unique char per partition | Minimal | Large datasets (recommended) |

```python
# Approach 1: Basic — emits N pairs where N = total characters
rdd.flatMap(lambda rec: [(c, 1) for w in rec.split() for c in w])
   .reduceByKey(lambda a, b: a + b)

# Approach 2: Per-record — emits at most |alphabet| pairs per record
def per_record(rec):
    freq = defaultdict(int)
    for w in rec.split():
        for c in w: freq[c] += 1
    return list(freq.items())
rdd.flatMap(per_record).reduceByKey(lambda a, b: a + b)

# Approach 3: Per-partition — emits at most |alphabet| pairs per PARTITION
def per_partition(partition):
    freq = defaultdict(int)
    for rec in partition:
        for w in rec.split():
            for c in w: freq[c] += 1
    return list(freq.items())
rdd.mapPartitions(per_partition).reduceByKey(lambda a, b: a + b)
```

### Top-N Pattern

Find the top-N items efficiently using local-then-global merge:

```
Partition 1  →  local top-5  →  ┐
Partition 2  →  local top-5  →  ├→  global top-5
Partition 3  →  local top-5  →  ┘
```

Each partition emits at most N pairs, so the reducer handles at most `N × num_partitions` candidates — not a bottleneck.

```python
import heapq

def top_n_per_partition(n):
    """Returns a mapPartitions function using a min-heap of size N."""
    def _find_top_n(partition):
        heap = []
        for url, freq in partition:
            if len(heap) < n:
                heapq.heappush(heap, (freq, url))
            elif freq > heap[0][0]:
                heapq.heapreplace(heap, (freq, url))
        return heap
    return _find_top_n

local_tops = rdd.mapPartitions(top_n_per_partition(10))
# Collect and merge into final top-10
```

**Bottom-N**: same approach, but negate frequencies (use max-heap via negation) and pop the largest.

**Built-in alternative**: `rdd.takeOrdered(N, key=lambda x: -x[1])` — simpler but transfers all data to the driver.

### MinMax Pattern

Find global min, max, and count using partition-level summarization:

```python
def minmax_per_partition(partition):
    """Emit at most 1 triple (min, max, count) per partition."""
    local_min = local_max = None
    count = 0
    for record in partition:
        numbers = [int(n) for n in record.split(",")]
        ...  # update local_min, local_max, count
    if local_min is not None:
        return [(local_min, local_max, count)]
    return []  # empty partition

# O(num_partitions) data to merge
partition_results = rdd.mapPartitions(minmax_per_partition).collect()
```

| Solution | Shuffle | Scalability |
| --- | --- | --- |
| Naive (emit per number) | O(N) | Poor — huge shuffle, 3 keys |
| Sort then pick | O(N log N) | Poor — expensive sort |
| mapPartitions | O(partitions) | Excellent — 1 triple per partition |

### Monoids and Combiner Correctness

For `reduceByKey()` to work correctly across partitions, the reduction must be a **monoid** — a set with an associative binary operation and identity element.

| Operation | Monoid? | Identity | Notes |
| --- | --- | --- | --- |
| Addition `a + b` | Yes | 0 | Commutative + associative |
| Multiplication `a * b` | Yes | 1 | Commutative + associative |
| Max `max(a, b)` | Yes | -∞ / 0 | Commutative + associative |
| Min `min(a, b)` | Yes | +∞ | Commutative + associative |
| Union `a ∪ b` | Yes | ∅ | Commutative + associative |
| List concat `a + b` | Yes | [] | Associative (NOT commutative) |
| Mean `(a+b)/2` | **No** | — | Not associative |
| Median | **No** | — | Not associative |
| Subtraction `a - b` | **No** | — | Not associative |
| Division `a / b` | **No** | — | Not associative |

**The fix for mean**: emit `(sum, count)` pairs — these ARE monoidal:

```
Identity: (0, 0)
Merge:    (s1, c1) + (s2, c2) = (s1+s2, c1+c2)
Final:    average = sum / count
```

### Binning

Organize data into **bins** (categories) to enable faster queries. Instead of scanning the entire dataset, search only the relevant bin.

```python
# Level 1: bin by chromosome
extract_chr_udf = F.udf(extract_chromosome, StringType())
df = df.withColumn("CHR_ID", extract_chr_udf("variant_key"))

# Level 2: bin by modulo of start position
compute_mod_udf = F.udf(compute_modulo, IntegerType())
df = df.withColumn("MODULO", compute_mod_udf("variant_key"))

# Physical binning — directory structure for partition pruning
df.write.partitionBy("CHR_ID", "MODULO").parquet(output_path)
# Creates: output/CHR_ID=chr5/MODULO=33/part-00000.parquet
```

Multi-level binning creates a directory hierarchy that Spark prunes at read time — only relevant partitions are loaded from disk.

### Sorting

PySpark sorting APIs:

| API | Scope | Notes |
| --- | --- | --- |
| `rdd.sortByKey()` | RDD | Sort by key, ascending/descending |
| `rdd.sortBy(keyfunc)` | RDD | Sort by custom function |
| `df.sort()` / `df.orderBy()` | DataFrame | Sort by column(s), identical behavior |
| `df.sortWithinPartitions()` | DataFrame | Sort within each partition (no global order) |
| `df.write.sortBy()` | DataFrameWriter | Sort within partitions before writing |

## Performance Considerations

| Pattern | Tip |
| --- | --- |
| In-mapper combining | Use per-partition (not per-record) for large datasets — single dict per partition |
| Top-N | Each partition emits at most N pairs; safe with any number of partitions |
| MinMax | Each partition emits 1 triple; handle empty partitions gracefully |
| Monoids | Ensure reducer is associative + commutative; use (sum, count) for averages |
| Binning | Use prime numbers for modulo bins; combine with `partitionBy()` for physical pruning |
| Sorting | Use `coalesce()` to reduce partitions (no shuffle); `repartition()` for increase (shuffle) |
| UDFs | UDFs are slower than native Spark functions — use built-in functions when possible |

## Additional Resources

- [MapReduce Design Patterns](https://www.oreilly.com/library/view/mapreduce-design-patterns/9781449341954/) — Donald Miner & Adam Shook
- [Monoidify! Monoids as a Design Principle for Efficient MapReduce Algorithms](https://arxiv.org/abs/1304.7544) — Jimmy Lin
- [PySpark UDF Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html)
- [PySpark Sorting API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sort.html)

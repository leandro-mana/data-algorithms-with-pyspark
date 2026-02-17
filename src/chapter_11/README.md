# Chapter 11: Join Design Patterns

This chapter covers the essential join patterns for combining datasets in Spark. It progresses from basic join types (inner, left, right) through optimized patterns: **map-side joins** that eliminate shuffle by broadcasting small tables, and **Bloom filter joins** that use probabilistic filtering to reduce join candidates.

## Examples

| Example | Description | Key Concepts |
| --- | --- | --- |
| `join_types.py` | Inner, left, right joins + custom MapReduce-style join | DataFrame join, RDD union+groupByKey, Cartesian product, relation tags |
| `map_side_join.py` | Broadcast join with RDD and DataFrame approaches | broadcast, collectAsMap, UDF, `F.broadcast()` hint |

## Running Examples

```bash
# Join types (inner, left, right + custom RDD join)
make run-spark CHAPTER=chapter_11 EXAMPLE=join_types

# Map-side join (broadcast pattern)
make run-spark CHAPTER=chapter_11 EXAMPLE=map_side_join
```

## Key Concepts

### Join Types

| Join Type | Returns | NULLs |
| --- | --- | --- |
| **INNER** | Only rows with matching keys in BOTH tables | None |
| **LEFT** | All rows from left table | Right columns NULL when no match |
| **RIGHT** | All rows from right table | Left columns NULL when no match |
| **FULL OUTER** | All rows from both tables | NULLs on both sides where no match |
| **CROSS** | Cartesian product (every combination) | None (produces M × N rows) |

```python
# DataFrame joins — clean, declarative
result = t1.join(t2, "key", "inner")   # or "left", "right", "outer", "cross"

# RDD join — Spark provides built-in join()
joined = t1_rdd.join(t2_rdd)          # inner join on key
joined = t1_rdd.leftOuterJoin(t2_rdd) # left outer join
joined = t1_rdd.fullOuterJoin(t2_rdd) # full outer join
```

### How Joins Work in MapReduce

Under the hood, a distributed join follows this pattern:

```
Map Phase:     Tag each record with its source table
               T1: (key, val) → (key, ("T1", val))
               T2: (key, val) → (key, ("T2", val))

Shuffle:       groupByKey — group all tagged values by key
               (key, [("T1", v1), ("T1", v2), ("T2", w1), ...])

Reduce Phase:  Cartesian product of T1 × T2 values per key
               (key, (v1, w1)), (key, (v1, w2)), (key, (v2, w1)), ...
```

```python
# Custom RDD inner join (without using Spark's join())
tagged_t1 = t1_rdd.map(lambda x: (x[0], ("T1", x[1])))
tagged_t2 = t2_rdd.map(lambda x: (x[0], ("T2", x[1])))

combined = tagged_t1.union(tagged_t2)
grouped = combined.groupByKey()

def cartesian_product(entry):
    key, values = entry
    t1_vals = [v for tag, v in values if tag == "T1"]
    t2_vals = [v for tag, v in values if tag == "T2"]
    return [(key, combo) for combo in itertools.product(t1_vals, t2_vals)]

joined = grouped.flatMap(cartesian_product)
```

### Map-Side Join (Broadcast Join)

Eliminates the shuffle phase entirely by broadcasting the smaller table to all executors:

```
Traditional join:   Fact table ──shuffle──┐
                                          ├── Join
                    Dim table  ──shuffle──┘

Map-side join:      Dim table ──broadcast──→ [copy on every executor]
                    Fact table ──map────────→ lookup from local copy
```

```python
# Step 1: Collect dimension table into a dict
dept_dict = dept_rdd.collectAsMap()  # {dept_id: (name, location)}

# Step 2: Broadcast to all executors
dept_broadcast = sc.broadcast(dept_dict)

# Step 3: Map over fact table — no shuffle needed
def lookup(record):
    dept_id = record[0]
    dept_info = dept_broadcast.value.get(dept_id)
    return (record, dept_info)

joined = fact_rdd.map(lookup)
```

**DataFrame equivalent** — Spark's Catalyst optimizer often auto-detects small tables, but you can hint:

```python
from pyspark.sql.functions import broadcast
result = fact_df.join(broadcast(dim_df), "dept_id", "inner")
```

### Bloom Filter Join (Reference)

For very large datasets where even map-side join is impractical (dimension table too large for memory), Bloom filters provide probabilistic filtering:

```
Step 1: Build a Bloom filter from the smaller table's keys
Step 2: Broadcast the Bloom filter (compact — just a bit array)
Step 3: Filter the larger table: keep only rows whose key is "possibly in" the filter
Step 4: Perform the actual join on the filtered (much smaller) dataset
```

A Bloom filter is a probabilistic data structure that tells you:
- **"Definitely not in set"** — guaranteed correct
- **"Possibly in set"** — may have false positives

| Property | Value |
| --- | --- |
| False negatives | 0% (never misses a match) |
| False positives | Configurable (typically 1%) |
| Memory usage | ~10 bits per element (much less than a hash table) |
| Optimal hash functions | k = (m/n) × ln(2) |

This pattern is useful when the dimension table is too large to broadcast as a hash table, but the Bloom filter (a compact bit array) fits easily.

## Join Strategy Decision Tree

```
Is one table small enough to fit in memory?
├── YES → Map-Side Join (broadcast)
│         Fastest: no shuffle, O(1) lookups
│
└── NO → Is the join key high-cardinality?
    ├── YES → Consider Bloom Filter pre-filter
    │         Reduces candidates before shuffle
    │
    └── NO → Standard Join (shuffle)
              Let Spark's Catalyst optimizer choose the strategy
```

## Performance Considerations

| Pattern | Shuffle | Memory | Best For |
| --- | --- | --- | --- |
| Standard join | Full shuffle of both tables | Moderate | Two large tables |
| Map-side join | None (broadcast) | Dimension table in memory | Large fact + small dimension |
| Bloom filter | Reduced shuffle (pre-filtered) | Bloom filter only (~10 bits/key) | Two large tables, one smaller |
| `broadcast()` hint | None | Auto-managed by Spark | DataFrame API convenience |
| `sortMergeJoin` | Shuffle + sort | Moderate | Two large sorted tables |

| Tip | Details |
| --- | --- |
| Broadcast threshold | Spark auto-broadcasts tables < 10MB (`spark.sql.autoBroadcastJoinThreshold`) |
| Skewed keys | Keys with many values cause data skew — consider salting |
| Join column types | Ensure join columns have the same type (avoid implicit casts) |
| Pre-filter | Filter both tables before joining to reduce data volume |
| Partition alignment | If both tables are partitioned by the join key, shuffle is avoided |

## Additional Resources

- [Spark SQL Join Strategies](https://spark.apache.org/docs/latest/sql-performance-tuning.html#join-strategy-hints-for-sql-queries)
- [Broadcast Variables](https://spark.apache.org/docs/latest/rdd-programming-guide.html#broadcast-variables)
- [Bloom Filters — Wikipedia](https://en.wikipedia.org/wiki/Bloom_filter)
- [PySpark Join API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)

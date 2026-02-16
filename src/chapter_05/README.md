# Chapter 5: Partitioning Data

This chapter covers how Spark partitions data for parallel processing and how to physically partition DataFrames to disk for optimized downstream queries. Two levels of partitioning are explored: **in-memory RDD/DataFrame partitioning** (for parallelism) and **physical disk partitioning** by column values (for query tools like Athena, BigQuery, and Hive).

## Examples

| Example | Description | Key Concepts |
| --- | --- | --- |
| `partition_basics.py` | RDD partition management — inspect, repartition, coalesce, mapPartitions | getNumPartitions, glom, repartition, coalesce |
| `physical_partitioning.py` | Partition DataFrames to disk by year/month in CSV and Parquet formats | partitionBy, partition pruning, Parquet vs CSV |

## Running Examples

```bash
# Run partition basics (RDD partition management)
make run CHAPTER=chapter_05 EXAMPLE=partition_basics

# Run physical partitioning with sample transaction data
make run-spark CHAPTER=chapter_05 EXAMPLE=physical_partitioning

# Run with a custom transactions file
make run-spark CHAPTER=chapter_05 EXAMPLE=physical_partitioning \
    ARGS="/path/to/transactions.csv"
```

## Key Concepts

### What Are Partitions?

Spark splits data into smaller chunks called **partitions** that are processed in parallel across executors. Each partition is assigned to a single worker — no synchronization is required between partitions.

```
Input: 10 billion records
       ↓
Split into 10,000 partitions (~1M records each)
       ↓
250 executors process partitions in parallel
       ↓
Each executor handles ~40 partitions sequentially
```

| Factor | Effect on Partitions |
| --- | --- |
| Too few partitions | Workers sit idle, underutilization |
| Too many partitions | Scheduling overhead per task |
| Right number | All workers busy, maximum throughput |

**Rule of thumb**: Aim for 2-4 partitions per CPU core in your cluster.

### Inspecting Partitions

```python
rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], 3)

# Check partition count
rdd.getNumPartitions()  # 3

# View elements per partition using glom()
rdd.glom().collect()
# [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]
```

`glom()` coalesces all elements within each partition into a list — useful for debugging but **never use in production** (materializes entire partitions).

### repartition() vs coalesce()

Two ways to change partition count, with very different performance characteristics:

| API | Direction | Shuffle | Use Case |
| --- | --- | --- | --- |
| `repartition(n)` | Increase or decrease | **Yes** (full shuffle) | Need more parallelism or even distribution |
| `coalesce(n)` | Decrease only | **No** (merges adjacent) | Reducing partitions before writing output |

```python
rdd = sc.parallelize(range(100), 10)

# Increase partitions — triggers shuffle
rdd.repartition(20).getNumPartitions()  # 20

# Decrease partitions — no shuffle, just merges
rdd.coalesce(3).getNumPartitions()  # 3
```

**Key insight**: `coalesce()` avoids a shuffle by combining adjacent partitions, but this can lead to **uneven partition sizes**. Use `repartition()` when you need balanced partitions.

### Physical Partitioning to Disk

Physical partitioning writes data into a directory structure based on column values. This is the foundation for partition pruning in query tools.

```python
# Add partition columns from date field
df = df.withColumn("year", ...).withColumn("month", ...)

# Write partitioned Parquet (one directory per year/month combination)
df.repartition("year", "month") \
  .write.partitionBy("year", "month") \
  .parquet(output_path)
```

The resulting directory structure:

```
output/
├── year=2023/
│   ├── month=1/
│   │   └── part-00000.snappy.parquet
│   ├── month=3/
│   │   └── part-00000.snappy.parquet
│   └── month=6/
│       └── part-00000.snappy.parquet
├── year=2024/
│   ├── month=1/
│   │   └── part-00000.snappy.parquet
│   └── ...
└── _SUCCESS
```

### Partition Pruning

When you filter on partition columns, the query engine **skips entire directories** instead of scanning the full dataset:

```python
# Only reads year=2024/month=9/ directory — skips everything else
df = spark.read.parquet(output_path)
result = df.filter((col("year") == 2024) & (col("month") == 9))
```

This is the same mechanism used by:
- **Amazon Athena** — `WHERE year = 2024 AND month = 9`
- **Google BigQuery** — partition filters
- **Hive** — `WHERE` on partitioned columns

### Text vs Parquet Output

| Format | Storage | Read Speed | Metadata | Best For |
| --- | --- | --- | --- | --- |
| CSV/Text | Row-based | Slower | None | Human inspection, simple pipelines |
| Parquet | Columnar | Faster | Schema + stats | Production queries, analytics |

```python
# Text (CSV) format — row-based, human-readable
df.write.partitionBy("year", "month").csv(output_path)

# Parquet format — columnar, compressed, metadata-rich
df.write.partitionBy("year", "month").parquet(output_path)
```

**Production rule**: Always use Parquet (or ORC) for analytical workloads. CSV is only for debugging or interchange with non-Spark systems.

### Single File Per Partition

By default, Spark may write multiple part files per partition. To produce exactly one file per partition, use `repartition()` on the partition columns before writing:

```python
# Multiple files per partition (default)
df.write.partitionBy("year", "month").parquet(output_path)

# Single file per partition (repartition first)
df.repartition("year", "month") \
  .write.partitionBy("year", "month") \
  .parquet(output_path)
```

## Choosing Partition Keys

Choosing the right partition columns depends on **how the data will be queried**:

| Query Pattern | Good Partition Keys | Why |
| --- | --- | --- |
| `WHERE year = 2024` | `year` | Prunes by year |
| `WHERE year = 2024 AND month = 6` | `year`, `month` | Prunes by both |
| `WHERE customer_id = 'C001'` | `customer_id` | Prunes by customer |
| `WHERE chromosome = 'chr7'` | `chromosome` | Prunes by chromosome |

**Guidelines:**
- Partition on columns used in `WHERE` clauses
- Avoid high-cardinality columns (millions of unique values = millions of directories)
- Order partition keys from coarsest to finest granularity (year → month → day)
- Think of partitioning as a simple **indexing mechanism** for big data

## Performance Considerations

| Aspect | Default Partitioning | Physical Partitioning |
| --- | --- | --- |
| **Purpose** | Parallelism during computation | Query optimization on disk |
| **Granularity** | Hash-based, automatic | Column-value-based, explicit |
| **Scope** | In-memory RDD/DataFrame | On-disk directory structure |
| **Managed by** | Spark automatically | Developer chooses columns |
| **Benefits** | Worker utilization | Partition pruning, reduced scan |

| Operation | Shuffle | When to Use |
| --- | --- | --- |
| `repartition(n)` | Yes | Need more parallelism or balanced distribution |
| `coalesce(n)` | No | Reducing partitions before write (fewer output files) |
| `repartition(*cols)` | Yes | Single file per partition in output |
| `write.partitionBy(*cols)` | Depends | Physical disk partitioning for downstream queries |

## Additional Resources

- [Spark RDD Programming Guide — Partitions](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [PySpark DataFrameWriter.partitionBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.partitionBy.html)
- [Amazon Athena — Partitioning Data](https://docs.aws.amazon.com/athena/latest/ug/partitions.html)
- [Apache Parquet Format](https://parquet.apache.org/)

# Chapter 1: Introduction to Spark and PySpark

This chapter introduces PySpark as the main component of the Spark ecosystem, covering Spark's architecture, data abstractions (RDDs and DataFrames), core transformations and actions, and a complete ETL pipeline example.

## Examples

| Example | Description | Key Concepts |
| --- | --- | --- |
| `rdd_map_transformation.py` | Demonstrates `map()` and `mapValues()` as 1-to-1 transformations | map(), mapValues(), NamedTuple |
| `rdd_transformations_overview.py` | Overview of core RDD transformations | filter, flatMap, groupByKey, reduceByKey, sortBy, cartesian |
| `average_by_key_reducebykey.py` | Calculates averages per key using the (sum, count) pattern | reduceByKey(), mapValues(), aggregation |
| `dataframe_basics.py` | DataFrame creation, filtering, selecting, and aggregations | DataFrame API, filter, groupBy, withColumn |
| `etl_census_dataframe.py` | Complete ETL pipeline with US Census data | Extract (JSON), Transform (filter + column), Load (CSV) |

## Running Examples

```bash
# Run RDD map transformation
make run CHAPTER=chapter_01 EXAMPLE=rdd_map_transformation

# Run RDD transformations overview
make run CHAPTER=chapter_01 EXAMPLE=rdd_transformations_overview

# Run average by key
make run CHAPTER=chapter_01 EXAMPLE=average_by_key_reducebykey

# Run DataFrame basics
make run-spark CHAPTER=chapter_01 EXAMPLE=dataframe_basics

# Run ETL pipeline
make run-spark CHAPTER=chapter_01 EXAMPLE=etl_census_dataframe
```

## Key Concepts

### Spark Architecture

Spark uses a master/worker architecture to distribute data processing across a cluster:

| Component | Role |
| --- | --- |
| **SparkSession** | Entry point for DataFrame and Dataset APIs. Created via the builder pattern |
| **SparkContext** | Main entry point for RDD operations. Available as `spark.sparkContext` |
| **Driver** | Coordinates all processes, runs `main()`, creates SparkContext |
| **Worker** | Executes tasks assigned by the cluster manager |
| **Cluster Manager** | Allocates resources (Standalone, YARN, Kubernetes, Mesos) |

```python
from pyspark.sql import SparkSession

# Create a SparkSession (entry point for everything)
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("my-app") \
    .getOrCreate()

# Access SparkContext for RDD operations
sc = spark.sparkContext
```

When you start the PySpark shell, `spark` (SparkSession) and `sc` (SparkContext) are automatically available.

### Spark Data Abstractions

PySpark supports two main data abstractions:

| Abstraction | API Level | Structure | Use Case |
| --- | --- | --- | --- |
| **RDD[T]** | Low-level | Unstructured collection of elements of type T | Fine-grained control, custom transformations |
| **DataFrame** | High-level | Table with named columns (like SQL/pandas) | Structured data, SQL queries, optimized execution |

> **Note**: The `Dataset` abstraction exists in Java/Scala but is **not available** in PySpark.

### Transformations vs Actions

RDDs support two types of operations:

| Type | Description | Evaluation | Examples |
| --- | --- | --- | --- |
| **Transformation** | Creates a new RDD from an existing one | Lazy (deferred until action) | `map()`, `flatMap()`, `filter()`, `reduceByKey()` |
| **Action** | Produces a non-RDD result | Immediate (triggers computation) | `collect()`, `count()`, `take()`, `saveAsTextFile()` |

Transformations are **lazily evaluated** — they build a lineage graph but don't execute until an action triggers computation. If an RDD fails during a transformation, the lineage rebuilds it automatically.

```python
# Transformation chain (nothing executes yet)
result = rdd.filter(lambda x: x[1] > 0) \
            .mapValues(lambda v: (v, 1)) \
            .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))

# Action triggers execution of the entire chain
result.collect()
```

> **Warning**: Avoid `collect()` on large RDDs — it copies the entire dataset to the driver's memory. Use `take(N)` or `takeSample()` instead.

### reduceByKey() vs groupByKey()

Both aggregate values by key, but with very different performance characteristics:

```python
# These produce the same results:
rdd.groupByKey().mapValues(lambda values: sum(values))
rdd.reduceByKey(lambda x, y: x + y)
```

| Aspect | reduceByKey() | groupByKey() |
| --- | --- | --- |
| **Local aggregation** | Combines per partition first | Sends all data over network |
| **Shuffle volume** | Only partial results | Entire dataset |
| **Memory risk** | Low | OOM if many values per key |
| **Scalability** | Preferred for large datasets | Use sparingly |

**Rule of thumb**: If you're grouping to perform an aggregation (sum, count, average), always prefer `reduceByKey()` or `combineByKey()`.

### DataFrames

DataFrames provide a higher-level abstraction with named columns, SQL-like operations, and automatic optimization through the Catalyst query optimizer:

```python
# Create DataFrame from data
dept_emps = [("Sales", "Barb", 40), ("Sales", "Dan", 20),
             ("IT", "Alex", 22), ("IT", "Jane", 24)]
df = spark.createDataFrame(dept_emps, ["dept", "name", "hours"])

# SQL-like operations
from pyspark.sql.functions import avg, sum
df.groupBy("dept") \
  .agg(avg("hours").alias("average"), sum("hours").alias("total")) \
  .show()
```

DataFrames can be created from JSON, CSV, Parquet files, databases, or existing RDDs.

### ETL Pattern

ETL (Extract, Transform, Load) is the standard pattern for data pipelines:

| Step | Purpose | Example |
| --- | --- | --- |
| **Extract** | Read data from source | `spark.read.json(path)` |
| **Transform** | Clean, filter, compute | `df.filter(col("age") > 54).withColumn(...)` |
| **Load** | Write to destination | `df.write.csv(path)` or `df.write.jdbc(...)` |

```python
# Extract
census_df = spark.read.json("census_2010.json")

# Transform — filter seniors and add total column
seniors = census_df.filter(col("age") > 54)
seniors = seniors.withColumn("total", col("males") + col("females"))

# Load — write to output
seniors.write.mode("overwrite").csv("output/seniors", header=True)
```

See `etl_census_dataframe.py` for the complete working example.

## Performance Considerations

| Topic | Recommendation |
| --- | --- |
| `reduceByKey()` vs `groupByKey()` | Prefer `reduceByKey()` — combines locally before shuffle |
| `collect()` on large RDDs | Avoid — use `take(N)` or `takeSample()` instead |
| Data partitioning | Spark auto-partitions; override only when needed |
| `combineByKey()` | Use when reducer output type differs from input type |

## Additional Resources

- [PySpark API Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Spark RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

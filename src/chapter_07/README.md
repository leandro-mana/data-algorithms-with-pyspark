# Chapter 7: Interacting with External Data Sources

This chapter covers how Spark reads data from and writes data to external storage systems. Spark's DataSource API provides a pluggable, consistent interface — the same builder pattern works across CSV, JSON, Parquet, Avro, JDBC databases, Amazon S3, HDFS, and more. Our examples focus on the formats you'll use most: **CSV**, **JSON**, and **Parquet**.

## Examples

| Example | Description | Key Concepts |
| --- | --- | --- |
| `csv_json_operations.py` | Read/write CSV and JSON with schema handling | Headers, inferSchema, explicit StructType, JSON arrays, SQL views |
| `parquet_operations.py` | Read/write Parquet with analytics optimizations | Column pruning, predicate pushdown, partitioning, SQL on Parquet |

## Running Examples

```bash
# Run CSV and JSON operations
make run-spark CHAPTER=chapter_07 EXAMPLE=csv_json_operations

# Run Parquet operations
make run-spark CHAPTER=chapter_07 EXAMPLE=parquet_operations
```

## Key Concepts

### DataFrameReader / DataFrameWriter

Spark provides a consistent builder-pattern API for all data sources:

```python
# Reading — spark.read returns a DataFrameReader
df = (
    spark.read
    .format("csv")              # csv, json, parquet, avro, jdbc, ...
    .option("header", "true")   # format-specific options
    .option("inferSchema", "true")
    .schema(my_schema)          # optional explicit schema
    .load(path)                 # file path or URL
)

# Writing — df.write returns a DataFrameWriter
(
    df.write
    .format("parquet")          # target format
    .mode("overwrite")          # append, overwrite, ignore, error
    .option("compression", "snappy")
    .partitionBy("year")        # optional disk partitioning
    .save(path)                 # output path
)
```

| Write Mode | Behavior |
| --- | --- |
| `append` | Add rows to existing data |
| `overwrite` | Replace existing data |
| `ignore` | Silently skip if data exists |
| `error` | Throw exception if data exists (default) |

### CSV — Text-Based Tabular Data

```python
# With header and automatic type inference
df = spark.read.csv(path, header=True, inferSchema=True)

# Without header — columns named _c0, _c1, _c2, ...
df = spark.read.csv(path, header=False)

# With explicit schema (no inference overhead)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
])
df = spark.read.csv(path, header=False, schema=schema)

# Write with custom separator
df.write.csv(output_path, header=True, sep="|", mode="overwrite")
```

**When to use CSV:** Human-readable, universal interchange format. Good for small datasets and debugging. Not ideal for analytics (no schema, no column pruning, no compression by default).

### JSON — Nested and Semi-Structured Data

```python
# Read line-delimited JSON (one object per line)
df = spark.read.json(path)

# Schema is inferred automatically, including nested types
# {"name": "Alice", "skills": ["Python", "Spark"]}
# → name: string, skills: array<string>

# Flatten arrays with explode()
from pyspark.sql.functions import explode
flat = df.select("name", explode("skills").alias("skill"))

# Write JSON
df.write.json(output_path, mode="overwrite")
```

**When to use JSON:** Semi-structured data with nested fields, arrays, or varying schemas. Good for APIs and document stores. Larger on disk than Parquet.

### Parquet — Columnar Analytics Format

```python
# Write Parquet (schema embedded, snappy compression by default)
df.write.parquet(output_path)

# Read Parquet (schema auto-preserved)
df = spark.read.parquet(path)

# Column pruning — only reads requested columns from disk
df.select("name", "salary")  # skips all other columns

# Predicate pushdown — filter pushed to storage layer
df.filter(col("salary") > 90000)  # rows skipped at read time

# Partitioned Parquet — directory-based partition pruning
df.write.partitionBy("department").parquet(output_path)
# Creates: output_path/department=Engineering/part-00000.parquet
#          output_path/department=Finance/part-00000.parquet
```

**When to use Parquet:** Default choice for analytics workloads. Self-describing schema, columnar layout (fast aggregations), built-in compression, and query optimizations (predicate pushdown, column pruning).

### Schema Inference vs Explicit Schema

| Approach | Pros | Cons |
| --- | --- | --- |
| `inferSchema=True` | Zero setup, good for exploration | Scans data twice, may guess wrong types |
| Explicit `StructType` | Exact types, no overhead, production-safe | More code to write |
| Parquet (embedded) | Schema travels with the data | N/A — best of both worlds |

### JDBC Databases (Reference)

The book covers reading/writing to JDBC databases (MySQL, SQL Server, PostgreSQL). The pattern is consistent with other sources:

```python
# Read from a database table
df = (
    spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://host/dbname")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("dbtable", "table_name")
    .option("user", "username")
    .option("password", "password")
    .load()
)

# Write to a database table
(
    df.write
    .format("jdbc")
    .option("url", "jdbc:mysql://host/dbname")
    .option("dbtable", "target_table")
    .option("user", "username")
    .option("password", "password")
    .mode("overwrite")  # or "append"
    .save()
)
```

**Note:** Requires the JDBC driver JAR on the classpath (e.g., `--jars mysql-connector.jar`).

### Other Data Sources (Reference)

The book also covers these sources — all follow the same `spark.read.format(...)` pattern:

| Source | Format String | Notes |
| --- | --- | --- |
| Amazon S3 | `csv`, `parquet`, etc. | Path uses `s3a://bucket/key` URI |
| HDFS | `csv`, `parquet`, etc. | Path uses `hdfs://host:port/path` URI |
| Avro | `avro` | Requires `spark-avro` package |
| SequenceFile | N/A | Use `sc.sequenceFile(path)` (RDD API) |
| Images | N/A | Use `spark.read.format("image").load(dir)` |

## Format Comparison

| Feature | CSV | JSON | Parquet | Avro |
| --- | --- | --- | --- | --- |
| **Layout** | Row | Row | Columnar | Row |
| **Schema** | External | Inferred | Embedded | Embedded |
| **Compression** | Optional | Optional | Built-in (snappy) | Built-in |
| **Column pruning** | No | No | Yes | No |
| **Predicate pushdown** | No | No | Yes | Limited |
| **Human-readable** | Yes | Yes | No | No |
| **Nested data** | No | Yes | Yes | Yes |
| **Best for** | Interchange | APIs, documents | Analytics | Streaming, Kafka |

## Performance Considerations

| Operation | Tip |
| --- | --- |
| Schema inference | Use explicit schemas in production — `inferSchema` scans data twice |
| CSV reads | Specify `header` and `schema` explicitly for large files |
| Parquet writes | Use `partitionBy()` on high-cardinality filter columns |
| Column pruning | Select only needed columns — Parquet skips unread columns on disk |
| Predicate pushdown | Filter early — Parquet pushes WHERE to the storage layer |
| Write modes | Use `overwrite` for idempotent pipelines, `append` for streaming |
| Single file output | Use `coalesce(1)` before writing (only for small datasets) |
| JDBC reads | Use `numPartitions`, `lowerBound`, `upperBound` for parallel reads |

## Additional Resources

- [PySpark DataFrameReader API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html)
- [PySpark DataFrameWriter API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html)
- [Spark SQL Data Sources Guide](https://spark.apache.org/docs/latest/sql-data-sources.html)
- [Parquet Format Specification](https://parquet.apache.org/documentation/latest/)
- [Apache Avro Specification](https://avro.apache.org/docs/current/specification/)

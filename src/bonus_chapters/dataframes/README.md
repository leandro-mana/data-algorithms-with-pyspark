# Bonus Chapter: DataFrame Window Functions

Window functions are one of the most powerful and frequently misunderstood features of the DataFrame API. They let you compute values that reference neighbouring rows — rankings, running totals, moving averages, period-over-period changes — without collapsing the dataset into fewer rows the way `groupBy` does.

## Examples

| Example | Description | Key Concepts |
| --- | --- | --- |
| `01_window_ranking.py` | Rank employees within department by sales — row_number vs rank vs dense_rank | Window spec, partitionBy, orderBy, row_number, rank, dense_rank, percent_rank, ntile |
| `02_window_analytics.py` | Time-series analytics: lag/lead, running totals, moving averages, sales share | lag, lead, rowsBetween, unboundedPreceding, cumulative sum, trailing moving average |

## Running Examples

```bash
# Ranking functions
make run-spark CHAPTER=bonus_chapters/dataframes EXAMPLE=01_window_ranking

# Analytic functions
make run-spark CHAPTER=bonus_chapters/dataframes EXAMPLE=02_window_analytics

# Custom sales file
make run-spark CHAPTER=bonus_chapters/dataframes EXAMPLE=01_window_ranking \
    ARGS="src/bonus_chapters/dataframes/data/sales.csv"
```

## Key Concepts

### Window vs groupBy

The fundamental difference:

| Operation | Rows in → Rows out | Use case |
| --- | --- | --- |
| `groupBy().agg()` | N rows → 1 per group | Total per group |
| `Window` function | N rows → N rows | Per-row value relative to its group |

```python
# groupBy — collapses to 1 row per department
df.groupBy("department").agg(F.sum("sales").alias("total"))

# Window — keeps all rows, adds total as a new column
dept_window = Window.partitionBy("department")
df.withColumn("dept_total", F.sum("sales").over(dept_window))
```

### Window Specification

A window is defined by three optional clauses:

```python
window = (
    Window
    .partitionBy("department")      # like GROUP BY — defines the peer group
    .orderBy(F.col("sales").desc()) # ordering within the partition
    .rowsBetween(-1, 0)             # frame: which rows to include
)
```

| Clause | Purpose | When to include |
| --- | --- | --- |
| `partitionBy` | Divide into groups | Almost always |
| `orderBy` | Order within group | Required for ranking and analytics |
| `rowsBetween` / `rangeBetween` | Sliding frame | Running totals, moving averages |

### Ranking Functions

All ranking functions require `orderBy` in the window spec.

```python
dept_window = Window.partitionBy("department").orderBy(F.col("sales").desc())

df.select(
    F.row_number().over(dept_window),  # unique: 1, 2, 3, 4
    F.rank().over(dept_window),        # ties share rank, gaps after: 1, 1, 3
    F.dense_rank().over(dept_window),  # ties share rank, no gaps: 1, 1, 2
    F.percent_rank().over(dept_window),# 0.0 to 1.0 relative position
    F.ntile(4).over(dept_window),      # quartile bucket: 1, 2, 3, or 4
)
```

| Function | Tie behaviour | Gap after tie? | Example (A=100, B=100, C=80) |
| --- | --- | --- | --- |
| `row_number` | Arbitrary tiebreak | — | 1, 2, 3 |
| `rank` | Tied rows share rank | Yes | 1, 1, 3 |
| `dense_rank` | Tied rows share rank | No | 1, 1, 2 |

**Top-N per group** — filter after ranking:

```python
ranked = df.withColumn("rank", F.rank().over(dept_window))
top2 = ranked.filter(F.col("rank") <= 2)
```

### Analytic Functions — lag and lead

Look at the value from a different row in the same partition:

```python
time_window = Window.partitionBy("employee").orderBy("month")

df.select(
    F.lag("sales", 1).over(time_window).alias("prev_month"),   # 1 row back
    F.lead("sales", 1).over(time_window).alias("next_month"),  # 1 row ahead
)
```

Rows at partition boundaries return `NULL` (no previous / next row). Common pattern — month-over-month change:

```python
df.withColumn("mom_change", F.col("sales") - F.lag("sales", 1).over(time_window))
```

### Window Frames — rowsBetween

Controls which rows are included in each function's calculation:

```python
# Running total: all rows from start of partition to current row
cumulative = Window.partitionBy("employee").orderBy("month") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# 3-month trailing moving average: current row and 2 rows before
trailing_3 = Window.partitionBy("employee").orderBy("month") \
    .rowsBetween(-2, Window.currentRow)

# Suffix sum: current row to end of partition
suffix = Window.partitionBy("employee").orderBy("month") \
    .rowsBetween(Window.currentRow, Window.unboundedFollowing)
```

| Frame | Meaning |
| --- | --- |
| `(unboundedPreceding, currentRow)` | Cumulative from start |
| `(-N, currentRow)` | N+1 row trailing window |
| `(currentRow, unboundedFollowing)` | Suffix from current row |
| `(unboundedPreceding, unboundedFollowing)` | Entire partition |

### Sales Share Pattern

Each row's percentage contribution to its partition total — useful for contribution analysis:

```python
dept_window = Window.partitionBy("department", "month")

df.withColumn("dept_total", F.sum("sales").over(dept_window)) \
  .withColumn("share_pct", F.col("sales") / F.col("dept_total") * 100)
```

## Performance Considerations

| Consideration | Detail |
| --- | --- |
| Shuffle | `partitionBy` triggers a shuffle — rows with the same partition key must land on the same executor |
| Large partitions | If one partition key has millions of rows, that executor becomes a bottleneck |
| `orderBy` without `partitionBy` | Creates a single global partition — all data to one node, use only for small DataFrames |
| Frame size | Unbounded frames scan the entire partition per row; bounded frames (`-N, 0`) are more efficient |
| `row_number` uniqueness | Use when you need a guaranteed unique rank, e.g. deduplication |

## Additional Resources

- [PySpark Window Functions API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html)
- [PySpark SQL Functions — Window](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Spark SQL Window Functions Guide](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)

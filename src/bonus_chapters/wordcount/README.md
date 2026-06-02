# Bonus Chapter: Word Count

Word Count is the "hello world" of distributed computing — simple enough to understand immediately, but rich enough to illustrate the core MapReduce pattern and compare multiple Spark APIs side by side.

## Examples

| Example | Description | Key Concepts |
| --- | --- | --- |
| `01_wordcount_rdd.py` | Three RDD approaches compared: reduceByKey, groupByKey, combineByKey + filter variant | flatMap tokenise, reduceByKey vs groupByKey efficiency, combineByKey |
| `02_wordcount_dataframe.py` | DataFrame approach: regexp_replace → split → explode → groupBy + SQL variant | regexp_replace, split, explode, groupBy, createOrReplaceTempView |

## Running Examples

```bash
# RDD word count (Top-10)
make run-spark CHAPTER=bonus_chapters/wordcount EXAMPLE=01_wordcount_rdd

# DataFrame word count
make run-spark CHAPTER=bonus_chapters/wordcount EXAMPLE=02_wordcount_dataframe

# Custom input and Top-N
make run-spark CHAPTER=bonus_chapters/wordcount EXAMPLE=01_wordcount_rdd \
    ARGS="src/bonus_chapters/wordcount/data/sample_text.txt 5"
```

## Key Concepts

### The MapReduce Pattern

Word count is the canonical MapReduce pipeline:

```
Input lines  →  flatMap (tokenise)  →  (word, 1) pairs  →  reduceByKey  →  (word, count)
```

```python
lines.flatMap(lambda line: line.split()) \
     .map(lambda word: (word, 1)) \
     .reduceByKey(lambda a, b: a + b)
```

### Three RDD Approaches Compared

| Approach | Shuffle volume | Local combining | Notes |
| --- | --- | --- | --- |
| `reduceByKey` | Low — partial sums only | Yes, per partition | Default choice |
| `groupByKey` | High — all (word,1) pairs | No | Avoid for aggregations |
| `combineByKey` | Low | Yes, per partition | Most general; same result here |

```python
# reduceByKey — simplest and efficient
words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)

# groupByKey — all values shuffled, then summed
words.map(lambda w: (w, 1)).groupByKey().mapValues(sum)

# combineByKey — explicit create/merge/combine phases
words.combineByKey(
    lambda v: v,                 # createCombiner
    lambda acc, v: acc + v,      # mergeValue
    lambda a, b: a + b,          # mergeCombiners
)
```

See Chapter 4 for a deep dive on when each approach is appropriate.

### DataFrame Tokenisation

The DataFrame pipeline replaces flatMap with a combination of built-in functions:

```python
words_df = lines_df.select(
    F.explode(
        F.split(F.regexp_replace(F.col("value"), r"[.,!?;:\"'()]", ""), r"\s+")
    ).alias("word")
)
```

| Step | Function | Purpose |
| --- | --- | --- |
| `regexp_replace` | Remove punctuation | `"hello," → "hello"` |
| `split` | Split on whitespace | `"hello world" → ["hello", "world"]` |
| `explode` | One word per row | `["hello", "world"] → 2 rows` |

### SQL Variant

Once words are in a DataFrame, standard SQL is available:

```python
words_df.createOrReplaceTempView("words")
spark.sql("SELECT word, COUNT(*) AS frequency FROM words GROUP BY word ORDER BY frequency DESC")
```

The SQL and DataFrame APIs produce the same execution plan — both go through Catalyst.

## Performance Considerations

| Approach | Notes |
| --- | --- |
| `reduceByKey` vs `groupByKey` | Always prefer `reduceByKey` for aggregations — see Chapter 4 |
| DataFrame vs RDD | DataFrame benefits from Catalyst optimisation and code generation |
| `explode` + `groupBy` | Equivalent to `flatMap` + `reduceByKey` but Catalyst-optimised |
| Punctuation removal | Do it before splitting, not after — avoids empty-string words |

## Additional Resources

- [Spark RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [PySpark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [MapReduce: Simplified Data Processing on Large Clusters](https://research.google/pubs/pub62/) — Dean & Ghemawat

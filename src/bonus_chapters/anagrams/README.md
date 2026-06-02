# Bonus Chapter: Anagrams

An anagram is a word formed by rearranging the letters of another word. "listen" and "silent" are anagrams — they contain the same letters (`e, i, l, n, s, t`). This chapter demonstrates how a simple canonical key (sorting the letters) turns the grouping problem into a straightforward MapReduce job.

## Examples

| Example | Description | Key Concepts |
| --- | --- | --- |
| `01_anagrams_rdd.py` | Group anagrams using RDD — groupByKey vs reduceByKey approaches | sorted letters as key, groupByKey, reduceByKey with set union |
| `02_anagrams_dataframe.py` | Group anagrams using DataFrame — UDF key + collect_set aggregation | UDF, collect_set, HAVING filter, SQL aggregation |

## Running Examples

```bash
# RDD anagram grouping
make run-spark CHAPTER=bonus_chapters/anagrams EXAMPLE=01_anagrams_rdd

# DataFrame anagram grouping
make run-spark CHAPTER=bonus_chapters/anagrams EXAMPLE=02_anagrams_dataframe

# Custom input file
make run-spark CHAPTER=bonus_chapters/anagrams EXAMPLE=01_anagrams_rdd \
    ARGS="/path/to/your/text.txt"
```

## Key Concepts

### The Canonical Key Trick

Every anagram group shares a unique canonical form: the word's letters sorted alphabetically.

```python
def anagram_key(word: str) -> str:
    return "".join(sorted(word))

anagram_key("listen")  # → "eilnst"
anagram_key("silent")  # → "eilnst"
anagram_key("enlist")  # → "eilnst"
```

All three words map to the same key — so a `groupByKey` on this key automatically assembles anagram groups.

### RDD Pipeline

```
text lines
  → flatMap(tokenise)               # one word per element
  → map(word → (sorted_key, word))  # canonical key
  → groupByKey()                    # all words with same letters
  → filter(group size > 1)          # discard non-anagram words
```

### groupByKey vs reduceByKey for Anagrams

| Approach | How it works | Notes |
| --- | --- | --- |
| `groupByKey` | Ships ALL word strings to the reducer, then deduplicates | Simple but shuffles more data |
| `reduceByKey` | Merges word sets locally per partition first | Less shuffle; same result |

```python
# groupByKey — ship everything, deduplicate at reducer
words.map(lambda w: (anagram_key(w), w)) \
     .groupByKey() \
     .mapValues(lambda ws: sorted(set(ws)))

# reduceByKey — local set union per partition first
words.map(lambda w: (anagram_key(w), [w])) \
     .reduceByKey(lambda a, b: list(set(a) | set(b))) \
     .mapValues(sorted)
```

For word count-style problems the difference is large (integers are tiny). For anagrams the values are strings — the distinction matters more at scale.

### DataFrame Aggregation with collect_set

```python
groups_df = (
    keyed_df
    .groupBy("anagram_key")
    .agg(
        F.collect_set("word").alias("words"),      # unique words per key
        F.sum("frequency").alias("total"),
    )
    .filter(F.size(F.col("words")) > 1)            # true anagram groups only
)
```

`collect_set` is the DataFrame equivalent of `.groupByKey().mapValues(set)` — it collects distinct values into an array column.

### Example Output

For the sample text containing "listen", "silent", "easter", "eaters", "death", "hated":

```
anagram_key  words                          total_occurrences
-----------  ----------------------------   -----------------
eilnst       [listen, silent]               5
adeht        [death, hated]                 3
aeerst       [easter, eaters]               3
amry         [army, mary]                   5
```

## Performance Considerations

| Approach | Shuffle volume | Notes |
| --- | --- | --- |
| `groupByKey` | All word strings | Fine for small vocabularies |
| `reduceByKey` with set union | Reduced via local combining | Better at scale |
| `collect_set` (DataFrame) | Catalyst-optimised | Preferred for structured data |

## Additional Resources

- [Anagram — Wikipedia](https://en.wikipedia.org/wiki/Anagram)
- [PySpark collect_set](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.collect_set.html)
- [PySpark UDF Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html)

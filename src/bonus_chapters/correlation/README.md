# Bonus Chapter: Correlation

All-versus-all Pearson correlation between genes across patient samples — a fundamental computation in bioinformatics for identifying co-expressed genes, and a general pattern for any scenario where you need pairwise similarity across many entities.

## Examples

| Example | Description | Key Concepts |
| --- | --- | --- |
| `01_correlation_rdd.py` | RDD all-vs-all correlation via cartesian product + manual Pearson r | groupByKey, cartesian, alignment by patient, pure-Python Pearson formula |
| `02_correlation_dataframe.py` | DataFrame approach: pivot → df.stat.corr() pairwise | pivot, df.stat.corr, combinations, expression matrix |

## Running Examples

```bash
# RDD all-vs-all correlation
make run-spark CHAPTER=bonus_chapters/correlation EXAMPLE=01_correlation_rdd

# DataFrame all-vs-all correlation
make run-spark CHAPTER=bonus_chapters/correlation EXAMPLE=02_correlation_dataframe

# Custom gene expression file
make run-spark CHAPTER=bonus_chapters/correlation EXAMPLE=01_correlation_rdd \
    ARGS="src/bonus_chapters/correlation/data/sample_genes.csv"
```

## Key Concepts

### Input Format

Gene expression data: one measurement per (gene, patient) combination.

```
gene_id,patient_id,value
g1,p1,1.0
g1,p2,1.5
g2,p1,1.1
...
```

### Pearson Correlation Coefficient

Measures the linear relationship between two variables. Ranges from -1 to +1:

| Range | Interpretation |
| --- | --- |
| r > 0.8 | Strong positive — genes move together |
| 0.5 < r ≤ 0.8 | Moderate positive |
| -0.5 ≤ r ≤ 0.5 | Weak / no correlation |
| -0.8 ≤ r < -0.5 | Moderate negative |
| r < -0.8 | Strong negative — genes move oppositely |

```python
def pearson_r(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    mean_x, mean_y = sum(xs) / n, sum(ys) / n
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    denom = (sum((x - mean_x)**2 for x in xs)**0.5
             * sum((y - mean_y)**2 for y in ys)**0.5)
    return numerator / denom if denom != 0 else 0.0
```

### RDD Pipeline — Cartesian Approach

```
parse records
  → map to (gene_id, (patient_id, value))
  → groupByKey → (gene_id, {patient_id: value})
  → cartesian(self)                              # all (gene_a, gene_b) pairs
  → filter gene_a < gene_b                       # deduplicate
  → map to Pearson r per pair
  → sortBy |r| descending
```

**Why `gene_a < gene_b`?** Correlation is symmetric: `corr(A,B) = corr(B,A)`. Without this filter, every pair would appear twice.

### DataFrame Pipeline — Pivot Approach

```
read CSV
  → groupBy(patient_id).pivot(gene_id).agg(avg(value))  # expression matrix
  → for each gene pair: df.stat.corr(col_a, col_b)       # built-in Pearson
```

The pivoted matrix looks like:

```
patient_id | g1  | g2  | g3  | g4
-----------+-----+-----+-----+----
p1         | 1.0 | 1.1 | 3.0 | 1.0
p2         | 1.5 | 1.6 | 2.5 | 2.0
p3         | 2.0 | 2.1 | 2.0 | 1.5
```

Then `df.stat.corr("g1", "g3")` computes Pearson r between the `g1` and `g3` columns.

### Expected Output for Sample Data

```
Gene A    Gene B    Pearson r    Interpretation
--------  --------  ----------   ---------------
g1        g2          0.9994     strong positive
g1        g3         -0.9988     strong negative
g2        g3         -0.9998     strong negative
g1        g4          0.8321     strong positive
g2        g4          0.8186     strong positive
g3        g4         -0.8429     strong negative
```

Genes g1 and g2 are strongly co-expressed. Gene g3 is strongly anti-correlated with both — its expression goes down as g1/g2 go up.

## Performance Considerations

| Approach | Scales with | Notes |
| --- | --- | --- |
| RDD cartesian | O(N²) pairs | Best for large N — pairs processed in parallel |
| DataFrame pivot + stat.corr | O(N²) sequential calls | Simple for small N (< 100 genes); pivot can be large for many genes |

For genomics scale (tens of thousands of genes), the cartesian approach can be further optimised with approximate methods (e.g., LSH for finding high-correlation pairs without computing all N² pairs).

## Additional Resources

- [Pearson Correlation — Wikipedia](https://en.wikipedia.org/wiki/Pearson_correlation_coefficient)
- [PySpark df.stat.corr](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameStatFunctions.corr.html)
- [PySpark RDD.cartesian](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.cartesian.html)

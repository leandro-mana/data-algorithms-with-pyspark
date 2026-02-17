# Chapter 12: Feature Engineering in PySpark

This chapter covers **feature engineering** — the process of transforming raw data into numeric feature vectors that machine learning algorithms can consume. Spark's ML Pipeline API provides a composable, reusable approach to building feature engineering workflows.

## Examples

| Example | Description | Use Case |
| --- | --- | --- |
| `feature_transformations.py` | MinMaxScaler, Normalizer, Bucketizer, QuantileDiscretizer, log transform | Numeric feature scaling and binning |
| `categorical_encoding.py` | StringIndexer, OneHotEncoder, VectorAssembler pipeline | Categorical-to-numeric conversion |
| `text_features.py` | TF-IDF with HashingTF and CountVectorizer | Text document vectorization |

## Running Examples

```bash
# Numeric feature transformations (scaling, normalizing, bucketing)
make run-spark CHAPTER=chapter_12 EXAMPLE=feature_transformations

# Categorical encoding (StringIndexer, OneHotEncoder, full pipeline)
make run-spark CHAPTER=chapter_12 EXAMPLE=categorical_encoding

# Text feature engineering (TF-IDF)
make run-spark CHAPTER=chapter_12 EXAMPLE=text_features
```

## Key Concepts

### The ML Pipeline API

Spark ML uses a **Pipeline** abstraction to chain transformations into reproducible workflows:

- **Transformer**: Takes a DataFrame, returns a DataFrame (e.g., `Tokenizer`, `VectorAssembler`)
- **Estimator**: Takes a DataFrame, returns a fitted Model/Transformer (e.g., `MinMaxScaler`, `IDF`)
- **Pipeline**: Chains Estimators and Transformers; `pipeline.fit(df)` produces a `PipelineModel`

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, HashingTF, IDF

pipeline = Pipeline(stages=[
    Tokenizer(inputCol="text", outputCol="words"),
    HashingTF(inputCol="words", outputCol="raw_features"),
    IDF(inputCol="raw_features", outputCol="tfidf_features"),
])

model = pipeline.fit(training_df)    # fit all estimators
result = model.transform(test_df)    # apply all transformations
```

### Numeric Feature Transformations

#### MinMaxScaler — Scale to [0, 1]

Rescales each feature to the range [0, 1] using min and max values:

```
scaled(x) = (x - x_min) / (x_max - x_min)
```

```python
assembler = VectorAssembler(inputCols=["revenue"], outputCol="revenue_vec")
scaler = MinMaxScaler(inputCol="revenue_vec", outputCol="revenue_scaled")
pipeline = Pipeline(stages=[assembler, scaler])
```

| Before | After (scaled) |
| --- | --- |
| 10345 | 0.0 |
| 30285 | 0.257 |
| 41560 | 0.402 |
| 77560 | 0.866 |
| 88000 | 1.0 |

#### Normalizer — Unit Norm Vectors

Scales each **row vector** to have unit norm (sum of components = 1 for L1, Euclidean length = 1 for L2):

```python
from pyspark.ml.feature import Normalizer

l1 = Normalizer(inputCol="features", outputCol="features_L1", p=1.0)  # Manhattan
l2 = Normalizer(inputCol="features", outputCol="features_L2", p=2.0)  # Euclidean
```

| Norm | Formula | Use Case |
| --- | --- | --- |
| L1 (Manhattan) | `sum(abs(xi)) = 1` | Sparse data, feature selection |
| L2 (Euclidean) | `sqrt(sum(xi^2)) = 1` | Default for most ML algorithms |

#### Bucketizer — User-Defined Bins

Maps continuous values into discrete buckets with explicit boundaries:

```python
from pyspark.ml.feature import Bucketizer

bucketizer = Bucketizer(
    splits=[0, 30, 40, 50, float("inf")],
    inputCol="age",
    outputCol="age_bucket",
)
# age=28 → bucket 0, age=34 → bucket 1, age=52 → bucket 3
```

#### QuantileDiscretizer — Auto-Binning

Automatically determines bucket boundaries based on data quantiles:

```python
from pyspark.ml.feature import QuantileDiscretizer

discretizer = QuantileDiscretizer(
    numBuckets=3,
    inputCol="revenue",
    outputCol="revenue_bucket",
)
```

#### Log Transform — Compress Skewed Distributions

```python
df.withColumn("log_revenue", F.log("revenue"))
df.withColumn("log10_revenue", F.log10("revenue"))
```

### Categorical Feature Encoding

#### StringIndexer — Labels to Indices

Converts string labels to numeric indices, ordered by frequency (most frequent = 0):

```python
from pyspark.ml.feature import StringIndexer

indexer = StringIndexer(inputCol="safety_level", outputCol="safety_index")
# "Medium" (3 occurrences) → 0.0
# "High"   (2 occurrences) → 1.0
# "Very-High" (2)          → 2.0
# "Low"    (1 occurrence)  → 3.0
```

#### OneHotEncoder — Sparse Binary Vectors

Converts indexed categories to sparse binary vectors (one `1` per category):

```python
from pyspark.ml.feature import OneHotEncoder

encoder = OneHotEncoder(inputCol="safety_index", outputCol="safety_vec")
# index 0.0 → (3,[0],[1.0])  i.e. [1, 0, 0]
# index 1.0 → (3,[1],[1.0])  i.e. [0, 1, 0]
# index 2.0 → (3,[2],[1.0])  i.e. [0, 0, 1]
# index 3.0 → (3,[],[])      i.e. [0, 0, 0]  (last category dropped)
```

**Note**: OneHotEncoder drops the last category by default (`dropLast=True`) to avoid multicollinearity in linear models.

#### VectorAssembler — Combine Features

Merges multiple columns (numeric + vector) into a single feature vector:

```python
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=["price", "safety_vec", "engine_vec"],
    outputCol="features",
)
```

#### Full Encoding Pipeline

The typical categorical encoding workflow chains all three stages:

```
StringIndexer → OneHotEncoder → VectorAssembler
```

```python
pipeline = Pipeline(stages=[
    StringIndexer(inputCol="safety_level", outputCol="safety_index"),
    StringIndexer(inputCol="engine_type", outputCol="engine_index"),
    OneHotEncoder(inputCol="safety_index", outputCol="safety_vec"),
    OneHotEncoder(inputCol="engine_index", outputCol="engine_vec"),
    VectorAssembler(inputCols=["price", "safety_vec", "engine_vec"], outputCol="features"),
])
```

### Text Feature Engineering — TF-IDF

TF-IDF (Term Frequency-Inverse Document Frequency) measures word importance: words that are frequent in one document but rare across the corpus get high weights.

```
TF-IDF(t, d) = TF(t, d) * IDF(t)
IDF(t) = log((N + 1) / (DF(t) + 1))
```

Where `N` = total documents, `DF(t)` = documents containing term `t`.

#### Approach 1: HashingTF + IDF

Uses the **hashing trick** to map terms to fixed-size feature vectors:

```python
pipeline = Pipeline(stages=[
    Tokenizer(inputCol="text", outputCol="words"),
    HashingTF(inputCol="words", outputCol="raw_features", numFeatures=128),
    IDF(inputCol="raw_features", outputCol="tfidf_features"),
])
```

#### Approach 2: CountVectorizer + IDF

Builds a **vocabulary** from the training data, then counts term occurrences:

```python
pipeline = Pipeline(stages=[
    Tokenizer(inputCol="text", outputCol="words"),
    CountVectorizer(inputCol="words", outputCol="raw_features"),
    IDF(inputCol="raw_features", outputCol="tfidf_features"),
])
```

## HashingTF vs CountVectorizer

| Property | HashingTF | CountVectorizer |
| --- | --- | --- |
| Vector size | Fixed (`numFeatures`) | Vocabulary size |
| Vocabulary needed | No | Yes (built during `fit`) |
| Hash collisions | Possible | None |
| Inverse mapping | Not possible | Term ↔ index lookup |
| Memory usage | Constant | Grows with vocabulary |
| Best for | Large-scale, streaming | Moderate-scale, offline |

## Performance Considerations

| Technique | When to Use | Watch Out For |
| --- | --- | --- |
| MinMaxScaler | Features on different scales | Sensitive to outliers |
| Normalizer | Row vectors need unit length | Doesn't handle columns independently |
| Bucketizer | Domain knowledge of meaningful ranges | Manual boundary selection |
| QuantileDiscretizer | No domain knowledge, want even bins | Small datasets may have irregular bins |
| StringIndexer | Categorical strings → numeric | Unseen labels at transform time |
| OneHotEncoder | Nominal categories (no ordering) | High cardinality → very wide vectors |
| HashingTF | Large vocabularies, streaming data | Hash collisions lose information |
| CountVectorizer | Need interpretable features | Vocabulary must fit in memory |
| Pipeline | Any multi-step feature engineering | Order of stages matters |

## Additional Resources

- [Spark ML Feature Extraction](https://spark.apache.org/docs/latest/ml-features.html)
- [Spark ML Pipelines](https://spark.apache.org/docs/latest/ml-pipeline.html)
- [PySpark ML Feature API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ml.html#feature)

# Bonus Chapter: UDFs (User-Defined Functions)

PySpark UDFs let you bring arbitrary Python logic into the DataFrame API. They bridge the gap when built-in `pyspark.sql.functions` don't cover a use case — wrapping business rules, custom string transformations, or existing Python libraries.

## Examples

| Example | Description | Key Concepts |
| --- | --- | --- |
| `01_udf_dataframe.py` | Three UDF registration patterns on an employee DataFrame | function UDF, lambda UDF, SQL UDF registration, return type declaration |

## Running Examples

```bash
# Run UDF examples (no data file needed — uses inline data)
make run-spark CHAPTER=bonus_chapters/udf EXAMPLE=01_udf_dataframe
```

## Key Concepts

### Registering a UDF

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def title_case(name: str | None) -> str | None:
    if name is None:
        return None
    return " ".join(word.capitalize() for word in name.split())

# Explicit return type — avoids Spark inferring it via reflection
title_case_udf = udf(title_case, StringType())

df.withColumn("name_formatted", title_case_udf(col("name")))
```

**Always specify the return type.** Without it, Spark defaults to `StringType` via reflection, which is slower and can silently produce wrong results for non-string types.

### Three Registration Patterns

#### Pattern 1: Named function UDF

Best for reusable logic. Testable in pure Python before registering.

```python
def salary_band(salary: int | None) -> str | None:
    if salary is None: return None
    if salary < 70000: return "junior"
    if salary < 90000: return "mid"
    return "senior"

salary_band_udf = udf(salary_band, StringType())
df.withColumn("band", salary_band_udf(col("salary")))
```

#### Pattern 2: Lambda UDF

For short, throwaway transformations where a named function adds no clarity.

```python
upper_udf = udf(lambda s: s.upper() if s else None, StringType())
df.withColumn("dept_upper", upper_udf(col("department")))
```

#### Pattern 3: SQL UDF registration

Makes UDFs available in `spark.sql()` queries — useful when mixing DataFrame and SQL code.

```python
spark.udf.register("salary_band_sql", salary_band, StringType())
df.createOrReplaceTempView("employees")
spark.sql("SELECT salary_band_sql(salary) AS band FROM employees")
```

### Handling NULL values

UDFs receive Python `None` for SQL `NULL`. Always guard against it:

```python
def my_udf(value: str | None) -> str | None:
    if value is None:       # ← always check before processing
        return None
    return value.upper()
```

If you don't guard and the function raises an exception on `None`, Spark will propagate the error at runtime.

### UDF Performance

Standard Python UDFs use row-by-row serialisation between the JVM and Python:

```
JVM Row → pickle → Python process → unpickle → apply function → pickle → JVM
```

| Approach | Throughput | Notes |
| --- | --- | --- |
| Built-in `pyspark.sql.functions` | Fastest | Runs entirely in JVM, no Python overhead |
| Python UDF | Moderate | Row-by-row serialisation; fine for moderate volume |
| Pandas UDF (`@pandas_udf`) | Fast | Vectorised — sends batches as pandas Series |

**Rule of thumb**: use built-in functions first. Only reach for a Python UDF when no built-in covers the logic. For high-throughput paths, consider `@pandas_udf` (Pandas UDF).

### When to use UDFs vs built-in functions

| Task | Prefer |
| --- | --- |
| String manipulation (upper, trim, regex) | `F.upper()`, `F.regexp_replace()` |
| Math (abs, log, round) | `F.abs()`, `F.log()`, `F.round()` |
| Conditional logic | `F.when().otherwise()` |
| Custom business rules | UDF |
| Wrapping a Python library | UDF (or Pandas UDF for performance) |

## Additional Resources

- [PySpark UDF Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html)
- [Pandas UDFs (vectorised)](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html)
- [PySpark SQL Functions Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)

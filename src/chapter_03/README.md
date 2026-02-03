# Chapter 3: Mapper Transformations

This chapter covers the fundamental mapper transformations in PySpark.

## Examples

| Example | Description |
|---------|-------------|
| `map_vs_flatmap.py` | Comparison of `map()` (1-to-1) vs `flatMap()` (1-to-many) |
| `mappartitions_transformation.py` | Using `mapPartitions()` for efficient batch processing |
| `mapvalues_transformation.py` | Using `mapValues()` to transform only values in pairs |

## Running Examples

```bash
# Run with built-in sample data
make run CHAPTER=chapter_03 EXAMPLE=map_vs_flatmap

# Run mapPartitions with a data file
make run-spark CHAPTER=chapter_03 EXAMPLE=mappartitions_transformation \
    ARGS="src/chapter_03/data/sample_numbers.txt"
```

## Key Concepts

### map() vs flatMap()

| Transformation | Output per input | Total elements |
|---------------|------------------|----------------|
| `map()` | Exactly 1 | Same as input |
| `flatMap()` | 0 or more | Can be different |

```python
# map() preserves structure
rdd.map(lambda s: s.split())       # [["a", "b"], ["c"]]

# flatMap() flattens results
rdd.flatMap(lambda s: s.split())   # ["a", "b", "c"]
```

### mapPartitions()

Processes entire partitions at once, enabling:
- **Local aggregation** - reduce data before shuffle
- **Batch operations** - efficient when setup/teardown is expensive
- **Resource management** - e.g., database connections per partition

### mapValues()

Transforms only the values of key-value pairs:
- More efficient than `map()` for value-only transforms
- Preserves partitioning (important for subsequent operations)

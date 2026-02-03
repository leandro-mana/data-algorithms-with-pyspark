# Chapter 2: MapReduce Pattern - DNA Base Count

This chapter demonstrates the classic MapReduce pattern using DNA sequence analysis as an example.

## Examples

| Example | Description |
|---------|-------------|
| `dna_base_count_basic.py` | Basic flatMap + reduceByKey approach |
| `dna_base_count_mappartitions.py` | Optimized version using InMapper Combiner pattern |

## Running Examples

```bash
# Run with sample data (built-in)
make run CHAPTER=chapter_02 EXAMPLE=dna_base_count_basic

# Run with a FASTA file
make run-spark CHAPTER=chapter_02 EXAMPLE=dna_base_count_basic ARGS="src/chapter_02/data/sample.fasta"
```

## FASTA File Format

FASTA is a text-based format for DNA/RNA sequences:

- Lines starting with `>` are headers (sequence identifiers)
- Following lines contain the sequence (A, T, C, G, N bases)

```text
>sequence_name description
ATCGATCGATCG
GCTAGCTAGCTA
```

## Key Concepts

### Basic MapReduce

1. **Map phase**: `flatMap()` emits (base, 1) for each base
2. **Reduce phase**: `reduceByKey()` sums all counts per base

### InMapper Combiner Pattern

- Uses `mapPartitions()` to aggregate locally within each partition
- Reduces shuffle data by emitting partial sums instead of individual counts
- Significantly more efficient for large datasets

# Data Algorithms with PySpark

This repository contains **Python-only** implementations with a simplified project structure and unified Makefile runner, for study examples from the book [Data Algorithms with Spark](https://www.oreilly.com/library/view/data-algorithms-with/9781492082378/) by Mahmoud Parsian.

## Requirements

- **make** (bootstrap requirement - see below)
- Python 3.12+
- Java JDK 17+ (required by PySpark 4.x)
- [Poetry](https://python-poetry.org/) for dependency management

## Bootstrap: Installing make

`make` is required to run the setup commands. Install it first:

```bash
# macOS (via Xcode Command Line Tools)
xcode-select --install

# Ubuntu/Debian
sudo apt-get install build-essential

# Fedora/RHEL
sudo dnf install make

# Verify installation
make --version
```

## Quick Start

```bash
# Check/install all prerequisites (Python 3.12+, Java 17+, Poetry)
make prereqs

# Install project dependencies
make install

# (Optional) Install pre-commit hooks for code quality on commit
make pre-commit-install

# Run an example
make run CHAPTER=chapter_01 EXAMPLE=rdd_map_transformation

# List available examples in a chapter
make list-examples CHAPTER=chapter_01

# List all chapters
make list-chapters
```

## Prerequisites (macOS)

The `make prereqs` command automatically checks and installs:

| Prerequisite | Version | Installer            |
|--------------|---------|----------------------|
| Python       | 3.12+   | pyenv via Homebrew   |
| Java         | 17+     | OpenJDK via Homebrew |
| Poetry       | latest  | Official installer   |

You can also check/install them individually:

```bash
make check-python   # Check/install Python 3.12+
make check-java     # Check/install Java 17+
make check-poetry   # Check/install Poetry
```

The scripts automatically configure your shell rc file (`~/.zshrc` or `~/.bashrc`).

## Project Structure

```bash
src/
├── common/                    # Shared utilities
│   └── spark_session.py       # SparkSession factory
├── chapter_01/                # Introduction to Data Algorithms
│   ├── README.md
│   ├── data/
│   └── examples/
├── chapter_02/                # DNA Base Count (MapReduce)
├── chapter_03/                # Mapper Transformations
├── ...                        # Chapters 4-12
└── bonus_chapters/            # Additional topics
    ├── wordcount/
    ├── anagrams/
    └── ...
```

## Running Examples

```bash
# Run with Python (simple scripts)
make run CHAPTER=chapter_03 EXAMPLE=map_vs_flatmap

# Run with spark-submit (full Spark context)
make run-spark CHAPTER=chapter_02 EXAMPLE=dna_base_count_basic

# Pass arguments to the script
make run-spark CHAPTER=chapter_02 EXAMPLE=dna_base_count_basic \
    ARGS="src/chapter_02/data/sample.fasta"
```

## Development

### Code Quality

```bash
# Run all checks (lint + type-check + tests)
make check

# Run individually
make lint          # Ruff linter
make lint-fix      # Ruff with auto-fix
make type-check    # MyPy type checker
make test          # Pytest
```

### Utilities

```bash
# Clean generated files (__pycache__, .mypy_cache, .logs/, etc.)
make clean

# See all available commands
make help
```

### Logging

Spark logs are written to `.logs/spark.log` (gitignored) to keep console output clean.
Only errors are shown on the console during script execution.

## Chapters Overview

| Chapter | Topic | Status |
| --- | --- | --- |
| 01 | [Introduction to Spark and PySpark](src/chapter_01/README.md) | Done |
| 02 | [MapReduce Pattern — DNA Base Count](src/chapter_02/README.md) | Done |
| 03 | [Mapper Transformations](src/chapter_03/README.md) | Done |
| 04 | [Reductions in Spark](src/chapter_04/README.md) | Done |
| 05 | [Partitioning Data](src/chapter_05/README.md) | Done |
| 06 | [Graph Algorithms](src/chapter_06/README.md) | Done |
| 07 | [Interacting with External Data Sources](src/chapter_07/README.md) | Done |
| 08 | [Ranking Algorithms](src/chapter_08/README.md) | Done |
| 09 | Classic Data Design Patterns | Pending |
| 10 | Practical Data Design Patterns | Pending |
| 11 | Join Design Patterns | Pending |
| 12 | Feature Engineering in PySpark | Pending |

## Patterns Quick Reference

A cross-chapter index of common data engineering patterns and which example implements them:

| Pattern | Chapter | Example | Key API |
| --- | --- | --- | --- |
| ETL Pipeline | 01 | `etl_census_dataframe.py` | `read.json()`, `filter()`, `withColumn()`, `write.csv()` |
| MapReduce (basic) | 02 | `dna_base_count_ver_1.py` | `flatMap()` + `reduceByKey()` |
| InMapper Combiner | 02 | `dna_base_count_ver_2.py` | `flatMap()` with local dict |
| Partition-level Aggregation | 02 | `dna_base_count_ver_3.py` | `mapPartitions()` + `reduceByKey()` |
| 1-to-1 Transformation | 03 | `map_vs_flatmap.py` | `map()` |
| 1-to-Many / Flattening | 03 | `map_vs_flatmap.py` | `flatMap()` |
| Batch Processing | 03 | `mappartitions_transformation.py` | `mapPartitions()` |
| Value-only Transform | 03 | `mapvalues_transformation.py` | `mapValues()` |
| Average by Key | 01, 04 | `average_by_key_reducebykey.py`, `movie_avg_rating.py` | `reduceByKey()` with (sum, count) |
| Sum by Key (4 ways) | 04 | `reduction_warmup.py` | `reduceByKey`, `groupByKey`, `aggregateByKey`, `combineByKey` |
| Monoid-safe Reduction | 04 | `movie_avg_rating.py` | (sum, count) pattern |
| RDD Partition Management | 05 | `partition_basics.py` | `getNumPartitions()`, `glom()`, `repartition()`, `coalesce()` |
| Physical Partitioning | 05 | `physical_partitioning.py` | `write.partitionBy()`, Parquet, partition pruning |
| Graph Degree Analysis | 06 | `graph_basics.py` | `groupBy()`, self-join, in/out degree |
| Graph Path Finding | 06 | `graph_basics.py`, `flight_analysis.py` | Self-join for 2-hop paths, triangles |
| Connecting Flights | 06 | `flight_analysis.py` | Self-join on edges, route aggregation |
| CSV/JSON Read/Write | 07 | `csv_json_operations.py` | `read.csv()`, `read.json()`, `StructType`, `explode()` |
| Parquet Analytics | 07 | `parquet_operations.py` | `read.parquet()`, column pruning, predicate pushdown, `partitionBy()` |
| Rank Product | 08 | `rank_product.py` | `combineByKey()`, `zipWithIndex()`, geometric mean |
| PageRank | 08 | `pagerank.py` | `join()` + `flatMap()` + `reduceByKey()`, iterative convergence |

## References

- LinkedIn: [Leandro Mana](https://www.linkedin.com/in/leandro-mana-2854553b/)
- Contact: <leandromana@gmail.com>
- Book: [Data Algorithms with Spark](https://www.oreilly.com/library/view/data-algorithms-with/9781492082378/) by Mahmoud Parsian
- Original repo: [mahmoudparsian/data-algorithms-with-spark](https://github.com/mahmoudparsian/data-algorithms-with-spark)

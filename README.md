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

| Chapter | Topic |
| --------- | ------- |
| 01 | Introduction to Data Algorithms |
| 02 | MapReduce Pattern - DNA Base Count |
| 03 | Mapper Transformations |
| 04 | Reducer Transformations |
| 05 | Partitioning Data |
| 06 | Graph Algorithms |
| 07 | Interacting with External Data Sources |
| 08 | Ranking Algorithms |
| 09 | Classic Data Design Patterns |
| 10 | Practical Data Design Patterns |
| 11 | Join Design Patterns |
| 12 | Feature Engineering in PySpark |

## References

- LinkedIn: [Leandro Mana](https://www.linkedin.com/in/leandro-mana-2854553b/)
- Contact: <leandromana@gmail.com>
- Book: [Data Algorithms with Spark](https://www.oreilly.com/library/view/data-algorithms-with/9781492082378/) by Mahmoud Parsian
- Original repo: [mahmoudparsian/data-algorithms-with-spark](https://github.com/mahmoudparsian/data-algorithms-with-spark)

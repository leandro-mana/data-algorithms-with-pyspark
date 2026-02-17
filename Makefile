.PHONY: help prereqs check-python check-java check-poetry install pre-commit-install run run-spark list-examples list-chapters clean lint lint-fix format format-check type-check check test

# Scripts directory
SCRIPTS_DIR := scripts

# Default target
help:
	@echo "Data Algorithms with PySpark - Makefile Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make prereqs              Check/install prerequisites (Python, Java, Poetry)"
	@echo "  make install              Install project dependencies with Poetry"
	@echo "  make pre-commit-install   Install pre-commit git hooks"
	@echo ""
	@echo "Individual prerequisite checks:"
	@echo "  make check-python     Check Python 3.12+ is available"
	@echo "  make check-java       Check Java 17+ is available"
	@echo "  make check-poetry     Check/install Poetry"
	@echo ""
	@echo "Running Examples:"
	@echo "  make run CHAPTER=chapter_03 EXAMPLE=map_transformation_1"
	@echo "  make run-spark CHAPTER=chapter_03 EXAMPLE=map_transformation_1"
	@echo ""
	@echo "  run        - Run example with python (for simple scripts)"
	@echo "  run-spark  - Run example with spark-submit (for Spark jobs)"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint                               Run ruff linter"
	@echo "  make lint-fix                           Run ruff with auto-fix"
	@echo "  make format-check                       Check code formatting"
	@echo "  make format                             Auto-format code"
	@echo "  make type-check                         Run mypy type checker"
	@echo "  make test                               Run pytest"
	@echo "  make check                              Run all checks (lint, format, types, test)"
	@echo ""
	@echo "Utilities:"
	@echo "  make list-examples CHAPTER=chapter_03   List examples in a chapter"
	@echo "  make list-chapters                      List all chapters"
	@echo "  make clean                              Remove generated files. cache, etc."

# ============================================================================
# Prerequisites
# ============================================================================

# Check all prerequisites
prereqs: check-python check-java check-poetry
	@echo ""
	@echo "✓ All prerequisites satisfied"

# Individual checks (can be run standalone)
check-python:
	@$(SCRIPTS_DIR)/ensure-python.sh

check-java:
	@$(SCRIPTS_DIR)/ensure-java.sh

check-poetry:
	@$(SCRIPTS_DIR)/ensure-poetry.sh

# ============================================================================
# Installation
# ============================================================================

# Install dependencies (checks prerequisites first)
install: prereqs
	@echo ""
	@echo "Installing project dependencies..."
	poetry install

# Install pre-commit hooks
pre-commit-install:
	poetry run pre-commit install
	@echo ""
	@echo "✓ Pre-commit hooks installed"

# ============================================================================
# Running Examples
# ============================================================================

# Run a Python example directly
# Usage: make run CHAPTER=chapter_03 EXAMPLE=map_transformation_1
run:
ifndef CHAPTER
	$(error CHAPTER is required. Usage: make run CHAPTER=chapter_03 EXAMPLE=example_name)
endif
ifndef EXAMPLE
	$(error EXAMPLE is required. Usage: make run CHAPTER=chapter_03 EXAMPLE=example_name)
endif
	poetry run python src/$(CHAPTER)/examples/$(EXAMPLE).py $(ARGS)

# Run with spark-submit (for jobs that need full Spark context)
# Usage: make run-spark CHAPTER=chapter_03 EXAMPLE=map_transformation_1 ARGS="--input data/file.txt"
run-spark:
ifndef CHAPTER
	$(error CHAPTER is required. Usage: make run-spark CHAPTER=chapter_03 EXAMPLE=example_name)
endif
ifndef EXAMPLE
	$(error EXAMPLE is required. Usage: make run-spark CHAPTER=chapter_03 EXAMPLE=example_name)
endif
	poetry run spark-submit \
		--master "local[*]" \
		--driver-memory 2g \
		src/$(CHAPTER)/examples/$(EXAMPLE).py $(ARGS)

# ============================================================================
# Utilities
# ============================================================================

# List available examples for a chapter
list-examples:
ifndef CHAPTER
	$(error CHAPTER is required. Usage: make list-examples CHAPTER=chapter_03)
endif
	@echo "Examples in $(CHAPTER):"
	@echo ""
	@ls -1 src/$(CHAPTER)/examples/*.py 2>/dev/null | xargs -I {} basename {} .py | grep -v __init__ || echo "  No examples found"

# List all chapters
list-chapters:
	@echo "Available chapters:"
	@echo ""
	@ls -1d src/chapter_* src/bonus_chapters/* 2>/dev/null | xargs -I {} basename {}

# Run linter
lint:
	poetry run ruff check src/

# Run linter with auto-fix
lint-fix:
	poetry run ruff check --fix src/

# Check code formatting
format-check:
	poetry run ruff format --check src/

# Auto-format code
format:
	poetry run ruff format src/

# Run type checker
type-check:
	poetry run mypy src/

# Run tests
test:
	poetry run pytest

# Run all checks (lint + format + type-check + test)
check: lint format-check type-check test
	@echo ""
	@echo "✓ All checks passed"

# Clean generated files
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "metastore_db" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "derby.log" -delete 2>/dev/null || true
	rm -rf .logs/ 2>/dev/null || true

"""
Common data loading utilities for examples.

This module provides functions to load data from CSV files,
enabling DRY principles across chapter examples.
"""

import csv
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent


T = TypeVar("T")


def load_csv_as_tuples(
    csv_path: str | Path,
    record_factory: Callable[..., T],
    skip_header: bool = True,
) -> list[T]:
    """
    Load a CSV file and convert rows to typed tuples.

    Args:
        csv_path: Path to the CSV file (absolute or relative to project root)
        record_factory: A NamedTuple class or callable that accepts row values
        skip_header: Whether to skip the first row (default: True)

    Returns:
        List of records created by record_factory

    Example:
        class PersonRecord(NamedTuple):
            name: str
            city: str
            age: int

        records = load_csv_as_tuples(
            "src/chapter_01/data/people.csv",
            lambda name, city, value: PersonRecord(name, city, int(value))
        )
    """
    # Resolve path relative to project root if not absolute
    path = Path(csv_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path

    records: list[T] = []

    with path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)

        if skip_header:
            next(reader, None)

        for row in reader:
            if row:  # Skip empty rows
                record = record_factory(*row)
                records.append(record)

    return records


def get_chapter_data_path(chapter: str, filename: str) -> Path:
    """
    Get the full path to a data file within a chapter's data directory.

    Args:
        chapter: Chapter name (e.g., "chapter_01")
        filename: Data file name (e.g., "people.csv")

    Returns:
        Full path to the data file
    """
    return PROJECT_ROOT / "src" / chapter / "data" / filename

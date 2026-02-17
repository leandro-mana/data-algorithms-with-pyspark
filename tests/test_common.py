"""
Tests for src/common/ utilities.
"""

import inspect
import tempfile
from pathlib import Path
from typing import NamedTuple

from pyspark.sql import SparkSession

from src.common.data_loader import (
    PROJECT_ROOT,
    get_chapter_data_path,
    load_csv_as_tuples,
)
from src.common.spark_session import create_spark_session, get_spark_context


class TestSparkSessionUtils:
    """Tests for spark_session.py utilities."""

    def test_create_spark_session_signature(self) -> None:
        """Verify create_spark_session has correct signature."""
        sig = inspect.signature(create_spark_session)
        params = list(sig.parameters.keys())

        assert "script_name" in params
        assert "master" in params

        # Check defaults
        assert sig.parameters["script_name"].default is None
        assert sig.parameters["master"].default == "local[*]"

    def test_get_spark_context_signature(self) -> None:
        """Verify get_spark_context has correct signature."""
        sig = inspect.signature(get_spark_context)
        params = list(sig.parameters.keys())

        assert "script_name" in params
        assert sig.parameters["script_name"].default is None

    def test_create_spark_session_returns_session(self, spark: SparkSession) -> None:
        """
        Verify create_spark_session returns a SparkSession.

        Note: We use the existing fixture session since only one SparkContext
        can be active per JVM. This test verifies the function can be called
        and getOrCreate returns the existing session.
        """
        # getOrCreate will return the existing session from fixture
        session = create_spark_session("TestApp")

        assert session is not None
        assert isinstance(session, SparkSession)

    def test_get_spark_context_returns_context(self, spark: SparkSession) -> None:
        """Verify get_spark_context returns a SparkContext."""
        # Will use existing session's context
        sc = get_spark_context("TestApp")

        assert sc is not None
        # Verify it's actually a SparkContext by checking for expected attributes
        assert hasattr(sc, "parallelize")
        assert hasattr(sc, "textFile")

    def test_session_has_expected_config(self, spark: SparkSession) -> None:
        """
        Verify sessions created via utility have reasonable config.

        Note: Since we can't create a fresh session, we just verify
        the test fixture session has valid configuration.
        """
        conf = spark.sparkContext.getConf()

        # Should have an app name
        app_name = conf.get("spark.app.name")
        assert app_name is not None
        assert len(app_name) > 0

        # Should have a master URL
        master = conf.get("spark.master")
        assert master is not None
        assert "local" in master


class TestDataLoader:
    """Tests for data_loader.py utilities."""

    def test_get_chapter_data_path_returns_correct_path(self) -> None:
        """Verify get_chapter_data_path constructs correct paths."""
        path = get_chapter_data_path("chapter_01", "people.csv")

        assert path.is_absolute()
        assert path.name == "people.csv"
        assert "chapter_01" in str(path)
        assert "data" in str(path)

    def test_get_chapter_data_path_uses_project_root(self) -> None:
        """Verify paths are relative to PROJECT_ROOT."""
        path = get_chapter_data_path("chapter_02", "test.txt")

        expected = PROJECT_ROOT / "src" / "chapter_02" / "data" / "test.txt"
        assert path == expected

    def test_load_csv_as_tuples_with_namedtuple(self) -> None:
        """Verify loading CSV into NamedTuple records."""

        class TestRecord(NamedTuple):
            name: str
            value: int

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write("name,value\n")
            f.write("alice,10\n")
            f.write("bob,20\n")
            temp_path = f.name

        try:
            records = load_csv_as_tuples(
                temp_path,
                lambda name, value: TestRecord(name, int(value)),
            )

            assert len(records) == 2
            assert records[0] == TestRecord("alice", 10)
            assert records[1] == TestRecord("bob", 20)
        finally:
            Path(temp_path).unlink()

    def test_load_csv_as_tuples_skip_header(self) -> None:
        """Verify header is skipped by default."""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write("header1,header2\n")
            f.write("data1,data2\n")
            temp_path = f.name

        try:
            records = load_csv_as_tuples(
                temp_path,
                lambda a, b: (a, b),
            )

            assert len(records) == 1
            assert records[0] == ("data1", "data2")
        finally:
            Path(temp_path).unlink()

    def test_load_csv_as_tuples_no_skip_header(self) -> None:
        """Verify header is included when skip_header=False."""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write("row1a,row1b\n")
            f.write("row2a,row2b\n")
            temp_path = f.name

        try:
            records = load_csv_as_tuples(
                temp_path,
                lambda a, b: (a, b),
                skip_header=False,
            )

            assert len(records) == 2
            assert records[0] == ("row1a", "row1b")
        finally:
            Path(temp_path).unlink()

    def test_load_csv_as_tuples_skips_empty_rows(self) -> None:
        """Verify empty rows are skipped."""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write("name,value\n")
            f.write("alice,10\n")
            f.write("\n")
            f.write("bob,20\n")
            f.write("\n")
            temp_path = f.name

        try:
            records = load_csv_as_tuples(
                temp_path,
                lambda name, value: (name, int(value)),
            )

            assert len(records) == 2
        finally:
            Path(temp_path).unlink()

    def test_load_csv_as_tuples_with_real_data(self) -> None:
        """Verify loading the actual people.csv file."""
        csv_path = get_chapter_data_path("chapter_01", "people.csv")

        if csv_path.exists():

            class PersonRecord(NamedTuple):
                name: str
                city: str
                value: int

            records = load_csv_as_tuples(
                csv_path,
                lambda name, city, value: PersonRecord(name, city, int(value)),
            )

            assert len(records) > 0
            assert all(isinstance(r, PersonRecord) for r in records)
            assert all(isinstance(r.value, int) for r in records)

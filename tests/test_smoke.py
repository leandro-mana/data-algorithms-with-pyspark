"""
Smoke tests to verify PySpark is working correctly.

These tests ensure the basic Spark functionality is operational
before running more complex tests.
"""

from pyspark.sql import SparkSession


class TestSparkSmoke:
    """Basic smoke tests for Spark functionality."""

    def test_spark_session_created(self, spark: SparkSession) -> None:
        """Verify SparkSession is created and accessible."""
        assert spark is not None
        assert spark.version is not None

    def test_spark_context_available(self, spark: SparkSession) -> None:
        """Verify SparkContext is available."""
        sc = spark.sparkContext
        assert sc is not None
        assert sc.appName == "pytest-pyspark"

    def test_rdd_operations(self, sc) -> None:
        """Verify basic RDD operations work."""
        data = [1, 2, 3, 4, 5]
        rdd = sc.parallelize(data)

        assert rdd.count() == 5
        assert rdd.sum() == 15
        assert rdd.collect() == data

    def test_rdd_transformations(self, sc) -> None:
        """Verify RDD transformations work correctly."""
        rdd = sc.parallelize([1, 2, 3, 4, 5])

        # map
        doubled = rdd.map(lambda x: x * 2)
        assert doubled.collect() == [2, 4, 6, 8, 10]

        # filter
        evens = rdd.filter(lambda x: x % 2 == 0)
        assert evens.collect() == [2, 4]

        # reduceByKey
        pairs = sc.parallelize([("a", 1), ("b", 2), ("a", 3)])
        summed = pairs.reduceByKey(lambda a, b: a + b)
        result = dict(summed.collect())
        assert result == {"a": 4, "b": 2}

    def test_dataframe_operations(self, spark: SparkSession) -> None:
        """Verify basic DataFrame operations work."""
        data = [("Alice", 30), ("Bob", 25), ("Carol", 35)]
        df = spark.createDataFrame(data, ["name", "age"])

        assert df.count() == 3
        assert df.columns == ["name", "age"]

        # Filter
        filtered = df.filter(df.age > 28)
        assert filtered.count() == 2

        # Select
        names = df.select("name").collect()
        assert len(names) == 3

    def test_sql_query(self, spark: SparkSession) -> None:
        """Verify Spark SQL works."""
        data = [("Alice", 30), ("Bob", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.createOrReplaceTempView("people")

        result = spark.sql("SELECT name FROM people WHERE age > 26")
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0].name == "Alice"

"""
Bonus Chapter: UDFs (User-Defined Functions)

PySpark UDFs extend the DataFrame API with custom Python logic when
built-in functions are not sufficient. Three registration patterns are
covered: function UDF, lambda UDF, and SQL UDF registration.

When to use UDFs:
    - Custom business logic with no built-in equivalent
    - Wrapping existing Python libraries for column-level operations

When NOT to use UDFs:
    - Anything achievable with pyspark.sql.functions — built-ins run
      on the JVM and avoid Python serialisation overhead
    - Hot paths on billions of rows — consider Pandas UDFs (vectorised)
      for throughput-sensitive work

UDF performance note:
    Standard Python UDFs serialise each row individually between the
    JVM and Python (row-by-row). This is slower than built-in functions
    but often acceptable for moderate data volumes.
"""

from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, StringType

from src.common.spark_session import create_spark_session

# --- Sample data (inline, no file needed) ---
EMPLOYEES = [
    (1, "alice johnson", "engineering", 95000),
    (2, "bob smith", "marketing", 72000),
    (3, "carol white", "engineering", 110000),
    (4, "dan brown", "hr", 65000),
    (5, "eve davis", "marketing", 80000),
    (6, None, "engineering", 88000),
]
SCHEMA = ["id", "name", "department", "salary"]


# --- UDF definitions (plain Python functions) ---


def title_case(name: str | None) -> str | None:
    """Capitalise the first letter of each word."""
    if name is None:
        return None
    return " ".join(word.capitalize() for word in name.split())


def salary_band(salary: int | None) -> str | None:
    """Classify salary into a band label."""
    if salary is None:
        return None
    if salary < 70000:
        return "junior"
    if salary < 90000:
        return "mid"
    if salary < 110000:
        return "senior"
    return "principal"


def name_length(name: str | None) -> int | None:
    """Return the number of characters in the name, ignoring spaces."""
    if name is None:
        return None
    return len(name.replace(" ", ""))


def main() -> None:
    spark = create_spark_session(__file__)

    print("=== PySpark UDFs ===\n")

    df = spark.createDataFrame(EMPLOYEES, SCHEMA)

    print("--- Raw data ---")
    df.show(truncate=False)

    # --- Pattern 1: Function UDF with explicit return type ---
    # Registering with an explicit StringType avoids type inference overhead
    title_case_udf = udf(title_case, StringType())
    salary_band_udf = udf(salary_band, StringType())
    name_length_udf = udf(name_length, IntegerType())

    result = (
        df.withColumn("name_formatted", title_case_udf(col("name")))
        .withColumn("salary_band", salary_band_udf(col("salary")))
        .withColumn("name_length", name_length_udf(col("name")))
    )

    print("--- After UDF transformations ---")
    result.select(
        "id", "name_formatted", "department", "salary", "salary_band", "name_length"
    ).show(truncate=False)

    # --- Pattern 2: Lambda UDF (inline, for simple one-liners) ---
    upper_udf = udf(lambda s: s.upper() if s else None, StringType())

    print("--- Lambda UDF: department to uppercase ---")
    df.withColumn("dept_upper", upper_udf(col("department"))).show(truncate=False)

    # --- Pattern 3: SQL UDF registration ---
    # Registered UDFs are callable in spark.sql() queries
    spark.udf.register("title_case_sql", title_case, StringType())
    spark.udf.register("salary_band_sql", salary_band, StringType())

    df.createOrReplaceTempView("employees")

    print("--- SQL UDF variant ---")
    spark.sql("""
        SELECT
            id,
            title_case_sql(name)    AS name,
            department,
            salary,
            salary_band_sql(salary) AS band
        FROM employees
        ORDER BY salary DESC
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

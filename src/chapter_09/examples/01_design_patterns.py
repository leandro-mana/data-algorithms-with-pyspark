"""
Chapter 9: Classic Data Design Patterns

Demonstrates the six fundamental data design patterns used in
big data solutions. Each pattern is implemented as a standalone
function using both RDD and DataFrame APIs where appropriate.

Patterns:
  1. Input-Map-Output           — transform/normalize records
  2. Input-Filter-Output        — keep/discard records by predicate
  3. Input-Map-Reduce-Output    — aggregate values by key
  4. Input-Multiple-Maps-Reduce — reduce-side join of two datasets
  5. Input-Map-Combiner-Reduce  — combiner-safe averaging (monoid)
  6. Input-MapPartitions-Reduce — partition-level summarization
"""

import sys
from typing import NamedTuple

from pyspark.rdd import RDD
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

FEMALE_CODES = {"0", "f", "female"}
MALE_CODES = {"1", "m", "male"}


class Employee(NamedTuple):
    name: str
    gender: str
    age: int
    salary: int


# ---------------------------------------------------------------------------
# Pattern 1: Input-Map-Output — normalize gender field
# ---------------------------------------------------------------------------


def normalize_gender(raw_gender: str) -> str:
    """Normalize varied gender representations to a canonical form."""
    lowered = raw_gender.strip().lower()
    if lowered in FEMALE_CODES:
        return "female"
    if lowered in MALE_CODES:
        return "male"
    return "unknown"


def parse_employee(line: str) -> Employee:
    """Parse a CSV line into an Employee with normalized gender."""
    tokens = line.split(",")
    return Employee(
        name=tokens[0],
        gender=normalize_gender(tokens[1]),
        age=int(tokens[2]),
        salary=int(tokens[3]),
    )


def pattern_map(rdd: RDD) -> RDD:
    """Input-Map-Output: normalize gender field via map()."""
    return rdd.map(parse_employee)


# ---------------------------------------------------------------------------
# Pattern 2: Input-Filter-Output — keep high earners
# ---------------------------------------------------------------------------


def pattern_filter_rdd(employees_rdd: RDD, min_salary: int = 60000) -> RDD:
    """Input-Filter-Output (RDD): keep employees above a salary threshold."""
    return employees_rdd.filter(lambda e: e.salary >= min_salary)


def pattern_filter_df(employees_df: DataFrame, min_salary: int = 60000) -> DataFrame:
    """Input-Filter-Output (DataFrame): filter() and where() are equivalent."""
    return employees_df.filter(F.col("salary") >= min_salary)


# ---------------------------------------------------------------------------
# Pattern 3: Input-Map-Reduce-Output — average salary by age group
# ---------------------------------------------------------------------------

AGE_BRACKETS = [
    (0, 25, "0-25"),
    (26, 35, "26-35"),
    (36, 45, "36-45"),
    (46, 55, "46-55"),
    (56, 100, "56-100"),
]


def age_to_group(age: int) -> str:
    """Map an age to its bracket label."""
    for low, high, label in AGE_BRACKETS:
        if low <= age <= high:
            return label
    return "unknown"


def pattern_map_reduce(employees_rdd: RDD) -> RDD:
    """Input-Map-Reduce-Output: average salary per age group.

    Uses combineByKey with (sum, count) for a correct, combiner-safe average.
    """
    age_salary = employees_rdd.map(lambda e: (age_to_group(e.age), e.salary))

    combined = age_salary.combineByKey(
        lambda v: (v, 1),
        lambda c, v: (c[0] + v, c[1] + 1),
        lambda c1, c2: (c1[0] + c2[0], c1[1] + c2[1]),
    )
    return combined.mapValues(lambda sc: round(sc[0] / sc[1], 2))


# ---------------------------------------------------------------------------
# Pattern 4: Input-Multiple-Maps-Reduce-Output — reduce-side join
# ---------------------------------------------------------------------------


def parse_movie(line: str) -> tuple[str, str]:
    """Parse movie CSV: movie_id,movie_name → (movie_id, movie_name)."""
    tokens = line.split(",", 1)
    return (tokens[0], tokens[1])


def parse_rating(line: str) -> tuple[str, int]:
    """Parse rating CSV: movie_id,rating,user_id → (movie_id, rating)."""
    tokens = line.split(",")
    return (tokens[0], int(tokens[1]))


def pattern_reduce_side_join(movies_rdd: RDD, ratings_rdd: RDD) -> RDD:
    """Input-Multiple-Maps-Reduce-Output: join movies with ratings,
    then compute average rating per movie.

    Steps:
      1. map1() — create (movie_id, movie_name) pairs
      2. map2() — create (movie_id, rating) pairs
      3. join on movie_id
      4. groupByKey + mapValues for average rating
    """
    joined = ratings_rdd.join(movies_rdd)
    # joined: (movie_id, (rating, movie_name))

    grouped = joined.mapValues(lambda v: (v[1], v[0])).groupByKey()
    # grouped: (movie_id, [(movie_name, rating), ...])

    def avg_rating(values: list) -> tuple[str, float]:
        vals = list(values)
        movie_name = vals[0][0]
        total = sum(v[1] for v in vals)
        return (movie_name, round(total / len(vals), 2))

    return grouped.mapValues(avg_rating)


# ---------------------------------------------------------------------------
# Pattern 5: Input-Map-Combiner-Reduce-Output — combiner correctness
# ---------------------------------------------------------------------------


def pattern_combiner_wrong(rdd: RDD) -> RDD:
    """WRONG: reduceByKey with mean — NOT associative.

    AVG(AVG(a, b), AVG(c, d, e)) != AVG(a, b, c, d, e)
    """
    gender_salary = rdd.map(lambda e: (e.gender, e.salary))
    return gender_salary.reduceByKey(lambda x, y: (x + y) / 2)


def pattern_combiner_correct(rdd: RDD) -> RDD:
    """CORRECT: reduceByKey with (sum, count) monoid — associative + commutative."""
    gender_salary = rdd.map(lambda e: (e.gender, (e.salary, 1)))
    reduced = gender_salary.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    return reduced.mapValues(lambda sc: round(sc[0] / sc[1], 2))


# ---------------------------------------------------------------------------
# Pattern 6: Input-MapPartitions-Reduce-Output — partition summarization
# ---------------------------------------------------------------------------


def summarize_partition(partition) -> list[tuple[str, tuple[int, int]]]:
    """Summarize an entire partition into a compact dict, then emit (key, value) pairs.

    This processes thousands of records but emits at most 3 pairs
    (one per gender), dramatically reducing shuffle volume.
    """
    summary: dict[str, tuple[int, int]] = {}
    for emp in partition:
        gender = emp.gender
        prev = summary.get(gender, (0, 0))
        summary[gender] = (prev[0] + emp.salary, prev[1] + 1)
    return list(summary.items())


def pattern_mappartitions(employees_rdd: RDD) -> RDD:
    """Input-MapPartitions-Reduce-Output: salary stats by gender.

    Uses mapPartitions to create compact summaries per partition,
    then reduceByKey to merge across partitions.
    """
    partition_summaries = employees_rdd.mapPartitions(summarize_partition)
    merged = partition_summaries.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    return merged.mapValues(lambda sc: (sc[0], sc[1], round(sc[0] / sc[1], 2)))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def load_rdd_skip_header(sc, path: str) -> RDD:
    """Load a text file as RDD, skipping the CSV header."""
    rdd = sc.textFile(path)
    header = rdd.first()
    return rdd.filter(lambda line: line != header)


def main() -> None:
    """Run all six classic data design patterns."""
    spark: SparkSession = create_spark_session(__file__)
    sc = spark.sparkContext

    emp_path = str(get_chapter_data_path("chapter_09", "employees.csv"))
    movies_path = str(get_chapter_data_path("chapter_09", "movies.csv"))
    ratings_path = str(get_chapter_data_path("chapter_09", "ratings.csv"))

    if len(sys.argv) > 1:
        emp_path = sys.argv[1]

    print("=" * 60)
    print("Chapter 9: Classic Data Design Patterns")
    print("=" * 60)

    # --- Pattern 1: Input-Map-Output ---
    print("\n--- Pattern 1: Input-Map-Output (normalize gender) ---")
    raw_rdd = load_rdd_skip_header(sc, emp_path)
    employees_rdd = pattern_map(raw_rdd)
    for emp in employees_rdd.collect():
        print(f"  {emp.name:8s} gender={emp.gender:8s} age={emp.age} salary={emp.salary}")

    # --- Pattern 2: Input-Filter-Output ---
    print("\n--- Pattern 2: Input-Filter-Output (salary >= 60000) ---")
    high_earners = pattern_filter_rdd(employees_rdd)
    for emp in high_earners.collect():
        print(f"  {emp.name:8s} salary={emp.salary}")

    print("\n  DataFrame equivalent (filter / where):")
    emp_df = spark.createDataFrame(employees_rdd.collect(), ["name", "gender", "age", "salary"])
    filtered_df = pattern_filter_df(emp_df)
    filtered_df.show(truncate=False)

    # --- Pattern 3: Input-Map-Reduce-Output ---
    print("--- Pattern 3: Input-Map-Reduce-Output (avg salary by age group) ---")
    avg_by_age = pattern_map_reduce(employees_rdd)
    for group, avg in sorted(avg_by_age.collect()):
        print(f"  {group:8s} avg_salary={avg}")

    # --- Pattern 4: Input-Multiple-Maps-Reduce-Output ---
    print("\n--- Pattern 4: Reduce-Side Join (avg movie rating) ---")
    movies_rdd = load_rdd_skip_header(sc, movies_path).map(parse_movie)
    ratings_rdd = load_rdd_skip_header(sc, ratings_path).map(parse_rating)
    avg_ratings = pattern_reduce_side_join(movies_rdd, ratings_rdd)
    for movie_id, (name, avg) in sorted(avg_ratings.collect()):
        print(f"  Movie {movie_id}: {name:25s} avg_rating={avg}")

    # --- Pattern 5: Input-Map-Combiner-Reduce-Output ---
    print("\n--- Pattern 5: Combiner Correctness (avg salary by gender) ---")
    wrong = pattern_combiner_wrong(employees_rdd)
    correct = pattern_combiner_correct(employees_rdd)
    print("  WRONG (mean of means — NOT associative):")
    for gender, avg in sorted(wrong.collect()):
        print(f"    {gender:8s} avg_salary={avg}")
    print("  CORRECT (sum, count monoid):")
    for gender, avg in sorted(correct.collect()):
        print(f"    {gender:8s} avg_salary={avg}")

    # --- Pattern 6: Input-MapPartitions-Reduce-Output ---
    print("\n--- Pattern 6: MapPartitions Summarization (salary by gender) ---")
    partitioned_rdd = sc.parallelize(employees_rdd.collect(), numSlices=3)
    gender_stats = pattern_mappartitions(partitioned_rdd)
    for gender, (total, count, avg) in sorted(gender_stats.collect()):
        print(f"  {gender:8s} total={total:>7d}  count={count}  avg={avg}")

    print()
    spark.stop()


if __name__ == "__main__":
    main()

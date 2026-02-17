"""
Chapter 4: Movie Average Rating — The Monoid Pattern

Demonstrates finding the average movie rating per user using three
reduction approaches, all based on the (sum, count) monoid pattern.

Key insight: mean() is NOT a monoid — the mean of means != mean of all values.
Solution: use (sum, count) pairs, which ARE monoidal, then compute mean last.

Wrong:  rdd.reduceByKey(lambda x, y: (x + y) / 2)  — produces incorrect results!
Right:  use (sum, count) pattern → mapValues(sum / count)
"""

import sys

from src.common.data_loader import get_chapter_data_path
from src.common.spark_session import create_spark_session

# ---------------------------------------------------------------------------
# Parsing function — transforms raw CSV records into (key, value) pairs
# ---------------------------------------------------------------------------


def parse_rating(line: str) -> tuple[str, float]:
    """Parse a CSV rating record into (userId, rating).

    Input format: userId,movieId,rating,timestamp
    """
    tokens = line.split(",")
    return (tokens[0], float(tokens[2]))


# ---------------------------------------------------------------------------
# Three monoidal reduction approaches — all produce the same correct result
# ---------------------------------------------------------------------------


def avg_with_reduce_by_key(rdd):
    """reduceByKey with (sum, count) monoid pattern.

    Step 1: map each rating to (userId, (rating, 1))
    Step 2: reduceByKey to aggregate (sum, count)
    Step 3: mapValues to compute average
    """
    sum_count = rdd.mapValues(lambda v: (v, 1)).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    return sum_count.mapValues(lambda sc: round(sc[0] / sc[1], 2))


def avg_with_aggregate_by_key(rdd):
    """aggregateByKey with (sum, count) combined type.

    zero_value: (0.0, 0) — neutral element for the monoid
    seq_func: merge a rating V into combined type C within a partition
    comb_func: merge two combined types C across partitions
    """
    sum_count = rdd.aggregateByKey(
        (0.0, 0),
        lambda c, v: (c[0] + v, c[1] + 1),
        lambda c1, c2: (c1[0] + c2[0], c1[1] + c2[1]),
    )
    return sum_count.mapValues(lambda sc: round(sc[0] / sc[1], 2))


def avg_with_combine_by_key(rdd):
    """combineByKey — the most general approach.

    create_combiner: turn first value V into combined type C = (V, 1)
    merge_value: merge a value V into existing C
    merge_combiners: merge two Cs from different partitions
    """
    sum_count = rdd.combineByKey(
        lambda v: (v, 1),
        lambda c, v: (c[0] + v, c[1] + 1),
        lambda c1, c2: (c1[0] + c2[0], c1[1] + c2[1]),
    )
    return sum_count.mapValues(lambda sc: round(sc[0] / sc[1], 2))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Compute average movie rating per user using three approaches."""
    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    input_path = str(get_chapter_data_path("chapter_04", "ratings.csv"))
    if len(sys.argv) > 1:
        input_path = sys.argv[1]

    # Read CSV, skip header, parse into (userId, rating) pairs
    raw_rdd = sc.textFile(input_path)
    header = raw_rdd.first()
    ratings_rdd = raw_rdd.filter(lambda line: line != header).map(parse_rating)

    print("=== Movie Average Rating: The Monoid Pattern ===\n")
    print(f"Input: {input_path}")
    print(f"Total ratings: {ratings_rdd.count()}\n")

    print("Sample ratings (userId, rating):")
    for r in ratings_rdd.take(5):
        print(f"  {r}")
    print()

    # --- Demonstrate the WRONG approach ---
    print("--- WRONG: reduceByKey with mean (NOT a monoid) ---")
    wrong_avg = ratings_rdd.reduceByKey(lambda x, y: (x + y) / 2)
    for user_id, avg in sorted(wrong_avg.collect()):
        print(f"  User {user_id}: {avg:.2f}  (INCORRECT)")
    print()

    # --- Three CORRECT approaches using (sum, count) monoid ---
    approaches = [
        ("reduceByKey + (sum, count)", avg_with_reduce_by_key),
        ("aggregateByKey", avg_with_aggregate_by_key),
        ("combineByKey", avg_with_combine_by_key),
    ]

    for name, func in approaches:
        result = sorted(func(ratings_rdd).collect())
        print(f"--- {name} ---")
        for user_id, avg in result:
            print(f"  User {user_id}: {avg}")
        print()

    spark.stop()


if __name__ == "__main__":
    main()

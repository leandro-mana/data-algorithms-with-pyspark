"""
Chapter 4: Reduction Warmup — Four Approaches Compared

Demonstrates four reduction transformations solving the same problem:
sum values per key using reduceByKey(), groupByKey(), aggregateByKey(),
and combineByKey().

All four produce the same result but with different performance characteristics.
"""

from operator import add

from src.common.spark_session import create_spark_session

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

SAMPLE_DATA: list[tuple[str, int]] = [
    ("alex", 2),
    ("alex", 4),
    ("alex", 8),
    ("jane", 3),
    ("jane", 7),
    ("rafa", 1),
    ("rafa", 3),
    ("rafa", 5),
    ("rafa", 6),
    ("clint", 9),
]


# ---------------------------------------------------------------------------
# Four reduction approaches — all produce the same result
# ---------------------------------------------------------------------------


def sum_with_reduce_by_key(rdd):
    """reduceByKey: merges values locally per partition before shuffle.

    Source: RDD[(K, V)] → Target: RDD[(K, V)]
    Constraint: output type V must equal input type V.
    """
    return rdd.reduceByKey(add)


def sum_with_group_by_key(rdd):
    """groupByKey: groups ALL values per key, then applies mapValues.

    Source: RDD[(K, V)] → Target: RDD[(K, [V])]
    Warning: sends all data over network before aggregation.
    """
    return rdd.groupByKey().mapValues(lambda values: sum(values))


def sum_with_aggregate_by_key(rdd):
    """aggregateByKey: uses zero_value + seq_func + comb_func.

    Source: RDD[(K, V)] → Target: RDD[(K, C)]
    Advantage: output type C can differ from input type V.
    """
    return rdd.aggregateByKey(
        0,  # zero_value
        lambda accumulator, value: accumulator + value,  # seq_func (within partition)
        lambda acc1, acc2: acc1 + acc2,  # comb_func (across partitions)
    )


def sum_with_combine_by_key(rdd):
    """combineByKey: the most general reduction transformation.

    Source: RDD[(K, V)] → Target: RDD[(K, C)]
    Uses: create_combiner + merge_value + merge_combiners.
    """
    return rdd.combineByKey(
        lambda v: v,  # create_combiner: V → C
        lambda c, v: c + v,  # merge_value: (C, V) → C
        lambda c1, c2: c1 + c2,  # merge_combiners: (C, C) → C
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Compare four reduction transformations on the same data."""
    spark = create_spark_session(__file__)
    sc = spark.sparkContext

    rdd = sc.parallelize(SAMPLE_DATA)

    print("=== Reduction Warmup: Four Approaches Compared ===\n")
    print(f"Input data: {SAMPLE_DATA}\n")
    print("Expected: alex=14, jane=10, rafa=15, clint=9\n")

    approaches = [
        ("reduceByKey()", sum_with_reduce_by_key),
        ("groupByKey()", sum_with_group_by_key),
        ("aggregateByKey()", sum_with_aggregate_by_key),
        ("combineByKey()", sum_with_combine_by_key),
    ]

    for name, func in approaches:
        result = sorted(func(rdd).collect())
        print(f"  {name:25s} → {result}")

    spark.stop()


if __name__ == "__main__":
    main()

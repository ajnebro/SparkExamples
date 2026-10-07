"""Closures fix: reproducible random numbers that do not depend on the data distribution."""

import random

from pyspark import SparkConf, SparkContext


def random_from_element(element: int, seed: int) -> int:
    """Generate a random number whose value only depends on the element and the seed.

    Args:
        element: Element used to derive the seed of a new generator.
        seed: Base seed.

    Returns:
        A random number in [0, 100].
    """
    return random.Random(seed + element).randint(0, 100)


def generate_reproducible_numbers(
    spark_context: SparkContext, data: list[int], seed: int, num_slices: int | None = None
) -> list[int]:
    """Generate one random number per element, independently of the partitioning.

    Unlike `closure_shared_generator`, no object with state is captured: a new generator
    is created for each element, so the result does not depend on the number of workers.

    Args:
        spark_context: Active Spark context.
        data: Elements to map.
        seed: Base seed.
        num_slices: Number of partitions of the RDD. Spark's default is used if None.

    Returns:
        A random number in [0, 100] for each element.
    """
    return (
        spark_context.parallelize(data, num_slices)
        .map(lambda x: random_from_element(x, seed))
        .collect()
    )


def main() -> None:
    """Run the example."""
    conf = SparkConf().setAppName("ClosureReproducibleRandom").setMaster("local[4]")
    spark_context = SparkContext(conf=conf)
    try:
        spark_context.setLogLevel("ERROR")
        data = list(range(1, 13))
        result = generate_reproducible_numbers(spark_context, data, 42)
        print("Generated reproducible numbers:", result)
    finally:
        spark_context.stop()


if __name__ == "__main__":
    main()

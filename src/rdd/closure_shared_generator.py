"""Closures pitfall: a captured random generator is copied, with its state, to every worker."""

import random

from pyspark import SparkConf, SparkContext


def generate_with_captured_generator(
    spark_context: SparkContext, data: list[int], seed: int, num_slices: int | None = None
) -> list[int]:
    """Generate one random number per element using a generator captured by the closure.

    The generator is serialized along with its state, so every partition receives its own
    copy starting from the same state. As a result, the same sequence is repeated in each
    partition, and the output depends on how the data is distributed.

    Args:
        spark_context: Active Spark context.
        data: Elements to map (their values are ignored).
        seed: Seed of the generator created in the driver.
        num_slices: Number of partitions of the RDD. Spark's default is used if None.

    Returns:
        A random number in [0, 100] for each element.
    """
    generator = random.Random(seed)
    return (
        spark_context.parallelize(data, num_slices)
        .map(lambda _: generator.randint(0, 100))
        .collect()
    )


def main() -> None:
    """Run the example."""
    conf = SparkConf().setAppName("ClosureSharedGenerator").setMaster("local[4]")
    spark_context = SparkContext(conf=conf)
    try:
        spark_context.setLogLevel("ERROR")
        data = list(range(1, 13))
        print("Generated numbers:", generate_with_captured_generator(spark_context, data, 42))
    finally:
        spark_context.stop()


if __name__ == "__main__":
    main()

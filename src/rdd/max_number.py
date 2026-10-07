"""Find the maximum of a list of numbers with Apache Spark."""

from pyspark import SparkConf, SparkContext


def max_number(spark_context: SparkContext, numbers: list[int]) -> int:
    """Find the largest integer of a list using an RDD.

    Args:
        spark_context: Active Spark context.
        numbers: Integers to compare. It must not be empty.

    Returns:
        The largest number.

    Raises:
        ValueError: If `numbers` is empty.
    """
    if not numbers:
        raise ValueError("numbers must not be empty")

    return spark_context.parallelize(numbers).reduce(lambda a, b: a if a > b else b)


def main() -> None:
    """Run the example."""
    conf = SparkConf().setAppName("MaxNumber").setMaster("local[*]")
    spark_context = SparkContext(conf=conf)
    try:
        spark_context.setLogLevel("ERROR")
        print(f"The maximum is {max_number(spark_context, [1, 2, 3, 4, 5, 6, 7, 8])}")
    finally:
        spark_context.stop()


if __name__ == "__main__":
    main()

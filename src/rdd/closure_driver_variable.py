"""Closures: a driver variable captured by a function is copied to the workers."""

from pyspark import SparkConf, SparkContext


def multiply_by_factor(spark_context: SparkContext, data: list[int], factor: int) -> list[int]:
    """Multiply every element by a factor defined in the driver.

    The lambda captures `factor`: it is serialized and a copy is sent to the workers. The
    copy is read-only in practice, since changing it in a worker would not affect the driver.

    Args:
        spark_context: Active Spark context.
        data: Integers to multiply.
        factor: Value stored in the driver and captured by the closure.

    Returns:
        The multiplied integers, in the original order.
    """
    return spark_context.parallelize(data).map(lambda x: x * factor).collect()


def count_with_captured_list(spark_context: SparkContext, data: list[int]) -> list[int]:
    """Try to count the elements by modifying a list captured by the closure.

    It does not work: every task works on its own copy of the list, so the list of the
    driver is never modified. Use an accumulator instead (see `add_numbers_with_accumulator`).

    Args:
        spark_context: Active Spark context.
        data: Elements to process.

    Returns:
        The list of the driver after the action, which is still `[0]`.
    """
    counter = [0]

    def increment(_: int) -> None:
        counter[0] += 1

    spark_context.parallelize(data).foreach(increment)

    return counter


def main() -> None:
    """Run the example."""
    conf = SparkConf().setAppName("ClosureDriverVariable").setMaster("local[4]")
    spark_context = SparkContext(conf=conf)
    try:
        spark_context.setLogLevel("ERROR")
        data = [1, 2, 3, 4]
        print("Original data:", data)
        print("After multiplying by factor:", multiply_by_factor(spark_context, data, 10))
        print("Counter modified in the workers:", count_with_captured_list(spark_context, data))
    finally:
        spark_context.stop()


if __name__ == "__main__":
    main()

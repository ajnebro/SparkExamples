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


def main() -> None:
    """Run the example."""
    conf = SparkConf().setAppName("ClosureDriverVariable").setMaster("local[4]")
    spark_context = SparkContext(conf=conf)
    try:
        spark_context.setLogLevel("ERROR")
        data = [1, 2, 3, 4]
        print("Original data:", data)
        print("After multiplying by factor:", multiply_by_factor(spark_context, data, 10))
    finally:
        spark_context.stop()


if __name__ == "__main__":
    main()

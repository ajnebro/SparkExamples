"""Sum a list of numbers with Apache Spark."""

from pyspark import SparkConf, SparkContext


def add_numbers(spark_context: SparkContext, numbers: list[int]) -> int:
    """Sum a list of integers using an RDD.

    Args:
        spark_context: Active Spark context.
        numbers: Integers to add.

    Returns:
        The sum of all numbers.
    """
    return spark_context \
        .parallelize(numbers)\
        .reduce(lambda a, b: a + b)


def main() -> None:
    """Run the example."""
    conf = SparkConf().setAppName("AddNumbers").setMaster("local[*]")
    spark_context = SparkContext(conf=conf)
    try:
        spark_context.setLogLevel("ERROR")
        print(f"The sum is {add_numbers(spark_context, [1, 2, 3, 4, 5, 6, 7, 8])}")
    finally:
        spark_context.stop()


if __name__ == "__main__":
    main()

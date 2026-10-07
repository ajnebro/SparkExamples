"""Sum a list of numbers with Apache Spark, counting the operations with an accumulator."""

from pyspark import SparkConf, SparkContext


def add_numbers_counting_operations(
    spark_context: SparkContext, numbers: list[int]
) -> tuple[int, int]:
    """Sum a list of integers and count how many additions are performed.

    The accumulator is a shared variable that workers can only add to, and whose value is
    read in the driver. It is updated inside an action (`reduce`), where Spark guarantees
    that each task update is applied only once.

    Args:
        spark_context: Active Spark context.
        numbers: Integers to add. It must not be empty.

    Returns:
        A tuple with the sum of all numbers and the number of additions performed.

    Raises:
        ValueError: If `numbers` is empty.
    """
    if not numbers:
        raise ValueError("numbers must not be empty")

    operations = spark_context.accumulator(0)

    def add(first: int, second: int) -> int:
        operations.add(1)
        return first + second

    total = spark_context.parallelize(numbers).reduce(add)

    return total, operations.value


def main() -> None:
    """Run the example."""
    conf = SparkConf().setAppName("AddNumbersWithAccumulator").setMaster("local[*]")
    spark_context = SparkContext(conf=conf)
    try:
        spark_context.setLogLevel("ERROR")
        total, operations = add_numbers_counting_operations(spark_context, [1, 2, 3, 4, 5, 6, 7, 8])
        print(f"The sum is {total}")
        print(f"The number of reduce operations is {operations}")
    finally:
        spark_context.stop()


if __name__ == "__main__":
    main()

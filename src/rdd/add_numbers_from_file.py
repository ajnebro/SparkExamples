"""Sum the numbers stored in a text file with Apache Spark and measure the computing time."""

import time

from pyspark import SparkConf, SparkContext

DEFAULT_INPUT_FILE = "data/numbers.txt"


def add_numbers_from_file(spark_context: SparkContext, file_name: str) -> int:
    """Sum the integers of a text file that contains one number per line.

    Args:
        spark_context: Active Spark context.
        file_name: Path of the text file.

    Returns:
        The sum of all the numbers in the file.
    """
    return spark_context.textFile(file_name).map(lambda line: int(line)).reduce(lambda a, b: a + b)


def main(file_name: str = DEFAULT_INPUT_FILE) -> None:
    """Run the example.

    Args:
        file_name: Path of the text file with the numbers to add.
    """
    conf = SparkConf().setAppName("AddNumbersFromFile").setMaster("local[*]")
    spark_context = SparkContext(conf=conf)
    try:
        spark_context.setLogLevel("ERROR")
        start_time = time.perf_counter()
        result = add_numbers_from_file(spark_context, file_name)
        computing_time = time.perf_counter() - start_time
        print(f"The sum is {result}")
        print(f"Computing time: {computing_time:.3f} s")
    finally:
        spark_context.stop()


if __name__ == "__main__":
    main()

"""Count and extract the lines of a text file that contain a given word."""

import sys

from pyspark import RDD, SparkConf, SparkContext

OUTPUT_DIRECTORY = "Dulcinea.txt"


def filter_lines_containing(lines: RDD[str], word: str) -> RDD[str]:
    """Keep only the lines that contain a word.

    Args:
        lines: RDD with the lines of a text.
        word: Word (or text) to look for. The search is case sensitive.

    Returns:
        An RDD with the lines that contain the word.
    """
    return lines.filter(lambda line: word in line)


def count_lines_containing(lines: RDD[str], word: str) -> int:
    """Count the lines that contain a word.

    Args:
        lines: RDD with the lines of a text.
        word: Word (or text) to look for. The search is case sensitive.

    Returns:
        The number of lines that contain the word.
    """
    return filter_lines_containing(lines, word).count()


def main(input_file: str) -> None:
    """Run the example.

    Args:
        input_file: Path of the text file to analyze.
    """
    conf = SparkConf().setAppName("CountLinesContainingWord")
    spark_context = SparkContext(conf=conf)
    try:
        spark_context.setLogLevel("ERROR")
        # The RDD is used by several actions, so it is kept in memory
        lines = spark_context.textFile(input_file).persist()
        print(f"Lines containing 'Quijote': {count_lines_containing(lines, 'Quijote')}")
        print(f"Lines containing 'Sancho': {count_lines_containing(lines, 'Sancho')}")
        filter_lines_containing(lines, "Dulcinea").saveAsTextFile(OUTPUT_DIRECTORY)
    finally:
        spark_context.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: spark-submit count_lines_containing_word.py <file>")
    main(sys.argv[1])

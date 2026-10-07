"""Count and extract the lines of a text file that contain a given word."""

import argparse
from collections.abc import Sequence

from pyspark import RDD, SparkConf, SparkContext

DEFAULT_COUNT_WORDS = ("Quijote", "Sancho")
DEFAULT_EXTRACT_WORD = "Dulcinea"
DEFAULT_OUTPUT_DIRECTORY = "output/lines_with_word"


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


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the command line arguments.

    Args:
        argv: Arguments to parse. `sys.argv[1:]` is used if None.

    Returns:
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_file", help="text file to analyze")
    parser.add_argument("--count-words", nargs="+", default=list(DEFAULT_COUNT_WORDS))
    parser.add_argument("--extract-word", default=DEFAULT_EXTRACT_WORD)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_DIRECTORY)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """Run the example.

    Args:
        argv: Command line arguments. `sys.argv[1:]` is used if None.
    """
    args = parse_args(argv)
    conf = SparkConf().setAppName("CountLinesContainingWord")
    spark_context = SparkContext(conf=conf)
    try:
        spark_context.setLogLevel("ERROR")
        # The RDD is used by several actions, so it is kept in memory
        lines = spark_context.textFile(args.input_file).persist()
        for word in args.count_words:
            print(f"Lines containing '{word}': {count_lines_containing(lines, word)}")
        filter_lines_containing(lines, args.extract_word).saveAsTextFile(args.output)
    finally:
        spark_context.stop()


if __name__ == "__main__":
    main()

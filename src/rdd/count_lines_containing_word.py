"""Count and extract the lines of a text file that contain a given word."""

from collections.abc import Sequence

from pyspark import RDD, SparkConf, SparkContext

DEFAULT_INPUT_FILE = "data/quijote.txt"
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


def main(
    file_name: str = DEFAULT_INPUT_FILE,
    count_words: Sequence[str] = DEFAULT_COUNT_WORDS,
    extract_word: str = DEFAULT_EXTRACT_WORD,
    output_directory: str = DEFAULT_OUTPUT_DIRECTORY,
) -> None:
    """Run the example.

    Args:
        file_name: Path of the text file to analyze.
        count_words: Words whose lines are counted.
        extract_word: Word whose lines are saved.
        output_directory: Directory where the lines are saved. It must not exist.
    """
    conf = SparkConf().setAppName("CountLinesContainingWord")
    spark_context = SparkContext(conf=conf)
    try:
        spark_context.setLogLevel("ERROR")
        # The RDD is used by several actions, so it is kept in memory
        lines = spark_context.textFile(file_name).persist()
        for word in count_words:
            print(f"Lines containing '{word}': {count_lines_containing(lines, word)}")
        filter_lines_containing(lines, extract_word).saveAsTextFile(output_directory)
    finally:
        spark_context.stop()


if __name__ == "__main__":
    main()

"""Count the words of a text file and show the most frequent ones."""

import re
import time

from pyspark import RDD, SparkConf, SparkContext

DEFAULT_INPUT_FILE = "data/quijote.txt"
DEFAULT_TOP = 100


def split_into_words(line: str) -> list[str]:
    """Split a line into lowercase words, ignoring spaces and punctuation.

    Args:
        line: Line of text.

    Returns:
        The words of the line. `\\w` matches letters with accents, such as "á" or "ñ".
    """
    return re.findall(r"\w+", line.lower())


def count_words(lines: RDD[str]) -> RDD[tuple[str, int]]:
    """Count how many times each word appears.

    Args:
        lines: RDD with the lines of a text.

    Returns:
        An RDD of (word, count) pairs.
    """
    # fmt: off
    return (
        lines
        .flatMap(split_into_words)
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
    )
    # fmt: on


def most_frequent_words(word_counts: RDD[tuple[str, int]], top: int) -> list[tuple[str, int]]:
    """Get the most frequent words, breaking ties alphabetically.

    `takeOrdered` keeps only the best `top` pairs of each partition, so the whole RDD does not
    need to be sorted.

    Args:
        word_counts: RDD of (word, count) pairs.
        top: Number of words to return.

    Returns:
        The `top` pairs with the highest counts, from the most to the least frequent.
    """
    return word_counts.takeOrdered(top, key=lambda pair: (-pair[1], pair[0]))


def main(file_name: str = DEFAULT_INPUT_FILE, top: int = DEFAULT_TOP) -> None:
    """Run the example.

    Args:
        file_name: Path of the text file.
        top: Number of words to show.
    """
    # fmt: off
    conf = (
        SparkConf()
        .setAppName("WordCount")
        .setMaster("local[*]")
    )
    # fmt: on
    spark_context = SparkContext(conf=conf)
    try:
        spark_context.setLogLevel("ERROR")
        start_time = time.perf_counter()
        word_counts = count_words(spark_context.textFile(file_name))
        result = most_frequent_words(word_counts, top)
        print(f"Computing time: {time.perf_counter() - start_time:.3f} s")
        for word, count in result:
            print(f"{word}: {count}")
    finally:
        spark_context.stop()


if __name__ == "__main__":
    main()

"""Count the words of a text file and show the most frequent ones, with the DataFrame API.

It is the DataFrame version of `rdd.word_count`: both split the text in the same way and print
the same result, so they can be compared. Here Spark knows the columns and the operations, so it
optimizes the query (Catalyst) instead of running the Python functions of the RDD version.

About the number of partitions
------------------------------
A `groupBy` needs a shuffle, whose number of partitions is set by `spark.sql.shuffle.partitions`
(200 by default). The default is meant for a cluster with a lot of data; with a book in `local`
mode, scheduling 200 tasks costs more than counting the words, so `main` sets a small value.
"""

import time

from pyspark.sql import DataFrame, SparkSession, functions

DEFAULT_INPUT_FILE = "data/quijote.txt"
DEFAULT_TOP = 100
SHUFFLE_PARTITIONS = 4
WORD_PATTERN = r"(?U)\w+"


def count_words(lines: DataFrame) -> DataFrame:
    """Count how many times each word appears, ignoring case and punctuation.

    A word is a sequence of letters (with accents), digits or underscores, like `\\w` in the
    RDD version.

    Args:
        lines: DataFrame with a column "value" that holds a line of text.

    Returns:
        A DataFrame with the columns "word" and "count", sorted from the most to the least
        frequent word (ties sorted alphabetically).
    """
    lower_case_text = functions.lower("value")
    words = lines.select(
        functions.explode(
            functions.regexp_extract_all(
                lower_case_text, functions.lit(WORD_PATTERN), functions.lit(0)
            )
        ).alias("word")
    )
    return words.groupBy("word").count().orderBy(functions.desc("count"), "word")


def main(file_name: str = DEFAULT_INPUT_FILE, top: int = DEFAULT_TOP) -> None:
    """Run the example.

    Args:
        file_name: Path of the text file.
        top: Number of words to show.
    """
    spark_session = (
        SparkSession.builder.appName("WordCountWithDataFrame")
        .master("local[*]")
        # The default is 200 partitions, too many for this data (see the module docstring)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        start_time = time.perf_counter()
        result = count_words(spark_session.read.text(file_name)).limit(top).collect()
        print(f"Computing time: {time.perf_counter() - start_time:.3f} s")
        for row in result:
            print(f"{row['word']}: {row['count']}")
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()

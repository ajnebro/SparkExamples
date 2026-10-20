"""Count the words of a text file, leaving out the stop words, with the DataFrame API.

The most frequent words of a text are usually the stop words: articles, prepositions and
conjunctions such as "que", "de" or "y". They appear everywhere, so they say nothing about the
text. This variant of `dataframes.word_count_with_dataframe` removes them before counting, and
the most frequent words that remain ("don", "quijote", "sancho"...) are meaningful.

The removal is done by `StopWordsRemover`, a Spark ML transformer. It works on a column of
arrays of words, which is what `regexp_extract_all` returns, so it is applied before `explode`.
Spark brings a list of stop words for several languages; here the Spanish one is used. Frequent
words that are not in the list, such as "dijo" or "respondió", remain in the result: pass a
longer list to remove them too.

About the number of partitions
------------------------------
A `groupBy` needs a shuffle, whose number of partitions is set by `spark.sql.shuffle.partitions`
(200 by default). The default is meant for a cluster with a lot of data; with a book in `local`
mode, scheduling 200 tasks costs more than counting the words, so `main` sets a small value.
"""

import time

from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import DataFrame, SparkSession, functions

DEFAULT_INPUT_FILE = "data/quijote.txt"
DEFAULT_TOP = 100
LANGUAGE = "spanish"
SHUFFLE_PARTITIONS = 4
WORD_PATTERN = r"(?U)\w+"


def count_words(lines: DataFrame, stop_words: list[str]) -> DataFrame:
    """Count how many times each word appears, ignoring case, punctuation and stop words.

    A word is a sequence of letters (with accents), digits or underscores, like `\\w` in the
    RDD version.

    Args:
        lines: DataFrame with a column "value" that holds a line of text.
        stop_words: Words that are not counted.

    Returns:
        A DataFrame with the columns "word" and "count", sorted from the most to the least
        frequent word (ties sorted alphabetically).
    """
    words = lines.select(
        functions.regexp_extract_all(
            functions.lower("value"), functions.lit(WORD_PATTERN), functions.lit(0)
        ).alias("words")
    )
    remover = StopWordsRemover(
        inputCol="words", outputCol="filtered", stopWords=stop_words, locale="es"
    )

    # fmt: off
    return (
        remover
        .transform(words)
        .select(functions.explode("filtered").alias("word"))
        .groupBy("word")
        .count()
        .orderBy(functions.desc("count"), "word")
    )
    # fmt: on


def main(file_name: str = DEFAULT_INPUT_FILE, top: int = DEFAULT_TOP) -> None:
    """Run the example.

    Args:
        file_name: Path of the text file.
        top: Number of words to show.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("WordCountWithoutStopWords")
        .master("local[*]")
        # The default is 200 partitions, too many for this data (see the module docstring)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    # fmt: on

    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        stop_words = StopWordsRemover.loadDefaultStopWords(LANGUAGE)
        start_time = time.perf_counter()

        # fmt: off
        result = (
            count_words(spark_session.read.text(file_name), stop_words)
            .limit(top)
            .collect()
        )
        # fmt: on

        print(f"Computing time: {time.perf_counter() - start_time:.3f} s")
        for row in result:
            print(f"{row['word']}: {row['count']}")
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()

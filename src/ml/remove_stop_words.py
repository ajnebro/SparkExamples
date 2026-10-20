"""Remove the stop words of some sentences with the Spark ML `StopWordsRemover`.

Stop words are very frequent words, such as "the" or "a", that carry little meaning. The
remover works on a column of arrays of words, and it removes the words that are in a list. Spark
brings a list for several languages: `StopWordsRemover.loadDefaultStopWords("english")`. The
comparison ignores the case, so "The" and "the" are the same word.

This is the smallest use of the remover. `dataframes.word_count_without_stop_words` uses it to
count the words of a book.
"""

from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import DataFrame, SparkSession

LANGUAGE = "english"
LOCALE = "en"
SAMPLE_SENTENCES = [
    (0, ["I", "saw", "the", "red", "balloon", "the", "a", "any"]),
    (1, ["Mary", "had", "a", "little", "lamb", "this", "or"]),
]


def remove_stop_words(sentences: DataFrame, stop_words: list[str]) -> DataFrame:
    """Remove the stop words from an array of words.

    The locale is set explicitly, because otherwise the case-insensitive comparison depends on
    the default locale of the JVM.

    Args:
        sentences: DataFrame with a column "words", an array of words per row.
        stop_words: Words to remove.

    Returns:
        The same DataFrame with the extra column "filtered".
    """
    remover = StopWordsRemover(
        inputCol="words", outputCol="filtered", stopWords=stop_words, locale=LOCALE
    )

    return remover.transform(sentences)


def main() -> None:
    """Run the example."""
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("RemoveStopWords")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on

    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        sentences = spark_session.createDataFrame(SAMPLE_SENTENCES, ["id", "words"])
        stop_words = StopWordsRemover.loadDefaultStopWords(LANGUAGE)
        print(f"The default list for {LANGUAGE} has {len(stop_words)} stop words")
        remove_stop_words(sentences, stop_words).show(truncate=False)
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()

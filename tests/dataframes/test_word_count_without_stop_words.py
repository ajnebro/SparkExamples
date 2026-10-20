"""Tests for dataframes.word_count_without_stop_words."""

from pathlib import Path

import pytest
from pyspark.errors import PySparkException
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import SparkSession

from dataframes import word_count_with_dataframe
from dataframes.word_count_without_stop_words import DEFAULT_INPUT_FILE, LANGUAGE, count_words


def counts(
    lines: list[str], stop_words: list[str], spark_session: SparkSession
) -> list[tuple[str, int]]:
    """Count the words of some lines and return them as (word, count) pairs."""
    data_frame = spark_session.createDataFrame([(line,) for line in lines], "value string")
    return [(row["word"], row["count"]) for row in count_words(data_frame, stop_words).collect()]


class TestCountWords:
    @pytest.mark.parametrize(
        ("lines", "stop_words", "expected"),
        [
            (["a b a c"], ["b"], [("a", 2), ("c", 1)]),
            (["El hidalgo, el Quijote", "hidalgo"], ["el"], [("hidalgo", 2), ("quijote", 1)]),
            (["Que dijo"], ["que"], [("dijo", 1)]),
            (["función ñandú"], [], [("función", 1), ("ñandú", 1)]),
            (["de la de"], ["de", "la"], []),
        ],
    )
    def test_should_count_words_leaving_out_stop_words(
        self,
        spark_session: SparkSession,
        lines: list[str],
        stop_words: list[str],
        expected: list[tuple[str, int]],
    ) -> None:
        """Words are normalized, stop words are left out whatever their case, the rest is kept."""
        assert counts(lines, stop_words, spark_session) == expected

    def test_should_drop_the_stop_words_of_the_default_list(
        self, spark_session: SparkSession
    ) -> None:
        """With the Spanish list of Spark, "que" and "de" disappear and "sancho" remains."""
        stop_words = StopWordsRemover.loadDefaultStopWords(LANGUAGE)

        result = dict(counts(["Sancho dijo que de molinos"], stop_words, spark_session))

        assert result == {"sancho": 1, "dijo": 1, "molinos": 1}

    def test_should_raise_error_if_value_column_is_missing(
        self, spark_session: SparkSession
    ) -> None:
        """Given a DataFrame without the column "value", the query is rejected."""
        data_frame = spark_session.createDataFrame([("a",)], ["text"])

        with pytest.raises(PySparkException):
            count_words(data_frame, [])

    @pytest.mark.skipif(not Path(DEFAULT_INPUT_FILE).exists(), reason="data/quijote.txt not found")
    def test_should_give_same_counts_as_word_count_minus_stop_words(
        self, spark_session: SparkSession
    ) -> None:
        """The result is the plain word count without the rows of the stop words."""
        stop_words = StopWordsRemover.loadDefaultStopWords(LANGUAGE)
        lines = spark_session.read.text(DEFAULT_INPUT_FILE)

        result = {row["word"]: row["count"] for row in count_words(lines, stop_words).collect()}

        expected = {
            row["word"]: row["count"]
            for row in word_count_with_dataframe.count_words(lines).collect()
            if row["word"] not in stop_words
        }
        assert result == expected

"""Tests for dataframes.word_count_with_dataframe."""

from pathlib import Path

import pytest
from pyspark import SparkContext
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from dataframes.word_count_with_dataframe import DEFAULT_INPUT_FILE, count_words
from rdd.word_count import count_words as count_words_with_rdd


def counts(lines: list[str], spark_session: SparkSession) -> list[tuple[str, int]]:
    """Count the words of some lines and return them as (word, count) pairs."""
    data_frame = spark_session.createDataFrame([(line,) for line in lines], "value string")
    return [(row["word"], row["count"]) for row in count_words(data_frame).collect()]


class TestCountWords:
    @pytest.mark.parametrize(
        ("lines", "expected"),
        [
            (["a b a"], [("a", 2), ("b", 1)]),
            (["El el EL"], [("el", 3)]),
            (["hidalgo, hidalgo. (hidalgo)"], [("hidalgo", 3)]),
            (["una  palabra", "", "  "], [("palabra", 1), ("una", 1)]),
            (["función ñandú"], [("función", 1), ("ñandú", 1)]),
            (["año 2026, año 2027"], [("año", 2), ("2026", 1), ("2027", 1)]),
        ],
    )
    def test_should_normalize_text_before_counting(
        self, spark_session: SparkSession, lines: list[str], expected: list[tuple[str, int]]
    ) -> None:
        """Case, punctuation, blank spaces and empty lines do not create different words."""
        assert counts(lines, spark_session) == expected

    def test_should_sort_by_frequency_then_alphabetically(
        self, spark_session: SparkSession
    ) -> None:
        """The most frequent word comes first, and ties are sorted by word."""
        assert counts(["c b a b c b"], spark_session) == [("b", 3), ("c", 2), ("a", 1)]

    def test_should_return_no_rows_without_words(self, spark_session: SparkSession) -> None:
        """Lines without letters or digits produce no words."""
        assert counts(["", "...", " , "], spark_session) == []

    def test_should_raise_error_if_value_column_is_missing(
        self, spark_session: SparkSession
    ) -> None:
        """Given a DataFrame without the column "value", the query is rejected."""
        data_frame = spark_session.createDataFrame([("a",)], ["text"])

        with pytest.raises(PySparkException):
            count_words(data_frame)

    @pytest.mark.skipif(not Path(DEFAULT_INPUT_FILE).exists(), reason="data/quijote.txt not found")
    def test_should_give_same_result_as_rdd_version(
        self, spark_session: SparkSession, spark_context: SparkContext
    ) -> None:
        """Both APIs count exactly the same words of Don Quijote."""
        data_frame_result = {
            row["word"]: row["count"]
            for row in count_words(spark_session.read.text(DEFAULT_INPUT_FILE)).collect()
        }

        rdd_result = dict(
            count_words_with_rdd(spark_context.textFile(DEFAULT_INPUT_FILE)).collect()
        )

        assert data_frame_result == rdd_result

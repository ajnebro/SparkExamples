"""Tests for ml.remove_stop_words."""

import pytest
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import SparkSession

from ml.remove_stop_words import LANGUAGE, SAMPLE_SENTENCES, remove_stop_words


def filtered(words: list[str], stop_words: list[str], spark_session: SparkSession) -> list[str]:
    """Remove the stop words from a list of words and return the rest."""
    data_frame = spark_session.createDataFrame([(0, words)], ["id", "words"])

    return remove_stop_words(data_frame, stop_words).first()["filtered"]


class TestRemoveStopWords:
    @pytest.mark.parametrize(
        ("words", "stop_words", "expected"),
        [
            (["the", "red", "balloon"], ["the"], ["red", "balloon"]),
            (["The", "red", "THE"], ["the"], ["red"]),
            (["red", "balloon"], ["the"], ["red", "balloon"]),
            (["red", "balloon"], [], ["red", "balloon"]),
            (["the", "a"], ["the", "a"], []),
        ],
    )
    def test_should_remove_only_the_stop_words(
        self,
        spark_session: SparkSession,
        words: list[str],
        stop_words: list[str],
        expected: list[str],
    ) -> None:
        """The stop words are removed ignoring the case, and the rest of the words are kept."""
        assert filtered(words, stop_words, spark_session) == expected

    def test_should_remove_the_words_of_the_default_english_list(
        self, spark_session: SparkSession
    ) -> None:
        """Given the sample sentences, the default list leaves only the meaningful words."""
        stop_words = StopWordsRemover.loadDefaultStopWords(LANGUAGE)
        sentences = spark_session.createDataFrame(SAMPLE_SENTENCES, ["id", "words"])

        rows = remove_stop_words(sentences, stop_words).orderBy("id").collect()

        assert [row["filtered"] for row in rows] == [
            ["saw", "red", "balloon"],
            ["Mary", "little", "lamb"],
        ]

    def test_should_keep_the_original_column(self, spark_session: SparkSession) -> None:
        """The words with the stop words are still in the result."""
        sentences = spark_session.createDataFrame(SAMPLE_SENTENCES, ["id", "words"])

        assert remove_stop_words(sentences, ["the"]).columns == ["id", "words", "filtered"]

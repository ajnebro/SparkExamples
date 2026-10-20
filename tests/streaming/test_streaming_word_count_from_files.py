"""Tests for streaming.streaming_word_count_from_files."""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from streaming.streaming_word_count_from_files import count_words, read_lines


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
        result = counts(["c b a b c b"], spark_session)

        assert result == [("b", 3), ("c", 2), ("a", 1)]

    def test_should_return_no_rows_without_words(self, spark_session: SparkSession) -> None:
        """Lines without letters or digits produce no words."""
        assert counts(["", "...", " , "], spark_session) == []

    def test_should_raise_error_if_value_column_is_missing(
        self, spark_session: SparkSession
    ) -> None:
        """Given a DataFrame without the column "value", the query is rejected."""
        data_frame = spark_session.createDataFrame([("a",)], ["text"])

        with pytest.raises(Exception, match="value"):
            count_words(data_frame)


class TestReadLines:
    def test_should_count_words_of_files_in_directory(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """The streaming query counts the words of all the files in the directory."""
        (tmp_path / "1.txt").write_text("El hidalgo, el Quijote\n")
        (tmp_path / "2.txt").write_text("Hidalgo\n")
        stream = count_words(read_lines(spark_session, str(tmp_path)))

        # fmt: off
        query = (
            stream.writeStream
            .outputMode("complete")
            .format("memory")
            .queryName("word_count_test")
            .trigger(availableNow=True)
            .start()
        )
        # fmt: on
        query.awaitTermination()
        rows = spark_session.table("word_count_test").collect()

        assert [(row["word"], row["count"]) for row in rows] == [
            ("el", 2),
            ("hidalgo", 2),
            ("quijote", 1),
        ]

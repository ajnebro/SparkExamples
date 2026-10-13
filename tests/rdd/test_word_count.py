"""Tests for rdd.word_count."""

import pytest
from pyspark import SparkContext

from rdd.word_count import count_words, most_frequent_words, split_into_words


class TestSplitIntoWords:
    @pytest.mark.parametrize(
        ("line", "expected"),
        [
            ("En un lugar de la Mancha", ["en", "un", "lugar", "de", "la", "mancha"]),
            ("¿Qué dijo,  Sancho?", ["qué", "dijo", "sancho"]),
            ("", []),
            ("  ... !", []),
        ],
    )
    def test_should_return_lowercase_words_without_punctuation(
        self, line: str, expected: list[str]
    ) -> None:
        """Spaces and punctuation are not words, and accented letters are kept."""
        result = split_into_words(line)

        assert result == expected


class TestCountWords:
    def test_should_count_each_word_ignoring_case_and_punctuation(
        self, spark_context: SparkContext
    ) -> None:
        """Words that differ only in case or punctuation are counted together."""
        lines = spark_context.parallelize(["Que sí, que no.", "", "¿que?"])

        result = count_words(lines).collect()

        assert sorted(result) == [("no", 1), ("que", 3), ("sí", 1)]

    def test_should_return_empty_rdd_on_empty_text(self, spark_context: SparkContext) -> None:
        """Given no lines, there are no words."""
        lines = spark_context.parallelize([], 2)

        result = count_words(lines).collect()

        assert result == []


class TestMostFrequentWords:
    @pytest.mark.parametrize(
        ("top", "expected"),
        [
            (2, [("de", 5), ("la", 3)]),
            (4, [("de", 5), ("la", 3), ("a", 1), ("mancha", 1)]),
            (10, [("de", 5), ("la", 3), ("a", 1), ("mancha", 1)]),
        ],
    )
    def test_should_return_the_words_with_highest_counts(
        self, spark_context: SparkContext, top: int, expected: list[tuple[str, int]]
    ) -> None:
        """Words are sorted by count, ties alphabetically, and at most `top` are returned."""
        word_counts = spark_context.parallelize([("mancha", 1), ("de", 5), ("a", 1), ("la", 3)], 2)

        result = most_frequent_words(word_counts, top)

        assert result == expected

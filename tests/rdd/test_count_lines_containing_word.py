"""Tests for rdd.count_lines_containing_word."""

import pytest
from pyspark import SparkContext

from rdd.count_lines_containing_word import count_lines_containing, filter_lines_containing

LINES = ["Don Quijote", "Sancho y Quijote", "Dulcinea", "nothing here"]


class TestCountLinesContaining:
    @pytest.mark.parametrize(
        ("word", "expected"),
        [("Quijote", 2), ("Dulcinea", 1), ("quijote", 0), ("absent", 0)],
    )
    def test_should_count_lines_with_word(
        self, spark_context: SparkContext, word: str, expected: int
    ) -> None:
        """The search is case sensitive and counts each matching line once."""
        lines = spark_context.parallelize(LINES)

        result = count_lines_containing(lines, word)

        assert result == expected


class TestFilterLinesContaining:
    def test_should_keep_only_matching_lines(self, spark_context: SparkContext) -> None:
        """Only the lines that contain the word are kept."""
        lines = spark_context.parallelize(LINES)

        result = filter_lines_containing(lines, "Quijote").collect()

        assert sorted(result) == ["Don Quijote", "Sancho y Quijote"]

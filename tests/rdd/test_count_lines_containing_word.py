"""Tests for rdd.count_lines_containing_word."""

import pytest
from pyspark import SparkContext

from rdd.count_lines_containing_word import (
    count_lines_containing,
    filter_lines_containing,
    parse_args,
)

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


class TestParseArgs:
    def test_should_use_defaults_when_only_input_file_is_given(self) -> None:
        """Given only the input file, the default words and output are used."""
        args = parse_args(["quijote.txt"])

        assert args.input_file == "quijote.txt"
        assert args.count_words == ["Quijote", "Sancho"]
        assert args.extract_word == "Dulcinea"

    def test_should_accept_custom_words_and_output(self) -> None:
        """Given options, they override the defaults."""
        args = parse_args(
            ["a.txt", "--count-words", "x", "y", "--extract-word", "z", "--output", "o"]
        )

        assert (args.count_words, args.extract_word, args.output) == (["x", "y"], "z", "o")

    def test_should_exit_when_input_file_is_missing(self) -> None:
        """Given no arguments, the parser exits with an error."""
        with pytest.raises(SystemExit):
            parse_args([])

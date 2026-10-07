"""Tests for rdd.add_numbers_from_file."""

from pathlib import Path

import pytest
from pyspark import SparkContext

from rdd.add_numbers_from_file import add_numbers_from_file


class TestAddNumbersFromFile:
    @pytest.mark.parametrize(
        ("content", "expected"),
        [("1\n2\n3\n", 6), ("5\n", 5), ("-1\n1\n", 0)],
    )
    def test_should_add_numbers_stored_in_file(
        self, spark_context: SparkContext, tmp_path: Path, content: str, expected: int
    ) -> None:
        """Given a file with one number per line, the sum of all of them is returned."""
        input_file = tmp_path / "numbers.txt"
        input_file.write_text(content)

        result = add_numbers_from_file(spark_context, str(input_file))

        assert result == expected

    def test_should_fail_when_file_contains_non_numeric_line(
        self, spark_context: SparkContext, tmp_path: Path
    ) -> None:
        """Given a file with a non numeric line, the computation fails."""
        input_file = tmp_path / "numbers.txt"
        input_file.write_text("1\nabc\n")

        with pytest.raises(Exception, match="ValueError"):
            add_numbers_from_file(spark_context, str(input_file))

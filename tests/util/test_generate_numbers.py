"""Tests for util.generate_numbers."""

from pathlib import Path

import pytest

from util.generate_numbers import CHUNK_SIZE, MAX_VALUE, generate_numbers


def read_numbers(path: Path) -> list[int]:
    """Read the integers of a file with one integer per line.

    Args:
        path: Path of the file.

    Returns:
        The integers in the file.
    """
    return [int(line) for line in path.read_text().splitlines()]


class TestGenerateNumbers:
    @pytest.mark.parametrize("count", [0, 1, 10, CHUNK_SIZE + 5])
    def test_should_write_the_requested_number_of_integers(
        self, tmp_path: Path, count: int
    ) -> None:
        """The file contains exactly ``count`` lines, also when several chunks are needed."""
        output_file = tmp_path / "numbers.txt"

        generate_numbers(str(output_file), count, seed=1)

        assert len(read_numbers(output_file)) == count

    def test_should_write_integers_in_range(self, tmp_path: Path) -> None:
        """Every integer is in [0, MAX_VALUE)."""
        output_file = tmp_path / "numbers.txt"

        generate_numbers(str(output_file), 1000, seed=1)

        assert all(0 <= number < MAX_VALUE for number in read_numbers(output_file))

    def test_should_be_reproducible_with_the_same_seed(self, tmp_path: Path) -> None:
        """Two files generated with the same seed are identical."""
        first_file = tmp_path / "first.txt"
        second_file = tmp_path / "second.txt"

        generate_numbers(str(first_file), 100, seed=7)
        generate_numbers(str(second_file), 100, seed=7)

        assert first_file.read_text() == second_file.read_text()

    def test_should_raise_error_on_negative_count(self, tmp_path: Path) -> None:
        """A negative count raises ValueError."""
        with pytest.raises(ValueError):
            generate_numbers(str(tmp_path / "numbers.txt"), -1, seed=1)

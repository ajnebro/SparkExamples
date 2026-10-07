"""Smoke tests: every example runs end to end as a separate process."""

import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.smoke


def run_example(module: str, *args: str) -> str:
    """Run an example module and return its standard output."""
    completed = subprocess.run(
        [sys.executable, "-m", module, *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )
    return completed.stdout


class TestExamples:
    @pytest.mark.parametrize(
        ("module", "expected_output"),
        [
            ("rdd.add_numbers", "The sum is 36"),
            ("rdd.add_numbers_from_file", "The sum is 138"),
            ("rdd.add_numbers_with_accumulator", "The number of reduce operations is 7"),
            ("rdd.max_number", "The maximum is 8"),
            ("rdd.closure_driver_variable", "[10, 20, 30, 40]"),
            ("rdd.closure_shared_generator", "Generated numbers:"),
            ("rdd.closure_reproducible_random", "Generated reproducible numbers:"),
        ],
    )
    def test_should_run_example_and_print_result(self, module: str, expected_output: str) -> None:
        """Given no arguments, the example finishes and prints its result."""
        output = run_example(module)

        assert expected_output in output

    def test_should_run_count_lines_example(self, tmp_path: Path) -> None:
        """Given a text file, the example counts words and writes the extracted lines."""
        output = run_example(
            "rdd.count_lines_containing_word",
            "data/quijote.txt",
            "--output",
            str(tmp_path / "out"),
        )

        assert "Lines containing 'Quijote':" in output
        assert (tmp_path / "out" / "_SUCCESS").exists()

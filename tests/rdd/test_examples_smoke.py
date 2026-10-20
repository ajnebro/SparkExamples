"""Smoke tests: every example runs end to end as a separate process."""

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from streaming.temperature_sensor_generator import write_reading

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
            ("rdd.word_count", "que: "),
            ("rdd.closure_driver_variable", "[10, 20, 30, 40]"),
            ("rdd.closure_shared_generator", "Generated numbers:"),
            ("rdd.closure_reproducible_random", "Generated reproducible numbers:"),
        ],
    )
    def test_should_run_example_and_print_result(self, module: str, expected_output: str) -> None:
        """Given no arguments, the example finishes and prints its result."""
        output = run_example(module)

        assert expected_output in output

    def test_should_run_count_lines_example(self) -> None:
        """Given Don Quijote, the example counts words and writes the extracted lines."""
        output_directory = PROJECT_ROOT / "output" / "lines_with_word"
        # saveAsTextFile does not overwrite, so the output of a previous run is removed
        shutil.rmtree(output_directory, ignore_errors=True)

        output = run_example("rdd.count_lines_containing_word")

        assert "Lines containing 'Quijote':" in output
        assert (output_directory / "_SUCCESS").exists()

    def test_should_run_dataframe_basic_operations_example(self) -> None:
        """Given the personal data file, the example prints aggregations and writes the output."""
        output = run_example("dataframes.basic_operations")

        output_directory = PROJECT_ROOT / "output" / "basic_operations"
        assert "Mean age (DataFrame): 49.75" in output
        assert (output_directory / "json" / "_SUCCESS").exists()
        assert (output_directory / "csv" / "_SUCCESS").exists()

    @pytest.mark.skipif(
        not (PROJECT_ROOT / "data" / "airports.csv").exists(),
        reason="data/airports.csv must be downloaded from https://ourairports.com/data/",
    )
    @pytest.mark.parametrize(
        "module",
        [
            "dataframes.count_airports_by_type",
            "dataframes.count_airports_by_type_with_schema",
            "dataframes.count_airports_by_type_with_sql",
            "dataframes.count_airports_by_type_with_parquet",
        ],
    )
    def test_should_run_count_airports_example(self, module: str) -> None:
        """Given the OurAirports data file, the example prints the airports of Spain by type."""
        output = run_example(module)

        assert "type|count|" in output
        assert "large_airport" in output

    @pytest.mark.skipif(
        not (PROJECT_ROOT / "data" / "airports.csv").exists()
        or not (PROJECT_ROOT / "data" / "countries.csv").exists(),
        reason="data/airports.csv and data/countries.csv must be downloaded from OurAirports",
    )
    def test_should_run_count_airports_by_country_example(self) -> None:
        """Given the airports and countries files, the example prints the top countries."""
        output = run_example("dataframes.count_airports_by_country")

        assert "United States" in output
        assert "BroadcastHashJoin" in output

    def test_should_run_streaming_average_temperature_example(self) -> None:
        """Given the files of a sensor, the streaming query prints their average and stops."""
        directory = PROJECT_ROOT / "output" / "streaming"
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True)
        for temperature in (10.0, 20.0, 33.0):
            write_reading(directory, temperature)

        try:
            completed = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "from streaming.streaming_average_temperature import main; main(seconds=20)",
                ],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=120,
                check=True,
            )
        finally:
            shutil.rmtree(directory, ignore_errors=True)

        assert "average_temperature" in completed.stdout
        assert "21.0" in completed.stdout

    def test_should_run_streaming_find_high_temperature_example(self) -> None:
        """Given the files of a sensor, the streaming query prints the high temperatures."""
        directory = PROJECT_ROOT / "output" / "streaming"
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True)
        for temperature in (10.5, 25.5, 33.0):
            write_reading(directory, temperature)

        try:
            completed = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "from streaming.streaming_find_high_temperature import main; main(seconds=20)",
                ],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=120,
                check=True,
            )
        finally:
            shutil.rmtree(directory, ignore_errors=True)

        assert "25.5" in completed.stdout
        assert "33.0" in completed.stdout
        assert "10.5" not in completed.stdout

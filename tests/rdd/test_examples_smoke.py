"""Smoke tests: every example runs end to end as a separate process.

The examples that read or write directories receive a temporary one, which is deleted as soon as
the test ends, so the tests never touch `output/`: they do not interfere with, or delete, the
data of the programs that the user may be running at the same time.
"""

import subprocess
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

from streaming.temperature_sensor_generator import write_reading
from streaming.temperature_sensor_with_timestamp_generator import (
    write_reading as write_sensor_reading,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STREAMING_SECONDS = 20

pytestmark = pytest.mark.smoke

requires_airports = pytest.mark.skipif(
    not (PROJECT_ROOT / "data" / "airports.csv").exists(),
    reason="data/airports.csv must be downloaded from https://ourairports.com/data/",
)


@pytest.fixture
def temporary_directory() -> Iterator[Path]:
    """Provide an empty directory that is deleted as soon as the test ends."""
    with tempfile.TemporaryDirectory() as directory:
        yield Path(directory)


def run_example(module: str) -> str:
    """Run an example module with its default arguments and return its standard output."""
    completed = subprocess.run(
        [sys.executable, "-m", module],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )
    return completed.stdout


def run_main(module: str, **arguments: object) -> str:
    """Call the `main` function of an example with some arguments and return its output."""
    code = f"from {module} import main; main(**{arguments!r})"
    completed = subprocess.run(
        [sys.executable, "-c", code],
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
            ("dataframes.word_count_with_dataframe", "que: "),
            ("ml.normalizer_example", "Normalized using the L1 norm"),
            ("ml.read_libsvm", "features: vector"),
            ("ml.remove_stop_words", "[saw, red, balloon]"),
            ("rdd.closure_driver_variable", "[10, 20, 30, 40]"),
            ("rdd.closure_shared_generator", "Generated numbers:"),
            ("rdd.closure_reproducible_random", "Generated reproducible numbers:"),
        ],
    )
    def test_should_run_example_and_print_result(self, module: str, expected_output: str) -> None:
        """Given no arguments, the example finishes and prints its result."""
        output = run_example(module)

        assert expected_output in output

    def test_should_train_evaluate_and_predict_with_the_svm_examples(
        self, temporary_directory: Path
    ) -> None:
        """Given a trained model, the evaluation and the prediction examples can load it."""
        model_directory = str(temporary_directory / "svm_model")

        training = run_main("ml.svm_training", model_directory=model_directory)
        evaluation = run_main("ml.svm_evaluation", model_directory=model_directory)
        prediction = run_main("ml.svm_prediction", model_directory=model_directory)

        assert "Model saved in" in training
        assert "Accuracy = 1" in evaluation
        assert "prediction" in prediction

    def test_should_train_and_use_the_kmeans_model(self, temporary_directory: Path) -> None:
        """Given a trained model, the prediction example can load it."""
        model_directory = str(temporary_directory / "kmeans_model")

        training = run_main("ml.kmeans_training", model_directory=model_directory)
        prediction = run_main("ml.kmeans_prediction", model_directory=model_directory)

        assert "Model saved in" in training
        assert "Silhouette = " in prediction

    def test_should_run_kmeans_choose_k_example(self) -> None:
        """Given the sample data, the example prints the best number of clusters."""
        output = run_example("ml.kmeans_choose_k")

        assert "Best k: 2" in output

    def test_should_run_svm_classification_workflow_example(
        self, temporary_directory: Path
    ) -> None:
        """Given the Iris data, the workflow prints its accuracy and saves the model."""
        model_directory = temporary_directory / "iris_model"

        output = run_main("ml.svm_classification_workflow", model_directory=str(model_directory))

        assert "Test accuracy:" in output
        assert (model_directory / "metadata").exists()

    def test_should_run_count_lines_example(self, temporary_directory: Path) -> None:
        """Given Don Quijote, the example counts words and writes the extracted lines."""
        output_directory = temporary_directory / "lines_with_word"

        output = run_main("rdd.count_lines_containing_word", output_directory=str(output_directory))

        assert "Lines containing 'Quijote':" in output
        assert (output_directory / "_SUCCESS").exists()

    def test_should_run_dataframe_basic_operations_example(self, temporary_directory: Path) -> None:
        """Given the personal data file, the example prints aggregations and writes the output."""
        output = run_main("dataframes.basic_operations", output_directory=str(temporary_directory))

        assert "Mean age (DataFrame): 49.75" in output
        assert (temporary_directory / "json" / "_SUCCESS").exists()
        assert (temporary_directory / "csv" / "_SUCCESS").exists()

    def test_should_run_word_count_without_stop_words_example(self) -> None:
        """Given Don Quijote, the example prints its most frequent words without stop words."""
        output = run_example("dataframes.word_count_without_stop_words")

        assert "sancho: " in output
        assert "\nque: " not in output

    @requires_airports
    @pytest.mark.parametrize(
        "module",
        [
            "dataframes.count_airports_by_type",
            "dataframes.count_airports_by_type_with_schema",
            "dataframes.count_airports_by_type_with_sql",
        ],
    )
    def test_should_run_count_airports_example(self, module: str) -> None:
        """Given the OurAirports data file, the example prints the airports of Spain by type."""
        output = run_example(module)

        assert "type|count|" in output
        assert "large_airport" in output

    @requires_airports
    def test_should_run_count_airports_with_parquet_example(
        self, temporary_directory: Path
    ) -> None:
        """Given the OurAirports data file, the example converts it to Parquet and counts."""
        output = run_main(
            "dataframes.count_airports_by_type_with_parquet",
            parquet_directory=str(temporary_directory / "airports_parquet"),
        )

        assert "type|count|" in output
        assert "large_airport" in output

    @requires_airports
    @pytest.mark.skipif(
        not (PROJECT_ROOT / "data" / "countries.csv").exists(),
        reason="data/countries.csv must be downloaded from OurAirports",
    )
    def test_should_run_count_airports_by_country_example(self) -> None:
        """Given the airports and countries files, the example prints the top countries."""
        output = run_example("dataframes.count_airports_by_country")

        assert "United States" in output
        assert "BroadcastHashJoin" in output


class TestStreamingExamples:
    def test_should_run_streaming_average_temperature_example(
        self, temporary_directory: Path
    ) -> None:
        """Given the files of a sensor, the streaming query prints their average and stops."""
        for temperature in (10.0, 20.0, 33.0):
            write_reading(temporary_directory, temperature)

        output = run_main(
            "streaming.streaming_average_temperature",
            directory=str(temporary_directory),
            seconds=STREAMING_SECONDS,
        )

        assert "average_temperature" in output
        assert "21.0" in output

    def test_should_run_streaming_average_and_max_temperature_example(
        self, temporary_directory: Path
    ) -> None:
        """Given the files of a sensor, the streaming query prints the average and the maximum."""
        for temperature in (9.5, 30.0, 12.5):
            write_reading(temporary_directory, temperature)

        output = run_main(
            "streaming.streaming_average_and_max_temperature",
            directory=str(temporary_directory),
            seconds=STREAMING_SECONDS,
        )

        assert "max_temperature" in output
        assert "30.0" in output
        assert "17.333" in output

    def test_should_run_streaming_find_high_temperature_example(
        self, temporary_directory: Path
    ) -> None:
        """Given the files of a sensor, the streaming query prints only the high temperatures."""
        for temperature in (10.5, 25.5, 33.0):
            write_reading(temporary_directory, temperature)

        output = run_main(
            "streaming.streaming_find_high_temperature",
            directory=str(temporary_directory),
            seconds=STREAMING_SECONDS,
        )

        assert "25.5" in output
        assert "33.0" in output
        assert "10.5" not in output

    def test_should_run_streaming_average_temperature_from_several_sensors_example(
        self, temporary_directory: Path
    ) -> None:
        """Given the files of two sensors, the query prints the statistics of each one."""
        for sensor, temperature in (("sensor_a", 10.0), ("sensor_a", 20.0), ("sensor_b", 33.0)):
            write_sensor_reading(temporary_directory, sensor, temperature)

        output = run_main(
            "streaming.streaming_average_temperature_from_several_sensors",
            directory=str(temporary_directory),
            seconds=STREAMING_SECONDS,
        )

        assert "sensor_a" in output
        assert "sensor_b" in output
        assert "15.0" in output

    def test_should_run_streaming_average_temperature_with_time_windows_example(
        self, temporary_directory: Path
    ) -> None:
        """Given the files of a sensor, the query prints the average of each time window."""
        for temperature in (10.0, 20.0):
            write_sensor_reading(temporary_directory, "sensor_a", temperature)

        output = run_main(
            "streaming.streaming_average_temperature_with_time_windows",
            directory=str(temporary_directory),
            seconds=STREAMING_SECONDS,
        )

        assert "sensor_a" in output
        assert "window" in output
        assert "15.0" in output

    def test_should_run_streaming_word_count_from_files_example(
        self, temporary_directory: Path
    ) -> None:
        """Given text files in the directory, the query prints the count of each word."""
        (temporary_directory / "text.txt").write_text("El hidalgo, el Quijote\n")

        output = run_main(
            "streaming.streaming_word_count_from_files",
            directory=str(temporary_directory),
            seconds=STREAMING_SECONDS,
        )

        assert "hidalgo" in output
        assert "quijote" in output
        assert "El" not in output

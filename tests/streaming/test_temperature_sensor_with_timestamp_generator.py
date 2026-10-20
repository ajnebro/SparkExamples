"""Tests for streaming.temperature_sensor_with_timestamp_generator."""

import datetime
from pathlib import Path

import pytest

from streaming.temperature_sensor_with_timestamp_generator import (
    MAX_TEMPERATURE,
    MIN_TEMPERATURE,
    generate_readings,
    parse_arguments,
    read_temperature,
    write_reading,
)


class TestReadTemperature:
    def test_should_return_temperature_within_range(self) -> None:
        """Temperatures are decimal numbers inside the range of the sensor."""
        temperatures = [read_temperature() for _ in range(200)]

        assert all(MIN_TEMPERATURE <= value <= MAX_TEMPERATURE for value in temperatures)
        assert any(value != int(value) for value in temperatures)


class TestWriteReading:
    def test_should_write_sensor_temperature_and_time_stamp(self, tmp_path: Path) -> None:
        """The file holds one CSV line with the sensor, the temperature and a valid time stamp."""
        result = write_reading(tmp_path, "sensor-1", 18.25)

        sensor, temperature, time_stamp = result.read_text().strip().split(",")
        assert (sensor, float(temperature)) == ("sensor-1", 18.25)
        assert datetime.datetime.fromisoformat(time_stamp) <= datetime.datetime.now()

    def test_should_leave_no_temporary_file(self, tmp_path: Path) -> None:
        """Only the final file remains in the directory."""
        result = write_reading(tmp_path, "sensor-1", 20.0)

        assert [path.name for path in tmp_path.iterdir()] == [result.name]
        assert result.suffix == ".csv"

    def test_should_raise_error_if_directory_does_not_exist(self, tmp_path: Path) -> None:
        """Given a missing directory, the write fails."""
        with pytest.raises(FileNotFoundError):
            write_reading(tmp_path / "missing", "sensor-1", 20.0)


class TestGenerateReadings:
    def test_should_create_the_directory_and_write_the_requested_files(
        self, tmp_path: Path
    ) -> None:
        """Each reading is a different file, and the directory is created if needed."""
        directory = tmp_path / "new" / "directory"

        generate_readings(directory, "sensor-1", seconds=0, readings=3)

        assert len(list(directory.glob("*.csv"))) == 3


class TestParseArguments:
    def test_should_use_default_seconds_and_no_limit(self) -> None:
        """Only the directory and the sensor are mandatory."""
        result = parse_arguments(["output/streaming", "sensor-1"])

        assert (result.directory, result.sensor, result.seconds, result.readings) == (
            "output/streaming",
            "sensor-1",
            5.0,
            None,
        )

    def test_should_parse_optional_arguments(self) -> None:
        """The pause and the number of readings can be given."""
        result = parse_arguments(["d", "s", "--seconds", "0.5", "--readings", "4"])

        assert (result.seconds, result.readings) == (0.5, 4)

    def test_should_exit_if_arguments_are_missing(self) -> None:
        """Without the sensor, the parser exits with a usage message."""
        with pytest.raises(SystemExit):
            parse_arguments(["output/streaming"])

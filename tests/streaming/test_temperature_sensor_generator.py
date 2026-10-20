"""Tests for streaming.temperature_sensor_generator."""

from pathlib import Path

import pytest

from streaming.temperature_sensor_generator import (
    MAX_TEMPERATURE,
    MIN_TEMPERATURE,
    main,
    read_temperature,
    write_reading,
)


class TestReadTemperature:
    def test_should_return_temperature_within_range(self) -> None:
        """Every simulated reading is between the minimum and the maximum temperature."""
        temperatures = [read_temperature() for _ in range(100)]

        assert all(MIN_TEMPERATURE <= value <= MAX_TEMPERATURE for value in temperatures)


class TestWriteReading:
    def test_should_write_temperature_in_txt_file(self, tmp_path: Path) -> None:
        """The temperature is the only content of a file with the `.txt` extension."""
        written_file = write_reading(tmp_path, 21.5)

        assert written_file.suffix == ".txt"
        assert written_file.read_text() == "21.5"

    def test_should_leave_no_temporary_file(self, tmp_path: Path) -> None:
        """Once written, the directory contains only the final file."""
        written_file = write_reading(tmp_path, -3.25)

        assert list(tmp_path.iterdir()) == [written_file]

    def test_should_raise_error_if_directory_does_not_exist(self, tmp_path: Path) -> None:
        """Given a missing directory, a FileNotFoundError is raised."""
        with pytest.raises(FileNotFoundError):
            write_reading(tmp_path / "missing", 10.0)


class TestMain:
    @pytest.mark.parametrize("readings", [0, 1, 3])
    def test_should_write_one_file_per_reading(self, tmp_path: Path, readings: int) -> None:
        """The program writes as many files as the requested readings."""
        main(str(tmp_path), readings, seconds_between_readings=0)

        assert len(list(tmp_path.glob("*.txt"))) == readings

    def test_should_create_missing_directory(self, tmp_path: Path) -> None:
        """Given a directory that does not exist, it is created."""
        directory = tmp_path / "streaming"

        main(str(directory), 1, seconds_between_readings=0)

        assert len(list(directory.glob("*.txt"))) == 1

"""Simulate a named temperature sensor that writes its readings, with a time stamp, as CSV files.

It is a data source for the streaming examples that need to know which sensor measured a
temperature and when. Every reading is a line `<sensor>,<temperature>,<time stamp>` in a new
file of the directory. Run it in a separate terminal and stop it with Ctrl+C.

Usage (from the project root):

    python -m streaming.temperature_sensor_with_timestamp_generator <directory> <sensor> \
        [--seconds S] [--readings N]
"""

import argparse
import datetime
import itertools
import os
import random
import time
from pathlib import Path

DEFAULT_SECONDS_BETWEEN_READINGS = 5.0
MIN_TEMPERATURE = 15.0
MAX_TEMPERATURE = 23.0


def read_temperature() -> float:
    """Simulate a temperature reading.

    Returns:
        A random temperature between `MIN_TEMPERATURE` and `MAX_TEMPERATURE`.
    """
    return random.uniform(MIN_TEMPERATURE, MAX_TEMPERATURE)


def write_reading(directory: str | Path, sensor: str, temperature: float) -> Path:
    """Write a reading in a new file named after the current time stamp.

    The line is first written in a hidden temporary file (Spark ignores the files whose name
    starts with a dot) and then renamed. As the rename is atomic, Spark never sees a file that
    is empty or only partially written.

    Args:
        directory: Directory where the file is created.
        sensor: Identifier of the sensor.
        temperature: Temperature to write.

    Returns:
        The path of the created file, named `<time stamp>.csv`.

    Raises:
        FileNotFoundError: If the directory does not exist.
    """
    time_stamp = time.time_ns()
    reading_time = datetime.datetime.fromtimestamp(time_stamp / 1e9).isoformat(sep=" ")
    temporary_file = Path(directory) / f".{time_stamp}.tmp"
    final_file = Path(directory) / f"{time_stamp}.csv"

    temporary_file.write_text(f"{sensor},{temperature},{reading_time}\n")
    os.replace(temporary_file, final_file)

    return final_file


def generate_readings(
    directory: str | Path, sensor: str, seconds: float, readings: int | None = None
) -> None:
    """Write a reading every few seconds.

    Args:
        directory: Directory where the files are written. It is created if it does not exist.
        sensor: Identifier of the sensor.
        seconds: Pause between two consecutive readings.
        readings: Number of files to write. If `None`, it runs until it is interrupted.
    """
    Path(directory).mkdir(parents=True, exist_ok=True)

    for _ in itertools.islice(itertools.count(), readings):
        temperature = read_temperature()
        written_file = write_reading(directory, sensor, temperature)
        print(f"Wrote {written_file.name} with temperature {temperature:.2f}")
        time.sleep(seconds)


def parse_arguments(arguments: list[str] | None = None) -> argparse.Namespace:
    """Parse the command line.

    Args:
        arguments: Arguments to parse. If `None`, the ones of the command line are used.

    Returns:
        The namespace with `directory`, `sensor`, `seconds` and `readings`.
    """
    parser = argparse.ArgumentParser(description="Simulate a temperature sensor.")
    parser.add_argument("directory", help="directory where the files are written")
    parser.add_argument("sensor", help="identifier of the sensor")
    parser.add_argument(
        "--seconds",
        type=float,
        default=DEFAULT_SECONDS_BETWEEN_READINGS,
        help="seconds between two readings (default: %(default)s)",
    )
    parser.add_argument("--readings", type=int, help="number of readings (default: no limit)")

    return parser.parse_args(arguments)


def main() -> None:
    """Run the program with the arguments of the command line."""
    arguments = parse_arguments()
    generate_readings(arguments.directory, arguments.sensor, arguments.seconds, arguments.readings)


if __name__ == "__main__":
    main()

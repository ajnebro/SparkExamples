"""Simulate a temperature sensor that writes its readings as files into a directory.

It is the data source of the streaming examples: Spark watches the directory and processes each
new file as it arrives. Run it in a separate terminal and stop it with Ctrl+C.
"""

import itertools
import os
import random
import time
from pathlib import Path

DEFAULT_STREAMING_DIRECTORY = "output/streaming"
SECONDS_BETWEEN_READINGS = 5
MIN_TEMPERATURE = -10.0
MAX_TEMPERATURE = 40.0


def read_temperature() -> float:
    """Simulate a temperature reading.

    Returns:
        A random temperature between `MIN_TEMPERATURE` and `MAX_TEMPERATURE`.
    """
    return random.uniform(MIN_TEMPERATURE, MAX_TEMPERATURE)


def write_reading(directory: str | Path, temperature: float) -> Path:
    """Write a temperature in a new file named after the current time stamp.

    The value is first written in a hidden temporary file (Spark ignores the files whose name
    starts with a dot) and then renamed. As the rename is atomic, Spark never sees a file that
    is empty or only partially written.

    Args:
        directory: Directory where the file is created.
        temperature: Temperature to write.

    Returns:
        The path of the created file, named `<time stamp>.txt`.

    Raises:
        FileNotFoundError: If the directory does not exist.
    """
    time_stamp = time.time_ns()
    temporary_file = Path(directory) / f".{time_stamp}.tmp"
    final_file = Path(directory) / f"{time_stamp}.txt"

    temporary_file.write_text(str(temperature))
    os.replace(temporary_file, final_file)

    return final_file


def main(
    directory: str = DEFAULT_STREAMING_DIRECTORY,
    readings: int | None = None,
    seconds_between_readings: float = SECONDS_BETWEEN_READINGS,
) -> None:
    """Run the example: write a temperature file every few seconds.

    Args:
        directory: Directory where the files are written. It is created if it does not exist.
        readings: Number of files to write. If `None`, the program runs until it is interrupted.
        seconds_between_readings: Pause between two consecutive files.
    """
    Path(directory).mkdir(parents=True, exist_ok=True)

    for _ in itertools.islice(itertools.count(), readings):
        temperature = read_temperature()
        written_file = write_reading(directory, temperature)
        print(f"Wrote {written_file.name} with temperature {temperature:.2f}")
        time.sleep(seconds_between_readings)


if __name__ == "__main__":
    main()

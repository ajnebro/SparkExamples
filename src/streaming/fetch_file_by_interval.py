"""Download a file from a URL at regular intervals into a directory.

It is a data source for the streaming examples when the data are published on a web site, e.g.
a CSV that the server updates periodically. Each download is stored in a new file, so Spark,
watching the directory, processes it as a new piece of the stream. Stop it with Ctrl+C.

Usage (from the project root):

    python -m streaming.fetch_file_by_interval <url> <directory> <seconds> [--downloads N]
"""

import argparse
import itertools
import os
import shutil
import time
import urllib.request
from pathlib import Path


def fetch_file(url: str, directory: str | Path) -> Path:
    """Download a URL into a new file named after the current time stamp.

    The content is first written in a hidden temporary file (Spark ignores the files whose name
    starts with a dot) and then renamed. As the rename is atomic, Spark never sees a file that
    is empty or only partially downloaded.

    Args:
        url: URL of the file to download (any scheme supported by `urllib`, e.g. https).
        directory: Directory where the file is created.

    Returns:
        The path of the created file, named `<time stamp>.csv`.

    Raises:
        urllib.error.URLError: If the URL cannot be fetched.
        FileNotFoundError: If the directory does not exist.
    """
    time_stamp = time.time_ns()
    temporary_file = Path(directory) / f".{time_stamp}.tmp"
    final_file = Path(directory) / f"{time_stamp}.csv"

    with urllib.request.urlopen(url) as response, temporary_file.open("wb") as output:
        shutil.copyfileobj(response, output)
    os.replace(temporary_file, final_file)

    return final_file


def fetch_periodically(
    url: str, directory: str | Path, seconds: float, downloads: int | None = None
) -> None:
    """Download a URL again and again, pausing between two consecutive downloads.

    Args:
        url: URL of the file to download.
        directory: Directory where the files are written. It is created if it does not exist.
        seconds: Pause between two consecutive downloads.
        downloads: Number of files to download. If `None`, it runs until it is interrupted.
    """
    Path(directory).mkdir(parents=True, exist_ok=True)

    for _ in itertools.islice(itertools.count(), downloads):
        print(f"Downloaded {fetch_file(url, directory).name}")
        time.sleep(seconds)


def parse_arguments(arguments: list[str] | None = None) -> argparse.Namespace:
    """Parse the command line.

    Args:
        arguments: Arguments to parse. If `None`, the ones of the command line are used.

    Returns:
        The namespace with `url`, `directory`, `seconds` and `downloads`.
    """
    parser = argparse.ArgumentParser(description="Download a URL at regular intervals.")
    parser.add_argument("url", help="URL of the file to download")
    parser.add_argument("directory", help="directory where the files are written")
    parser.add_argument("seconds", type=float, help="seconds between two downloads")
    parser.add_argument("--downloads", type=int, help="number of downloads (default: no limit)")

    return parser.parse_args(arguments)


def main() -> None:
    """Run the program with the arguments of the command line."""
    arguments = parse_arguments()
    fetch_periodically(arguments.url, arguments.directory, arguments.seconds, arguments.downloads)


if __name__ == "__main__":
    main()

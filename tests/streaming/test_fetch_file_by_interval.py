"""Tests for streaming.fetch_file_by_interval."""

import urllib.error
from pathlib import Path

import pytest

from streaming.fetch_file_by_interval import fetch_file, fetch_periodically, parse_arguments

CSV_CONTENT = "name,free\nCentro,12\nPuerto,3\n"


@pytest.fixture
def source_url(tmp_path: Path) -> str:
    """URL (file scheme) of a small CSV, so that the tests do not need the network."""
    source = tmp_path / "source.csv"
    source.write_text(CSV_CONTENT)
    return source.as_uri()


class TestFetchFile:
    def test_should_store_the_content_in_a_new_csv_file(
        self, source_url: str, tmp_path: Path
    ) -> None:
        """The downloaded file has the content of the URL and no temporary file is left."""
        directory = tmp_path / "out"
        directory.mkdir()

        result = fetch_file(source_url, directory)

        assert result.suffix == ".csv"
        assert result.read_text() == CSV_CONTENT
        assert [path.name for path in directory.iterdir()] == [result.name]

    def test_should_raise_error_if_url_cannot_be_fetched(self, tmp_path: Path) -> None:
        """Given a URL that does not exist, the download fails."""
        with pytest.raises(urllib.error.URLError):
            fetch_file((tmp_path / "missing.csv").as_uri(), tmp_path)

    def test_should_raise_error_if_directory_does_not_exist(
        self, source_url: str, tmp_path: Path
    ) -> None:
        """Given a missing directory, the download fails."""
        with pytest.raises(FileNotFoundError):
            fetch_file(source_url, tmp_path / "missing")


class TestFetchPeriodically:
    def test_should_create_the_directory_and_download_the_requested_files(
        self, source_url: str, tmp_path: Path
    ) -> None:
        """Each download is a different file, and the directory is created if needed."""
        directory = tmp_path / "new" / "directory"

        fetch_periodically(source_url, directory, seconds=0, downloads=3)

        assert len(list(directory.glob("*.csv"))) == 3


class TestParseArguments:
    def test_should_parse_the_arguments(self) -> None:
        """The three positional arguments and the optional number of downloads are read."""
        result = parse_arguments(["http://host/a.csv", "output/data", "30", "--downloads", "2"])

        assert (result.url, result.directory, result.seconds, result.downloads) == (
            "http://host/a.csv",
            "output/data",
            30.0,
            2,
        )

    def test_should_exit_if_arguments_are_missing(self) -> None:
        """Without the mandatory arguments, the parser exits with a usage message."""
        with pytest.raises(SystemExit):
            parse_arguments(["http://host/a.csv"])

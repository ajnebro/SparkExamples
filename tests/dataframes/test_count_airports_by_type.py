"""Tests for dataframes.count_airports_by_type."""

from pathlib import Path

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from dataframes.count_airports_by_type import (
    count_airports_by_type,
    read_airports,
)

AIRPORTS_CSV = """\
"id","ident","type","name","iso_country"
1,"LEMD","large_airport","Madrid-Barajas","ES"
2,"LEMG","large_airport","Malaga-Costa del Sol","ES"
3,"LEGT","medium_airport","Madrid-Getafe","ES"
4,"ES-0001","heliport","Some Heliport","ES"
5,"LFPG","large_airport","Paris-Charles de Gaulle","FR"
6,"EST1","small_airport","Estonian field","EE"
"""


@pytest.fixture
def airports_file(tmp_path: Path) -> Path:
    """Provide a small airports CSV file."""
    file = tmp_path / "airports_sample.csv"
    file.write_text(AIRPORTS_CSV)
    return file


@pytest.fixture
def airports(spark_session: SparkSession, airports_file: Path) -> DataFrame:
    """Provide a DataFrame read from the small airports CSV file."""
    return read_airports(spark_session, str(airports_file))


class TestReadAirports:
    def test_should_read_one_row_per_airport_with_inferred_types(self, airports: DataFrame) -> None:
        """The header gives the column names and the id is inferred as an integer."""
        assert airports.count() == 6
        assert ("id", "int") in airports.dtypes

    def test_should_raise_error_when_file_does_not_exist(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """Given a missing file, Spark raises an AnalysisException."""
        with pytest.raises(AnalysisException):
            read_airports(spark_session, str(tmp_path / "missing.csv"))


class TestCountAirportsByType:
    @pytest.mark.parametrize(
        ("country", "expected"),
        [
            ("ES", [("large_airport", 2), ("heliport", 1), ("medium_airport", 1)]),
            ("es", []),
            ("FR", [("large_airport", 1)]),
            ("PT", []),
        ],
    )
    def test_should_count_airports_of_the_country_by_type(
        self, airports: DataFrame, country: str, expected: list[tuple[str, int]]
    ) -> None:
        """Codes match exactly (case sensitive); types are sorted by count, then by name."""
        result = count_airports_by_type(airports, country)

        assert [tuple(row) for row in result.collect()] == expected

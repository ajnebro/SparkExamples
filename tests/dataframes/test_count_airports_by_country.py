"""Tests for dataframes.count_airports_by_country."""

from pathlib import Path

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from dataframes.count_airports_by_country import count_airports_by_country, read_countries

AIRPORT_COUNTRIES = ["ES", "ES", "ES", "FR", "FR", "PT", "XX"]
COUNTRIES = [
    (1, "ES", "Spain"),
    (2, "FR", "France"),
    (3, "PT", "Portugal"),
    (4, "NA", "Namibia"),
]


@pytest.fixture
def airports(spark_session: SparkSession) -> DataFrame:
    """Provide airports of several countries, one of them ("XX") not in the countries table."""
    return spark_session.createDataFrame(
        [(code,) for code in AIRPORT_COUNTRIES], "iso_country STRING"
    )


@pytest.fixture
def countries(spark_session: SparkSession) -> DataFrame:
    """Provide a small countries table."""
    return spark_session.createDataFrame(COUNTRIES, "id INT, code STRING, name STRING")


class TestReadCountries:
    def test_should_keep_the_code_of_namibia_as_text(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """The code "NA" (Namibia) is not read as a null value."""
        file = tmp_path / "countries.csv"
        file.write_text('"id","code","name"\n302591,"NA","Namibia"\n')

        result = read_countries(spark_session, str(file))

        assert result.select("code", "name").collect() == [("NA", "Namibia")]

    def test_should_raise_error_when_file_does_not_exist(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """Given a missing file, Spark raises an AnalysisException."""
        with pytest.raises(AnalysisException):
            read_countries(spark_session, str(tmp_path / "missing.csv"))


class TestCountAirportsByCountry:
    @pytest.mark.parametrize(
        ("top", "expected"),
        [
            (10, [("Spain", 3), ("France", 2), ("Portugal", 1)]),
            (2, [("Spain", 3), ("France", 2)]),
        ],
    )
    def test_should_return_the_countries_with_most_airports(
        self, airports: DataFrame, countries: DataFrame, top: int, expected: list[tuple[str, int]]
    ) -> None:
        """Codes without a country and countries without airports are left out by the join."""
        result = count_airports_by_country(airports, countries, top)

        assert [tuple(row) for row in result.collect()] == expected

    def test_should_broadcast_the_countries_table(
        self,
        airports: DataFrame,
        countries: DataFrame,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """The physical plan uses a broadcast hash join."""
        count_airports_by_country(airports, countries, 10).explain()

        assert "BroadcastHashJoin" in capsys.readouterr().out

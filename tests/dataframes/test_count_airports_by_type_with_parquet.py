"""Tests for dataframes.count_airports_by_type_with_parquet."""

from pathlib import Path

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from dataframes.count_airports_by_type import count_airports_by_type
from dataframes.count_airports_by_type_with_parquet import (
    read_airports_from_parquet,
    write_airports_as_parquet,
)

AIRPORTS = [
    (1, "large_airport", "ES"),
    (2, "heliport", "ES"),
    (3, "large_airport", "FR"),
]


@pytest.fixture
def airports(spark_session: SparkSession) -> DataFrame:
    """Provide a small DataFrame of airports."""
    return spark_session.createDataFrame(AIRPORTS, "id INT, type STRING, iso_country STRING")


@pytest.fixture
def parquet_directory(airports: DataFrame, tmp_path: Path) -> Path:
    """Write the airports in Parquet format and provide the directory."""
    directory = tmp_path / "airports_parquet"
    write_airports_as_parquet(airports, str(directory))
    return directory


class TestWriteAirportsAsParquet:
    def test_should_create_one_directory_per_country(self, parquet_directory: Path) -> None:
        """Each country is written to its own partition directory."""
        partitions = sorted(path.name for path in parquet_directory.glob("iso_country=*"))

        assert partitions == ["iso_country=ES", "iso_country=FR"]

    def test_should_replace_previous_data(
        self, spark_session: SparkSession, airports: DataFrame, parquet_directory: Path
    ) -> None:
        """Writing twice to the same directory keeps only the last data."""
        write_airports_as_parquet(airports, str(parquet_directory))

        result = read_airports_from_parquet(spark_session, str(parquet_directory))

        assert result.count() == len(AIRPORTS)


class TestReadAirportsFromParquet:
    def test_should_read_back_the_same_rows(
        self, spark_session: SparkSession, airports: DataFrame, parquet_directory: Path
    ) -> None:
        """The partition column is restored from the directory names."""
        result = read_airports_from_parquet(spark_session, str(parquet_directory))

        assert sorted(result.select(*airports.columns).collect()) == sorted(airports.collect())

    def test_should_read_only_the_partition_of_the_country(
        self,
        spark_session: SparkSession,
        parquet_directory: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """The filter on the country becomes a partition filter, and only "type" is read."""
        airports = read_airports_from_parquet(spark_session, str(parquet_directory))

        count_airports_by_type(airports, "ES").explain()

        plan = capsys.readouterr().out
        assert "PartitionFilters: [isnotnull(iso_country" in plan
        assert "ReadSchema: struct<type:string>" in plan

    def test_should_raise_error_when_directory_does_not_exist(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """Given a missing directory, Spark raises an AnalysisException."""
        with pytest.raises(AnalysisException):
            read_airports_from_parquet(spark_session, str(tmp_path / "missing"))

"""Tests for dataframes.count_airports_by_type_with_schema."""

from pathlib import Path

import pytest
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession

from dataframes.count_airports_by_type_with_schema import (
    AIRPORTS_SCHEMA,
    read_airports_with_schema,
)

HEADER = ",".join(AIRPORTS_SCHEMA.fieldNames())
# Only some of the 19 columns have a value: id, ident, type, name, ..., iso_country, ...
ROW = "1,LEMD,large_airport,Madrid-Barajas,40.47,-3.56,1998,EU,ES" + "," * 10


def write_csv(directory: Path, header: str, row: str) -> str:
    """Write a CSV file with a header and one row, and return its path."""
    file = directory / "airports.csv"
    file.write_text(f"{header}\n{row}\n")
    return str(file)


class TestReadAirportsWithSchema:
    def test_should_use_the_types_of_the_schema(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """The header is skipped and each column has the declared type."""
        file_name = write_csv(tmp_path, HEADER, ROW)

        result = read_airports_with_schema(spark_session, file_name)

        assert result.schema == AIRPORTS_SCHEMA
        assert result.select("id", "type", "elevation_ft", "iso_country").collect() == [
            (1, "large_airport", 1998, "ES")
        ]

    def test_should_fail_when_header_does_not_match_the_schema(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """Given a file with one column less than the schema, the columns are not shifted."""
        header_without_keywords = HEADER.removesuffix(",keywords")
        file_name = write_csv(tmp_path, header_without_keywords, ROW[:-1])

        with pytest.raises(Py4JJavaError, match="CSV header"):
            read_airports_with_schema(spark_session, file_name).collect()

"""Tests for dataframes.count_airports_by_type_with_sql."""

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from dataframes.count_airports_by_type import count_airports_by_type
from dataframes.count_airports_by_type_with_sql import count_airports_by_type_with_sql

AIRPORTS = [
    ("large_airport", "ES"),
    ("large_airport", "ES"),
    ("medium_airport", "ES"),
    ("heliport", "ES"),
    ("large_airport", "FR"),
    ("small_airport", "EE"),
]


@pytest.fixture
def airports(spark_session: SparkSession) -> DataFrame:
    """Provide a small DataFrame with the type and country of some airports."""
    return spark_session.createDataFrame(AIRPORTS, "type STRING, iso_country STRING")


class TestCountAirportsByTypeWithSql:
    @pytest.mark.parametrize(
        ("country", "expected"),
        [
            ("ES", [("large_airport", 2), ("heliport", 1), ("medium_airport", 1)]),
            ("es", []),
            ("PT", []),
        ],
    )
    def test_should_count_airports_of_the_country_by_type(
        self,
        spark_session: SparkSession,
        airports: DataFrame,
        country: str,
        expected: list[tuple[str, int]],
    ) -> None:
        """Codes match exactly (case sensitive); types are sorted by count, then by name."""
        result = count_airports_by_type_with_sql(spark_session, airports, country)

        assert [tuple(row) for row in result.collect()] == expected

    @pytest.mark.parametrize("country", ["ES", "FR", "EE"])
    def test_should_return_the_same_as_the_dataframe_api(
        self, spark_session: SparkSession, airports: DataFrame, country: str
    ) -> None:
        """The SQL query and the DataFrame API version produce the same rows."""
        result = count_airports_by_type_with_sql(spark_session, airports, country)

        assert result.collect() == count_airports_by_type(airports, country).collect()

    def test_should_raise_error_when_a_column_is_missing(self, spark_session: SparkSession) -> None:
        """Given a DataFrame without the iso_country column, the query cannot be analyzed."""
        airports = spark_session.createDataFrame([("heliport",)], "type STRING")

        with pytest.raises(AnalysisException):
            count_airports_by_type_with_sql(spark_session, airports, "ES")

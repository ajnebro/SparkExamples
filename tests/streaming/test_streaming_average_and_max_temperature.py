"""Tests for streaming.streaming_average_and_max_temperature."""

from pathlib import Path

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from streaming.streaming_average_and_max_temperature import (
    average_and_max_temperature,
    read_temperatures,
)
from streaming.temperature_sensor_generator import write_reading


class TestAverageAndMaxTemperature:
    @pytest.mark.parametrize(
        ("values", "expected_average", "expected_max"),
        [
            (["10.5", "20.5"], 15.5, 20.5),
            (["-5"], -5.0, -5.0),
            (["9.5", "30.1", "-5"], 34.6 / 3, 30.1),
            (["-10", "-2.5"], -6.25, -2.5),
        ],
    )
    def test_should_return_average_and_maximum_as_numbers(
        self,
        spark_session: SparkSession,
        values: list[str],
        expected_average: float,
        expected_max: float,
    ) -> None:
        """The maximum compares numbers, not text: "30.1" is greater than "9.5"."""
        lines = spark_session.createDataFrame([(value,) for value in values], ["value"])

        row = average_and_max_temperature(lines).first()

        assert row["average_temperature"] == pytest.approx(expected_average)
        assert row["max_temperature"] == pytest.approx(expected_max)

    def test_should_return_double_columns(self, spark_session: SparkSession) -> None:
        """Both aggregations are numbers, so they can be compared or formatted as such."""
        lines = spark_session.createDataFrame([("1.5",)], ["value"])

        result = average_and_max_temperature(lines)

        assert [field.dataType.simpleString() for field in result.schema] == ["double", "double"]

    def test_should_raise_error_if_value_is_not_a_number(self, spark_session: SparkSession) -> None:
        """Given a value that is not a number, the query fails."""
        lines = spark_session.createDataFrame([("10",), ("hot",)], ["value"])

        with pytest.raises(PySparkException, match="CAST_INVALID_INPUT"):
            average_and_max_temperature(lines).collect()


class TestReadTemperatures:
    def test_should_compute_statistics_of_files_in_directory(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """The streaming query processes every file that is in the directory."""
        for temperature in (9.5, 30.1, 12.0):
            write_reading(tmp_path, temperature)
        stream = average_and_max_temperature(read_temperatures(spark_session, str(tmp_path)))

        # fmt: off
        query = (
            stream.writeStream
            .outputMode("complete")
            .format("memory")
            .queryName("average_and_max_temperature_test")
            .trigger(availableNow=True)
            .start()
        )
        # fmt: on

        query.awaitTermination()
        row = spark_session.table("average_and_max_temperature_test").first()

        assert row["max_temperature"] == pytest.approx(30.1)
        assert row["average_temperature"] == pytest.approx(51.6 / 3)

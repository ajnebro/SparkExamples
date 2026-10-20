"""Tests for streaming.streaming_average_temperature."""

from pathlib import Path

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from streaming.streaming_average_temperature import average_temperature, read_temperatures
from streaming.temperature_sensor_generator import write_reading


class TestAverageTemperature:
    @pytest.mark.parametrize(
        ("values", "expected"),
        [(["10.5", "20.5"], 15.5), (["-5"], -5.0), (["0", "10", "20", "30"], 15.0)],
    )
    def test_should_return_average_of_temperatures(
        self, spark_session: SparkSession, values: list[str], expected: float
    ) -> None:
        """The average is computed over the temperatures, which are read as text."""
        lines = spark_session.createDataFrame([(value,) for value in values], ["value"])

        result = average_temperature(lines).first()["average_temperature"]

        assert result == pytest.approx(expected)

    def test_should_raise_error_if_value_is_not_a_number(self, spark_session: SparkSession) -> None:
        """Given a value that is not a number, the query fails."""
        lines = spark_session.createDataFrame([("10",), ("hot",)], ["value"])

        with pytest.raises(PySparkException, match="CAST_INVALID_INPUT"):
            average_temperature(lines).collect()


class TestReadTemperatures:
    def test_should_compute_average_of_files_in_directory(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """The streaming query processes every file that is in the directory."""
        for temperature in (10.0, 20.0, 33.0):
            write_reading(tmp_path, temperature)
        stream = average_temperature(read_temperatures(spark_session, str(tmp_path)))

        # fmt: off
        query = (
            stream.writeStream
            .outputMode("complete")
            .format("memory")
            .queryName("average_temperature_test")
            .trigger(availableNow=True)
            .start()
        )
        # fmt: on
        query.awaitTermination()
        result = spark_session.table("average_temperature_test").first()["average_temperature"]

        assert result == pytest.approx(21.0)

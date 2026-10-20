"""Tests for streaming.streaming_average_temperature_from_several_sensors."""

from pathlib import Path

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from streaming.streaming_average_temperature_from_several_sensors import (
    read_readings,
    temperature_by_sensor,
)
from streaming.temperature_sensor_with_timestamp_generator import write_reading


class TestTemperatureBySensor:
    def test_should_return_average_and_maximum_of_each_sensor(
        self, spark_session: SparkSession
    ) -> None:
        """Each sensor has its own average and maximum."""
        readings = spark_session.createDataFrame(
            [("a", 10.0), ("a", 20.0), ("b", -5.0), ("b", 3.0), ("b", 8.0)],
            ["sensor", "temperature"],
        )

        result = {row["sensor"]: row for row in temperature_by_sensor(readings).collect()}

        assert result["a"]["average_temperature"] == pytest.approx(15.0)
        assert result["a"]["max_temperature"] == pytest.approx(20.0)
        assert result["b"]["average_temperature"] == pytest.approx(2.0)
        assert result["b"]["max_temperature"] == pytest.approx(8.0)

    def test_should_return_no_rows_without_readings(self, spark_session: SparkSession) -> None:
        """Without readings there is no sensor to report."""
        readings = spark_session.createDataFrame([], "sensor string, temperature double")

        assert temperature_by_sensor(readings).count() == 0

    def test_should_raise_error_if_column_is_missing(self, spark_session: SparkSession) -> None:
        """Given a DataFrame without the temperature, the query fails."""
        readings = spark_session.createDataFrame([("a", 1.0)], ["sensor", "value"])

        with pytest.raises(PySparkException):
            temperature_by_sensor(readings)


class TestReadReadings:
    def test_should_compute_statistics_of_files_in_directory(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """The streaming query parses the files of several sensors and groups them."""
        for sensor, temperature in (("a", 10.0), ("a", 20.0), ("b", 30.0)):
            write_reading(tmp_path, sensor, temperature)
        stream = temperature_by_sensor(read_readings(spark_session, str(tmp_path)))

        query = (
            stream.writeStream.outputMode("complete")
            .format("memory")
            .queryName("temperature_by_sensor_test")
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        rows = spark_session.table("temperature_by_sensor_test").collect()

        result = {row["sensor"]: row["average_temperature"] for row in rows}
        assert result == pytest.approx({"a": 15.0, "b": 30.0})

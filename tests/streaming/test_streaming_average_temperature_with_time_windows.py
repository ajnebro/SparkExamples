"""Tests for streaming.streaming_average_temperature_with_time_windows."""

from datetime import datetime
from pathlib import Path

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from streaming.streaming_average_temperature_with_time_windows import (
    average_temperature_by_window,
    read_readings,
)


def at(time: str) -> datetime:
    """Build a time stamp of a fixed day from a "HH:MM:SS" text."""
    return datetime.fromisoformat(f"2026-01-01 {time}")


class TestAverageTemperatureByWindow:
    def test_should_assign_each_reading_to_two_overlapping_windows(
        self, spark_session: SparkSession
    ) -> None:
        """With windows of 20 s sliding every 10 s, a reading belongs to two windows."""
        readings = spark_session.createDataFrame(
            [("a", 10.0, at("00:00:15"))], ["sensor", "temperature", "timestamp"]
        )

        rows = average_temperature_by_window(readings).collect()

        assert [(row["window"]["start"], row["window"]["end"]) for row in rows] == [
            (at("00:00:00"), at("00:00:20")),
            (at("00:00:10"), at("00:00:30")),
        ]

    def test_should_average_the_readings_of_each_sensor_in_each_window(
        self, spark_session: SparkSession
    ) -> None:
        """Readings of the same sensor and window are averaged; sensors are kept apart."""
        readings = spark_session.createDataFrame(
            [
                ("a", 10.0, at("00:00:12")),
                ("a", 20.0, at("00:00:18")),
                ("a", 40.0, at("00:00:25")),
                ("b", 5.0, at("00:00:12")),
            ],
            ["sensor", "temperature", "timestamp"],
        )

        rows = average_temperature_by_window(readings, "20 seconds", "20 seconds").collect()

        result = {
            (row["sensor"], row["window"]["start"]): row["average_temperature"] for row in rows
        }
        assert result == pytest.approx(
            {("a", at("00:00:00")): 15.0, ("a", at("00:00:20")): 40.0, ("b", at("00:00:00")): 5.0}
        )

    def test_should_raise_error_if_slide_is_longer_than_window(
        self, spark_session: SparkSession
    ) -> None:
        """Given a slide longer than the window, the query is rejected."""
        readings = spark_session.createDataFrame(
            [("a", 10.0, at("00:00:15"))], ["sensor", "temperature", "timestamp"]
        )

        with pytest.raises(PySparkException):
            average_temperature_by_window(readings, "10 seconds", "20 seconds").collect()


class TestReadReadings:
    def test_should_compute_windows_of_files_in_directory(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """The streaming query uses the time stamp written in the files as the event time."""
        (tmp_path / "1.csv").write_text("a,10.0,2026-01-01 00:00:05\n")
        (tmp_path / "2.csv").write_text("a,20.0,2026-01-01 00:00:08\n")
        stream = average_temperature_by_window(
            read_readings(spark_session, str(tmp_path)), "10 seconds", "10 seconds"
        )

        # fmt: off
        query = (
            stream.writeStream
            .outputMode("complete")
            .format("memory")
            .queryName("windows_test")
            .trigger(availableNow=True)
            .start()
        )
        # fmt: on

        query.awaitTermination()
        rows = spark_session.table("windows_test").collect()

        assert [(row["window"]["start"], row["average_temperature"]) for row in rows] == [
            (at("00:00:00"), pytest.approx(15.0))
        ]

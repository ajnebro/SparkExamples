"""Tests for streaming.streaming_find_high_temperature."""

from pathlib import Path

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from streaming.streaming_find_high_temperature import high_temperatures, read_temperatures
from streaming.temperature_sensor_generator import write_reading


class TestHighTemperatures:
    @pytest.mark.parametrize(
        ("values", "threshold", "expected"),
        [
            (["25.37", "12.5", "30"], 20.0, [25.37, 30.0]),
            (["20", "20.01"], 20.0, [20.01]),
            (["-5", "-1.5"], -3.0, [-1.5]),
            (["10", "15"], 20.0, []),
        ],
    )
    def test_should_keep_only_temperatures_above_threshold(
        self,
        spark_session: SparkSession,
        values: list[str],
        threshold: float,
        expected: list[float],
    ) -> None:
        """Decimal temperatures are compared as numbers, and the threshold is excluded."""
        lines = spark_session.createDataFrame([(value,) for value in values], ["value"])

        result = high_temperatures(lines, threshold).collect()

        assert sorted(row["temperature"] for row in result) == pytest.approx(expected)

    def test_should_raise_error_if_value_is_not_a_number(self, spark_session: SparkSession) -> None:
        """Given a value that is not a number, the query fails."""
        lines = spark_session.createDataFrame([("30",), ("hot",)], ["value"])

        with pytest.raises(PySparkException, match="CAST_INVALID_INPUT"):
            high_temperatures(lines).collect()


class TestReadTemperatures:
    def test_should_find_high_temperatures_of_files_in_directory(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """The streaming query keeps only the high temperatures of the files in the directory."""
        for temperature in (10.5, 25.5, 33.0):
            write_reading(tmp_path, temperature)
        stream = high_temperatures(read_temperatures(spark_session, str(tmp_path)))

        # fmt: off
        query = (
            stream.writeStream
            .outputMode("append")
            .format("memory")
            .queryName("high_temperature_test")
            .trigger(availableNow=True)
            .start()
        )
        # fmt: on
        query.awaitTermination()
        result = spark_session.table("high_temperature_test").collect()

        assert sorted(row["temperature"] for row in result) == pytest.approx([25.5, 33.0])

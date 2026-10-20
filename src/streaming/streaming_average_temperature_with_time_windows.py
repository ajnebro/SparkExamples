"""Compute the average temperature of each sensor in sliding time windows.

It reads the directory as a stream of CSV files, one line `<sensor>,<temperature>,<time stamp>`
per file, and groups the temperatures in windows of 20 seconds that slide every 10 seconds, using
the time stamp written by the sensor (the event time). As the windows overlap, each temperature
belongs to two of them. The result is printed on the console after each micro-batch.

Start `streaming.temperature_sensor_with_timestamp_generator` first, in one or more other
terminals (one per sensor), so that the directory exists and receives data. The files that were
already in the directory when the query starts are also processed (`make clean` removes the
ones from previous runs).
"""

from pyspark.sql import DataFrame, SparkSession, functions
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from streaming.temperature_sensor_generator import DEFAULT_STREAMING_DIRECTORY

DEFAULT_WINDOW_DURATION = "20 seconds"
DEFAULT_SLIDE_DURATION = "10 seconds"

READING_SCHEMA = StructType(
    [
        StructField("sensor", StringType()),
        StructField("temperature", DoubleType()),
        StructField("timestamp", TimestampType()),
    ]
)


def read_readings(spark_session: SparkSession, directory: str) -> DataFrame:
    """Read as a stream the CSV files written in a directory.

    A streaming source cannot infer its schema, so it is given explicitly.

    Args:
        spark_session: Active Spark session.
        directory: Directory watched for new files.

    Returns:
        A streaming DataFrame with the columns of `READING_SCHEMA`.
    """
    # fmt: off
    return (
        spark_session.readStream
        .format("csv")
        .schema(READING_SCHEMA)
        .load(directory)
    )
    # fmt: on


def average_temperature_by_window(
    readings: DataFrame,
    window_duration: str = DEFAULT_WINDOW_DURATION,
    slide_duration: str = DEFAULT_SLIDE_DURATION,
) -> DataFrame:
    """Compute the average temperature of each sensor in each time window.

    Args:
        readings: DataFrame with the columns "sensor", "temperature" and "timestamp".
        window_duration: Length of the windows, e.g. "20 seconds".
        slide_duration: Interval between the starts of two windows, e.g. "10 seconds". It cannot
            be longer than the window.

    Returns:
        A DataFrame with the columns "window" (a struct with "start" and "end"), "sensor" and
        "average_temperature", sorted by sensor and window.

    Raises:
        PySparkException: If the slide duration is longer than the window duration.
    """
    time_window = functions.window("timestamp", window_duration, slide_duration)
    # fmt: off
    return (
        readings
        .groupBy(time_window, "sensor")
        .agg(functions.avg("temperature").alias("average_temperature"))
        .orderBy("sensor", "window")
    )
    # fmt: on


def main(directory: str = DEFAULT_STREAMING_DIRECTORY, seconds: float | None = None) -> None:
    """Run the example.

    Args:
        directory: Directory where the sensors write their files.
        seconds: Time the query runs. If `None`, it runs until it is interrupted.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("StreamingAverageTemperatureWithTimeWindows")
        .master("local[*]")
        # Each micro-batch shuffles the data by window and sensor: 200 partitions are too many
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate()
    )
    # fmt: on
    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        readings = read_readings(spark_session, directory)
        readings.printSchema()
        # Sorting a streaming aggregation is only allowed in the "complete" mode
        # fmt: off
        query = (
            average_temperature_by_window(readings)
            .writeStream
            .outputMode("complete")
            .format("console")
            .option("truncate", False)
            .start()
        )
        # fmt: on
        query.awaitTermination(seconds)
        query.stop()
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()

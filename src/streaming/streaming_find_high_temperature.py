"""Find the temperatures higher than a threshold in the files that a sensor writes.

It reads the directory as a stream: every new file is processed as it arrives, and only the
temperatures higher than the threshold are printed on the console after each micro-batch.

Start `streaming.temperature_sensor_generator` first, in another terminal, so that the
directory exists and receives data. The files that were already in the directory when the
query starts are also processed (`make clean` removes the ones from previous runs).
"""

from pyspark.sql import DataFrame, SparkSession, functions

from streaming.temperature_sensor_generator import DEFAULT_STREAMING_DIRECTORY

DEFAULT_THRESHOLD = 20.0


def read_temperatures(spark_session: SparkSession, directory: str) -> DataFrame:
    """Read as a stream the files written in a directory.

    Args:
        spark_session: Active Spark session.
        directory: Directory watched for new files.

    Returns:
        A streaming DataFrame with one row per line of text, in the column "value".
    """
    # fmt: off
    return (
        spark_session.readStream
        .format("text")
        .load(directory)
    )
    # fmt: on


def high_temperatures(lines: DataFrame, threshold: float = DEFAULT_THRESHOLD) -> DataFrame:
    """Keep the temperatures that are higher than a threshold.

    Args:
        lines: DataFrame with a column "value" that holds a temperature as text.
        threshold: Temperatures must be strictly higher than this value.

    Returns:
        A DataFrame with the column "temperature" (double).

    Raises:
        PySparkException: When the query runs, if a value is not a number.
    """
    temperature = functions.col("value").cast("double")
    # fmt: off
    return (
        lines
        .select(temperature.alias("temperature"))
        .where(temperature > threshold)
    )
    # fmt: on


def main(
    directory: str = DEFAULT_STREAMING_DIRECTORY,
    threshold: float = DEFAULT_THRESHOLD,
    seconds: float | None = None,
) -> None:
    """Run the example.

    Args:
        directory: Directory where the sensor writes its files.
        threshold: Temperatures must be strictly higher than this value to be shown.
        seconds: Time the query runs. If `None`, it runs until it is interrupted.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("StreamingFindHighTemperature")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on
    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        temperatures = read_temperatures(spark_session, directory)
        temperatures.printSchema()
        # A query without aggregation only supports the "append" mode: rows are written once
        # fmt: off
        query = (
            high_temperatures(temperatures, threshold)
            .writeStream
            .outputMode("append")
            .format("console")
            .start()
        )
        # fmt: on
        query.awaitTermination(seconds)
        query.stop()
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()

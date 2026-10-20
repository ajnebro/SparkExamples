"""Compute the average and the maximum temperature of the files that a sensor writes.

It reads the directory as a stream: every new file is processed as it arrives, and the average
and the maximum of all the temperatures received so far are printed on the console after each
micro-batch. It is a variant of `streaming.streaming_average_temperature` that computes two
aggregations in the same query.

The values are read as text, so they are converted to numbers before aggregating: the maximum
of the text "9.5" and the text "30.1" would be "9.5", because strings are compared character by
character.

Start `streaming.temperature_sensor_generator` first, in another terminal, so that the
directory exists and receives data. The files that were already in the directory when the
query starts are also processed (`make clean` removes the ones from previous runs).
"""

from pyspark.sql import DataFrame, SparkSession, functions

from streaming.temperature_sensor_generator import DEFAULT_STREAMING_DIRECTORY


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


def average_and_max_temperature(lines: DataFrame) -> DataFrame:
    """Compute the average and the maximum of the temperatures.

    Args:
        lines: DataFrame with a column "value" that holds a temperature as text.

    Returns:
        A DataFrame with a single row and the columns "average_temperature" and
        "max_temperature", both of type double.

    Raises:
        PySparkException: When the query runs, if a value is not a number.
    """
    temperature = functions.col("value").cast("double")
    return lines.select(
        functions.avg(temperature).alias("average_temperature"),
        functions.max(temperature).alias("max_temperature"),
    )


def main(directory: str = DEFAULT_STREAMING_DIRECTORY, seconds: float | None = None) -> None:
    """Run the example.

    Args:
        directory: Directory where the sensor writes its files.
        seconds: Time the query runs. If `None`, it runs until it is interrupted.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("StreamingAverageAndMaxTemperature")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on
    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        temperatures = read_temperatures(spark_session, directory)
        temperatures.printSchema()
        # An aggregation without window needs the "complete" mode: all the rows are rewritten
        # fmt: off
        query = (
            average_and_max_temperature(temperatures)
            .writeStream
            .outputMode("complete")
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

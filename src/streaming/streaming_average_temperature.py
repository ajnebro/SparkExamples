"""Compute the average temperature of the files that a sensor writes into a directory.

It reads the directory as a stream: every new file is processed as it arrives, and the average
of all the temperatures received so far is printed on the console after each micro-batch.

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
    return spark_session.readStream.format("text").load(directory)


def average_temperature(lines: DataFrame) -> DataFrame:
    """Compute the average of the temperatures.

    Args:
        lines: DataFrame with a column "value" that holds a temperature as text.

    Returns:
        A DataFrame with a single row and the column "average_temperature".

    Raises:
        PySparkException: When the query runs, if a value is not a number.
    """
    temperature = functions.col("value").cast("double")
    return lines.select(functions.avg(temperature).alias("average_temperature"))


def main(directory: str = DEFAULT_STREAMING_DIRECTORY, seconds: float | None = None) -> None:
    """Run the example.

    Args:
        directory: Directory where the sensor writes its files.
        seconds: Time the query runs. If `None`, it runs until it is interrupted.
    """
    spark_session = (
        SparkSession.builder.appName("StreamingAverageTemperature").master("local[*]").getOrCreate()
    )
    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        temperatures = read_temperatures(spark_session, directory)
        temperatures.printSchema()
        # An aggregation without window needs the "complete" mode: all the rows are rewritten
        query = (
            average_temperature(temperatures)
            .writeStream.outputMode("complete")
            .format("console")
            .start()
        )
        query.awaitTermination(seconds)
        query.stop()
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()

"""Compute the average and maximum temperature of each sensor from the files they write.

It reads the directory as a stream of CSV files, one line `<sensor>,<temperature>,<time stamp>`
per file, and prints on the console, after each micro-batch, the sensors whose statistics have
changed. The time stamp of each line is not used here, so it is left out of the schema: Spark
ignores the columns of the file that the schema does not declare.

Start `streaming.temperature_sensor_with_timestamp_generator` first, in one or more other
terminals (one per sensor), so that the directory exists and receives data. The files that were
already in the directory when the query starts are also processed (`make clean` removes the
ones from previous runs).
"""

from pyspark.sql import DataFrame, SparkSession, functions
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from streaming.temperature_sensor_generator import DEFAULT_STREAMING_DIRECTORY

READING_SCHEMA = StructType(
    [
        StructField("sensor", StringType()),
        StructField("temperature", DoubleType()),
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
    return spark_session.readStream.format("csv").schema(READING_SCHEMA).load(directory)


def temperature_by_sensor(readings: DataFrame) -> DataFrame:
    """Compute the average and the maximum temperature of each sensor.

    Args:
        readings: DataFrame with the columns "sensor" and "temperature".

    Returns:
        A DataFrame with one row per sensor and the columns "sensor", "average_temperature"
        and "max_temperature".
    """
    return readings.groupBy("sensor").agg(
        functions.avg("temperature").alias("average_temperature"),
        functions.max("temperature").alias("max_temperature"),
    )


def main(directory: str = DEFAULT_STREAMING_DIRECTORY, seconds: float | None = None) -> None:
    """Run the example.

    Args:
        directory: Directory where the sensors write their files.
        seconds: Time the query runs. If `None`, it runs until it is interrupted.
    """
    spark_session = (
        SparkSession.builder.appName("StreamingAverageTemperatureFromSeveralSensors")
        .master("local[*]")
        # Each micro-batch shuffles the data by sensor: the default 200 partitions are too many
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate()
    )
    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        readings = read_readings(spark_session, directory)
        readings.printSchema()
        # In the "update" mode only the sensors whose row has changed are printed
        query = (
            temperature_by_sensor(readings)
            .writeStream.outputMode("update")
            .format("console")
            .start()
        )
        query.awaitTermination(seconds)
        query.stop()
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()

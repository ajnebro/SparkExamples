"""Count the airports of a country grouped by airport type, using the OurAirports data.

The data is the `airports.csv` file published by OurAirports (https://ourairports.com/data/),
which has one row per airport with columns such as `type`, `name` and `iso_country`.
Spark cannot read a CSV file from an HTTP(S) URL, so the file must be downloaded first:

    curl -o data/airports.csv https://davidmegginson.github.io/ourairports-data/airports.csv
"""

import time

from pyspark.sql import DataFrame, SparkSession, functions

DEFAULT_INPUT_FILE = "data/airports.csv"
DEFAULT_COUNTRY = "ES"


def read_airports(spark_session: SparkSession, file_name: str) -> DataFrame:
    """Read the airports CSV file, inferring the type of each column.

    Args:
        spark_session: Active Spark session.
        file_name: Path of the CSV file.

    Returns:
        A DataFrame with one row per airport.
    """
    return spark_session.read.option("header", "true").option("inferSchema", "true").csv(file_name)


def count_airports_by_type(airports: DataFrame, country: str) -> DataFrame:
    """Count the airports of a country by type, from the most to the least frequent type.

    Args:
        airports: DataFrame with the columns "iso_country" and "type".
        country: ISO 3166-1 alpha-2 code of the country, e.g. "ES".

    Returns:
        A DataFrame with the columns "type" and "count".
    """
    return (
        airports.filter(functions.col("iso_country") == country)
        .groupBy("type")
        .count()
        .orderBy(functions.col("count").desc(), functions.col("type"))
    )


def main(file_name: str = DEFAULT_INPUT_FILE, country: str = DEFAULT_COUNTRY) -> None:
    """Run the example.

    Args:
        file_name: Path of the airports CSV file.
        country: ISO 3166-1 alpha-2 code of the country whose airports are counted.
    """
    spark_session = (
        SparkSession.builder.appName("CountAirportsByType").master("local[*]").getOrCreate()
    )
    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        start_time = time.perf_counter()
        # The DataFrame is used by several actions, so it is kept in memory
        airports = read_airports(spark_session, file_name).persist()
        airports.printSchema()
        airports.show(20)
        count_airports_by_type(airports, country).show()
        print(f"Computing time: {time.perf_counter() - start_time:.3f} s")
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()

"""Count the airports of a country by type, reading the CSV file with an explicit schema.

It does the same as `count_airports_by_type`, but the type of each column is declared instead
of inferred, so Spark reads the file only once. The file must be downloaded first:

    curl -o data/airports.csv https://davidmegginson.github.io/ourairports-data/airports.csv
"""

import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from dataframes.count_airports_by_type import count_airports_by_type

DEFAULT_INPUT_FILE = "data/airports.csv"
DEFAULT_COUNTRY = "ES"

# The fields must be in the same order as the columns of the file
AIRPORTS_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("ident", StringType()),
        StructField("type", StringType()),
        StructField("name", StringType()),
        StructField("latitude_deg", DoubleType()),
        StructField("longitude_deg", DoubleType()),
        StructField("elevation_ft", IntegerType()),
        StructField("continent", StringType()),
        StructField("iso_country", StringType()),
        StructField("iso_region", StringType()),
        StructField("municipality", StringType()),
        StructField("scheduled_service", StringType()),
        StructField("icao_code", StringType()),
        StructField("iata_code", StringType()),
        StructField("gps_code", StringType()),
        StructField("local_code", StringType()),
        StructField("home_link", StringType()),
        StructField("wikipedia_link", StringType()),
        StructField("keywords", StringType()),
    ]
)


def read_airports_with_schema(spark_session: SparkSession, file_name: str) -> DataFrame:
    """Read the airports CSV file using the explicit schema.

    With `enforceSchema` set to false, Spark checks the header against the schema when it parses
    the columns, and fails if they do not match. Otherwise, it would silently assign the columns
    by position, so a missing field would shift the values of all the following columns.

    Args:
        spark_session: Active Spark session.
        file_name: Path of the CSV file.

    Returns:
        A DataFrame with one row per airport.
    """
    # fmt: off
    return (
        spark_session.read
        .schema(AIRPORTS_SCHEMA)
        .option("header", "true")
        .option("enforceSchema", "false")
        .csv(file_name)
    )
    # fmt: on


def main(file_name: str = DEFAULT_INPUT_FILE, country: str = DEFAULT_COUNTRY) -> None:
    """Run the example.

    Args:
        file_name: Path of the airports CSV file.
        country: ISO 3166-1 alpha-2 code of the country whose airports are counted.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("CountAirportsByTypeWithSchema")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on
    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        start_time = time.perf_counter()
        # The DataFrame is used by several actions, so it is kept in memory
        airports = read_airports_with_schema(spark_session, file_name).persist()
        airports.printSchema()
        airports.show(20)
        count_airports_by_type(airports, country).show()
        print(f"Computing time: {time.perf_counter() - start_time:.3f} s")
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()

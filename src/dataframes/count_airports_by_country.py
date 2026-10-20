"""Get the countries with the most airports, joining the OurAirports airports and countries data.

The `airports.csv` file only contains the ISO code of the country of each airport, so it is
joined with `countries.csv` to get the name of the country. Both files are published by
OurAirports (https://ourairports.com/data/) and must be downloaded first:

    curl -o data/airports.csv https://davidmegginson.github.io/ourairports-data/airports.csv
    curl -o data/countries.csv https://davidmegginson.github.io/ourairports-data/countries.csv
"""

import time

from pyspark.sql import DataFrame, SparkSession, functions

from dataframes.count_airports_by_type import read_airports

DEFAULT_AIRPORTS_FILE = "data/airports.csv"
DEFAULT_COUNTRIES_FILE = "data/countries.csv"
DEFAULT_TOP = 10


def read_countries(spark_session: SparkSession, file_name: str) -> DataFrame:
    """Read the countries CSV file, inferring the type of each column.

    Args:
        spark_session: Active Spark session.
        file_name: Path of the CSV file.

    Returns:
        A DataFrame with one row per country.
    """
    # fmt: off
    return (
        spark_session.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_name)
    )
    # fmt: on


def count_airports_by_country(airports: DataFrame, countries: DataFrame, top: int) -> DataFrame:
    """Get the countries with the most airports, from the most to the least.

    The countries table is small, so it is broadcast: a copy is sent to every executor and the
    join does not need to shuffle the airports.

    Args:
        airports: DataFrame with an "iso_country" column.
        countries: DataFrame with the columns "code" and "name".
        top: Number of countries to return.

    Returns:
        A DataFrame with the columns "country" and "airports".
    """
    country_names = countries.select(
        functions.col("code").alias("iso_country"), functions.col("name").alias("country")
    )
    # fmt: off
    return (
        airports
        .join(functions.broadcast(country_names), on="iso_country")
        .groupBy("country")
        .agg(functions.count("*").alias("airports"))
        .orderBy(functions.col("airports").desc(), functions.col("country"))
        .limit(top)
    )
    # fmt: on


def main(
    airports_file: str = DEFAULT_AIRPORTS_FILE,
    countries_file: str = DEFAULT_COUNTRIES_FILE,
    top: int = DEFAULT_TOP,
) -> None:
    """Run the example.

    Args:
        airports_file: Path of the airports CSV file.
        countries_file: Path of the countries CSV file.
        top: Number of countries to show.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("CountAirportsByCountry")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on
    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        start_time = time.perf_counter()
        airports = read_airports(spark_session, airports_file)
        countries = read_countries(spark_session, countries_file)
        result = count_airports_by_country(airports, countries, top)
        result.show()
        # The physical plan shows a BroadcastHashJoin instead of a SortMergeJoin
        result.explain()
        print(f"Computing time: {time.perf_counter() - start_time:.3f} s")
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()

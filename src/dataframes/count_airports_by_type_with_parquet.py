"""Count the airports of a country by type, reading the data from CSV and from Parquet.

The CSV file is converted to Parquet, a columnar format, partitioned by country: there is one
directory per country (`iso_country=ES`, `iso_country=FR`, ...). Then the same query is run on
both formats. With Parquet, Spark only reads the columns used by the query (column pruning) and
only the directory of the country (partition pruning), as the physical plans show. The CSV file
must be downloaded first:

    curl -o data/airports.csv https://davidmegginson.github.io/ourairports-data/airports.csv
"""

import time

from pyspark.sql import DataFrame, SparkSession

from dataframes.count_airports_by_type import count_airports_by_type, read_airports

DEFAULT_INPUT_FILE = "data/airports.csv"
DEFAULT_PARQUET_DIRECTORY = "output/airports_parquet"
DEFAULT_COUNTRY = "ES"


def write_airports_as_parquet(airports: DataFrame, directory: str) -> None:
    """Write the airports in Parquet format, partitioned by country, replacing any previous data.

    Args:
        airports: DataFrame with an "iso_country" column.
        directory: Directory where the Parquet files are written.
    """
    airports.write.mode("overwrite").partitionBy("iso_country").parquet(directory)


def read_airports_from_parquet(spark_session: SparkSession, directory: str) -> DataFrame:
    """Read the airports from Parquet files. The schema is stored in the files themselves.

    Args:
        spark_session: Active Spark session.
        directory: Directory with the Parquet files.

    Returns:
        A DataFrame with one row per airport.
    """
    return spark_session.read.parquet(directory)


def run_query(airports: DataFrame, country: str) -> None:
    """Show the airports of a country by type, the physical plan and the time of the query.

    Args:
        airports: DataFrame with the columns "iso_country" and "type".
        country: ISO 3166-1 alpha-2 code of the country.
    """
    start_time = time.perf_counter()
    result = count_airports_by_type(airports, country)
    result.show()
    print(f"Query time: {time.perf_counter() - start_time:.3f} s")
    result.explain()


def main(
    file_name: str = DEFAULT_INPUT_FILE,
    parquet_directory: str = DEFAULT_PARQUET_DIRECTORY,
    country: str = DEFAULT_COUNTRY,
) -> None:
    """Run the example.

    Args:
        file_name: Path of the airports CSV file.
        parquet_directory: Directory where the airports are written in Parquet format.
        country: ISO 3166-1 alpha-2 code of the country whose airports are counted.
    """
    spark_session = (
        SparkSession.builder.appName("CountAirportsByTypeWithParquet")
        .master("local[*]")
        .getOrCreate()
    )
    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        csv_airports = read_airports(spark_session, file_name)
        write_airports_as_parquet(csv_airports, parquet_directory)
        print("Query on the CSV file:")
        run_query(csv_airports, country)
        print("Query on the Parquet files:")
        run_query(read_airports_from_parquet(spark_session, parquet_directory), country)
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()

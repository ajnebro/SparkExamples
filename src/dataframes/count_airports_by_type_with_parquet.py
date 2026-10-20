"""Count the airports of a country by type, reading the data from Parquet files.

Parquet is a columnar file format widely used with Spark. Unlike CSV, it stores the schema
together with the data, it is compressed, and the values of each column are stored together,
so Spark only reads the columns that a query uses.

The CSV file is converted to Parquet partitioned by country: there is one directory per country
(`iso_country=ES`, `iso_country=FR`, ...). When a query filters by country, Spark only reads the
directory of that country. The physical plan shows both things: `ReadSchema` lists the columns
that are read and `PartitionFilters` the partitions that are selected.

The CSV file must be downloaded first:

    curl -o data/airports.csv https://davidmegginson.github.io/ourairports-data/airports.csv
"""

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
    # fmt: off
    (
        airports.write
        .mode("overwrite")
        .partitionBy("iso_country")
        .parquet(directory)
    )
    # fmt: on


def read_airports_from_parquet(spark_session: SparkSession, directory: str) -> DataFrame:
    """Read the airports from Parquet files. The schema is stored in the files themselves.

    Args:
        spark_session: Active Spark session.
        directory: Directory with the Parquet files.

    Returns:
        A DataFrame with one row per airport.
    """
    return spark_session.read.parquet(directory)


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
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("CountAirportsByTypeWithParquet")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on

    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        write_airports_as_parquet(read_airports(spark_session, file_name), parquet_directory)
        airports = read_airports_from_parquet(spark_session, parquet_directory)
        airports.printSchema()
        result = count_airports_by_type(airports, country)
        result.show()
        # ReadSchema contains only "type", and PartitionFilters selects the country directory
        result.explain()
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()

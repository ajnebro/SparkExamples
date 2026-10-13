"""Count the airports of a country by type with a Spark SQL query.

It does the same as `count_airports_by_type`, but the query is written in SQL over a temporary
view instead of with the DataFrame API. The file must be downloaded first:

    curl -o data/airports.csv https://davidmegginson.github.io/ourairports-data/airports.csv
"""

import time

from pyspark.sql import DataFrame, SparkSession

from dataframes.count_airports_by_type import read_airports

DEFAULT_INPUT_FILE = "data/airports.csv"
DEFAULT_COUNTRY = "ES"

# `:country` is a named parameter: Spark replaces it with a value given in `args`
COUNT_BY_TYPE_QUERY = """
    SELECT type, COUNT(*) AS count
    FROM airports
    WHERE iso_country = :country
    GROUP BY type
    ORDER BY count DESC, type
"""


def count_airports_by_type_with_sql(
    spark_session: SparkSession, airports: DataFrame, country: str
) -> DataFrame:
    """Count the airports of a country by type, from the most to the least frequent type.

    Args:
        spark_session: Active Spark session.
        airports: DataFrame with the columns "iso_country" and "type".
        country: ISO 3166-1 alpha-2 code of the country, e.g. "ES".

    Returns:
        A DataFrame with the columns "type" and "count".
    """
    # The view makes the DataFrame available to SQL queries with the name "airports"
    airports.createOrReplaceTempView("airports")
    return spark_session.sql(COUNT_BY_TYPE_QUERY, args={"country": country})


def main(file_name: str = DEFAULT_INPUT_FILE, country: str = DEFAULT_COUNTRY) -> None:
    """Run the example.

    Args:
        file_name: Path of the airports CSV file.
        country: ISO 3166-1 alpha-2 code of the country whose airports are counted.
    """
    spark_session = (
        SparkSession.builder.appName("CountAirportsByTypeWithSql").master("local[*]").getOrCreate()
    )
    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        start_time = time.perf_counter()
        airports = read_airports(spark_session, file_name)
        count_airports_by_type_with_sql(spark_session, airports, country).show()
        print(f"Computing time: {time.perf_counter() - start_time:.3f} s")
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()

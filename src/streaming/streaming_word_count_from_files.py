"""Count the words of the text files that are written into a directory.

It reads the directory as a stream: every new file is processed as it arrives, and the number of
times that each word has appeared in all the files received so far is printed on the console
after each micro-batch, from the most to the least frequent. The text is normalized first: it is
converted to lower case and split by any character that is not a letter or a digit, so "El",
"el" and "el," are the same word.

There is no generator for this example: copy text files into the directory while it runs, e.g.

    cp data/quijote.txt output/streaming/

The files that were already in the directory when the query starts are also processed
(`make clean` removes the ones from previous runs).

About the number of partitions
------------------------------
A `groupBy` needs a shuffle: all the rows of a word must end up in the same partition. The
number of partitions of that shuffle is set by `spark.sql.shuffle.partitions`, whose default
value is 200. It is a reasonable value for a cluster that processes a lot of data, but not for
this example: in a streaming query, each micro-batch runs the aggregation as 200 tasks, and the
state with the counts is also split in 200 pieces, even if there are only a few files and a few
hundred words. In `local` mode, the cost of scheduling that many tasks is much bigger than the
cost of computing the counts, and every micro-batch becomes slow. That is why `main` sets it to
a small value. In a real application the value must be chosen carefully from the start,
because a streaming query that stores its state in a checkpoint cannot change it when it is
restarted.
"""

from pyspark.sql import DataFrame, SparkSession, functions

from streaming.temperature_sensor_generator import DEFAULT_STREAMING_DIRECTORY

SHUFFLE_PARTITIONS = 4
NOT_LETTER_NOR_DIGIT = r"[^\p{L}\p{N}]+"


def read_lines(spark_session: SparkSession, directory: str) -> DataFrame:
    """Read as a stream the text files written in a directory.

    Args:
        spark_session: Active Spark session.
        directory: Directory watched for new files.

    Returns:
        A streaming DataFrame with one row per line of text, in the column "value".
    """
    return spark_session.readStream.format("text").load(directory)


def count_words(lines: DataFrame) -> DataFrame:
    """Count how many times each word appears, ignoring case and punctuation.

    Args:
        lines: DataFrame with a column "value" that holds a line of text.

    Returns:
        A DataFrame with the columns "word" and "count", sorted from the most to the least
        frequent word (ties sorted alphabetically).
    """
    words = lines.select(
        functions.explode(functions.split(functions.lower("value"), NOT_LETTER_NOR_DIGIT)).alias(
            "word"
        )
    )
    return (
        words.where(functions.col("word") != "")
        .groupBy("word")
        .count()
        .orderBy(functions.desc("count"), "word")
    )


def main(directory: str = DEFAULT_STREAMING_DIRECTORY, seconds: float | None = None) -> None:
    """Run the example.

    Args:
        directory: Directory where the text files are written.
        seconds: Time the query runs. If `None`, it runs until it is interrupted.
    """
    spark_session = (
        SparkSession.builder.appName("StreamingWordCountFromFiles")
        .master("local[*]")
        # The default is 200 partitions, too many for this data (see the module docstring)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        lines = read_lines(spark_session, directory)
        lines.printSchema()
        # Sorting a streaming aggregation is only allowed in the "complete" mode
        query = count_words(lines).writeStream.outputMode("complete").format("console").start()
        query.awaitTermination(seconds)
        query.stop()
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()

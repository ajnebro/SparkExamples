"""SparkSession creation."""

from pyspark.sql import SparkSession


def create_spark_session(app_name: str, master: str = "local[*]") -> SparkSession:
    """Create (or reuse) a SparkSession.

    Args:
        app_name: Name of the Spark application.
        master: Spark master URL.

    Returns:
        The active SparkSession.

    Raises:
        ValueError: If `app_name` is empty.
    """
    if not app_name:
        raise ValueError("app_name must not be empty")

    return SparkSession.builder.appName(app_name).master(master).getOrCreate()

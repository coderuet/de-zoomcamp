import sys
from typing import Optional, Dict, Union
from pyspark.sql import SparkSession, DataFrame


def init_spark(
    app_name: str = "ZoomCamp",
    config_params: Optional[Dict[str, Union[str, int]]] = None,
) -> SparkSession:
    """
    Args:
        app_name (str, optional): _description_. Defaults to "ZoomCamp".
        config_params (Optional[Dict[str, Union[str, int]]], optional): _description_. Defaults to None.
    Returns:
        SparkSession: _description_
    """
    spark_builder = SparkSession.builder.appName(app_name)
    if config_params:
        for key, value in config_params.items():
            spark_builder = spark_builder.config(key, str(value))
    spark = spark_builder.getOrCreate()
    return spark


def read_file(spark: SparkSession, path_file: str, **kwargs) -> DataFrame:
    """
    Read data from a file or a database connection string.

    :param spark: SparkSession instance.
    :param path_file: Path to the file OR a JDBC connection string.
    :param kwargs: Additional options for Spark reader
    :return: A Spark DataFrame or None if an error occurs.
    """
    if path_file.startswith("jdbc:"):  # Detect if it's a JDBC connection string
        db_table = kwargs.get("dbtable")
        if not db_table:
            raise ValueError("Missing 'dbtable' argument for database reading")
        df = (
            spark.read.format("jdbc")
            .options(
                url=path_file,
                dbtable=db_table,
                **{
                    k: v
                    for k, v in kwargs.items()
                    if k in ["user", "password", "driver"]
                },
            )
            .load()
        )
    elif path_file.endswith(".csv"):
        df = spark.read.csv(path=path_file, **kwargs)
    elif path_file.endswith(".parquet"):
        df = spark.read.parquet(path=path_file, **kwargs)
    elif path_file.endswith(".json"):
        df = spark.read.json(path=path_file, **kwargs)
    else:
        raise ValueError(f"Unsupported file format or connection string: {path_file}")
    return df

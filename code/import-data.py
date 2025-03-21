# rewrite by pyspark
from constants.constants import Constants
from pyspark_utils import init_spark, read_file
import os, glob, argparse
from pyspark.sql import DataFrame

BASE_DIR = "/home/thanh-ubuntu/workspace/DE/DE-study/de-zoomcamp/"


def main(params):

    # Get env var
    port = Constants.MAIN_DB_PORT
    username = Constants.MAIN_DB_USERNAME
    password = Constants.MAIN_DB_PASSWORD
    db_name = Constants.MAIN_DB_DATABASE_NAME
    hostname = Constants.MAIN_DB_HOST
    table = params.table
    JDBC_URL = f"""jdbc:postgresql://{hostname}:{port}/{db_name}"""
    print("JDBC_URL", JDBC_URL)
    # Config spark
    config = {
        "spark.jars": BASE_DIR + "jars/postgresql-42.7.5.jar",
    }
    properties = {
        "user": username,
        "password": password,
        "driver": "org.postgresql.Driver",
    }
    # Init spark
    spark = init_spark(config_params=config)

    # read all file from path
    all_files = glob.glob(BASE_DIR + "data/*.csv")
    for file_path in all_files:
        df: DataFrame = read_file(spark=spark, path_file=file_path, header=True)
        filename = os.path.basename(file_path)
        table = filename.split(".")[0]
        print("-----Writing to table ", table)
        df.write.jdbc(
            url=JDBC_URL, table=table, mode="overwrite", properties=properties
        )
        print("-----Writing to table done")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--table", help="Import data to table.")
    args = parser.parse_args()
    main(args)

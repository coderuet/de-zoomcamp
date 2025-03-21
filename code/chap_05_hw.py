# https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/05-batch/homework.md


from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
from io import BytesIO
from datetime import datetime

PATH_FILE = "./data/yellow_tripdata_2024-10.parquet"


# def question_2():
#     spark: SparkSession = SparkSession.builder.appName("Homework Chap 5 ").getOrCreate()


#     df: DataFrame = spark.read.parquet(PATH_FILE, header=True)
#     df.repartition(4)
#     df.write.parquet("./data/question_2")
# def question_3():
#     spark: SparkSession = SparkSession.builder.appName("Homework Chap 5 ").getOrCreate()
#     df: DataFrame = spark.read.parquet(PATH_FILE, header=True)
#     count_records = df.where(
#         (f.to_date(f.col("tpep_pickup_datetime")) == "2024-10-15")
#         & (f.to_date(f.col("tpep_dropoff_datetime")) == "2024-10-15")
#     ).count()
#     print("count", count_records)


# def question_4():
#     spark: SparkSession = SparkSession.builder.appName("Homework Chap 5 ").getOrCreate()
#     df: DataFrame = spark.read.parquet(PATH_FILE, header=True)
#     df = df.withColumn(
#         "time_driving",
#         (
#             f.unix_timestamp(f.col("tpep_dropoff_datetime"))
#             - f.unix_timestamp(f.col("tpep_pickup_datetime"))
#         )
#         / 3600,
#     )
#     df.select(f.max("time_driving")).show()
# df.show(10)


def question_5():
    spark: SparkSession = SparkSession.builder.appName("Homework Chap 5 ").getOrCreate()
    df: DataFrame = spark.read.parquet(PATH_FILE, header=True)
    zone_df: DataFrame = spark.read.csv("./data/taxi_zone_lookup.csv", header=True)
    df = df.join(zone_df, df["PULocationID"] == zone_df["LocationID"])
    df.groupBy("Zone").count().orderBy(f.asc("count")).select(["Zone", "count"]).limit(
        1
    ).show()


if __name__ == "__main__":
    question_5()


# Question 1 :
# 25/03/18 01:19:57 WARN Utils: Your hostname, thanh-ubuntu resolves to a loopback address: 127.0.1.1; using 192.168.100.248 instead (on interface enp5s0)
# 25/03/18 01:19:57 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
# Welcome to
#       ____              __
#      / __/__  ___ _____/ /__
#     _\ \/ _ \/ _ `/ __/  '_/
#    /___/ .__/\_,_/_/ /_/\_\   version 3.5.5
#       /_/

# Using Scala version 2.12.18, OpenJDK 64-Bit Server VM, 17.0.14
# Branch HEAD
# Compiled by user ubuntu on 2025-02-23T20:30:46Z
# Revision 7c29c664cdc9321205a98a14858aaf8daaa19db2
# Url https://github.com/apache/spark
# Type --help for more information


# Question 2:

# 4.0K	part-00000-67783f97-cd02-4792-baa1-7dc91fb65660-c000.snappy.parquet
# 21M	part-00001-67783f97-cd02-4792-baa1-7dc91fb65660-c000.snappy.parquet
# 21M	part-00004-67783f97-cd02-4792-baa1-7dc91fb65660-c000.snappy.parquet
# 21M	part-00007-67783f97-cd02-4792-baa1-7dc91fb65660-c000.snappy.parquet
# 14M	part-00010-67783f97-cd02-4792-baa1-7dc91fb65660-c000.snappy.parquet


# Question 3 : 128893

# Question 4 : 162.61777777777777

# Question 5 :4040

# Question 6 : Governor's Island/Ellis Island/Liberty Island 1

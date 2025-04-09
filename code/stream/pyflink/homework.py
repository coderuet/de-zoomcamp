# This file for homework pyflink

# Link: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/06-streaming/homework.md


# Question 1 : Redpanda version
# command : docker exec -it redpanda-1 rpk --version
# Result : rpk version v24.2.18 (rev f9a22d4430)


# Question 2 : Creating a topic
# command: docker exec -it redpanda-1 rpk topic create green-trips


# Question 3 : Connecting to the Kafka server
# Return True


# Question 4: Sending the Trip Data
# Code :

# import json
# from time import time
# import csv
# from kafka import KafkaProducer
# import pandas as pd


# def json_serializer(data):
#     return json.dumps(data).encode("utf-8")


# server = "localhost:9092"

# producer = KafkaProducer(bootstrap_servers=[server], value_serializer=json_serializer)


# csv_file = "/home/thanh-ubuntu/workspace/DE/DE-study/de-zoomcamp/data/green_tripdata_2019-10.csv"
# df = pd.read_csv(
#     csv_file,
#     usecols=[
#         "lpep_pickup_datetime",
#         "lpep_dropoff_datetime",
#         "PULocationID",
#         "DOLocationID",
#         "passenger_count",
#         "trip_distance",
#         "tip_amount",
#     ],
# )
# start_time = time()
# for _, row in df.iterrows():
#     producer.send("green-trips", row.to_dict())
# producer.flush()
# end_time = time()
# print(f"Time taken: {end_time - start_time} seconds")

# 84.8409514427185 seconds

# Question 5 :

# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.table import (
#     EnvironmentSettings,
#     TableEnvironment,
#     StreamTableEnvironment,
# )


# def taxi_session_analysis():
#     # Set up the streaming environment
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)

#     # Create a table environment
#     settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
#     t_env = StreamTableEnvironment.create(env, settings)

#     # Define the Kafka source table using SQL
#     t_env.execute_sql(
#         """
#         CREATE TABLE green_trips (
#             VendorID INT,
#             lpep_pickup_datetime BIGINT,
#             lpep_dropoff_datetime BIGINT,
#             store_and_fwd_flag STRING,
#             RatecodeID INT,
#             PULocationID INT,
#             DOLocationID INT,
#             passenger_count INT,
#             trip_distance DOUBLE,
#             fare_amount DOUBLE,
#             extra DOUBLE,
#             mta_tax DOUBLE,
#             tip_amount DOUBLE,
#             tolls_amount DOUBLE,
#             ehail_fee DOUBLE,
#             improvement_surcharge DOUBLE,
#             total_amount DOUBLE,
#             payment_type INT,
#             trip_type INT,
#             event_time AS TO_TIMESTAMP(FROM_UNIXTIME(lpep_dropoff_datetime / 1000)),
#             WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
#         ) WITH (
#             'connector' = 'kafka',
#             'topic' = 'green-trips',
#             'properties.bootstrap.servers' = 'localhost:9092',
#             'properties.group.id' = 'session-window-group',
#             'scan.startup.mode' = 'earliest-offset',
#             'format' = 'json',
#             'json.fail-on-missing-field' = 'false',
#             'json.ignore-parse-errors' = 'true'
#         )
#     """
#     )

#     # In Flink 1.16.0, use different syntax for session windows
#     t_env.execute_sql(
#         """
#         CREATE VIEW session_results AS
#         SELECT
#             PULocationID,
#             DOLocationID,
#             COUNT(*) AS trip_count,
#             MIN(lpep_dropoff_datetime) AS start_time,
#             MAX(lpep_dropoff_datetime) AS end_time
#         FROM TABLE(
#             TUMBLE(TABLE green_trips, DESCRIPTOR(event_time), INTERVAL '5' MINUTES)
#         )
#         GROUP BY PULocationID, DOLocationID, window_start, window_end
#     """
#     )

#     # Define a sink to view results
#     t_env.execute_sql(
#         """
#         CREATE TABLE print_sink (
#             PULocationID INT,
#             DOLocationID INT,
#             trip_count BIGINT,
#             start_time BIGINT,
#             end_time BIGINT
#         ) WITH (
#             'connector' = 'print'
#         )
#     """
#     )

#     # Insert results into print sink
#     t_env.execute_sql(
#         """
#         INSERT INTO print_sink
#         SELECT
#             PULocationID,
#             DOLocationID,
#             trip_count,
#             start_time,
#             end_time
#         FROM session_results
#     """
#     )


# if __name__ == "__main__":
#     taxi_session_analysis()


# Job has been submitted with JobID 81c3f0b2c742f1f324795e23fef8af0b

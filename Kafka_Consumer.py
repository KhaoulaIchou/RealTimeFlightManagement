import os
import json
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from elasticsearch import Elasticsearch
from subprocess import check_output

from pyspark.sql.types import StructField, DoubleType, IntegerType, LongType

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.elasticsearch:elasticsearch-spark-30_2.12:7.14.2 pyspark-shell"

es = Elasticsearch(hosts=['localhost'], port=9200)

topic = "flight-realtime"
ETH0_IP = check_output(["hostname"]).decode(encoding="utf-8").strip()
SPARK_MASTER_URL = "local[*]"
SPARK_DRIVER_HOST = ETH0_IP

spark_conf = SparkConf()
spark_conf.setAll(
    [
        ("spark.master", SPARK_MASTER_URL),
        ("spark.driver.bindAddress", "0.0.0.0"),
        ("spark.driver.host", SPARK_DRIVER_HOST),
        ("spark.app.name", "Flight-infos"),
        ("spark.submit.deployMode", "client"),
        ("spark.ui.showConsoleProgress", "true"),
        ("spark.eventLog.enabled", "false"),
        ("spark.logConf", "false"),
    ]
)

if __name__ == "__main__":
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "192.168.81.44:9092")
        .option("subscribe", "flight-realtime")
        .option("enable.auto.commit", "true")
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING)")
    )

    schema = StructType([
        StructField("hex", StringType()),
        StructField("reg_number", StringType()),
        StructField("flag", StringType()),
        StructField("lat", DoubleType()),
        StructField("lng", DoubleType()),
        StructField("alt", IntegerType()),
        StructField("dir", IntegerType()),
        StructField("speed", IntegerType()),
        StructField("flight_number", StringType()),
        StructField("flight_icao", StringType()),
        StructField("flight_iata", StringType()),
        StructField("dep_icao", StringType()),
        StructField("dep_iata", StringType()),
        StructField("arr_icao", StringType()),
        StructField("arr_iata", StringType()),
        StructField("airline_icao", StringType()),
        StructField("airline_iata", StringType()),
        StructField("aircraft_icao", StringType()),
        StructField("updated", LongType()),
        StructField("status", StringType()),
    ])


    def func_call(df, batch_id):
        try:
            df = df.withColumn("value", from_json("value", schema))
            df.persist()

            for row in df.collect():
                dictio = row['value'].asDict()
                print(f"Data: {dictio}")  # Print the data

                try:
                    dictio["speed"] = float(dictio.get("speed", "0"))
                except ValueError:
                    dictio["speed"] = 0.0

                try:
                    dictio["speed"] = max(min(dictio["speed"], 1000), 0) / 1000
                except Exception as e:
                    dictio["speed"] = 0.0

                if dictio["speed"] > 0.8:
                    print("Incident detected: excessive speed")

                es.index(
                    index="flight-realtime-visualization",
                    id=dictio["hex"],
                    body=dictio
                )
        except Exception as e:
            print(f"An error occurred: {e}")


    query = df.writeStream \
        .format('console') \
        .foreachBatch(func_call) \
        .trigger(processingTime="30 seconds") \
        .start().awaitTermination()

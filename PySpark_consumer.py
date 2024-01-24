import os
import json
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col
from pyspark import SparkContext
from elasticsearch import Elasticsearch
from subprocess import check_output

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.elasticsearch:elasticsearch-spark-30_2.12:7.14.2 pyspark-shell"

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

# Adjust the configuration according to your Elasticsearch setup
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

if __name__ == "__main__":
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "192.168.43.224:9092")
        .option("subscribe", topic)
        .option("enable.auto.commit", "true")
        .load()
    )

    df = df.selectExpr("CAST(value AS STRING)")

    schema = StructType(
        [
            StructField("hex", StringType()),
            StructField("flag", StringType()),
            StructField("lat", DoubleType()),
            StructField("lng", DoubleType()),
            StructField("alt", IntegerType()),
            StructField("dir", IntegerType()),
        ]
    )

    df = df.select(from_json(col("value"), schema).alias("flight"))

    """    def func_call(df, batch_id):
        df = df.select("flight.*")
        flight = df.collect()
        for i in flight:
            print("writing_to_Elasticsearch")
            # print the flight data to the console
            print({
                "hex": i.hex,
                "flag": i.flag,
                "lat": i.lat,
                "lng": i.lng,
                "alt": i.alt,
                "dir": i.dir,
            })
            es.index(
                index="flight-realtime-project",
                id=i.hex,  # assuming i.hex is the unique identifier
                body={
                    "hex": i.hex,
                    "flag": i.flag,
                    "lat": i.lat,
                    "lng": i.lng,
                    "alt": i.alt,
                    "dir": i.dir,
                },
            )
    """


    def getrows(df, rownums=None):
        return df.rdd.zipWithIndex().filter(lambda x: x[1] in rownums).map(lambda x: x[0])


    from pyspark.sql.functions import col
    import json


    def func_call(df, batch_id):
        df = df.selectExpr("CAST(flight AS STRING) as json")
        requests = df.rdd.map(lambda x: x.json).collect()

        for request in requests:
            dictio = json.loads(request)
            print(dictio)
            print("writing_to_Elasticsearch")
            es.index(
                index="flight-realtime",
                id=dictio["hex"],
                body={
                    "hex": dictio["hex"],
                    "flag": dictio.get("flag"),
                    "lat": dictio.get("lat"),
                    "lng": dictio.get("lng"),
                    "alt": dictio.get("alt"),
                    "dir": dictio.get("dir"),
                }
            )


    query = df.writeStream \
        .outputMode("append") \
        .foreachBatch(func_call) \
        .trigger(processingTime="30 seconds") \
        .start().awaitTermination()

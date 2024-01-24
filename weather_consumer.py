# Kafka consommateur :
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
import os

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.elasticsearch:elasticsearch-spark-30_2.12:7.14.2 pyspark-shell"

def process_weather_data(message):
    # Define the schema to parse the JSON message from Kafka
    schema = StructType([
        StructField("name", StringType()),
        StructField("weather", ArrayType(StructType([
            StructField("main", StringType()),
            StructField("description", StringType())
        ]))),
        StructField("main", StructType([
            StructField("temp", DoubleType()),
            StructField("humidity", IntegerType())
        ])),
        StructField("wind", StructType([
            StructField("speed", DoubleType())
        ]))
    ])

    # Read the JSON message from Kafka
    weather_data = message.selectExpr("CAST(value AS STRING)")
    parsed_data = weather_data.select(from_json(weather_data.value, schema).alias("weather_data"))

    # Select required columns for further processing
    processed_data = parsed_data.select(
        "weather_data.name",
        "weather_data.weather.main",
        "weather_data.weather.description",
        "weather_data.main.temp",
        "weather_data.wind.speed",
        "weather_data.main.humidity"
    )

    return processed_data


if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .getOrCreate()

    kafka_broker = "192.168.1.42:9092"
    topic = "weather_data_topic"

    # Create a DataFrame representing the stream of input lines from Kafka
    weather_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", topic) \
        .load()

    # Process the data
    processed_data = process_weather_data(weather_stream)

    # Start the query to write the processed data to the console
    query = processed_data.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Await Termination
    query.awaitTermination()
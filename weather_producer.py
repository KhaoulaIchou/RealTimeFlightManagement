import time
import json
import requests
from kafka import KafkaProducer

def get_weather_data(api_key, city):
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": api_key, "units": "metric"}
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        weather_data = response.json()
        return weather_data
    elif response.status_code == 401:
        print("Unauthorized API key. Please check your API key.")
    else:
        print(f"Error fetching weather data: {response.status_code}")
    return None


def publish_weather_data_to_kafka(producer, topic, weather_data):
    producer.send(topic, json.dumps(weather_data).encode('utf-8'))
    producer.flush()

def main():
    api_key = '4215ed683602d009ed259b432a94280f'  #API key
    city = 'Taroudant'
    kafka_broker = '192.168.1.42:9092'
    topic = 'weather_data_topic'

    producer = KafkaProducer(bootstrap_servers=kafka_broker)

    while True:
        weather_data = get_weather_data(api_key, city)
        if weather_data:
            publish_weather_data_to_kafka(producer, topic, weather_data)
            print(f"Weather data published to Kafka: {weather_data}")
        else:
            print("Weather data not available.")
        time.sleep(10)

if __name__ == "__main__":
    main()
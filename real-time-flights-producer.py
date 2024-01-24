import json
import time
import urllib.request
from kafka import KafkaProducer

API_KEY = "85341520-c035-40d5-8327-2dff38f26c51"
url = f"https://airlabs.co/api/v9/flights?_view=array&_fields=hex,flag,lat,lng,dir,alt&api_key={API_KEY}"

producer = KafkaProducer(bootstrap_servers="192.168.43.224:9092")

while True:
    req = urllib.request.Request(
        url,
        data=None,
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
    )
    response = urllib.request.urlopen(req)
    flights = json.loads(response.read().decode())

    for flight in flights:
        print(flight)
        producer.send("flight-realtime", json.dumps(flight).encode())

    print("{} Produced {} station records".format(time.time(), len(flights)))
    time.sleep(1)


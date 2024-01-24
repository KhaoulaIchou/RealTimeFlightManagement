import json
import time
import datetime

import urllib.request
from urllib.request import Request, urlopen

from kafka import KafkaProducer

API_KEY = "85341520-c035-40d5-8327-2dff38f26c51"

url = "https://airlabs.co/api/v9/flights?api_key={}".format(API_KEY)

producer = KafkaProducer(bootstrap_servers="192.168.81.44:9092")
while True:

 response = urllib.request.Request(url,headers={'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0'})
 flights = json.loads(urlopen(response).read().decode())

 for i, flight in enumerate(flights["response"]):

   print(flight)

   key = '{}{}'.format(datetime.datetime.now().timestamp(), i)
   producer.send("flight-realtime", key=key.encode(), value=json.dumps(flight).encode())

 print("{} Produced {} station records".format(time.time(), len(flights)))

 time.sleep(1)

import requests
import json
from datetime import datetime
from confluent_kafka import Producer
import time

api_key = "Your_Accuweather_API_Key"
city_name = "Your_City_Name"

def get_current_weather(api_key, location_key):
    url = f"http://dataservice.accuweather.com/currentconditions/v1/{location_key}"
    params = {
        "apikey": api_key,
        "details": True
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        current_data = response.json()[0]
        current_data["RetrievedDateTime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        weather_data = {
            "Temperature": current_data['Temperature']['Metric']['Value'],
            "Weather Condition": current_data['WeatherText'],
            "Relative Humidity": current_data['RelativeHumidity'],
            "Wind Speed": current_data['Wind']['Speed']['Metric']['Value'],
            "Wind Direction": current_data['Wind']['Direction']['Localized']
        }
        print(weather_data)

        send_to_kafka('Weather_Maltepe', json.dumps(weather_data))

        return current_data
    else:
        print("Error:", response.status_code)
        return None

def get_location_key(api_key, city_name):
    url = "http://dataservice.accuweather.com/locations/v1/cities/search"
    params = {
        "apikey": api_key,
        "q": city_name
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        if data:
            location_key = data[0]["Key"]
            return location_key
    print("Error: Location key not found")
    return None

def send_to_kafka(topic, data):
    config = {
        'bootstrap.servers': 'localhost:9092', 
    }

    producer = Producer(config)
    producer.produce(topic, data)
    producer.flush()




interval_seconds = 1800

while True:
    location_key = get_location_key(api_key, city_name)
    if location_key:
        get_current_weather(api_key, location_key)
    time.sleep(interval_seconds)

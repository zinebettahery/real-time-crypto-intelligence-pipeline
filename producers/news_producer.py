import requests
import json
import time
from confluent_kafka import Producer
import os
from dotenv import load_dotenv

load_dotenv()

producer = Producer({
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET")
})

NEWS_API_KEY = os.getenv("NEWSAPI_KEY")

while True:
    url = f"https://newsapi.org/v2/everything?q=bitcoin&apiKey={NEWS_API_KEY}"
    response = requests.get(url)
    articles = response.json().get("articles", [])

    for article in articles[:3]:
        news = {
            "symbol": "BTCUSDT",
            "title": article["title"],
            "timestamp": int(time.time() * 1000)
        }

        producer.produce("news_topic", json.dumps(news))
        producer.flush()
        print("News envoyée:", news)

    time.sleep(60)
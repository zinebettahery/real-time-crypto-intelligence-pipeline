import json
import websocket
from confluent_kafka import Producer
import os
from dotenv import load_dotenv

load_dotenv()

conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET")
}

producer = Producer(conf)

def on_message(ws, message):
    data = json.loads(message)

    trade = {
        "symbol": data["s"],
        "price": float(data["p"]),
        "volume": float(data["q"]),
        "timestamp": data["T"]
    }

    producer.produce("trades_topic", json.dumps(trade))
    producer.flush()
    print("Trade envoyé:", trade)

socket = "wss://stream.binance.com:9443/ws/btcusdt@trade"
ws = websocket.WebSocketApp(socket, on_message=on_message)

ws.run_forever()
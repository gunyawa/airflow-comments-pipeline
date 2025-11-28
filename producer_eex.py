# producer_eex.py
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import json
import time
import uuid
import random
import os
import sys
import signal

# Configuration 
TOPIC_NAME = os.environ.get("LAB_TOPIC", "eex-trades")
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
SEND_INTERVAL = float(os.environ.get("LAB_INTERVAL", 1.5))

# Domain values 
MARKETS = ["EEX", "NordPool", "OTC"]
PRODUCTS = ["Power Baseload", "Power Peak", "Gas"]

# Value ranges
VOLUME_MIN = 0.1
VOLUME_MAX = 500.0
PRICE_MIN = 10.0
PRICE_MAX = 200.0

# Global flag for clean shutdown
_running = True

def signal_handler(sig, frame):
    global _running
    print("\nReceived shutdown signal")
    _running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def create_producer():
    """
    Create and return a KafkaProducer configured with a JSON value serializer.
    Key serializer encodes string keys to bytes (UTF-8).
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else None,
            linger_ms=5,  # small batching
        )
        return producer
    except NoBrokersAvailable as e:
        print(f"Unable to connect to Kafka brokers: {e}")
        raise

def build_trade_message():
    """Create one trade message using the simplified schema."""
    return {
        "trade_id": str(uuid.uuid4()),
        "market": random.choice(MARKETS),
        "product": random.choice(PRODUCTS),
        "volume_mwh": round(random.uniform(VOLUME_MIN, VOLUME_MAX), 3),
        "price_eur_per_mwh": round(random.uniform(PRICE_MIN, PRICE_MAX), 2),
        "timestamp": int(time.time() * 1000),
    }

def run_producer(topic=TOPIC_NAME, interval=SEND_INTERVAL):
    """
    Run producer loop.
    Uses market as message key (so partitioning can be based on market).
    """
    producer = create_producer()
    print(f"Producer started. Sending simplified EEX trades to topic '{topic}' every {interval}s")
    try:
        global _running
        while _running:
            # Create trade message
            msg = build_trade_message()
            # Choose partition key (market)
            key = msg["market"]
            try:
                # Send message with key and value. key will be serialized by key_serializer.
                future = producer.send(topic, key=key, value=msg)
                # Optionally block for send result or handle asynchronously:
                # record_metadata = future.get(timeout=10)
                # we won't block here to maintain throughput
            except KafkaError as e:
                print(f"[ERROR] Failed to send trade {msg['trade_id']}: {e}", file=sys.stderr)

            # Compact log line
            print(
                f"[SENT] trade_id={msg['trade_id']} market={msg['market']} "
                f"product={msg['product']} volume={msg['volume_mwh']} price={msg['price_eur_per_mwh']}"
            )

            time.sleep(interval)

    except Exception as e:
        print(f"[ERROR] Producer loop exception: {e}", file=sys.stderr)

    finally:
        # Flush and close producer on shutdown
        try:
            print("Flushing producer...")
            producer.flush(timeout=10)
            producer.close(timeout=10)
            print("Producer closed.")
        except Exception as e:
            print(f"Error during producer close: {e}", file=sys.stderr)


if __name__ == "__main__":
    run_producer()

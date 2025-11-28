# consumer_eex.py
from kafka import KafkaConsumer
import json
import os
import signal
import sys

TOPIC_NAME = os.environ.get("LAB_TOPIC", "eex-trades")
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")

_running = True
def signal_handler(sig, frame):
    global _running
    print("\nReceived shutdown signal")
    _running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def run_consumer():
    """
    Create a KafkaConsumer connected to localhost:9092, JSON deserializer,
    group id 'lab-consumer-group', auto commit enabled, start from latest.
    """
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKERS.split(","),
        group_id="lab-consumer-group",
        enable_auto_commit=True,
        auto_offset_reset="latest",  # start consuming new messages
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
        consumer_timeout_ms=1000  # so loop can check for shutdown periodically
    )

    print(f"Consumer started. Listening to topic '{TOPIC_NAME}' on {KAFKA_BROKERS}")
    try:
        global _running
        while _running:
            for msg in consumer:
                # msg is a ConsumerRecord
                key = msg.key
                data = msg.value
                # Print in required format:
                # Received -> key=<key> | product=<product>
                product = data.get("product") if isinstance(data, dict) else None
                print(f"Received -> key={key} | product={product}")
                # break to check _running flag periodically (because consumer is blocking)
            # loop continues; consumer_timeout_ms causes the for loop to exit if no messages
    except Exception as e:
        print(f"[ERROR] Consumer loop exception: {e}", file=sys.stderr)
    finally:
        print("Closing consumer...")
        try:
            consumer.close()
        except Exception as e:
            print(f"Error closing consumer: {e}", file=sys.stderr)
        print("Consumer closed.")


if __name__ == "__main__":
    run_consumer()

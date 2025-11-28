
import time
import uuid
import random
import queue
import threading

MARKETS = ["EEX", "NordPool", "OTC"]
PRODUCTS = ["Power Baseload", "Power Peak", "Gas"]
VOLUME_MIN, VOLUME_MAX = 0.1, 500.0
PRICE_MIN, PRICE_MAX = 10.0, 200.0
SEND_INTERVAL = 0.5

# Общая очередь для сообщений
message_queue = queue.Queue()

def build_trade_message():
    return {
        "trade_id": str(uuid.uuid4()),
        "market": random.choice(MARKETS),
        "product": random.choice(PRODUCTS),
        "volume_mwh": round(random.uniform(VOLUME_MIN, VOLUME_MAX), 3),
        "price_eur_per_mwh": round(random.uniform(PRICE_MIN, PRICE_MAX), 2),
        "timestamp": int(time.time() * 1000)
    }

def producer():
    while True:
        msg = build_trade_message()
        message_queue.put(msg)
        print(f"[SENT] trade_id={msg['trade_id']} market={msg['market']} product={msg['product']}")
        time.sleep(SEND_INTERVAL)

def consumer():
    while True:
        msg = message_queue.get()
        print(f"Received -> key={msg['market']} | product={msg['product']}")

# Запуск потоков
t_prod = threading.Thread(target=producer, daemon=True)
t_cons = threading.Thread(target=consumer, daemon=True)

t_prod.start()
t_cons.start()

# Блокировка главного потока
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopped.")

from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

log_levels = ["INFO", "WARN", "ERROR", "DEBUG"]
services = ["auth-service", "order-service", "payment-service"]

while True:
    log_entry = {
        "timestamp": int(time.time()),
        "service": random.choice(services),
        "log_level": random.choice(log_levels),
        "message": "This is a sample log",
        "request_id": f"req-{random.randint(1000, 9999)}",
        "user_id": f"user-{random.randint(100, 999)}",
        "response_time": random.randint(50, 500)
    }
    producer.send("logs", value=log_entry)
    print(f"Sent log: {log_entry}")
    time.sleep(1)

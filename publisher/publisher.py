from __future__ import annotations
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
import httpx

AGGREGATOR_URL = os.environ.get("AGGREGATOR_URL", "http://aggregator:3000")
TOTAL_EVENTS = int(os.environ.get("TOTAL_EVENTS", "20000"))
DUPLICATE_RATE = float(os.environ.get("DUPLICATE_RATE", "0.30"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))

TOPICS = ["payment.created", "payment.completed", "auth.login", "auth.logout", "order.created", "order.shipped", "user.registered", "notification.sent", "inventory.updated", "report.generated"]
SOURCES = ["publisher-1", "publisher-2", "publisher-3", "mobile-app", "web-app"]

def generate_unique_event(index: int) -> dict:
    topic = random.choice(TOPICS)
    event_id = str(uuid.uuid4())
    timestamp = (datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 3600))).isoformat()
    source = random.choice(SOURCES)
    payload = generate_payload(topic, index)
    return {"topic": topic, "event_id": event_id, "timestamp": timestamp, "source": source, "payload": payload}

def generate_payload(topic: str, index: int) -> dict:
    if topic.startswith("payment"):
        return {"user_id": f"U{random.randint(1, 1000):04d}", "amount": random.randint(10000, 5000000), "currency": random.choice(["IDR", "USD", "EUR"]), "method": random.choice(["credit_card", "bank_transfer", "ewallet"])}
    elif topic.startswith("auth"):
        return {"user_id": f"U{random.randint(1, 1000):04d}", "ip_address": f"192.168.{random.randint(1,255)}.{random.randint(1,255)}", "user_agent": random.choice(["Chrome", "Firefox", "Safari", "Mobile"])}
    elif topic.startswith("order"):
        return {"order_id": f"ORD-{random.randint(10000, 99999)}", "user_id": f"U{random.randint(1, 1000):04d}", "total_items": random.randint(1, 20), "total_price": random.randint(50000, 10000000)}
    elif topic.startswith("user"):
        return {"user_id": f"U{random.randint(1, 1000):04d}", "email": f"user{random.randint(1,1000)}@example.com", "plan": random.choice(["free", "basic", "premium"])}
    else:
        return {"index": index, "data": f"event-data-{index}", "value": random.randint(1, 1000)}

def wait_for_aggregator(max_retries: int = 30, delay: float = 2.0) -> None:
    for attempt in range(1, max_retries + 1):
        try:
            resp = httpx.get(f"{AGGREGATOR_URL}/health", timeout=5.0)
            if resp.status_code == 200 and resp.json().get("status") == "ok":
                return
        except (httpx.ConnectError, httpx.TimeoutException):
            pass
        if attempt < max_retries:
            time.sleep(delay)
    sys.exit(1)

def run_benchmark():
    num_duplicates = int(TOTAL_EVENTS * DUPLICATE_RATE)
    num_unique = TOTAL_EVENTS - num_duplicates
    unique_events = [generate_unique_event(i) for i in range(num_unique)]
    duplicate_events = []
    for _ in range(num_duplicates):
        original = random.choice(unique_events)
        dup = {"topic": original["topic"], "event_id": original["event_id"], "timestamp": datetime.now(timezone.utc).isoformat(), "source": original["source"], "payload": original["payload"]}
        duplicate_events.append(dup)
    
    all_events = unique_events + duplicate_events
    random.shuffle(all_events)
    
    start_time = time.time()
    total_sent = total_accepted = total_queued = failed_batches = 0

    with httpx.Client(timeout=30.0) as client:
        for i in range(0, len(all_events), BATCH_SIZE):
            batch = all_events[i : i + BATCH_SIZE]
            try:
                resp = client.post(f"{AGGREGATOR_URL}/publish", json=batch, headers={"Content-Type": "application/json"})
                if resp.status_code == 200:
                    data = resp.json()
                    total_accepted += data.get("accepted", 0)
                    total_queued += data.get("queued", 0)
                else:
                    failed_batches += 1
            except Exception:
                failed_batches += 1
            total_sent += len(batch)

    time.sleep(5)
    
    try:
        resp = httpx.get(f"{AGGREGATOR_URL}/stats", timeout=10.0)
        stats = resp.json() if resp.status_code == 200 else {}
    except Exception:
        stats = {}

    print("Benchmark complete!")
    print(f"Total Sent: {total_sent}, Accepted: {total_accepted}")

def main():
    wait_for_aggregator()
    run_benchmark()

if __name__ == "__main__":
    main()

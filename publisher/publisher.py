"""
publisher.py — Event Simulator & Benchmark Tool.

Publisher mengirim event ke aggregator melalui HTTP POST /publish.

Mode operasi:
1. BENCHMARK (default): Generate TOTAL_EVENTS event dengan DUPLICATE_RATE duplikat
2. Mengirim batch event untuk efisiensi

Publisher menghasilkan:
- Event dengan berbagai topic (payment.created, auth.login, order.created, dll.)
- Event duplikat sengaja untuk menguji deduplication
- Minimal 20.000 event dengan minimal 30% duplikat

Environment Variables:
- AGGREGATOR_URL: URL aggregator service (default: http://aggregator:3000)
- TOTAL_EVENTS: jumlah total event yang digenerate (default: 20000)
- DUPLICATE_RATE: rasio duplikasi 0.0-1.0 (default: 0.30)
- BATCH_SIZE: ukuran batch per request (default: 100)
"""

from __future__ import annotations

import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta

import httpx

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AGGREGATOR_URL = os.environ.get("AGGREGATOR_URL", "http://aggregator:3000")
TOTAL_EVENTS = int(os.environ.get("TOTAL_EVENTS", "20000"))
DUPLICATE_RATE = float(os.environ.get("DUPLICATE_RATE", "0.30"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))

# Topic pool untuk variasi event
TOPICS = [
    "payment.created",
    "payment.completed",
    "auth.login",
    "auth.logout",
    "order.created",
    "order.shipped",
    "user.registered",
    "notification.sent",
    "inventory.updated",
    "report.generated",
]

# Source pool
SOURCES = [
    "publisher-1",
    "publisher-2",
    "publisher-3",
    "mobile-app",
    "web-app",
]


def generate_unique_event(index: int) -> dict:
    """Generate satu event unik dengan data random."""
    topic = random.choice(TOPICS)
    event_id = str(uuid.uuid4())
    timestamp = (
        datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 3600))
    ).isoformat()
    source = random.choice(SOURCES)

    # Payload bervariasi berdasarkan topic
    payload = generate_payload(topic, index)

    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": timestamp,
        "source": source,
        "payload": payload,
    }


def generate_payload(topic: str, index: int) -> dict:
    """Generate payload berdasarkan jenis topic."""
    if topic.startswith("payment"):
        return {
            "user_id": f"U{random.randint(1, 1000):04d}",
            "amount": random.randint(10000, 5000000),
            "currency": random.choice(["IDR", "USD", "EUR"]),
            "method": random.choice(["credit_card", "bank_transfer", "ewallet"]),
        }
    elif topic.startswith("auth"):
        return {
            "user_id": f"U{random.randint(1, 1000):04d}",
            "ip_address": f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
            "user_agent": random.choice(["Chrome", "Firefox", "Safari", "Mobile"]),
        }
    elif topic.startswith("order"):
        return {
            "order_id": f"ORD-{random.randint(10000, 99999)}",
            "user_id": f"U{random.randint(1, 1000):04d}",
            "total_items": random.randint(1, 20),
            "total_price": random.randint(50000, 10000000),
        }
    elif topic.startswith("user"):
        return {
            "user_id": f"U{random.randint(1, 1000):04d}",
            "email": f"user{random.randint(1,1000)}@example.com",
            "plan": random.choice(["free", "basic", "premium"]),
        }
    else:
        return {
            "index": index,
            "data": f"event-data-{index}",
            "value": random.randint(1, 1000),
        }


def wait_for_aggregator(max_retries: int = 30, delay: float = 2.0) -> None:
    """Tunggu aggregator siap sebelum mengirim event."""
    print(f"⏳ Waiting for aggregator at {AGGREGATOR_URL}...")

    for attempt in range(1, max_retries + 1):
        try:
            resp = httpx.get(f"{AGGREGATOR_URL}/health", timeout=5.0)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "ok":
                    print(f"✅ Aggregator ready (attempt {attempt})")
                    return
        except (httpx.ConnectError, httpx.TimeoutException):
            pass

        if attempt < max_retries:
            print(f"   Attempt {attempt}/{max_retries}, retrying in {delay}s...")
            time.sleep(delay)

    print("❌ Aggregator not ready after maximum retries")
    sys.exit(1)


def run_benchmark():
    """
    Jalankan benchmark: generate dan kirim TOTAL_EVENTS event.

    Strategi duplikasi:
    1. Generate pool event unik terlebih dahulu
    2. Hitung jumlah event unik vs duplikat berdasarkan DUPLICATE_RATE
    3. Event duplikat dipilih random dari pool event unik
    4. Shuffle semua event agar duplikat tersebar merata
    5. Kirim dalam batch ke aggregator
    """
    print("=" * 70)
    print("  PUB-SUB LOG AGGREGATOR — BENCHMARK")
    print("=" * 70)
    print(f"  Aggregator URL : {AGGREGATOR_URL}")
    print(f"  Total Events   : {TOTAL_EVENTS:,}")
    print(f"  Duplicate Rate : {DUPLICATE_RATE:.0%}")
    print(f"  Batch Size     : {BATCH_SIZE}")
    print("=" * 70)

    # Hitung jumlah event unik dan duplikat
    num_duplicates = int(TOTAL_EVENTS * DUPLICATE_RATE)
    num_unique = TOTAL_EVENTS - num_duplicates

    print(f"\n📊 Generating {num_unique:,} unique + {num_duplicates:,} duplicate events...")

    # Step 1: Generate event unik
    unique_events = []
    for i in range(num_unique):
        unique_events.append(generate_unique_event(i))

    # Step 2: Generate duplikat dari pool event unik
    duplicate_events = []
    for _ in range(num_duplicates):
        original = random.choice(unique_events)
        # Duplikat = topic + event_id sama, tapi timestamp mungkin berbeda
        dup = {
            "topic": original["topic"],
            "event_id": original["event_id"],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": original["source"],
            "payload": original["payload"],
        }
        duplicate_events.append(dup)

    # Step 3: Gabung dan shuffle
    all_events = unique_events + duplicate_events
    random.shuffle(all_events)

    print(f"✅ Generated {len(all_events):,} total events")
    print(f"   Unique: {num_unique:,}, Duplicates: {num_duplicates:,}")

    # Step 4: Kirim dalam batch
    print(f"\n🚀 Sending events in batches of {BATCH_SIZE}...")
    start_time = time.time()

    total_sent = 0
    total_accepted = 0
    total_queued = 0
    failed_batches = 0

    with httpx.Client(timeout=30.0) as client:
        for i in range(0, len(all_events), BATCH_SIZE):
            batch = all_events[i : i + BATCH_SIZE]

            try:
                resp = client.post(
                    f"{AGGREGATOR_URL}/publish",
                    json=batch,
                    headers={"Content-Type": "application/json"},
                )

                if resp.status_code == 200:
                    data = resp.json()
                    total_accepted += data.get("accepted", 0)
                    total_queued += data.get("queued", 0)
                else:
                    failed_batches += 1
                    print(f"   ⚠️  Batch {i//BATCH_SIZE + 1} failed: HTTP {resp.status_code}")

            except Exception as e:
                failed_batches += 1
                print(f"   ❌ Batch {i//BATCH_SIZE + 1} error: {e}")

            total_sent += len(batch)

            # Progress setiap 10 batch
            if (i // BATCH_SIZE + 1) % 10 == 0:
                elapsed = time.time() - start_time
                rate = total_sent / elapsed if elapsed > 0 else 0
                print(
                    f"   📤 Sent {total_sent:,}/{len(all_events):,} "
                    f"({total_sent/len(all_events)*100:.0f}%) "
                    f"@ {rate:.0f} events/s"
                )

    elapsed = time.time() - start_time
    throughput = total_sent / elapsed if elapsed > 0 else 0

    # Step 5: Tunggu worker selesai memproses
    print(f"\n⏳ Waiting for workers to process events...")
    time.sleep(5)

    # Step 6: Ambil stats dari aggregator
    try:
        resp = httpx.get(f"{AGGREGATOR_URL}/stats", timeout=10.0)
        stats = resp.json() if resp.status_code == 200 else {}
    except Exception:
        stats = {}

    # Step 7: Tampilkan hasil
    print("\n" + "=" * 70)
    print("  BENCHMARK RESULTS")
    print("=" * 70)
    print(f"  Total Events Generated : {len(all_events):,}")
    print(f"  Unique Events          : {num_unique:,}")
    print(f"  Duplicate Events       : {num_duplicates:,}")
    print(f"  Total Sent             : {total_sent:,}")
    print(f"  Total Accepted         : {total_accepted:,}")
    print(f"  Total Queued           : {total_queued:,}")
    print(f"  Failed Batches         : {failed_batches}")
    print(f"  Duration               : {elapsed:.2f}s")
    print(f"  Throughput (send)      : {throughput:.0f} events/s")
    print(f"  ---")

    if stats:
        print(f"  Received (server)      : {stats.get('received_total', 'N/A'):,}")
        print(f"  Unique Processed       : {stats.get('unique_processed', 'N/A'):,}")
        print(f"  Duplicates Dropped     : {stats.get('duplicates_dropped', 'N/A'):,}")
        print(f"  Failed (server)        : {stats.get('failed_total', 'N/A'):,}")
        print(f"  Topic Count            : {stats.get('topic_count', 'N/A')}")
        print(f"  Duplicate Rate         : {stats.get('duplicate_rate', 'N/A')}")
        print(f"  Throughput (process)   : {stats.get('throughput_events_per_second', 'N/A')} events/s")

    print("=" * 70)
    print("✅ Benchmark complete!")


def main():
    """Entry point."""
    # Tunggu aggregator siap
    wait_for_aggregator()

    # Jalankan benchmark
    run_benchmark()


if __name__ == "__main__":
    main()

# Pub-Sub Log Aggregator Terdistribusi

Sistem Pub-Sub log aggregator multi-service menggunakan **FastAPI**, **Redis Stream**, **PostgreSQL**, dan **Docker Compose** dengan fitur **idempotent consumer**, **deduplication**, dan **transaction/concurrency control**.

## Deskripsi Singkat

Sistem ini menerima event log dari berbagai sumber (publisher), memasukkannya ke antrian internal (Redis Stream), lalu memprosesnya secara aman ke database (PostgreSQL). Event yang sama tidak akan diproses lebih dari satu kali berkat mekanisme deduplication berbasis database constraint `UNIQUE(topic, event_id)` dan transaksi database.

## Arsitektur

```
┌───────────┐     HTTP POST      ┌─────────────┐     Redis Stream     ┌─────────────┐
│ Publisher  │ ─────────────────→ │ Aggregator  │ ──────────────────→ │   Worker    │
│ (simulator)│     /publish       │ (FastAPI)   │    events_stream     │ (consumer)  │
└───────────┘                    └─────────────┘                     └──────┬──────┘
                                       │                                    │
                                       │ GET /events                        │ INSERT
                                       │ GET /stats                         │ (transaction +
                                       │ GET /health                        │  dedup)
                                       ▼                                    ▼
                                 ┌─────────────┐                     ┌─────────────┐
                                 │   Redis     │                     │ PostgreSQL  │
                                 │  (broker)   │                     │ (storage)   │
                                 └─────────────┘                     └─────────────┘
```

### Services

| Service      | Fungsi                                        | Port         |
|-------------|-----------------------------------------------|--------------|
| `aggregator` | FastAPI API — menerima & query event         | `3000` (exposed) |
| `worker`     | Consumer — proses event dari Redis ke PostgreSQL | internal     |
| `publisher`  | Simulator — generate & kirim event           | internal     |
| `redis`      | Broker — Redis Stream message queue          | internal     |
| `postgres`   | Storage — database persisten                  | internal     |

> **Catatan**: Hanya `aggregator` yang expose port ke host (`3000:3000`). Redis dan PostgreSQL hanya bisa diakses dari dalam Docker network.

## Fitur Utama

- ✅ Publish event single dan batch via HTTP API
- ✅ Queue internal dengan Redis Stream + consumer group
- ✅ **Deduplication** berbasis `(topic, event_id)` dengan PostgreSQL constraint
- ✅ **Idempotent consumer** — event yang sama tidak diproses ulang
- ✅ **Transaksi PostgreSQL** dengan `INSERT ... ON CONFLICT DO NOTHING`
- ✅ **Concurrency control** — race condition dicegah oleh UNIQUE constraint
- ✅ Statistik real-time via `/stats`
- ✅ Health check via `/health`
- ✅ **Persistence** dengan Docker named volume
- ✅ 20 test case (unit + integration)
- ✅ Benchmark 20.000 event dengan 30% duplikat
- ✅ **Dashboard UI** interaktif terintegrasi di FastAPI

## Cara Menjalankan

### Prerequisites

- Docker & Docker Compose

### Build dan Run

```bash
docker compose up --build

docker compose up --build -d
```

### Buka Dashboard

Setelah `docker compose up --build`, buka browser:

```
http://localhost:3000/dashboard
```

> **Dashboard sudah terintegrasi di FastAPI.** Tidak perlu install atau menjalankan apapun tambahan. Cukup `docker compose up --build` lalu buka URL di atas.

Dashboard menyediakan 5 halaman untuk demo:
1. **Overview** — Health status, metrik real-time, live chart
2. **Publish Event** — Kirim event langsung dari browser
3. **Event Log** — Lihat event yang sudah diproses di database
4. **Demo Dedup** — Demo deduplication & idempotency secara visual
5. **Arsitektur** — Diagram arsitektur, fitur, dan tech stack

### Cek Container

```bash
docker compose ps
```

### Cek Health

```bash
curl http://localhost:3000/health
```

## Endpoint API

### 1. `POST /publish`

Menerima single event atau batch event.

**Single event:**
```bash
curl -X POST http://localhost:3000/publish \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "payment.created",
    "event_id": "550e8400-e29b-41d4-a716-446655440000",
    "timestamp": "2026-06-12T10:00:00Z",
    "source": "publisher-1",
    "payload": {"user_id": "U001", "amount": 150000, "currency": "IDR"}
  }'
```

**Batch event:**
```bash
curl -X POST http://localhost:3000/publish \
  -H "Content-Type: application/json" \
  -d '[
    {"topic": "payment.created", "event_id": "event-001", "timestamp": "2026-06-12T10:00:00Z", "source": "pub-1", "payload": {"amount": 100000}},
    {"topic": "auth.login", "event_id": "event-002", "timestamp": "2026-06-12T10:00:01Z", "source": "pub-1", "payload": {"user_id": "U001"}}
  ]'
```

**Response:**
```json
{
  "accepted": 2,
  "queued": 2,
  "message": "events accepted"
}
```

### 2. `GET /events?topic=...`

Mengembalikan daftar event unik yang sudah diproses.

```bash
curl "http://localhost:3000/events?topic=payment.created"
```

**Response:**
```json
{
  "topic": "payment.created",
  "count": 1,
  "events": [
    {
      "topic": "payment.created",
      "event_id": "550e8400-e29b-41d4-a716-446655440000",
      "timestamp": "2026-06-12T10:00:00+00:00",
      "source": "publisher-1",
      "payload": {"user_id": "U001", "amount": 150000, "currency": "IDR"},
      "processed_at": "2026-06-12T10:00:05+00:00"
    }
  ]
}
```

### 3. `GET /stats`

Menampilkan metrik sistem.

```bash
curl http://localhost:3000/stats
```

**Response:**
```json
{
  "received_total": 20000,
  "queued_total": 20000,
  "unique_processed": 14000,
  "duplicates_dropped": 6000,
  "failed_total": 0,
  "topic_count": 10,
  "uptime_seconds": 321.45,
  "duplicate_rate": 0.30,
  "throughput_events_per_second": 43.56
}
```

### 4. `GET /health`

Health check endpoint.

```bash
curl http://localhost:3000/health
```

**Response:**
```json
{
  "status": "ok",
  "database": "ok",
  "broker": "ok"
}
```

## Cara Menjalankan Test

```bash
docker compose run --rm aggregator pytest -v

docker compose run --rm aggregator pytest tests/test_dedup.py -v
docker compose run --rm aggregator pytest tests/test_concurrency.py -v
```

## Cara Benchmark

```bash
docker compose --profile benchmark run --rm publisher

curl http://localhost:3000/stats
```

## Demo Scaling Worker

```bash
docker compose up --scale worker=3
```

## Demo Deduplication

```bash
for i in $(seq 1 5); do
  curl -X POST http://localhost:3000/publish \
    -H "Content-Type: application/json" \
    -d '{"topic":"payment.created","event_id":"dup-test-001","timestamp":"2026-06-12T10:00:00Z","source":"demo","payload":{"amount":150000}}'
done

curl "http://localhost:3000/events?topic=payment.created"

curl http://localhost:3000/stats
```

## Bukti Persistence

Data tersimpan di Docker named volume `pg_data` dan `redis_data`:

```bash
docker volume ls

docker compose down
docker compose up -d

curl http://localhost:3000/events?topic=payment.created
curl http://localhost:3000/stats
```

> ⚠️ **Jangan** gunakan `docker compose down -v` karena itu menghapus volume dan data.

## Struktur Repository

```
pubsub-log-aggregator/
├── aggregator/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py
│   │   ├── models.py
│   │   ├── db.py
│   │   ├── broker.py
│   │   ├── worker.py
│   │   ├── stats.py
│   │   └── migrations.py
│   ├── tests/
│   │   ├── conftest.py
│   │   ├── test_schema.py
│   │   ├── test_publish.py
│   │   ├── test_events.py
│   │   ├── test_dedup.py
│   │   ├── test_stats.py
│   │   ├── test_concurrency.py
│   │   ├── test_health.py
│   │   └── test_persistence.py
│   ├── static/
│   │   ├── index.html
│   │   ├── style.css
│   │   └── app.js
│   ├── Dockerfile
│   ├── requirements.txt
│   └── pytest.ini
├── publisher/
│   ├── publisher.py
│   ├── Dockerfile
│   └── requirements.txt
├── docs/
│   ├── architecture.md
│   ├── api.md
│   └── demo_script.md
├── tests/
│   ├── integration_test_plan.md
│   └── benchmark_plan.md
├── docker-compose.yml
├── README.md
├── report.md
└── .gitignore
```
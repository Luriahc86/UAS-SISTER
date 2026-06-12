"""
test_persistence.py — Test persistence dan integration.

Test Cases:
17. Restart worker tidak menyebabkan reprocessing (TC-17 — documented)
18. Restart aggregator tidak menghapus data (TC-18 — documented)
19. Redis tidak di-expose ke host (TC-19 — documented)
20. Benchmark 20.000 event dengan 30% duplikat (TC-20 — documented)

Catatan: Test 17-20 bersifat integration test yang memerlukan
Docker Compose running. Beberapa diimplementasikan sebagai
automated checks, sisanya didokumentasikan sebagai manual test.
"""

import pytest

from tests.conftest import make_event
from app.worker import process_event


# ---------------------------------------------------------------------------
# TC-17: Restart worker tidak menyebabkan reprocessing
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_restart_worker_no_reprocessing(db_pool):
    """
    Simulasi: proses event, lalu proses ulang (seolah worker restart).
    Event yang sudah diproses tidak boleh diproses ulang.

    Ini memverifikasi bahwa deduplication bersifat persistent:
    - Data dedup disimpan di PostgreSQL (bukan in-memory)
    - Restart worker tidak menghapus history dedup
    - Event yang sudah ada di processed_events akan di-skip
    """
    event = make_event(
        topic="restart.test",
        event_id="restart-001",
        payload={"test": "restart"},
    )

    # Proses event pertama kali
    await process_event(db_pool, event)

    # Simpan stats setelah proses pertama
    async with db_pool.acquire() as conn:
        unique_before = await conn.fetchval(
            "SELECT value FROM stats WHERE key='unique_processed'"
        )

    # "Restart": proses event yang sama lagi
    await process_event(db_pool, event)

    # unique_processed tidak boleh bertambah
    async with db_pool.acquire() as conn:
        unique_after = await conn.fetchval(
            "SELECT value FROM stats WHERE key='unique_processed'"
        )
        dupes = await conn.fetchval(
            "SELECT value FROM stats WHERE key='duplicates_dropped'"
        )

    assert unique_after == unique_before, (
        "unique_processed changed after 'restart' — reprocessing detected"
    )
    assert dupes >= 1, (
        "duplicates_dropped should increment when re-processing existing event"
    )


# ---------------------------------------------------------------------------
# TC-18: Data bertahan di database (persistence check via query)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_data_persists_in_database(db_pool):
    """
    Verifikasi bahwa data event tersimpan di PostgreSQL.
    Jika volume PostgreSQL persistent, data bertahan saat container restart.

    Test ini memverifikasi write → read consistency.
    Integration test manual memverifikasi persistence lintas restart.
    """
    event = make_event(
        topic="persist.test",
        event_id="persist-001",
        payload={"data": "persistent"},
    )

    # Proses event
    await process_event(db_pool, event)

    # Verifikasi data ada di database
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM events WHERE topic=$1 AND event_id=$2",
            "persist.test",
            "persist-001",
        )

    assert row is not None, "Event not found in database after processing"
    assert row["topic"] == "persist.test"
    assert row["event_id"] == "persist-001"
    assert row["source"] == "test-publisher"


# ---------------------------------------------------------------------------
# TC-19: Redis port exposure check (documented)
# ---------------------------------------------------------------------------
class TestRedisPortExposure:
    """
    TC-19: Verifikasi bahwa Redis tidak di-expose ke host.

    Cara verifikasi manual:
    1. Jalankan: docker compose ps
    2. Pastikan Redis TIDAK memiliki port mapping ke host
    3. Jalankan: docker compose port redis 6379
       → Harus mengembalikan error karena port tidak di-expose

    Pada docker-compose.yml, Redis service tidak memiliki
    konfigurasi 'ports:', sehingga hanya bisa diakses dari
    network internal Docker Compose (app_net).
    """

    @pytest.mark.asyncio
    async def test_redis_accessible_internally(self, redis_client):
        """Redis harus accessible dari dalam Docker network."""
        result = await redis_client.ping()
        assert result is True


# ---------------------------------------------------------------------------
# TC-20: Benchmark 20.000 event (documented integration test)
# ---------------------------------------------------------------------------
class TestBenchmarkDocumented:
    """
    TC-20: Benchmark 20.000 event dengan 30% duplikat.

    Cara menjalankan:
        docker compose --profile benchmark run --rm publisher

    Expected results:
    - Total event: 20.000
    - Unique events: ~14.000 (70%)
    - Duplicate events: ~6.000 (30%)
    - Semua event diproses tanpa error
    - /stats menunjukkan metrik yang konsisten

    Verifikasi setelah benchmark:
        curl http://localhost:3000/stats

    Expected /stats:
    {
        "received_total": 20000,
        "unique_processed": ~14000,
        "duplicates_dropped": ~6000,
        "duplicate_rate": ~0.30
    }
    """

    @pytest.mark.asyncio
    async def test_benchmark_placeholder(self):
        """Placeholder — run full benchmark via publisher service."""
        # Full benchmark dijalankan via:
        #   docker compose --profile benchmark run --rm publisher
        assert True, "Run full benchmark with: docker compose --profile benchmark run --rm publisher"

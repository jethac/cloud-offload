"""The queue clock must keep the naive ISO format that existing rows use."""

import sqlite3
from datetime import datetime, timedelta

from cloud_offload.queue import JobQueue, utc_now


def test_utc_now_is_naive_and_offset_free():
    now = utc_now()
    assert now.tzinfo is None
    assert "+" not in now.isoformat()
    assert not now.isoformat().endswith("Z")


def test_new_rows_keep_the_legacy_timestamp_format(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create(model="m", input_path="in")
    with sqlite3.connect(queue.db_path) as conn:
        created_at, updated_at = conn.execute(
            "SELECT created_at, updated_at FROM jobs WHERE id = ?", (job.id,)
        ).fetchone()
    for value in (created_at, updated_at):
        assert "+" not in value
        assert datetime.fromisoformat(value).tzinfo is None


def test_rows_written_by_older_versions_still_compare(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    lease = queue.create_lease(provider="runpod", runtime_profile="comfyui")
    legacy_expiry = (utc_now() - timedelta(seconds=1)).isoformat()
    assert "+" not in legacy_expiry
    with sqlite3.connect(queue.db_path) as conn:
        conn.execute(
            "UPDATE job_leases SET expires_at = ? WHERE id = ?",
            (legacy_expiry, lease.id),
        )
    refreshed = queue.get_lease(lease.id)
    assert datetime.fromisoformat(refreshed.expires_at) <= utc_now()

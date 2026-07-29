import json
import time
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from cloud_offload import server
from cloud_offload.job_visibility import project_job_visibility, visibility_page
from cloud_offload.queue import JobQueue, JobStatus


def _stamp(start: datetime, seconds: int) -> str:
    return (start + timedelta(seconds=seconds)).isoformat()


def _visible_job(queue: JobQueue):
    return queue.create(
        "comfyui-partition-v1",
        "private://input/path",
        provider="runpod",
        status=JobStatus.QUEUED,
        params={
            "secret_option": "do-not-return",
            "preflight": {
                "provider": "runpod",
                "gpu_type": "A100 SXM",
                "region": "US-MD-1",
                "hourly_rate": 0.72,
                "preparation_class": "prepared-local",
                "confirmation": {"action": "start_now"},
                "estimate": {
                    "startup_seconds": [30, 60],
                    "preparation_seconds": [20, 40],
                    "execution_seconds": [60, 120],
                    "termination_seconds": [5, 15],
                    "paid_idle_seconds": 300,
                    "paid_lifetime_seconds": [415, 535],
                    "total_job_cost_usd": [0.083, 0.107],
                    "cost_complete": True,
                    "confidence": "medium",
                    "history_used": True,
                    "history_sample_count": 3,
                },
            },
        },
        request={
            "partition": {
                "partition_id": "part-safe",
                "workflow": {"70": {"prompt": "private prompt text"}},
            },
            "signed_url": "https://private.invalid/token",
        },
    )


def test_projection_combines_identity_progress_transfer_eta_and_spend_without_raw_data(
    tmp_path,
):
    queue = JobQueue(tmp_path / "queue.db")
    job = _visible_job(queue)
    start = datetime.fromisoformat(job.created_at).replace(tzinfo=timezone.utc)
    queue.append_event(
        job.id,
        {
            "type": "provider_request_completed",
            "phase": "provider_request",
            "provider": "runpod",
            "worker_instance_id": "pod-safe",
        },
        occurred_at=_stamp(start, 5),
    )
    queue.append_event(
        job.id,
        {
            "type": "runner_starting",
            "phase": "worker_boot",
            "gpu_type": "A100 SXM",
            "hourly_rate": 0.72,
            "worker_instance_id": "pod-safe",
            "overall_progress": 2,
        },
        occurred_at=_stamp(start, 6),
    )
    queue.append_event(
        job.id,
        {
            "type": "cache_mount_ready",
            "volume_id": "volume-safe",
            "datacenter_id": "US-MD-1",
        },
        occurred_at=_stamp(start, 10),
    )
    queue.append_event(
        job.id,
        {
            "type": "cache_artifact_hit",
            "digest": "private-digest",
            "bytes": 100,
            "verification_mode": "trusted_metadata_sample",
            "verification_bytes": 16,
        },
        occurred_at=_stamp(start, 20),
    )
    queue.append_event(
        job.id,
        {
            "type": "cache_population_progress",
            "digest": "private-digest-2",
            "bytes_completed": 100,
            "bytes_total": 400,
            "elapsed_seconds": 10,
        },
        occurred_at=_stamp(start, 30),
    )
    queue.append_event(
        job.id,
        {
            "type": "cache_population_progress",
            "digest": "private-digest-2",
            "bytes_completed": 300,
            "bytes_total": 400,
            "elapsed_seconds": 20,
        },
        occurred_at=_stamp(start, 40),
    )
    queue.append_event(
        job.id,
        {
            "type": "executing",
            "phase": "execution",
            "node_id": "private-node-id",
            "overall_progress": 42,
        },
        occurred_at=_stamp(start, 50),
    )
    queue.update_status(job.id, JobStatus.RUNNING, progress=42)

    view = project_job_visibility(
        queue.get(job.id), queue.list_events(job.id), now=start + timedelta(seconds=60)
    )

    assert view["status"] == "running"
    assert view["lifecycle_stage"] == "execution"
    assert view["progress"] > 42
    assert view["progress_basis"] == "stage_time_estimate"
    assert view["resource"] == {
        "provider": "runpod",
        "gpu_type": "A100 SXM",
        "region": "US-MD-1",
        "pod_id": "pod-safe",
        "volume_id": "volume-safe",
            "hourly_rate_usd": 0.72,
            "lease_id": None,
    }
    assert view["transfer"]["bytes_completed"] == 400
    assert view["transfer"]["bytes_total"] == 500
    assert view["transfer"]["throughput_bps"] == 13.5
    assert view["eta_seconds"] == [55.0, 165.0]
    assert view["eta_confidence"] == "medium"
    assert view["cost"]["estimated_spend_usd"] == 0.011
    assert view["cache"] == {
        "hits": 1,
        "misses": 0,
        "hit_bytes": 100,
        "verification_bytes": 16,
        "trusted_hits": 1,
        "full_verified_hits": 0,
        "items_saved": 0,
        "prepared": True,
    }
    assert view["billing"] == {
        "state": "accruing",
        "termination_confirmed": False,
        "termination_confirmed_at": None,
    }
    encoded = json.dumps(view)
    for private_value in (
        "private prompt text",
        "private://input/path",
        "https://private.invalid/token",
        "do-not-return",
        "private-digest",
        "private-node-id",
    ):
        assert private_value not in encoded
    def keys(value):
        if isinstance(value, dict):
            return set(value).union(*(keys(item) for item in value.values()))
        if isinstance(value, list):
            return set().union(*(keys(item) for item in value))
        return set()

    assert not {"workflow", "request", "params", "event"}.intersection(keys(view))


def test_active_stage_progress_changes_without_a_new_event(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = _visible_job(queue)
    start = datetime.fromisoformat(job.created_at).replace(tzinfo=timezone.utc)
    queue.append_event(
        job.id,
        {
            "type": "runner_starting",
            "phase": "worker_boot",
            "overall_progress": 2,
        },
        occurred_at=_stamp(start, 1),
    )
    events = queue.list_events(job.id)

    first = project_job_visibility(job, events, now=start + timedelta(seconds=10))
    second = project_job_visibility(job, events, now=start + timedelta(seconds=20))

    assert first["lifecycle_stage"] == "worker_boot"
    assert second["progress"] > first["progress"] >= 2
    assert second["progress_basis"] == "stage_time_estimate"


def test_terminal_success_is_100_but_billing_waits_for_a_termination_receipt(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = _visible_job(queue)
    start = datetime.fromisoformat(job.created_at).replace(tzinfo=timezone.utc)
    queue.append_event(
        job.id,
        {"type": "runner_starting", "worker_instance_id": "pod-safe"},
        occurred_at=_stamp(start, 1),
    )
    queue.complete_job(job.id, {"private_result": "must-not-return"})

    unconfirmed = project_job_visibility(
        queue.get(job.id), queue.list_events(job.id), now=start + timedelta(seconds=20)
    )
    assert unconfirmed["progress"] == 100
    assert unconfirmed["billing"]["state"] == "termination_unconfirmed"
    assert unconfirmed["active_operation"] == (
        "Result is ready; GPU closure is not confirmed"
    )

    queue.append_event(
        job.id,
        {"type": "provider_termination_completed", "worker_instance_id": "pod-safe"},
        occurred_at=_stamp(start, 25),
    )
    confirmed = project_job_visibility(
        queue.get(job.id), queue.list_events(job.id), now=start + timedelta(seconds=30)
    )
    assert confirmed["billing"] == {
        "state": "stopped",
        "termination_confirmed": True,
        "termination_confirmed_at": (start + timedelta(seconds=25)).isoformat(),
    }
    assert "must-not-return" not in json.dumps(confirmed)


def test_spend_uses_first_pod_observation_when_old_history_has_no_allocation_event(
    tmp_path,
):
    queue = JobQueue(tmp_path / "queue.db")
    job = _visible_job(queue)
    start = datetime.fromisoformat(job.created_at).replace(tzinfo=timezone.utc)
    queue.append_event(
        job.id,
        {
            "type": "runner_starting_progress",
            "worker_instance_id": "pod-safe",
        },
        occurred_at=_stamp(start, 10),
    )

    view = project_job_visibility(
        job, queue.list_events(job.id), now=start + timedelta(seconds=110)
    )

    assert view["paid_elapsed_seconds"] == 100
    assert view["cost"]["estimated_spend_usd"] == 0.02
    assert view["cost"]["spend_basis"] == "first_pod_observation_elapsed"


def test_visibility_page_orders_active_first_and_rebuilds_well_under_two_seconds(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    terminal_ids = []
    for _ in range(40):
        item = queue.create("test", "private://input", status=JobStatus.QUEUED)
        queue.complete_job(item.id, {})
        terminal_ids.append(item.id)
    active = [
        queue.create("test", "private://input", status=JobStatus.QUEUED)
        for _ in range(40)
    ]

    started = time.perf_counter()
    page = visibility_page(queue, limit=50)
    elapsed = time.perf_counter() - started

    assert elapsed < 2.0
    assert len(page["jobs"]) == 50
    assert [item["job_id"] for item in page["jobs"][:40]] == [
        item.id for item in reversed(active)
    ]
    assert all(not item["terminal"] for item in page["jobs"][:40])
    assert all(item["terminal"] for item in page["jobs"][40:])


def test_visibility_endpoint_returns_the_safe_page(monkeypatch, tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = _visible_job(queue)
    monkeypatch.setattr(server, "_queue", lambda: (None, queue))

    response = TestClient(server.app).get("/api/job-visibility?limit=10")

    assert response.status_code == 200
    assert response.json()["schema"] == "cloud-offload.job-visibility.v1"
    assert response.json()["jobs"][0]["job_id"] == job.id
    assert "private prompt text" not in response.text


def test_visibility_uses_the_newest_event_window_and_reports_full_event_bounds(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create("test", "private://input", status=JobStatus.QUEUED)
    for index in range(5):
        queue.append_event(
            job.id,
            {"type": "runner_starting_progress", "elapsed_seconds": index},
        )

    recent = queue.list_recent_events(job.id, limit=2)
    count, cursor = queue.event_bounds(job.id)

    assert [item["event"]["elapsed_seconds"] for item in recent] == [3, 4]
    assert count == 6
    assert cursor == recent[-1]["sequence"]

import sqlite3
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from cloud_offload import server
from cloud_offload.config import CloudConfig
from cloud_offload.dispatcher import Dispatcher
from cloud_offload.job_visibility import project_job_visibility
from cloud_offload.providers.base import Instance
from cloud_offload.queue import JobQueue, JobStatus
from cloud_offload.worker import Worker


class LeaseProvider:
    name = "runpod"

    def __init__(self, *, remove_on_terminate=False):
        self.instances = {}
        self.termination_requests = []
        self.remove_on_terminate = remove_on_terminate
        self.launches = 0

    def find_cheapest(self, **kwargs):
        return {"id": "offer-1", "gpu_type": "A100", "hourly_rate": 0.72}

    def launch(self, *args, **kwargs):
        self.launches += 1
        instance = Instance(
            id=f"pod-{self.launches}",
            provider="runpod",
            gpu_type="A100",
            gpu_count=1,
            hourly_rate=0.72,
            status="running",
            metadata={"name": kwargs.get("resource_name")},
        )
        self.instances[instance.id] = instance
        return instance

    def get_instance(self, instance_id):
        return self.instances.get(instance_id)

    def list_instances(self):
        return list(self.instances.values())

    def terminate(self, instance_id):
        self.termination_requests.append(instance_id)
        if self.remove_on_terminate:
            self.instances.pop(instance_id, None)
        return instance_id in self.instances or self.remove_on_terminate


def lease_config(tmp_path, **values):
    return CloudConfig(
        provider="runpod",
        provider_order=["runpod"],
        queue_db_path=str(tmp_path / "queue.db"),
        coordinator_url="https://coordinator.invalid",
        min_queue_depth=1,
        worker_profiles={
            "comfyui": {
                "image": "ghcr.io/example/comfyui@sha256:" + "a" * 64,
                "models": ["comfyui-partition-v1"],
                "providers": ["runpod"],
            }
        },
        **values,
    )


def queued_job(queue):
    return queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"runtime_profile": "comfyui"},
        status=JobStatus.QUEUED,
    )


def bound_lease(queue, job, *, ttl_seconds=300):
    lease = queue.create_lease(
        provider="runpod",
        runtime_profile="comfyui",
        job_ids=[job.id],
        hourly_rate=0.72,
        max_runtime_seconds=7200,
        max_cost_usd=2.0,
        ttl_seconds=ttl_seconds,
    )
    return queue.bind_lease(lease.id, "pod-1", ttl_seconds=ttl_seconds)


def test_lease_persists_exact_resource_and_worker_renewal(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queued_job(queue)
    lease = bound_lease(queue, job)

    reopened = JobQueue(tmp_path / "queue.db")
    restored = reopened.get_lease(lease.id)
    assert restored.instance_id == "pod-1"
    assert restored.status == "active"
    before = restored.expires_at

    claimed = reopened.claim_jobs(
        "worker-1",
        provider="runpod",
        models=["comfyui-partition-v1"],
        lease_id=lease.id,
        lease_ttl_seconds=600,
    )

    assert [item.id for item in claimed] == [job.id]
    renewed = reopened.get_lease(lease.id)
    assert renewed.worker_id == "worker-1"
    assert renewed.expires_at > before
    with pytest.raises(PermissionError, match="different worker"):
        reopened.renew_lease(lease.id, worker_id="worker-2")


@pytest.mark.parametrize(
    ("status", "phase"),
    [
        (JobStatus.QUEUED, "provisioning"),
        (JobStatus.DISPATCHED, "worker_boot"),
        (JobStatus.DISPATCHED, "dependency_preparation"),
        (JobStatus.RUNNING, "execution"),
        (JobStatus.RUNNING, "result_transfer"),
    ],
)
def test_cancellation_revokes_and_closes_the_exact_pod(
    tmp_path, monkeypatch, status, phase
):
    config = lease_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    job = queued_job(queue)
    lease = bound_lease(queue, job)
    if status != JobStatus.QUEUED:
        queue.update_status(job.id, status, worker_id="worker-1")
    queue.append_event(job.id, {"type": "phase_timing", "phase": phase})
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))

    response = TestClient(server.app).post(f"/api/jobs/{job.id}/cancel")

    assert response.status_code == 200
    assert queue.get(job.id).status == JobStatus.FAILED
    assert queue.get_lease(lease.id).status == "revocation_requested"

    provider = LeaseProvider(remove_on_terminate=True)
    provider.instances["pod-1"] = Instance(
        "pod-1", "runpod", "A100", 1, 0.72, "running"
    )
    Dispatcher(config, queue=queue, provider=provider)._reconcile_leases()

    assert provider.termination_requests == ["pod-1"]
    assert queue.get_lease(lease.id).status == "closed"
    events = queue.list_events(job.id)
    assert any(item["type"] == "provider_termination_completed" for item in events)


def test_billing_stays_unconfirmed_until_provider_reports_absence(tmp_path):
    config = lease_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    job = queued_job(queue)
    lease = bound_lease(queue, job)
    queue.update_status(job.id, JobStatus.FAILED, error="Cancelled")
    queue.request_lease_revocation(lease.id, "user_cancelled")
    provider = LeaseProvider()
    provider.instances["pod-1"] = Instance(
        "pod-1", "runpod", "A100", 1, 0.72, "running"
    )
    dispatcher = Dispatcher(config, queue=queue, provider=provider)

    dispatcher._reconcile_leases()

    view = project_job_visibility(queue.get(job.id), queue.list_events(job.id))
    assert view["billing"]["state"] == "termination_unconfirmed"
    assert queue.get_lease(lease.id).status == "terminating"

    provider.instances.pop("pod-1")
    dispatcher._reconcile_leases()

    view = project_job_visibility(queue.get(job.id), queue.list_events(job.id))
    assert view["billing"]["state"] == "stopped"
    assert view["billing"]["termination_confirmed"] is True


def test_stopped_but_not_removed_resource_does_not_close_billing(tmp_path):
    config = lease_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    job = queued_job(queue)
    lease = bound_lease(queue, job)
    queue.update_status(job.id, JobStatus.FAILED, error="Cancelled")
    queue.request_lease_revocation(lease.id, "user_cancelled")
    provider = LeaseProvider()
    provider.instances["pod-1"] = Instance(
        "pod-1", "runpod", "A100", 1, 0.72, "stopped"
    )

    Dispatcher(config, queue=queue, provider=provider)._reconcile_leases()

    assert queue.get_lease(lease.id).status == "terminating"
    assert project_job_visibility(
        queue.get(job.id), queue.list_events(job.id)
    )["billing"]["state"] == "termination_unconfirmed"


@pytest.mark.parametrize("deadline", ["runtime_deadline", "cost_deadline"])
def test_reconciliation_enforces_runtime_and_dollar_circuit_breakers(
    tmp_path, deadline
):
    config = lease_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    job = queued_job(queue)
    lease = bound_lease(queue, job)
    queue.update_status(job.id, JobStatus.RUNNING, worker_id="worker-1")
    with sqlite3.connect(queue.db_path) as conn:
        conn.execute(
            f"UPDATE job_leases SET {deadline} = ? WHERE id = ?",
            ((datetime.utcnow() - timedelta(seconds=1)).isoformat(), lease.id),
        )
    provider = LeaseProvider(remove_on_terminate=True)
    provider.instances["pod-1"] = Instance(
        "pod-1", "runpod", "A100", 1, 0.72, "running"
    )

    Dispatcher(config, queue=queue, provider=provider)._reconcile_leases()

    assert queue.get(job.id).status == JobStatus.FAILED
    assert queue.get(job.id).error.startswith("Cancelled:")
    assert queue.get_lease(lease.id).status == "closed"
    assert provider.termination_requests == ["pod-1"]
    assert any(
        item["type"] == "circuit_breaker_triggered"
        for item in queue.list_events(job.id)
    )


def test_restarted_dispatcher_reconciles_a_revoked_lease(tmp_path):
    config = lease_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    job = queued_job(queue)
    lease = bound_lease(queue, job)
    queue.request_lease_revocation(lease.id, "coordinator_restart")
    provider = LeaseProvider(remove_on_terminate=True)
    provider.instances["pod-1"] = Instance(
        "pod-1", "runpod", "A100", 1, 0.72, "running"
    )

    replacement = Dispatcher(config, queue=JobQueue(queue.db_path), provider=provider)
    replacement._reconcile_leases()

    assert provider.instances == {}
    assert replacement.queue.get_lease(lease.id).status == "closed"


def test_provider_loss_closes_lease_and_requeues_unfinished_work(tmp_path):
    config = lease_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    job = queued_job(queue)
    lease = bound_lease(queue, job)
    claimed = queue.claim_jobs(
        "worker-1",
        provider="runpod",
        models=["comfyui-partition-v1"],
        lease_id=lease.id,
    )[0]
    queue.update_status(claimed.id, JobStatus.RUNNING)

    Dispatcher(config, queue=queue, provider=LeaseProvider())._reconcile_leases()

    assert queue.get_lease(lease.id).status == "closed"
    assert queue.get(job.id).status == JobStatus.QUEUED
    assert any(
        item["type"] == "provider_resource_lost"
        for item in queue.list_events(job.id)
    )


def test_cancelled_job_cannot_publish_shared_cache_state(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queued_job(queue)
    queue.update_status(job.id, JobStatus.FAILED, error="Cancelled")
    worker = Worker.__new__(Worker)
    worker.queue = queue
    worker.lease_id = "lease-1"
    worker.running = True

    with pytest.raises(RuntimeError, match="Cancelled"):
        worker._publish_prepared_artifacts([{"digest": "sha256:" + "a" * 64}], job)


def test_uncertain_launch_is_recovered_by_durable_resource_name(tmp_path):
    class UncertainProvider(LeaseProvider):
        def launch(self, *args, **kwargs):
            instance = super().launch(*args, **kwargs)
            raise TimeoutError(f"readiness timed out for {instance.id}")

    config = lease_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    job = queued_job(queue)
    provider = UncertainProvider()
    dispatcher = Dispatcher(config, queue=queue, provider=provider)

    instance = dispatcher._launch_worker("runpod", "comfyui", [job])

    assert instance is not None
    assert provider.launches == 1
    leases = queue.leases_for_job(job.id)
    assert len(leases) == 1
    assert leases[0].instance_id == instance.id
    assert leases[0].status == "active"

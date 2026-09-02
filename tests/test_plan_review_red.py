"""Review reproductions. These must fail on the rejected implementation."""
import copy
from fastapi.testclient import TestClient
import pytest
from cloud_offload import server
from cloud_offload.config import CloudConfig
from cloud_offload.plan_protocol import PlanProtocolStore
from cloud_offload.queue import JobQueue
from tests.test_plan_protocol import plan


def _client(tmp_path, monkeypatch):
    config = CloudConfig(queue_db_path=str(tmp_path / "queue.db"))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    return TestClient(server.app), config


def test_submit_queue_failure_does_not_leave_replayable_authority(tmp_path, monkeypatch):
    client, config = _client(tmp_path, monkeypatch)
    value = plan()
    pf = client.post("/api/plans/preflight", json={"plan": value}).json()
    payload = {"plan": value, "preflight_id": pf["preflight_id"], "plan_digest": value["plan_digest"], "candidate_id": pf["candidate_id"], "confirmation_action": "start_now", "client_request_id": "k"}
    def process_death(*args, **kwargs):
        raise BaseException("simulated process death")

    monkeypatch.setattr(JobQueue, "submit_plan_atomic", process_death)
    with pytest.raises(BaseException):
        client.post("/api/plans", headers={"Idempotency-Key": "k"}, json=payload)
    record = PlanProtocolStore(config.queue_db_path).get(value["plan_digest"])
    assert record is not None and record["job_id"] is None and record["state"] == "preflighted"
    assert JobQueue(config.queue_db_path).list_recent() == []


def test_public_job_does_not_expose_plan_secret_fields(tmp_path, monkeypatch):
    client, _ = _client(tmp_path, monkeypatch)
    value = plan()
    value["stages"][0]["settings"] = {"prompt": "secret", "path": "C:/private", "token": "x", "signed_url": "https://private"}
    value["plan_digest"] = __import__("cloud_offload.plan_protocol", fromlist=["canonical_plan_digest"]).canonical_plan_digest(value)
    pf = client.post("/api/plans/preflight", json={"plan": value, "timeout_seconds": 5}).json()
    payload = {"plan": value, "preflight_id": pf["preflight_id"], "plan_digest": value["plan_digest"], "candidate_id": pf["candidate_id"], "confirmation_action": "start_now", "client_request_id": "k2", "timeout_seconds": 5}
    response = client.post("/api/plans", headers={"Idempotency-Key": "k2"}, json=payload)
    job = client.get("/api/jobs/" + response.json()["job_id"])
    assert job.status_code == 200
    assert not any(secret in job.text for secret in ("secret", "C:/private", "https://private"))


def test_same_key_changed_bound_request_conflicts(tmp_path, monkeypatch):
    client, _ = _client(tmp_path, monkeypatch)
    value = plan()
    pf = client.post("/api/plans/preflight", json={"plan": value, "timeout_seconds": 5}).json()
    payload = {"plan": value, "preflight_id": pf["preflight_id"], "plan_digest": value["plan_digest"], "candidate_id": pf["candidate_id"], "confirmation_action": "start_now", "client_request_id": "k3", "timeout_seconds": 5}
    first = client.post("/api/plans", headers={"Idempotency-Key": "k3"}, json=payload)
    changed = copy.deepcopy(payload)
    changed["timeout_seconds"] = 6
    second = client.post("/api/plans", headers={"Idempotency-Key": "k3"}, json=changed)
    assert first.status_code == 202 and second.status_code == 409

from fastapi.testclient import TestClient

from cloud_offload import server
from cloud_offload.config import CloudConfig
from cloud_offload.plan_protocol import PlanProtocolStore
from tests.test_plan_protocol import plan


def test_plan_routes_preflight_submit_and_same_key_replay(tmp_path, monkeypatch):
    config = CloudConfig(queue_db_path=str(tmp_path / "queue.db"))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    client = TestClient(server.app)
    value = plan()
    preflight = client.post("/api/plans/preflight", json={"plan": value})
    assert preflight.status_code == 200
    body = preflight.json()
    payload = {"plan": value, "preflight_id": body["preflight_id"], "plan_digest": value["plan_digest"], "candidate_id": body["candidate_id"], "confirmation_action": "start_now", "client_request_id": "request-1"}
    first = client.post("/api/plans", headers={"Idempotency-Key": "request-1"}, json=payload)
    second = client.post("/api/plans", headers={"Idempotency-Key": "request-1"}, json=payload)
    assert first.status_code == second.status_code == 202
    assert first.json()["job_id"] == second.json()["job_id"]
    assert second.json()["replayed"] is True


def test_plan_submit_requires_matching_header(tmp_path, monkeypatch):
    config = CloudConfig(queue_db_path=str(tmp_path / "queue.db"))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    client = TestClient(server.app)
    value = plan()
    body = client.post("/api/plans/preflight", json={"plan": value}).json()
    payload = {"plan": value, "preflight_id": body["preflight_id"], "plan_digest": value["plan_digest"], "candidate_id": body["candidate_id"], "confirmation_action": "start_now", "client_request_id": "request-1"}
    response = client.post("/api/plans", headers={"Idempotency-Key": "request-2"}, json=payload)
    assert response.status_code == 409


def test_plan_store_survives_restart(tmp_path):
    store = PlanProtocolStore(str(tmp_path / "state.db"))
    value = plan()
    report = {"preflight_id": "pf", "status": "ready", "expires_at": "2999-01-01T00:00:00Z", "candidates": [{"candidate_id": "c"}]}
    store.preflight(value, report)
    assert store.submit(plan=value, preflight_id="pf", candidate_id="c", key="k", job_id="job")[0] == "job"
    reopened = PlanProtocolStore(str(tmp_path / "state.db"))
    assert reopened.submit(plan=value, preflight_id="pf", candidate_id="c", key="k", job_id="new")[0] == "job"

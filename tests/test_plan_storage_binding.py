"""HTTP regressions for candidate storage identity and submit freshness."""

import sqlite3

from fastapi.testclient import TestClient

from cloud_offload import server
from cloud_offload.config import CloudConfig
from cloud_offload.queue import JobQueue
from tests.test_plan_protocol import plan


class _MutableOfferConnector:
    def __init__(self, offer):
        self.offer = offer
        self.list_calls = 0

    def list_available(self, **kwargs):
        self.list_calls += 1
        return [self.offer]


def _client(tmp_path, monkeypatch, connector):
    config = CloudConfig(queue_db_path=str(tmp_path / "queue.db"))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    monkeypatch.setattr(
        server.app.state,
        "plan_connector_factory",
        lambda provider, cfg: connector,
        raising=False,
    )
    # Initialize queue tables before taking the preflight-only snapshot.
    JobQueue(config.queue_db_path)
    return TestClient(server.app), config


def _offer(*, persistent=False):
    return {
        "id": "offer-storage-binding",
        "provider": "offline",
        "profile": "offline",
        "gpu_type": "offline",
        "gpu_ram_gb": 256,
        "hourly_rate": 0.01,
        "region": "offline-test",
        "storage": {"region": "offline-test", "persistent": persistent},
    }


def _submit_payload(client, value):
    response = client.post("/api/plans/preflight", json={"plan": value})
    assert response.status_code == 200, response.text
    preflight = response.json()
    return {
        "plan": value,
        "preflight_id": preflight["preflight_id"],
        "plan_digest": value["plan_digest"],
        "candidate_id": preflight["candidate_id"],
        "confirmation_action": "start_now",
        "client_request_id": "storage-binding-key",
    }


def _state_snapshot(config, plan_digest):
    with sqlite3.connect(config.queue_db_path) as db:
        plan_row = db.execute(
            "SELECT state,job_id,idempotency_key,request_digest,preflight_json "
            "FROM cloud_plans WHERE plan_digest=?",
            (plan_digest,),
        ).fetchone()
        authority_row = db.execute(
            "SELECT request_json,job_id,preflight_json "
            "FROM cloud_plan_authority WHERE plan_digest=?",
            (plan_digest,),
        ).fetchone()
        return {
            "plan": plan_row,
            "authority": authority_row,
            "jobs": db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0],
            "events": db.execute("SELECT COUNT(*) FROM job_events").fetchone()[0],
        }


def test_submit_rejects_same_region_storage_mutation_before_any_authority_or_queue_write(
    tmp_path, monkeypatch
):
    connector = _MutableOfferConnector(_offer(persistent=False))
    client, config = _client(tmp_path, monkeypatch, connector)
    value = plan()
    payload = _submit_payload(client, value)
    before = _state_snapshot(config, value["plan_digest"])

    connector.offer["storage"]["persistent"] = True

    response = client.post(
        "/api/plans",
        headers={"Idempotency-Key": payload["client_request_id"]},
        json=payload,
    )

    assert response.status_code == 409, response.text
    assert _state_snapshot(config, value["plan_digest"]) == before
    assert connector.list_calls == 2


def test_submit_accepts_unchanged_storage_after_live_offer_revalidation(tmp_path, monkeypatch):
    connector = _MutableOfferConnector(_offer(persistent=False))
    client, config = _client(tmp_path, monkeypatch, connector)
    value = plan()
    payload = _submit_payload(client, value)

    response = client.post(
        "/api/plans",
        headers={"Idempotency-Key": payload["client_request_id"]},
        json=payload,
    )

    assert response.status_code == 202, response.text
    assert response.json()["replayed"] is False
    assert connector.list_calls == 2
    state = _state_snapshot(config, value["plan_digest"])
    assert state["plan"][0:2] == ("submitting", response.json()["job_id"])
    assert state["jobs"] == 1
    assert state["events"] == 1


def test_submit_rejects_unknown_current_storage_fields_before_authority_or_queue_write(
    tmp_path, monkeypatch
):
    connector = _MutableOfferConnector(_offer(persistent=False))
    client, config = _client(tmp_path, monkeypatch, connector)
    value = plan()
    payload = _submit_payload(client, value)
    before = _state_snapshot(config, value["plan_digest"])

    connector.offer["storage"]["unexpected"] = "tampered"

    response = client.post(
        "/api/plans",
        headers={"Idempotency-Key": payload["client_request_id"]},
        json=payload,
    )

    assert response.status_code == 409, response.text
    assert _state_snapshot(config, value["plan_digest"]) == before
    assert connector.list_calls == 2


def test_preflight_rejects_unknown_storage_fields_without_creating_authority(
    tmp_path, monkeypatch
):
    connector = _MutableOfferConnector(_offer(persistent=False))
    connector.offer["storage"]["unexpected"] = "tampered"
    client, config = _client(tmp_path, monkeypatch, connector)

    response = client.post("/api/plans/preflight", json={"plan": plan()})

    assert response.status_code == 409, response.text
    with sqlite3.connect(config.queue_db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM cloud_plans").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM cloud_plan_authority").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM job_events").fetchone()[0] == 0

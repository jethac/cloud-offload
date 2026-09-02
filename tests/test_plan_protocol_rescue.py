"""Red tests for the plan protocol review findings."""

import copy
import hashlib
import json
import multiprocessing
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from cloud_offload import server
from cloud_offload.config import CloudConfig
from cloud_offload.plan_protocol import (
    PlanError,
    PlanProtocolStore,
    binding_digest,
    canonical_bytes,
    canonical_plan_digest,
    validate_cloud_plan,
)
from cloud_offload.queue import JobQueue
from tests.test_plan_protocol import plan


def _client(tmp_path, monkeypatch):
    config = CloudConfig(queue_db_path=str(tmp_path / "queue.db"))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    return TestClient(server.app), config


def _submit_payload(client, value, *, key="rescue-key", **changes):
    response = client.post("/api/plans/preflight", json={"plan": value})
    assert response.status_code == 200, response.text
    preflight = response.json()
    payload = {
        "plan": value,
        "preflight_id": preflight["preflight_id"],
        "plan_digest": value["plan_digest"],
        "candidate_id": preflight["candidate_id"],
        "confirmation_action": "start_now",
        "client_request_id": key,
    }
    payload.update(changes)
    return payload


def _preflight_process(arguments):
    path, encoded_plan = arguments
    value = json.loads(encoded_plan)
    return PlanProtocolStore(path).preflight(
        value,
        {
            "preflight_id": "process-preflight",
            "status": "ready",
            "expires_at": "2999-01-01T00:00:00Z",
            "candidates": [{"candidate_id": "process-candidate"}],
        },
        request_digest="sha256:" + "e" * 64,
    )


def test_baseexception_between_plan_and_job_rolls_back_both_rows(tmp_path, monkeypatch):
    client, config = _client(tmp_path, monkeypatch)
    value = plan()
    payload = _submit_payload(client, value)

    original = JobQueue._append_event_in_transaction

    def die(*args, **kwargs):
        raise BaseException("simulated process death")

    monkeypatch.setattr(JobQueue, "_append_event_in_transaction", die)
    with pytest.raises(BaseException):
        client.post(
            "/api/plans", headers={"Idempotency-Key": payload["client_request_id"]}, json=payload
        )
    monkeypatch.setattr(JobQueue, "_append_event_in_transaction", original)

    store = PlanProtocolStore(config.queue_db_path)
    assert store.get(value["plan_digest"]) is not None
    with sqlite3.connect(config.queue_db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0
        assert db.execute(
            "SELECT state, job_id FROM cloud_plans WHERE plan_digest = ?",
            (value["plan_digest"],),
        ).fetchone() == ("preflighted", None)


def test_expiry_never_deletes_an_active_or_terminal_authority(tmp_path):
    store = PlanProtocolStore(str(tmp_path / "queue.db"))
    value = plan()
    accepted = {
        "preflight_id": "expired",
        "status": "ready",
        "expires_at": "2999-01-01T00:00:00Z",
        "candidates": [{"candidate_id": "candidate"}],
    }
    store.preflight(value, accepted)
    store.submit(
        plan=value,
        preflight_id="expired",
        candidate_id="candidate",
        key="expiry-key",
        request_digest="sha256:" + "a" * 64,
        job_id="job-expiry",
    )
    store.close(value["plan_digest"], {"receipt_id": "receipt", "terminated": True})

    replacement = copy.deepcopy(accepted)
    replacement["preflight_id"] = "replacement"
    result = store.preflight(value, replacement)
    assert result["preflight_id"] == "expired"
    record = store.get(value["plan_digest"])
    assert record["job_id"] == "job-expiry"
    assert record["closure"]["receipt_id"] == "receipt"


@pytest.mark.parametrize("expiry_value", ["not-a-time", "2999-01-01T00:00:00"])
@pytest.mark.parametrize("terminal_state", ["submitting", "cancelled", "completed", "failed"])
def test_expired_or_malformed_rows_are_never_deleted(tmp_path, terminal_state, expiry_value):
    path = str(tmp_path / f"{terminal_state}.db")
    store = PlanProtocolStore(path)
    value = plan()
    accepted = {
        "preflight_id": "expired-row",
        "status": "ready",
        "expires_at": "2999-01-01T00:00:00Z",
        "candidates": [{"candidate_id": "candidate"}],
    }
    store.preflight(value, accepted)
    if terminal_state != "preflighted":
        store.submit(
            plan=value,
            preflight_id="expired-row",
            candidate_id="candidate",
            key="expired-key",
            request_digest="sha256:" + "b" * 64,
            job_id="job-expired",
        )
    if terminal_state == "cancelled":
        store.close(value["plan_digest"], {"receipt_id": "cancel-receipt", "status": "cancelled", "provider_resource_absent": True})
    elif terminal_state == "completed":
        store.close(value["plan_digest"], {"receipt_id": "complete-receipt", "status": "completed", "provider_resource_absent": True})
    elif terminal_state == "failed":
        store.close(value["plan_digest"], {"receipt_id": "failed-receipt", "status": "failed", "provider_resource_absent": True})
    with sqlite3.connect(path) as db:
        db.execute(
            "UPDATE cloud_plans SET preflight_json=? WHERE plan_digest=?",
            (json.dumps({**accepted, "expires_at": expiry_value}), value["plan_digest"]),
        )
    with pytest.raises(PlanError):
        store.preflight(value, accepted)
    record = store.get(value["plan_digest"])
    assert record is not None
    assert record["state"] == terminal_state


def test_corrupt_preflight_json_fails_closed_as_protocol_error(tmp_path):
    path = str(tmp_path / "corrupt-json.db")
    store = PlanProtocolStore(path)
    value = plan()
    store.preflight(
        value,
        {
            "preflight_id": "corrupt-json",
            "status": "ready",
            "expires_at": "2999-01-01T00:00:00Z",
            "candidates": [{"candidate_id": "candidate"}],
        },
    )
    with sqlite3.connect(path) as db:
        db.execute("UPDATE cloud_plans SET preflight_json=? WHERE plan_digest=?", ("{", value["plan_digest"]))
    with pytest.raises(PlanError):
        store.lookup_preflight(value["plan_digest"], "sha256:" + "a" * 64)


def test_corrupt_preflight_json_cannot_escape_submit_as_server_error(tmp_path, monkeypatch):
    client, config = _client(tmp_path, monkeypatch)
    value = plan()
    payload = _submit_payload(client, value, key="corrupt-submit")
    with sqlite3.connect(config.queue_db_path) as db:
        db.execute(
            "UPDATE cloud_plans SET preflight_json=? WHERE plan_digest=?",
            ("{", value["plan_digest"]),
        )
    response = client.post(
        "/api/plans",
        headers={"Idempotency-Key": "corrupt-submit"},
        json=payload,
    )
    assert response.status_code == 409
    assert "JSONDecodeError" not in response.text


def test_public_preflight_rejects_mismatched_nested_plan_digest():
    from cloud_offload.plan_protocol import public_preflight_report

    with pytest.raises(PlanError):
        public_preflight_report(
            {
                "preflight_id": "nested-binding",
                "plan_digest": "sha256:" + "a" * 64,
                "expires_at": "2999-01-01T00:00:00Z",
                "candidates": [{"candidate_id": "candidate"}],
                "plan": {
                    "plan_digest": "sha256:" + "b" * 64,
                    "stages": [],
                },
            }
        )


def test_new_expired_preflight_is_not_accepted(tmp_path):
    store = PlanProtocolStore(str(tmp_path / "expired-new.db"))
    with pytest.raises(PlanError):
        store.preflight(
            plan(),
            {
                "preflight_id": "expired-new",
                "status": "ready",
                "expires_at": "2000-01-01T00:00:00Z",
                "candidates": [{"candidate_id": "candidate"}],
            },
        )


def test_public_plan_projection_never_contains_operation_or_provider(tmp_path, monkeypatch):
    client, _ = _client(tmp_path, monkeypatch)
    value = plan()
    value["operation"] = "render"
    value["stages"][0]["settings"] = {
        "prompt": "secret-prompt",
        "path": "C:/private/file",
        "token": "secret-token",
        "signed_url": "https://secret.invalid/x",
    }
    value["plan_digest"] = canonical_plan_digest(value)
    payload = _submit_payload(client, value, key="privacy-key")
    response = client.post(
        "/api/plans", headers={"Idempotency-Key": "privacy-key"}, json=payload
    )
    assert response.status_code == 202, response.text
    job = client.get("/api/jobs/" + response.json()["job_id"])
    assert job.status_code == 200
    public = job.text
    assert '"operation"' not in public
    assert "secret-prompt" not in public
    assert "C:/private/file" not in public
    assert "secret-token" not in public
    assert "https://secret.invalid/x" not in public
    assert "offline" not in public


def test_plan_validation_errors_do_not_echo_private_request_input(tmp_path, monkeypatch):
    client, _ = _client(tmp_path, monkeypatch)
    marker = "PRIVATE_VALIDATION_MARKER"
    response = client.post(
        "/api/plans/preflight",
        json={"plan": plan(), "unexpected": {"prompt": marker}},
    )
    assert response.status_code == 422
    assert marker not in response.text


def test_plan_queue_request_projection_hashes_input_names(tmp_path, monkeypatch):
    client, config = _client(tmp_path, monkeypatch)
    value = plan()
    value["input_artifacts"][0]["name"] = "PRIVATE_INPUT_NAME"
    value["plan_digest"] = canonical_plan_digest(value)
    payload = _submit_payload(client, value, key="input-name")
    accepted = client.post(
        "/api/plans", headers={"Idempotency-Key": "input-name"}, json=payload
    )
    assert accepted.status_code == 202, accepted.text
    with sqlite3.connect(config.queue_db_path) as db:
        request_json = db.execute(
            "SELECT request_json FROM jobs WHERE id=?", (accepted.json()["job_id"],)
        ).fetchone()[0]
    assert "PRIVATE_INPUT_NAME" not in request_json


def test_public_preflight_hashes_storage_identifier(tmp_path):
    from cloud_offload.plan_protocol import public_preflight_report

    report = public_preflight_report(
        {
            "preflight_id": "storage-public",
            "plan_digest": "sha256:" + "a" * 64,
            "expires_at": "2999-01-01T00:00:00Z",
            "candidates": [
                {
                    "candidate_id": "candidate",
                    "offer_id": "offer",
                    "region": "offline-test",
                    "hourly_rate": 0.01,
                    "storage": {
                        "region": "offline-test",
                        "persistent": True,
                        "storage_id": "private-storage-id",
                    },
                }
            ],
        }
    )
    assert "private-storage-id" not in json.dumps(report)


def test_closure_reason_is_a_finite_code_not_private_error_text():
    from cloud_offload.plan_protocol import validate_closure_receipt

    result = validate_closure_receipt(
        {
            "receipt_id": "closure-reason",
            "status": "failed",
            "provider_resource_absent": True,
            "reason": "PRIVATE_ERROR_MESSAGE",
        }
    )
    assert result["reason_code"] == "unknown"


def test_plan_link_must_reference_declared_producer_output():
    value = plan()
    value["stages"].append(
        {
            "id": "consumer",
            "kind": "tool",
            "depends_on": ["render"],
            "operation": "offline-render",
            "settings": {},
            "inputs": [
                {
                    "from_stage": "render",
                    "output": "missing",
                    "required": True,
                    "role": "input",
                    "media_type": "image/png",
                }
            ],
            "outputs": [
                {"name": "final", "role": "output", "media_type": "image/png"}
            ],
            "runner": {"profile": "offline"},
            "retry": {"max_attempts": 1},
            "checkpoint": {"required": True},
            "fan_out": {"max_items": 1},
        }
    )
    value["final_outputs"] = [{"stage_id": "consumer", "output": "final"}]
    value["plan_digest"] = canonical_plan_digest(value)
    with pytest.raises(PlanError, match="output"):
        validate_cloud_plan(value)


def test_result_manifest_requires_nonempty_typed_artifact():
    from cloud_offload.plan_protocol import validate_result_manifest

    with pytest.raises(PlanError):
        validate_result_manifest({"artifacts": []})
    with pytest.raises(PlanError):
        validate_result_manifest(
            {
                "artifacts": [
                    {
                        "id": "artifact",
                        "sha256": "not-a-digest",
                        "size": 1,
                        "media_type": "image/png",
                        "role": "output",
                    }
                ]
            }
        )
def test_result_and_closure_opaque_identifiers_are_strict_strings():
    from cloud_offload.plan_protocol import validate_closure_receipt, validate_result_manifest

    with pytest.raises(PlanError):
        validate_result_manifest(
            {
                "schema": "cloud-offload.result-manifest.v1",
                "artifacts": [
                    {
                        "id": 123,
                        "sha256": "a" * 64,
                        "size": 1,
                        "media_type": "image/png",
                        "role": "output",
                        "producer": "stage-render",
                    }
                ],
            }
        )
    with pytest.raises(PlanError):
        validate_closure_receipt(
            {"receipt_id": 123, "status": "failed", "provider_resource_absent": True}
        )


def test_result_manifest_rejects_unbounded_artifact_size():
    from cloud_offload.plan_protocol import validate_result_manifest

    with pytest.raises(PlanError):
        validate_result_manifest(
            {
                "schema": "cloud-offload.result-manifest.v1",
                "artifacts": [
                    {
                        "id": "artifact-size",
                        "sha256": "a" * 64,
                        "size": 2**63,
                        "media_type": "image/png",
                        "role": "output",
                        "producer": "stage-render",
                    }
                ],
            }
        )


def test_http_offline_cancel_has_one_job_monotonic_events_and_no_provider_mutation(tmp_path, monkeypatch):
    from cloud_offload.plan_protocol import OfflineConnector

    client, config = _client(tmp_path, monkeypatch)
    connector = OfflineConnector()
    monkeypatch.setattr(
        server.app.state,
        "plan_connector_factory",
        lambda provider, cfg: connector,
        raising=False,
    )
    value = plan()
    payload = _submit_payload(client, value, key="offline-http")
    first = client.post(
        "/api/plans", headers={"Idempotency-Key": "offline-http"}, json=payload
    )
    assert first.status_code == 202, first.text
    job_id = first.json()["job_id"]
    before = client.get(f"/api/jobs/{job_id}/events").json()
    cancelled = client.post(f"/api/jobs/{job_id}/cancel")
    assert cancelled.status_code == 200, cancelled.text
    after = client.get(f"/api/jobs/{job_id}/events").json()
    cursors = [item["sequence"] for item in after["events"]]
    assert cursors == sorted(cursors) and all(a < b for a, b in zip(cursors, cursors[1:]))
    assert len(after["events"]) > len(before["events"])
    record = PlanProtocolStore(config.queue_db_path).get(value["plan_digest"])
    assert record["state"] == "cancelled"
    assert record["closure"]["provider_resource_absent"] is True
    assert connector.launches == connector.terminations == connector.network_calls == 0


def test_plan_worker_event_replay_uses_producer_sequence_idempotency(tmp_path, monkeypatch):
    client, config = _client(tmp_path, monkeypatch)
    value = plan()
    payload = _submit_payload(client, value, key="event-replay")
    accepted = client.post(
        "/api/plans", headers={"Idempotency-Key": "event-replay"}, json=payload
    )
    assert accepted.status_code == 202, accepted.text
    job_id = accepted.json()["job_id"]
    queue = JobQueue(config.queue_db_path)
    queue.set_worker_token("worker-replay")
    event = {"type": "progress", "phase": "execution", "status": "running", "progress": 20}
    for _ in range(2):
        response = client.post(
            f"/api/workers/jobs/{job_id}/events",
            headers={"Authorization": "Bearer worker-replay"},
            json={"event": event, "producer_id": "worker:one", "producer_sequence": 7},
        )
        assert response.status_code == 200, response.text
    with sqlite3.connect(config.queue_db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM job_events WHERE job_id=?", (job_id,)).fetchone()[0] == 2


def test_plan_worker_event_drops_overflowing_metric_without_500(tmp_path, monkeypatch):
    client, config = _client(tmp_path, monkeypatch)
    value = plan()
    payload = _submit_payload(client, value, key="event-overflow")
    accepted = client.post(
        "/api/plans", headers={"Idempotency-Key": "event-overflow"}, json=payload
    )
    assert accepted.status_code == 202, accepted.text
    job_id = accepted.json()["job_id"]
    queue = JobQueue(config.queue_db_path)
    queue.set_worker_token("worker-overflow")
    response = client.post(
        f"/api/workers/jobs/{job_id}/events",
        headers={"Authorization": "Bearer worker-overflow"},
        json={
            "event": {
                "type": "progress",
                "phase": "execution",
                "status": "running",
                "metrics": {"progress": 10**1000},
            }
        },
    )
    assert response.status_code == 200, response.text


def test_plan_job_can_be_claimed_by_bound_provider_without_public_provider_name(tmp_path, monkeypatch):
    client, config = _client(tmp_path, monkeypatch)
    value = plan()
    payload = _submit_payload(client, value, key="claim-plan")
    accepted = client.post(
        "/api/plans", headers={"Idempotency-Key": "claim-plan"}, json=payload
    )
    assert accepted.status_code == 202, accepted.text
    queue = JobQueue(config.queue_db_path)
    queue.set_worker_token("worker-claim")
    claimed = queue.claim_jobs(
        "worker-1", token="worker-claim", provider="offline", models=["comfyui-plan"]
    )
    assert [job.id for job in claimed] == [accepted.json()["job_id"]]
    assert PlanProtocolStore(config.queue_db_path).get(value["plan_digest"])["state"] == "submitted"


def test_preflight_rejects_offer_with_mismatched_storage_region(tmp_path, monkeypatch):
    client, _ = _client(tmp_path, monkeypatch)

    class Connector:
        def list_available(self):
            return [{
                "id": "offer-storage",
                "provider": "offline",
                "region": "offline-test",
                "gpu_type": "test",
                "gpu_ram_gb": 1,
                "hourly_rate": 0.01,
                "storage": {"region": "wrong-region", "persistent": True},
            }]

    monkeypatch.setattr(
        server.app.state, "plan_connector_factory", lambda provider, config: Connector(), raising=False
    )
    response = client.post("/api/plans/preflight", json={"plan": plan()})
    assert response.status_code == 409


def test_preflight_rejects_integer_offer_that_overflows_float(tmp_path, monkeypatch):
    client, _ = _client(tmp_path, monkeypatch)

    class Connector:
        def list_available(self):
            return [{
                "id": "offer-overflow",
                "provider": "offline",
                "region": "offline-test",
                "gpu_type": "test",
                "gpu_ram_gb": 1,
                "hourly_rate": 10**1000,
            }]

    monkeypatch.setattr(
        server.app.state, "plan_connector_factory", lambda provider, config: Connector(), raising=False
    )
    response = client.post("/api/plans/preflight", json={"plan": plan()})
    assert response.status_code == 409


def test_preflight_hides_connector_factory_failure(tmp_path, monkeypatch):
    client, _ = _client(tmp_path, monkeypatch)

    def broken_factory(provider, config):
        raise RuntimeError("provider secret endpoint")

    monkeypatch.setattr(server.app.state, "plan_connector_factory", broken_factory, raising=False)
    response = client.post("/api/plans/preflight", json={"plan": plan()})
    assert response.status_code == 409
    assert "provider secret endpoint" not in response.text


def test_preflight_selects_a_policy_compatible_candidate_not_first_offer(tmp_path, monkeypatch):
    client, _ = _client(tmp_path, monkeypatch)

    class Connector:
        def list_available(self):
            return [
                {
                    "id": "expensive",
                    "provider": "offline",
                    "region": "offline-test",
                    "gpu_type": "test",
                    "gpu_ram_gb": 1,
                    "hourly_rate": 5.0,
                },
                {
                    "id": "cheap",
                    "provider": "offline",
                    "region": "offline-test",
                    "gpu_type": "test",
                    "gpu_ram_gb": 1,
                    "hourly_rate": 0.01,
                },
            ]

    monkeypatch.setattr(server.app.state, "plan_connector_factory", lambda provider, config: Connector(), raising=False)
    response = client.post(
        "/api/plans/preflight", json={"plan": plan(), "max_hourly_rate": 0.5}
    )
    assert response.status_code == 200, response.text
    assert response.json()["candidates"][0]["offer_id"] == "sha256:" + hashlib.sha256(canonical_bytes("cheap")).hexdigest()


def test_preflight_rejects_candidate_with_wrong_residency(tmp_path, monkeypatch):
    client, _ = _client(tmp_path, monkeypatch)
    value = plan()
    value["policy"]["residency"] = "on-prem"
    value["plan_digest"] = canonical_plan_digest(value)

    class Connector:
        def list_available(self):
            return [{
                "id": "cloud-offer",
                "provider": "offline",
                "region": "offline-test",
                "residency": "cloud",
                "gpu_type": "test",
                "gpu_ram_gb": 1,
                "hourly_rate": 0.01,
            }]

    monkeypatch.setattr(server.app.state, "plan_connector_factory", lambda provider, config: Connector(), raising=False)
    response = client.post("/api/plans/preflight", json={"plan": value})
    assert response.status_code == 409


def test_preflight_rejects_infinite_request_cost_limit(tmp_path, monkeypatch):
    from cloud_offload.server import PlanPreflightRequest

    with pytest.raises(ValueError):
        PlanPreflightRequest(plan=plan(), max_hourly_rate=float("inf"))


def test_preflight_replay_returns_exact_stored_report_without_second_offer_probe(tmp_path, monkeypatch):
    client, _ = _client(tmp_path, monkeypatch)
    calls = []

    class Connector:
        def list_available(self):
            calls.append("offer")
            return [{"id": "offer", "provider": "offline", "region": "offline-test", "gpu_type": "test", "gpu_ram_gb": 1, "hourly_rate": 0.01}]

    monkeypatch.setattr(server.app.state, "plan_connector_factory", lambda p, c: Connector(), raising=False)
    value = plan()
    one = client.post("/api/plans/preflight", json={"plan": value})
    two = client.post("/api/plans/preflight", json={"plan": value})
    assert one.status_code == two.status_code == 200
    assert one.json() == two.json()
    assert calls == ["offer"]


def test_all_plan_public_surfaces_are_allow_listed(tmp_path, monkeypatch):
    client, config = _client(tmp_path, monkeypatch)
    value = plan()
    marker = "PRIVATE_MARKER"
    value["plan_id"] = marker
    value["project_id"] = marker + "-project"
    value["input_revision"] = marker + "-revision"
    value["input_artifacts"][0].update(
        {"name": marker + "-input", "filename": marker + ".bin", "path": "D:/" + marker + ".bin"}
    )
    value["stages"][0]["id"] = marker + "-stage"
    value["stages"][0]["outputs"][0]["name"] = marker + "-output"
    value["stages"][0]["runner"]["profile"] = marker + "-profile"
    value["stages"][0]["settings"] = {
        "prompt": marker,
        "path": "D:/" + marker + "/input.safetensors",
        "token": marker,
        "signed_url": "https://private.invalid/" + marker,
    }
    value["final_outputs"][0]["stage_id"] = marker + "-stage"
    value["final_outputs"][0]["output"] = marker + "-output"
    value["plan_digest"] = canonical_plan_digest(value)
    initial_preflight = client.post("/api/plans/preflight", json={"plan": value})
    assert initial_preflight.status_code == 200, initial_preflight.text
    preflight_body = initial_preflight.json()
    payload = {
        "plan": value,
        "preflight_id": preflight_body["preflight_id"],
        "plan_digest": value["plan_digest"],
        "candidate_id": preflight_body["candidate_id"],
        "confirmation_action": "start_now",
        "client_request_id": "surface-key",
    }
    accepted = client.post(
        "/api/plans", headers={"Idempotency-Key": "surface-key"}, json=payload
    )
    assert accepted.status_code == 202, accepted.text
    job_id = accepted.json()["job_id"]
    JobQueue(config.queue_db_path).set_worker_token("worker-private")
    worker_event = client.post(
        f"/api/workers/jobs/{job_id}/events",
        headers={"Authorization": "Bearer worker-private"},
        json={"event": {"type": "bad", "prompt": marker, "provider": marker, "error": marker}},
    )
    assert worker_event.status_code == 200
    urls = [
        "/api/jobs",
        f"/api/jobs/{job_id}",
        f"/api/jobs/{job_id}/snapshot",
        f"/api/jobs/{job_id}/support-bundle",
        f"/api/jobs/{job_id}/events",
        f"/api/jobs/{job_id}/result-manifest",
        f"/api/jobs/{job_id}/visibility",
        "/api/job-visibility",
    ]
    bodies = [initial_preflight.text, worker_event.text]
    worker_status = client.get(
        f"/api/workers/jobs/{job_id}",
        headers={"Authorization": "Bearer worker-private"},
    )
    assert worker_status.status_code == 200
    bodies.append(worker_status.text)
    for url in urls[1:]:
        response = client.get(url)
        assert response.status_code == 200, (url, response.text)
        bodies.append(response.text)
    public_text = "\n".join(bodies)
    for forbidden in (marker, "private.invalid", '"operation"', '"provider":"offline"'):
        assert forbidden not in public_text
    with sqlite3.connect(config.queue_db_path) as db:
        public_rows = db.execute(
            "SELECT plan_json,preflight_json FROM cloud_plans"
        ).fetchall()
        assert public_rows
        assert all(
            not any(secret in json.dumps(row) for secret in (marker, "private.invalid"))
            for row in public_rows
        )
        event_rows = db.execute("SELECT event_json FROM job_events").fetchall()
        assert all(marker not in row[0] for row in event_rows)
        private = db.execute(
            "SELECT plan_json FROM cloud_plan_authority"
        ).fetchone()[0]
        assert marker in private


def test_concurrent_submit_same_key_has_one_job_and_one_authority(tmp_path, monkeypatch):
    client, config = _client(tmp_path, monkeypatch)
    value = plan()
    payload = _submit_payload(client, value, key="thread-key")

    def submit_once():
        return client.post(
            "/api/plans", headers={"Idempotency-Key": "thread-key"}, json=payload
        )

    with ThreadPoolExecutor(max_workers=6) as pool:
        responses = list(pool.map(lambda _: submit_once(), range(6)))
    assert all(response.status_code == 202 for response in responses)
    ids = {response.json()["job_id"] for response in responses}
    assert len(ids) == 1
    with sqlite3.connect(config.queue_db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM cloud_plans").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM cloud_plan_authority").fetchone()[0] == 1


def test_concurrent_preflight_replay_has_one_stored_identity(tmp_path, monkeypatch):
    client, _ = _client(tmp_path, monkeypatch)
    calls = []

    class Connector:
        def list_available(self):
            calls.append("offer")
            return [{
                "id": "offer-concurrent",
                "provider": "offline",
                "region": "offline-test",
                "gpu_type": "test",
                "gpu_ram_gb": 1,
                "hourly_rate": 0.01,
            }]

    monkeypatch.setattr(
        server.app.state,
        "plan_connector_factory",
        lambda provider, config: Connector(),
        raising=False,
    )
    value = plan()
    with ThreadPoolExecutor(max_workers=6) as pool:
        responses = list(
            pool.map(lambda _: client.post("/api/plans/preflight", json={"plan": value}), range(6))
        )
    assert all(response.status_code == 200 for response in responses)
    assert len({response.json()["preflight_id"] for response in responses}) == 1
    assert calls  # The injected connector was used for offers, never for launch.


def test_multiprocess_preflight_replay_has_one_exact_report(tmp_path):
    path = str(tmp_path / "process.db")
    encoded = json.dumps(plan(), sort_keys=True)
    context = multiprocessing.get_context("spawn")
    with context.Pool(3) as pool:
        reports = pool.map(_preflight_process, [(path, encoded)] * 3)
    assert reports[0] == reports[1] == reports[2]
    assert PlanProtocolStore(path).get(plan()["plan_digest"])["preflight"] == reports[0]


@pytest.mark.parametrize(
    "field",
    [
        "plan",
        "input_artifacts",
        "provider",
        "recommendation_policy",
        "max_hourly_rate",
        "max_total_job_cost",
        "allowed_regions",
        "timeout_seconds",
        "preflight_id",
        "plan_digest",
        "candidate_id",
        "confirmation_action",
        "client_request_id",
    ],
)
def test_same_key_rejects_each_changed_submit_field(tmp_path, monkeypatch, field):
    client, _ = _client(tmp_path, monkeypatch)
    value = plan()
    payload = _submit_payload(client, value, key="binding-key")
    first = client.post(
        "/api/plans", headers={"Idempotency-Key": "binding-key"}, json=payload
    )
    assert first.status_code == 202
    changed = copy.deepcopy(payload)
    if field == "plan":
        changed["plan"]["project_id"] = "changed-project"
        changed["plan"]["plan_digest"] = canonical_plan_digest(changed["plan"])
        changed["plan_digest"] = changed["plan"]["plan_digest"]
    elif field == "input_artifacts":
        changed["input_artifacts"] = {"source": "sha256:" + "c" * 64}
    elif field == "provider":
        changed["provider"] = "different-provider"
    elif field == "recommendation_policy":
        changed["recommendation_policy"] = "different-policy"
    elif field == "max_hourly_rate":
        changed["max_hourly_rate"] = 0.02
    elif field == "max_total_job_cost":
        changed["max_total_job_cost"] = 0.02
    elif field == "allowed_regions":
        changed["allowed_regions"] = ["different-region"]
    elif field == "timeout_seconds":
        changed["timeout_seconds"] = 10
    elif field == "preflight_id":
        changed["preflight_id"] = "different-preflight"
    elif field == "plan_digest":
        changed["plan_digest"] = "sha256:" + "d" * 64
    elif field == "candidate_id":
        changed["candidate_id"] = "different-candidate"
    elif field == "confirmation_action":
        changed["confirmation_action"] = "countdown_elapsed"
    else:
        changed["client_request_id"] = "different-client"
    rejected = client.post(
        "/api/plans", headers={"Idempotency-Key": "binding-key"}, json=changed
    )
    assert rejected.status_code in {400, 409}


def test_same_body_rejects_changed_idempotency_header(tmp_path, monkeypatch):
    client, _ = _client(tmp_path, monkeypatch)
    value = plan()
    payload = _submit_payload(client, value, key="header-key")
    first = client.post(
        "/api/plans", headers={"Idempotency-Key": "header-key"}, json=payload
    )
    assert first.status_code == 202
    changed = client.post(
        "/api/plans", headers={"Idempotency-Key": "changed-header"}, json=payload
    )
    assert changed.status_code == 409


def test_plan_completion_is_atomic_and_manifest_is_publicly_typed(tmp_path, monkeypatch):
    client, config = _client(tmp_path, monkeypatch)
    value = plan()
    payload = _submit_payload(client, value, key="complete-key")
    submitted = client.post(
        "/api/plans", headers={"Idempotency-Key": "complete-key"}, json=payload
    )
    job_id = submitted.json()["job_id"]
    queue = JobQueue(config.queue_db_path)
    queue.set_worker_token("worker-secret")
    malformed = client.post(
        f"/api/workers/jobs/{job_id}/complete",
        headers={"Authorization": "Bearer worker-secret"},
        json={"result": {"artifacts": []}},
    )
    assert malformed.status_code == 400
    assert PlanProtocolStore(config.queue_db_path).get(value["plan_digest"])["state"] == "submitting"
    result = {
        "schema": "cloud-offload.result-manifest.v1",
        "manifest_id": "manifest-1",
        "job_id": job_id,
        "artifacts": [
            {
                "id": "artifact-1",
                "sha256": "a" * 64,
                "size": 1,
                "media_type": "image/png",
                "role": "output",
                "producer": "stage-render",
                "job_id": job_id,
            }
        ],
    }
    completed = client.post(
        f"/api/workers/jobs/{job_id}/complete",
        headers={"Authorization": "Bearer worker-secret"},
        json={"result": result},
    )
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"
    manifest = client.get(f"/api/jobs/{job_id}/result-manifest")
    assert manifest.status_code == 200
    assert manifest.json()["result"]["artifacts"][0] == {
        "id": "artifact-1",
        "sha256": "sha256:" + "a" * 64,
        "size": 1,
        "media_type": "image/png",
        "role": "output",
        "producer": "stage-render",
        "job_id": job_id,
    }
    authority = PlanProtocolStore(config.queue_db_path).get(value["plan_digest"])
    assert authority["state"] == "completed"
    assert authority["closure"]["provider_resource_absent"] is True


def test_terminal_cancel_rejects_late_worker_running_and_result_callbacks(tmp_path, monkeypatch):
    client, config = _client(tmp_path, monkeypatch)
    value = plan()
    payload = _submit_payload(client, value, key="late-callback")
    submitted = client.post(
        "/api/plans", headers={"Idempotency-Key": "late-callback"}, json=payload
    )
    assert submitted.status_code == 202
    job_id = submitted.json()["job_id"]
    queue = JobQueue(config.queue_db_path)
    queue.set_worker_token("late-worker")
    cancelled = client.post(f"/api/jobs/{job_id}/cancel")
    assert cancelled.status_code == 200
    running = client.post(
        f"/api/workers/jobs/{job_id}/running",
        headers={"Authorization": "Bearer late-worker"},
    )
    assert running.status_code == 200
    assert running.json()["status"] == "cancelled"
    late_result = client.post(
        f"/api/workers/jobs/{job_id}/complete",
        headers={"Authorization": "Bearer late-worker"},
        json={
            "result": {
                "schema": "cloud-offload.result-manifest.v1",
                "job_id": job_id,
                "artifacts": [
                    {
                        "id": "late-artifact",
                        "sha256": "a" * 64,
                        "size": 1,
                        "media_type": "image/png",
                        "role": "output",
                        "producer": "stage-render",
                        "job_id": job_id,
                    }
                ],
            }
        },
    )
    assert late_result.status_code == 200
    assert late_result.json()["status"] == "cancelled"
    assert PlanProtocolStore(config.queue_db_path).get(value["plan_digest"])["state"] == "cancelled"


def test_baseexception_during_terminal_event_rolls_back_queue_and_closure(tmp_path, monkeypatch):
    client, config = _client(tmp_path, monkeypatch)
    value = plan()
    payload = _submit_payload(client, value, key="terminal-death")
    submitted = client.post(
        "/api/plans", headers={"Idempotency-Key": "terminal-death"}, json=payload
    )
    job_id = submitted.json()["job_id"]
    queue = JobQueue(config.queue_db_path)
    queue.set_worker_token("worker-secret")
    result = {
        "schema": "cloud-offload.result-manifest.v1",
        "manifest_id": "manifest-death",
        "job_id": job_id,
        "artifacts": [
            {
                "id": "artifact-death",
                "sha256": "b" * 64,
                "size": 1,
                "media_type": "image/png",
                "role": "output",
                "producer": "stage-render",
                "job_id": job_id,
            }
        ],
    }
    original = JobQueue._append_event_in_transaction

    def die(*args, **kwargs):
        raise BaseException("simulated process death")

    monkeypatch.setattr(JobQueue, "_append_event_in_transaction", die)
    with pytest.raises(BaseException):
        client.post(
            f"/api/workers/jobs/{job_id}/complete",
            headers={"Authorization": "Bearer worker-secret"},
            json={"result": result},
        )
    monkeypatch.setattr(JobQueue, "_append_event_in_transaction", original)
    with sqlite3.connect(config.queue_db_path) as db:
        assert db.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone() == ("queued",)
        assert db.execute("SELECT COUNT(*) FROM job_events WHERE job_id=?", (job_id,)).fetchone()[0] == 1
        assert db.execute("SELECT state,closure_json FROM cloud_plans WHERE plan_digest=?", (value["plan_digest"],)).fetchone() == ("submitting", None)


def test_lifecycle_transition_table_is_monotonic_and_terminal_is_immutable(tmp_path):
    store = PlanProtocolStore(str(tmp_path / "lifecycle.db"))
    value = plan()
    store.preflight(
        value,
        {
            "preflight_id": "life-preflight",
            "status": "ready",
            "expires_at": "2999-01-01T00:00:00Z",
            "candidates": [{"candidate_id": "life-candidate"}],
        },
    )
    assert store.sync_status(value["plan_digest"], "missing", "running") is None
    store.submit(
        plan=value,
        preflight_id="life-preflight",
        candidate_id="life-candidate",
        key="life-key",
        request_digest="life-request",
        job_id="life-job",
    )
    assert store.sync_status(value["plan_digest"], "life-job", "running")["state"] == "running"
    assert store.sync_status(value["plan_digest"], "life-job", "submitted")["state"] == "running"
    assert store.sync_status(value["plan_digest"], "life-job", "cancelling")["state"] == "cancelling"
    assert store.sync_status(value["plan_digest"], "life-job", "running")["state"] == "cancelling"
    store.close(
        value["plan_digest"],
        {"receipt_id": "life-closure", "status": "cancelled", "provider_resource_absent": True},
    )
    assert store.sync_status(value["plan_digest"], "life-job", "running")["state"] == "cancelled"
    store.close(
        value["plan_digest"],
        {"receipt_id": "other", "status": "completed", "provider_resource_absent": True},
    )
    assert store.get(value["plan_digest"])["closure"]["receipt_id"] == "life-closure"


@pytest.mark.parametrize(
    "receipt",
    [
        {"receipt_id": "missing-proof", "status": "completed"},
        {"receipt_id": "false-proof", "status": "completed", "provider_resource_absent": False},
        {"receipt_id": "free-text", "status": "completed", "provider_resource_absent": True, "unexpected": "x"},
    ],
)
def test_closure_receipts_fail_closed_without_strict_absence_proof(tmp_path, receipt):
    store = PlanProtocolStore(str(tmp_path / "closure.db"))
    value = plan()
    store.preflight(
        value,
        {
            "preflight_id": "closure-preflight",
            "status": "ready",
            "expires_at": "2999-01-01T00:00:00Z",
            "candidates": [{"candidate_id": "closure-candidate"}],
        },
    )
    store.submit(
        plan=value,
        preflight_id="closure-preflight",
        candidate_id="closure-candidate",
        key="closure-key",
        request_digest="closure-request",
        job_id="closure-job",
    )
    with pytest.raises(PlanError):
        store.close(value["plan_digest"], receipt)


@pytest.mark.parametrize(
    "artifact_change",
    [
        {"size": 0},
        {"size": True},
        {"media_type": "not media"},
        {"role": "private"},
        {"producer": ""},
        {"job_id": "wrong-job"},
    ],
)
def test_result_manifest_rejects_each_unsafe_artifact_field(artifact_change):
    from cloud_offload.plan_protocol import validate_result_manifest

    artifact = {
        "id": "typed-artifact",
        "sha256": "a" * 64,
        "size": 1,
        "media_type": "image/png",
        "role": "output",
        "producer": "stage-render",
        "job_id": "job-1",
    }
    artifact.update(artifact_change)
    with pytest.raises(PlanError):
        validate_result_manifest(
            {"schema": "cloud-offload.result-manifest.v1", "job_id": "job-1", "artifacts": [artifact]},
            expected_job_id="job-1",
        )


def test_offline_http_proof_hash_matches_independent_facts(tmp_path, monkeypatch):
    from cloud_offload.plan_protocol import OfflineConnector

    client, config = _client(tmp_path, monkeypatch)
    connector = OfflineConnector()
    monkeypatch.setattr(
        server.app.state,
        "plan_connector_factory",
        lambda provider, cfg: connector,
        raising=False,
    )
    value = plan()
    payload = _submit_payload(client, value, key="proof-key")
    accepted = client.post(
        "/api/plans", headers={"Idempotency-Key": "proof-key"}, json=payload
    )
    assert accepted.status_code == 202
    job_id = accepted.json()["job_id"]
    events_before = client.get(f"/api/jobs/{job_id}/events").json()
    cancelled = client.post(f"/api/jobs/{job_id}/cancel")
    assert cancelled.status_code == 200
    events_after = client.get(f"/api/jobs/{job_id}/events").json()
    # The deterministic proof facts are independent of UUIDs and timestamps.
    facts = {
        "job_count": 1,
        "accepted_submit_count": 1,
        "closure_count": 1,
        "cursor": [item["sequence"] for item in events_after["events"]],
        "before_cursor": events_before["next_after"],
        "provider_launches": connector.launches,
        "provider_terminations": connector.terminations,
        "network_calls": connector.network_calls,
    }
    proof_hash = "sha256:" + hashlib.sha256(canonical_bytes(facts)).hexdigest()
    assert proof_hash == "sha256:91082b5aef02e454b368fc0779398ea2affe92bbcd0db19a35c749e409b3f8da"
    assert facts["cursor"] == [1, 2, 3]
    assert PlanProtocolStore(config.queue_db_path).get(value["plan_digest"])["closure"]["provider_resource_absent"] is True


def test_incompatible_legacy_plan_tables_fail_closed_without_public_leak(tmp_path, monkeypatch):
    path = str(tmp_path / "legacy.db")
    marker = "legacy-private-prompt"
    with sqlite3.connect(path) as db:
        db.execute(
            "CREATE TABLE cloud_plans (plan_digest TEXT PRIMARY KEY, plan_json TEXT NOT NULL, preflight_json TEXT NOT NULL, job_id TEXT, idempotency_key TEXT UNIQUE, request_digest TEXT, state TEXT NOT NULL, closure_json TEXT)"
        )
        db.execute(
            "INSERT INTO cloud_plans VALUES (?, ?, ?, NULL, NULL, NULL, 'preflighted', NULL)",
            ("sha256:" + "a" * 64, json.dumps({"prompt": marker}), json.dumps({"expires_at": "2999-01-01T00:00:00Z"})),
        )
    with pytest.raises(PlanError, match="incompatible"):
        PlanProtocolStore(path)
    with sqlite3.connect(path) as db:
        assert marker in db.execute("SELECT plan_json FROM cloud_plans").fetchone()[0]
        assert db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='cloud_plan_authority'"
        ).fetchone() is None

    config = CloudConfig(queue_db_path=path)
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    client = TestClient(server.app, raise_server_exceptions=False)
    response = client.post("/api/plans/preflight", json={"plan": plan()})
    assert response.status_code == 500
    assert marker not in response.text
    listing = client.get("/api/jobs")
    assert listing.status_code == 200
    assert marker not in listing.text


def test_incompatible_legacy_authority_table_fails_closed(tmp_path):
    path = str(tmp_path / "legacy-authority.db")
    with sqlite3.connect(path) as db:
        db.execute(
            "CREATE TABLE cloud_plan_authority (plan_digest TEXT PRIMARY KEY, plan_json TEXT NOT NULL, preflight_json TEXT NOT NULL, request_json TEXT)"
        )
        db.execute(
            "INSERT INTO cloud_plan_authority VALUES (?, ?, ?, ?)",
            ("sha256:" + "b" * 64, json.dumps({"path": "C:/private"}), "{}", "{}"),
        )
    with pytest.raises(PlanError, match="incompatible"):
        PlanProtocolStore(path)


def test_wrong_plan_authority_schema_version_fails_closed(tmp_path):
    path = str(tmp_path / "wrong-version.db")
    PlanProtocolStore(path)
    with sqlite3.connect(path) as db:
        db.execute(
            "INSERT INTO cloud_plans(plan_digest, plan_json, preflight_json, state, schema_version) VALUES (?, '{}', '{}', 'preflighted', 'cloud-offload.plan-authority.v0')",
            ("sha256:" + "c" * 64,),
        )
    with pytest.raises(PlanError, match="version"):
        PlanProtocolStore(path)


def test_direct_queue_submit_rejects_hostile_public_projection_before_mutation(tmp_path, monkeypatch):
    client, config = _client(tmp_path, monkeypatch)
    value = plan()
    payload = _submit_payload(client, value, key="hostile-boundary")
    store = PlanProtocolStore(config.queue_db_path)
    record = store.get(value["plan_digest"])
    private = store.private(value["plan_digest"])
    assert record is not None and private is not None
    hostile = copy.deepcopy(record["plan"])
    hostile["operation"] = "private-operation"
    hostile["path"] = "C:/private/prompt"
    queue = JobQueue(config.queue_db_path)
    with pytest.raises(PlanError, match="public plan projection"):
        queue.submit_plan_atomic(
            plan_digest=value["plan_digest"],
            preflight_id=payload["preflight_id"],
            candidate_id=payload["candidate_id"],
            idempotency_key=payload["client_request_id"],
            request_digest=binding_digest({"request": "hostile-boundary"}),
            job_id="hostile-job",
            plan_public=hostile,
            preflight_public=record["preflight"],
            request_binding={"safe": True},
            provider_digest=private["provider_digest"],
            candidate_digest=private["candidate_digest"],
            input_digest=private["input_digest"],
            input_artifacts={},
        )
    with sqlite3.connect(config.queue_db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0
        assert db.execute(
            "SELECT state, job_id FROM cloud_plans WHERE plan_digest=?",
            (value["plan_digest"],),
        ).fetchone() == ("preflighted", None)

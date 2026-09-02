"""Offline end-to-end proof: no provider launch, termination, or network."""

from cloud_offload.plan_protocol import OfflineConnector, PlanProtocolStore
from tests.test_plan_protocol import plan


def test_loopback_coordinator_has_one_submit_and_closure(tmp_path):
    connector = OfflineConnector()
    store = PlanProtocolStore(str(tmp_path / "coordinator.db"))
    value = plan()
    store.preflight(value, {"preflight_id": "pf", "status": "ready", "expires_at": "2999-01-01T00:00:00Z", "candidates": [{"candidate_id": "c"}]})
    assert store.preflight(value, {"preflight_id": "different", "status": "ready", "expires_at": "2999-01-01T00:00:00Z", "candidates": [{"candidate_id": "c"}]})["preflight_id"] == "pf"
    job_id, replay = store.submit(plan=value, preflight_id="pf", candidate_id="c", key="request-1", request_digest="d", job_id="job-1")
    assert job_id == "job-1" and replay is None
    replay_id, _ = store.submit(plan=value, preflight_id="pf", candidate_id="c", key="request-1", request_digest="d", job_id="job-2")
    assert replay_id == "job-1"
    store.close(value["plan_digest"], {"receipt_id": "receipt-1", "provider": "offline", "terminated": True})
    record = store.get(value["plan_digest"])
    assert record["state"] == "terminal"
    assert record["closure"]["receipt_id"] == "receipt-1"
    assert connector.launches == connector.terminations == connector.network_calls == 0


def test_cancellation_and_unknown_submit_are_monotonic(tmp_path):
    store = PlanProtocolStore(str(tmp_path / "coordinator.db"))
    value = plan()
    store.preflight(value, {"preflight_id": "pf", "status": "ready", "expires_at": "2999-01-01T00:00:00Z", "candidates": [{"candidate_id": "c"}]})
    store.submit(plan=value, preflight_id="pf", candidate_id="c", key="request-1", request_digest="d", job_id="job-1")
    assert store.reconcile_unknown_submit(value["plan_digest"])["state"] == "submitted"
    assert store.cancel(value["plan_digest"])["state"] == "cancelling"
    store.close(value["plan_digest"], {"receipt_id": "r", "provider_resource_absent": True})
    assert store.cancel(value["plan_digest"])["state"] == "terminal"

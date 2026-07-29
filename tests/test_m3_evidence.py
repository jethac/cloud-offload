import json
import math
from pathlib import Path


EVIDENCE_FILE = (
    Path(__file__).parents[1]
    / "docs"
    / "evidence"
    / "m3-lease-closure-evidence-2026-07-30.json"
)

FORBIDDEN_KEYS = {
    "asset_path",
    "asset_paths",
    "credential",
    "credentials",
    "endpoint",
    "hook_argv",
    "path",
    "prompt",
    "request",
    "stderr",
    "stdout",
    "workflow",
}

FORBIDDEN_VALUE_FRAGMENTS = (
    ".runlogs",
    "authorization:",
    "bearer ",
    "file://",
    "hf_token",
    "runpod_api_key",
    "s3://",
)

EXPECTED_PHASES = {
    "provisioning",
    "worker_boot",
    "dependency_preparation",
    "execution",
    "result_transfer",
}


def _walk(value):
    yield value
    if isinstance(value, dict):
        for item in value.values():
            yield from _walk(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk(item)


def test_m3_evidence_is_complete_finite_and_redacted():
    evidence = json.loads(EVIDENCE_FILE.read_text(encoding="utf-8"))

    assert evidence["schema"] == "cloud-offload.m3-production-evidence.v1"
    assert evidence["accepted"] is True
    automated = evidence["automated_verification"]
    assert set(automated["cancellation_phases"]) == EXPECTED_PHASES
    assert automated["stopped_resource_is_not_closure"] is True
    assert automated["late_callback_terminal_precedence"] is True
    assert automated["cancelled_cache_publication_blocked"] is True

    campaign = evidence["paid_runpod_campaign"]
    assert campaign["accepted"] is True
    assert (
        campaign["estimated_compute_cost_upper_usd"]
        <= campaign["spend_limit_usd"]
    )
    assert campaign["manual_cleanup"] is False
    assert campaign["final_provider_resource_count"] == 0
    assert campaign["orphan_audit_error_count"] == 0
    assert {item["kind"] for item in campaign["scenarios"]} == {
        "worker_boot_cancellation",
        "coordinator_restart_after_provider_creation",
    }
    assert all(item["accepted"] is True for item in campaign["scenarios"])
    assert all(item["provider_absent"] is True for item in campaign["scenarios"])
    assert all(
        item["provider_termination_confirmed"] is True
        for item in campaign["scenarios"]
    )
    assert evidence["m3_exit_audit"]["all_exits_passed"] is True

    for mapping in (value for value in _walk(evidence) if isinstance(value, dict)):
        assert not (set(map(str.casefold, mapping)) & FORBIDDEN_KEYS)

    for value in _walk(evidence):
        if isinstance(value, float):
            assert math.isfinite(value)
        if isinstance(value, str):
            lowered = value.casefold()
            assert not any(fragment in lowered for fragment in FORBIDDEN_VALUE_FRAGMENTS)

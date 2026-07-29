import json
import math
from pathlib import Path


EVIDENCE_FILE = (
    Path(__file__).parents[1]
    / "docs"
    / "evidence"
    / "m5-workflow-capsule-evidence-2026-07-30.json"
)

FORBIDDEN_KEYS = {
    "asset_path",
    "asset_paths",
    "credential",
    "credentials",
    "endpoint",
    "job_id",
    "path",
    "prompt",
    "provider_resource_id",
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

EXPECTED_RUNTIME_ARTIFACTS = {
    "custom-node-bundle": 419840,
    "environment-bundle": 3420160,
}


def _walk(value):
    yield value
    if isinstance(value, dict):
        for item in value.values():
            yield from _walk(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk(item)


def test_m5_evidence_is_complete_finite_and_redacted():
    evidence = json.loads(EVIDENCE_FILE.read_text(encoding="utf-8"))

    assert evidence["schema"] == "cloud-offload.m5-production-evidence.v1"
    assert evidence["accepted"] is True

    preflight = evidence["free_restore_preflight"]
    assert preflight["accepted"] is True
    assert preflight["status"] == "ready"
    assert preflight["provider_mutation"] is False
    assert preflight["prepared_volume"] is True
    assert preflight["preparation_complete"] is True
    assert preflight["coverage_percent"] == 100.0

    campaign = evidence["paid_runpod_campaign"]
    assert campaign["accepted"] is True
    assert campaign["manual_cleanup"] is False
    assert campaign["final_provider_resource_count"] == 0
    assert campaign["orphan_audit_error_count"] == 0

    incident = campaign["bounded_incident"]
    assert incident["accepted_as_success"] is False
    assert incident["provider_absent"] is True
    assert incident["orphan_count"] == 0
    assert (
        incident["estimated_compute_cost_upper_usd"]
        <= campaign["scenario_spend_limit_usd"]
    )

    population = campaign["population"]
    restore = campaign["restore"]
    assert population["accepted"] is True
    assert restore["accepted"] is True
    assert population["fresh_instance"] is True
    assert restore["fresh_instance"] is True
    assert population["provider_absent"] is True
    assert restore["provider_absent"] is True
    assert population["orphan_count"] == 0
    assert restore["orphan_count"] == 0
    assert population["population_count"] == 2
    assert population["cache_hit_count"] == 0
    assert restore["population_count"] == 0
    assert restore["cache_hit_count"] == 2
    assert restore["restored_before_claim_count"] == 2
    assert restore["receipt_artifact_count"] == 2

    populated = {item["kind"]: item["bytes"] for item in population["artifacts"]}
    restored = {item["kind"]: item["bytes"] for item in restore["artifacts"]}
    assert populated == EXPECTED_RUNTIME_ARTIFACTS
    assert restored == EXPECTED_RUNTIME_ARTIFACTS
    assert all(item["verification_mode"] == "full_digest" for item in restore["artifacts"])
    assert all(item["verification_bytes"] == item["bytes"] for item in restore["artifacts"])
    assert all(item["restored_before_claim"] is True for item in restore["artifacts"])

    result = restore["workflow_result"]
    assert result["schema"] == "comfy.workflow.result.v1"
    assert result["artifact_count"] == 1
    assert result["output_count"] == 1
    assert result["digest_present"] is True
    assert evidence["m5_exit_audit"]["all_exits_passed"] is True

    for mapping in (value for value in _walk(evidence) if isinstance(value, dict)):
        assert not (set(map(str.casefold, mapping)) & FORBIDDEN_KEYS)

    for value in _walk(evidence):
        if isinstance(value, float):
            assert math.isfinite(value)
        if isinstance(value, str):
            lowered = value.casefold()
            assert not any(fragment in lowered for fragment in FORBIDDEN_VALUE_FRAGMENTS)

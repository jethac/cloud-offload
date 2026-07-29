import json
import math
from pathlib import Path


EVIDENCE_FILE = (
    Path(__file__).parents[1]
    / "docs"
    / "evidence"
    / "m1-production-evidence-2026-07-30.json"
)

FORBIDDEN_KEYS = {
    "asset_path",
    "asset_paths",
    "credential",
    "credentials",
    "endpoint",
    "path",
    "prompt",
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


def _walk(value):
    yield value
    if isinstance(value, dict):
        for item in value.values():
            yield from _walk(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk(item)


def test_m1_evidence_is_complete_finite_and_redacted():
    evidence = json.loads(EVIDENCE_FILE.read_text(encoding="utf-8"))

    assert evidence["schema"] == "cloud-offload.m1-production-evidence.v1"
    free = evidence["free_preflight"]
    assert free["accepted"] is True
    assert free["repeat_count"] == 2
    assert free["manifest_digest_stable"] is True
    assert free["workload_digest_stable"] is True
    assert free["job_count_unchanged"] is True
    assert free["provider_resource_count_unchanged"] is True
    assert free["provider_resource_identity_unchanged"] is True
    assert free["unknowns"] == []

    recommendation = evidence["recommendation"]
    assert recommendation["history"]["matched_completed_jobs"] == 2
    assert recommendation["history"]["confidence"] == "medium"
    estimate = recommendation["estimate"]
    assert estimate["paid_idle_seconds"] == 300
    assert estimate["incremental_transfer_cost_usd"] == [0.0, 0.0]
    assert estimate["cost_complete"] is True
    for index in (0, 1):
        expected = sum(
            values[index]
            for values in (
                estimate["compute_cost_usd"],
                estimate["incremental_transfer_cost_usd"],
                estimate["incremental_container_storage_cost_usd"],
            )
        )
        assert abs(estimate["total_job_cost_usd"][index] - expected) < 0.000002

    paid = evidence["accepted_paid_journey"]
    assert paid["accepted"] is True
    assert paid["cleanup"]["automatic_termination"] is True
    assert paid["cleanup"]["manual_cleanup"] is False
    assert paid["cleanup"]["provider_absent"] is True
    assert evidence["m1_exit_audit"]["all_exits_passed"] is True
    assert all(evidence["verification"].values())

    for mapping in (value for value in _walk(evidence) if isinstance(value, dict)):
        assert not (set(map(str.casefold, mapping)) & FORBIDDEN_KEYS)

    for value in _walk(evidence):
        if isinstance(value, float):
            assert math.isfinite(value)
        if isinstance(value, str):
            lowered = value.casefold()
            assert not any(
                fragment in lowered for fragment in FORBIDDEN_VALUE_FRAGMENTS
            )

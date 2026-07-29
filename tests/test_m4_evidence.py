import json
import math
import statistics
from pathlib import Path


EVIDENCE_FILE = (
    Path(__file__).parents[1]
    / "docs"
    / "evidence"
    / "m4-fast-restore-evidence-2026-07-30.json"
)

FORBIDDEN_KEYS = {
    "asset_path",
    "asset_paths",
    "credential",
    "credentials",
    "endpoint",
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


def _walk(value):
    yield value
    if isinstance(value, dict):
        for item in value.values():
            yield from _walk(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk(item)


def test_m4_evidence_is_complete_finite_and_redacted():
    evidence = json.loads(EVIDENCE_FILE.read_text(encoding="utf-8"))

    assert evidence["schema"] == "cloud-offload.m4-production-evidence.v1"
    assert evidence["accepted"] is True
    automated = evidence["automated_verification"]
    assert automated["background_selected_corruption_detected"] is True
    assert automated["corrupt_materialized_target_removed_before_return"] is True
    assert automated["corrupt_artifact_quarantined"] is True
    assert automated["private_and_audit_due_assets_use_full_digest"] is True

    campaign = evidence["paid_runpod_preparation_campaign"]
    assert campaign["preparation_evidence_accepted"] is True
    assert campaign["estimated_compute_cost_upper_usd"] <= campaign["spend_limit_usd"]
    assert campaign["manual_cleanup"] is False
    assert campaign["provider_absence_receipt_count"] == campaign["scenario_count"]
    assert campaign["final_provider_resource_count"] == 0
    assert campaign["orphan_audit_error_count"] == 0

    cold_median = statistics.median(campaign["cold_preparation_seconds"])
    hot_median = statistics.median(campaign["hot_preparation_seconds"])
    assert math.isclose(cold_median, campaign["cold_median_seconds"], abs_tol=1e-6)
    assert math.isclose(hot_median, campaign["hot_median_seconds"], abs_tol=1e-6)
    assert hot_median / cold_median <= 0.25

    for observation in campaign["fast_path_observations"]:
        assert observation["preparation_completed"] is True
        assert observation["provider_absent"] is True
        assert observation["full_digest_hits"] == 0
        assert observation["trusted_hits"] == observation["artifact_hits"]
        assert observation["background_scrub_hits"] == observation["artifact_hits"]
        assert observation["verification_bytes"] < observation["artifact_bytes"]

    first_seen = campaign["receipt_issue_observation"]
    assert first_seen["first_seen_integrity_preserved"] is True
    assert first_seen["full_digest_hits"] == first_seen["artifact_hits"]
    assert first_seen["verification_bytes"] == first_seen["artifact_bytes"]
    assert evidence["m4_exit_audit"]["all_exits_passed"] is True

    for mapping in (value for value in _walk(evidence) if isinstance(value, dict)):
        assert not (set(map(str.casefold, mapping)) & FORBIDDEN_KEYS)

    for value in _walk(evidence):
        if isinstance(value, float):
            assert math.isfinite(value)
        if isinstance(value, str):
            lowered = value.casefold()
            assert not any(fragment in lowered for fragment in FORBIDDEN_VALUE_FRAGMENTS)

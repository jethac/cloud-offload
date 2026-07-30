import json
import math
from pathlib import Path


EVIDENCE_FILE = (
    Path(__file__).parents[1]
    / "docs"
    / "evidence"
    / "m6-regional-replication-evidence-2026-07-30.json"
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
    "provider_volume_id",
    "recommendation_id",
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


def test_m6_evidence_is_complete_finite_and_redacted():
    evidence = json.loads(EVIDENCE_FILE.read_text(encoding="utf-8"))

    assert evidence["schema"] == "cloud-offload.m6-production-evidence.v1"
    assert evidence["accepted"] is True
    assert evidence["automated_verification"]["backend"] == {
        "passed": 649,
        "failed": 0,
    }

    campaign = evidence["paid_demand_campaign"]
    assert campaign["accepted"] is True
    assert campaign["positive_byte_observation_count"] == 6
    assert campaign["missing_bytes_per_accepted_observation"] == 3_840_000
    assert campaign["final_active_gpu_count"] == 0
    assert campaign["orphan_count"] == 0
    assert {item["region"] for item in campaign["regions"]} == {
        "EU-RO-1",
        "EUR-IS-1",
        "US-GA-2",
    }
    assert all(item["accepted_observations"] == 2 for item in campaign["regions"])
    assert all(
        cost <= campaign["scenario_spend_limit_usd"]
        for item in campaign["regions"]
        for cost in item["accepted_run_costs_usd"]
    )
    assert all(
        incident["provider_absent"] is True
        and incident["orphan_count"] == 0
        for incident in campaign["bounded_incidents"]
    )

    accuracy = evidence["shadow_accuracy"]
    assert accuracy["provider_mutation"] is False
    assert accuracy["unique_recommendations"] == 3
    assert accuracy["mature_recommendations"] == 3
    assert accuracy["validated_recommendations"] == 3
    assert accuracy["precision"] == 1.0
    assert accuracy["automation_gate_passed"] is True

    automatic = evidence["automatic_replication"]
    assert automatic["accepted"] is True
    assert automatic["completed_copy_count"] == 2
    assert automatic["bytes_per_completed_copy"] == 3_840_000
    assert automatic["artifact_count_per_completed_copy"] == 2
    assert automatic["peak_incremental_monthly_storage_cost_usd"] <= automatic[
        "replication_monthly_budget_usd"
    ]
    assert automatic["provider_gpu_mutation"] is False
    assert {item["kind"]: item["bytes"] for item in automatic["artifacts"]} == (
        EXPECTED_RUNTIME_ARTIFACTS
    )

    repeat = evidence["repeat_cycle"]
    assert repeat["new_action_created"] is False
    assert repeat["completed_actions_before"] == repeat["completed_actions_after"]
    assert repeat["provider_gpu_mutation"] is False

    placement = evidence["prepared_placement_before_loss"]
    assert placement["prepared_recommended"] is True
    assert placement["complete_prepared_candidate_count"] > 0
    assert placement["cold_fallback_candidate_count"] > 0

    loss = evidence["regional_loss_recovery"]
    assert loss["accepted"] is True
    assert loss["lost_target_count"] == 1
    assert loss["lost_action_count"] == 1
    assert loss["ready_target_count"] == 0
    assert loss["source_state_deleted"] is False
    assert loss["post_loss_prepared_candidate_count"] == 0
    assert loss["post_loss_cold_fallback_candidate_count"] > 0

    expiry = evidence["ttl_expiry"]
    assert expiry["accepted"] is True
    assert expiry["method"] == "controlled_clock_injection"
    assert expiry["original_ttl_days"] == 1
    assert expiry["expired_action_count"] == 1
    assert expiry["failure_count"] == 0
    assert expiry["deleted_empty_target_count"] == 1
    assert expiry["source_state_deleted"] is False
    assert expiry["source_manifest_present"] is True
    assert expiry["source_artifact_count"] == 2
    assert expiry["source_bytes"] == 3_840_000

    final = evidence["final_state"]
    assert final["saved_configuration_restored"] is True
    assert final["replication_mode"] == "shadow"
    assert final["active_runpod_gpu_count"] == 0
    assert final["automatic_test_volume_count"] == 0
    assert final["orphan_count"] == 0
    assert evidence["m6_exit_audit"]["all_exits_passed"] is True

    for mapping in (value for value in _walk(evidence) if isinstance(value, dict)):
        assert not (set(map(str.casefold, mapping)) & FORBIDDEN_KEYS)

    for value in _walk(evidence):
        if isinstance(value, float):
            assert math.isfinite(value)
        if isinstance(value, str):
            lowered = value.casefold()
            assert not any(fragment in lowered for fragment in FORBIDDEN_VALUE_FRAGMENTS)

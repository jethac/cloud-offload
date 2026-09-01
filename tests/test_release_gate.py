import json
import sys
import hashlib
from pathlib import Path

import pytest

from cloud_offload.benchmark import PLAN_SCHEMA, SCORECARD_SCHEMA
from cloud_offload.release_gate import (
    RELEASE_MATRIX_SCHEMA,
    RELEASE_PLAN_SCHEMA,
    REQUIRED_CANARIES,
    ReleasePlan,
    _scorecard_receipt,
    new_ledger,
    update_ledger,
)


REVISION = "a" * 40
IMAGE_DIGEST = "sha256:" + "b" * 64


def benchmark_plan(region="US-MD-1"):
    base_request = {
        "partition": {
            "schema": "cloud-offload.partition.v1",
            "partition_id": "private-partition",
            "runner": {"profile": "comfyui-partition-v1"},
            "workflow": {"private": "workflow"},
        },
        "private_prompt": "must-not-enter-safe-summary",
        "force_execution": True,
    }

    def selected(name, cache_state, **extra):
        return {
            "name": name,
            "cache_state": cache_state,
            "endpoint": "/api/partitions",
            "request": json.loads(json.dumps(base_request)),
            "timeout_seconds": 300,
            "expected_statuses": ["completed", "failed", "dead_letter"],
            "fresh_instance": True,
            "allowed_regions": [region],
            **extra,
        }

    return {
        "schema": PLAN_SCHEMA,
        "providers": ["runpod"],
        "exclusive": True,
        "limits": {
            "max_total_cost_usd": 0.5,
            "max_scenario_cost_usd": 0.1,
            "max_campaign_seconds": 1800,
            "cleanup_timeout_seconds": 30,
        },
        "scenarios": [
            selected("cold", "cold", prepared_storage_policy="off"),
            selected("hot", "hot", prepared_storage_policy="smart"),
            selected(
                "cancel",
                "failure",
                failure={"kind": "cancellation", "after_seconds": 1},
            ),
            selected(
                "provider",
                "failure",
                failure={"kind": "provider", "after_seconds": 1},
            ),
            selected(
                "storage",
                "failure",
                failure={
                    "kind": "storage",
                    "hook_argv": ["cloud-offload", "benchmark-hook", "storage"],
                },
            ),
            selected(
                "corruption",
                "failure",
                failure={
                    "kind": "corruption",
                    "before_submit": True,
                    "hook_argv": [
                        "cloud-offload",
                        "benchmark-hook",
                        "corruption",
                    ],
                },
            ),
            selected(
                "restart",
                "failure",
                failure={
                    "kind": "restart",
                    "hook_argv": ["cloud-offload", "benchmark-hook", "restart"],
                },
            ),
        ],
    }


def release_plan(tmp_path, *, regions=None, required=30):
    regions = regions or ["US-MD-1"]
    cases = []
    for region in regions:
        benchmark_path = tmp_path / f"benchmark-{region}.json"
        benchmark_path.write_text(
            json.dumps(benchmark_plan(region)), encoding="utf-8"
        )
        cases.append(
            {
                "name": f"comfyui-{region.lower()}",
                "profile": "comfyui",
                "region": region,
                "benchmark_plan": str(benchmark_path),
            }
        )
    value = {
        "schema": RELEASE_PLAN_SCHEMA,
        "required_consecutive_matrices": required,
        "repositories": [
            {"name": "backend", "path": str(tmp_path), "revision": REVISION},
            {"name": "extension", "path": str(tmp_path), "revision": REVISION},
        ],
        "profiles": [{"name": "comfyui", "image_digest": IMAGE_DIGEST}],
        "regions": regions,
        "cases": cases,
        "limits": {
            "max_total_cost_usd": 10,
            "max_matrix_cost_usd": 0.5,
            "max_total_seconds": 100000,
            "max_matrix_seconds": 2200,
            "contract_test_timeout_seconds": 300,
            "cancellation_slo_seconds": 90,
            "provider_closure_slo_seconds": 90,
            "reload_slo_seconds": 2,
            "hot_preparation_ratio_max": 0.25,
            "max_monthly_storage_cost_usd": 10,
        },
    }
    path = tmp_path / "release.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    return path, value


def test_release_plan_requires_full_axes_and_redacts_private_benchmark(tmp_path):
    path, _ = release_plan(tmp_path, regions=["US-MD-1", "EU-RO-1"])
    plan = ReleasePlan.load(path)

    assert plan.required_consecutive_matrices == 30
    assert {(item.profile, item.region) for item in plan.cases} == {
        ("comfyui", "US-MD-1"),
        ("comfyui", "EU-RO-1"),
    }
    safe = json.dumps(plan.safe_summary(), sort_keys=True)
    assert "must-not-enter-safe-summary" not in safe
    assert "workflow" not in safe
    assert all(item.benchmark_plan_digest.startswith("sha256:") for item in plan.cases)
    assert ReleasePlan.load(path).digest == plan.digest


def test_release_plan_binds_digest_to_the_same_bytes_it_parses(tmp_path, monkeypatch):
    path, _ = release_plan(tmp_path)
    benchmark_path = tmp_path / "benchmark-US-MD-1.json"
    original = benchmark_path.read_bytes()
    mutated = original.replace(b'"cold"', b'"hot"', 1)
    real_read = Path.read_bytes
    reads = {"benchmark": 0}

    def read_once(target):
        raw = real_read(target)
        if target == benchmark_path and reads["benchmark"] == 0:
            reads["benchmark"] += 1
            target.write_bytes(mutated)
        return raw

    monkeypatch.setattr(Path, "read_bytes", read_once)
    plan = ReleasePlan.load(path)
    assert plan.cases[0].benchmark_plan.scenarios[0].cache_state == "cold"
    assert plan.cases[0].benchmark_plan_digest == (
        "sha256:" + hashlib.sha256(original).hexdigest()
    )
    assert reads["benchmark"] == 1


def test_release_plan_rejects_short_gate_and_unbound_region(tmp_path):
    path, value = release_plan(tmp_path, required=29)
    with pytest.raises(ValueError, match="at least 30"):
        ReleasePlan.load(path)

    value["required_consecutive_matrices"] = 30
    benchmark_path = tmp_path / "benchmark-US-MD-1.json"
    broken = benchmark_plan()
    broken["scenarios"][0]["allowed_regions"] = []
    benchmark_path.write_text(json.dumps(broken), encoding="utf-8")
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValueError, match="bind every scenario"):
        ReleasePlan.load(path)


def matrix_receipt(index, case, passed=True):
    return {
        "schema": RELEASE_MATRIX_SCHEMA,
        "index": index,
        "case": case.name,
        "profile": case.profile,
        "region": case.region,
        "passed": passed,
        "duration_seconds": 10,
        "estimated_compute_cost_upper_usd": 0.01,
    }


def test_ledger_requires_trailing_passes_and_axis_coverage(tmp_path):
    path, _ = release_plan(tmp_path, regions=["US-MD-1", "EU-RO-1"])
    plan = ReleasePlan.load(path)
    ledger = new_ledger(plan)
    for index in range(1, 31):
        ledger["matrices"].append(
            matrix_receipt(index, plan.cases[(index - 1) % len(plan.cases)])
        )
    update_ledger(ledger, plan)

    assert ledger["consecutive_passes"] == 30
    assert ledger["passed"] is True
    assert ledger["coverage"]["regions"] == ["EU-RO-1", "US-MD-1"]

    ledger["matrices"].append(matrix_receipt(31, plan.cases[0], passed=False))
    update_ledger(ledger, plan)
    assert ledger["consecutive_passes"] == 0
    assert ledger["passed"] is False


def release_scorecard(case):
    scenario_kinds = {
        "cold": None,
        "hot": None,
        "cancel": "cancellation",
        "provider": "provider",
        "storage": "storage",
        "corruption": "corruption",
        "restart": "restart",
    }
    results = []
    for name, failure_kind in scenario_kinds.items():
        cache_state = name if name in {"cold", "hot"} else "failure"
        events = (
            [{"sequence": 1, "type": "cache_artifact_quarantined"}]
            if name == "corruption"
            else [{"sequence": 1, "type": "job_status_changed"}]
        )
        results.append(
            {
                "name": name,
                "cache_state": cache_state,
                "passed": True,
                "preparation_seconds": 20 if name == "hot" else 100 if name == "cold" else None,
                "resource_closure_seconds": 5,
                "cancellation_to_provider_absence_seconds": (
                    8 if name == "cancel" else None
                ),
                "submission_receipt": {
                    "profile": case.profile,
                    "image_digest": case.image_digest,
                    "region": case.region,
                    "allowed_regions": [case.region],
                    "cold_fallback_available": name == "cold",
                },
                "support_bundle": {
                    "schema": "cloud-offload.support-bundle.v1",
                    "events": events,
                },
            }
        )
    return {
        "schema": SCORECARD_SCHEMA,
        "passed": True,
        "estimated_compute_cost_upper_usd": 0.2,
        "orphaned_resources": [],
        "final_audit_error": None,
        "plan": {
            "scenarios": [
                {"name": name, "failure_kind": failure_kind}
                for name, failure_kind in scenario_kinds.items()
            ]
        },
        "results": results,
    }


def test_scorecard_receipt_enforces_all_m7_canaries_and_slos(tmp_path):
    path, _ = release_plan(tmp_path)
    plan = ReleasePlan.load(path)
    case = plan.cases[0]
    scorecard = release_scorecard(case)
    receipt = _scorecard_receipt(
        scorecard,
        case,
        plan.limits,
        {"passed": True},
        {"passed": True},
    )

    assert receipt["passed"] is True
    assert set(receipt["canaries"]) == REQUIRED_CANARIES
    assert receipt["hot_preparation_ratio"] == 0.2

    scorecard["results"][1]["preparation_seconds"] = 30
    scorecard["results"][0]["submission_receipt"]["region"] = "wrong"
    failed = _scorecard_receipt(
        scorecard,
        case,
        plan.limits,
        {"passed": True},
        {"passed": True},
    )
    assert failed["passed"] is False
    assert "hot_acceleration_target" in failed["failure_codes"]
    assert "image_region_receipt" in failed["failure_codes"]


def test_release_cli_validation_is_safe_and_run_requires_spend_confirmation(
    tmp_path, monkeypatch, capsys
):
    path, _ = release_plan(tmp_path)
    from cloud_offload.__main__ import main

    monkeypatch.setattr(
        sys, "argv", ["cloud-offload", "release", "validate", "--plan", str(path)]
    )
    main()
    validated = capsys.readouterr().out
    assert RELEASE_PLAN_SCHEMA in validated
    assert "must-not-enter-safe-summary" not in validated

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cloud-offload",
            "release",
            "run",
            "--plan",
            str(path),
            "--ledger",
            str(tmp_path / "ledger.json"),
            "--output-dir",
            str(tmp_path / "matrices"),
        ],
    )
    with pytest.raises(SystemExit) as stopped:
        main()
    assert stopped.value.code == 2
    assert "Release run not started" in capsys.readouterr().err

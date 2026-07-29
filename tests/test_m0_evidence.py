import json
import math
from pathlib import Path


EVIDENCE_FILE = (
    Path(__file__).parents[1]
    / "docs"
    / "evidence"
    / "m0-production-evidence-2026-07-29.json"
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

EXPECTED_SCENARIOS = {
    "cold",
    "hot",
    "cancellation",
    "provider",
    "storage",
    "restart",
    "corruption",
}


def _walk(value):
    yield value
    if isinstance(value, dict):
        for item in value.values():
            yield from _walk(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk(item)


def test_m0_evidence_is_complete_finite_and_redacted():
    evidence = json.loads(EVIDENCE_FILE.read_text(encoding="utf-8"))

    scenarios = evidence["scenarios"]
    assert {scenario["kind"] for scenario in scenarios} == EXPECTED_SCENARIOS
    assert len(scenarios) == len(EXPECTED_SCENARIOS)
    assert all(scenario["accepted"] is True for scenario in scenarios)
    assert all(
        receipt["provider_absent"] is True
        for scenario in scenarios
        for receipt in scenario["cleanup"]
    )
    assert all(
        scenario["estimated_compute_cost_upper_usd"] >= 0
        for scenario in scenarios
    )

    for mapping in (value for value in _walk(evidence) if isinstance(value, dict)):
        assert not (set(map(str.casefold, mapping)) & FORBIDDEN_KEYS)

    for value in _walk(evidence):
        if isinstance(value, float):
            assert math.isfinite(value)
        if isinstance(value, str):
            lowered = value.casefold()
            assert not any(fragment in lowered for fragment in FORBIDDEN_VALUE_FRAGMENTS)

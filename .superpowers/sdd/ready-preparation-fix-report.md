# Ready-with-preparation boundary fix

Base: `b60ca10524db5c79429a15d16413fc44e9cc8077`

## Findings and TDD evidence

1. RED: `python -m pytest tests/test_workflow_capsule.py::test_workflow_preflight_accepts_canonical_sha256_input_binding -q` failed because the canonical `sha256:<64hex>` digest was passed directly to the strict bare-hex storage-key validator. GREEN normalizes only the protocol prefix at the preflight storage boundary; `partition_artifact_key` remains strict and unchanged.
2. RED: `python -m pytest tests/test_plan_protocol_rescue.py::test_workflow_stage_accepts_readiness_with_preparation -q` failed with HTTP 409 because readiness authority was not accepted. GREEN accepts a complete `ready_with_preparation` report, binds its selected offer and storage placement, and carries its preparation facts into the public plan quote.

## Implementation

- `cloud_offload/preflight.py`: strip only `sha256:` before calling `partition_artifact_key`.
- `cloud_offload/server.py`: validate an exact workflow readiness authority (schema, empty blockers, one recommendation, unique candidates, selected candidate, execution placement, storage binding, and internally consistent preparation bytes/coverage/completion); reject malformed, absent, conflicting, or multiple authorities. Revalidation repeats the same authority check and requires the current offer/storage to match it.
- `cloud_offload/plan_protocol.py`: validate and retain bounded preparation facts in candidates and public/replayed projections. A `ready_with_preparation` projection must include those facts and cannot be rewritten to plain `ready`.
- `tests/test_workflow_capsule.py`: real production preflight regression for prefixed input digests.
- `tests/test_plan_protocol_rescue.py`: RED/GREEN route and fail-closed authority tests for zero candidates, duplicate candidates, missing preparation, mismatched offer/storage, inconsistent facts, public omission, and independent candidate rejection.

## Verification

- Focused plan/preflight/workflow suite: `173 passed`.
- Full Cloud Offload suite: `1077 passed, 6 skipped in 191.12s`.
- Ruff: passed.
- Compileall: passed.
- `git diff --check`: passed.
- No provider, BWS, RunPod, network install, or paid mutation was used.

Remaining concern: this commit requires Sol re-review of preparation authority semantics and downstream Megumi pinning. No release or real-provider proof is claimed.

## Round 2: Sol re-review fixes

Base: `9b6e0a0f5a14f6e5b746f004479e58f9a0da4a44`

### Findings and TDD evidence

1. RED: `python -m pytest tests/test_plan_protocol_rescue.py::test_workflow_stage_accepts_real_preflight_readiness_without_fabricated_storage -q` returned HTTP 409 even though a direct call to the production `build_workflow_preflight` had no blockers. The raw production candidate contains provider/offer/region/prepared-volume but no `storage`; route authority required that absent field before selecting a candidate. The real route regression now uses the production builder, a read-only RunPod-shaped connector, local content-addressed artifacts, and declared model data.
2. RED: `python -m pytest tests/test_plan_protocol_rescue.py::test_workflow_readiness_authority_rejects_arithmetic_and_status_tampering -q` accepted a forged `coverage_percent=1.0` for 0/10 cached/required bytes. The same RED matrix covered bool, NaN, out-of-range, completion, and status tampering.

### Implementation

- `cloud_offload/server.py`: workflow authority now binds only provider/offer/region facts present in every production report, while preserving optional authoritative storage when reported. Normalized candidate storage remains required and is checked against the selected authority when available; candidate digest revalidation continues to bind storage without fabricating it in the raw report. Authority now enforces exact rounded coverage arithmetic, non-boolean finite numeric coverage, `missing_bytes = required_bytes - cached_bytes`, `complete iff missing_bytes == 0`, and status/completion consistency.
- `cloud_offload/plan_protocol.py`: preparation validation applies the same zero-requirement (`100.0%`, complete) rule and exact arithmetic to public and cached projections; replay rejects a tampered `ready_with_preparation` status changed to `ready`.
- `tests/test_plan_protocol_rescue.py`: real full-route production-builder regression; arithmetic/status tampering matrix; zero-requirement semantics; cached replay status tampering. Existing independent offer/storage and atomic no-provider-mutation coverage remains green.

### Verification

- RED route/arithmetic checks: failed as described above before the corresponding authority validation was in place.
- GREEN focused round-2 gate: `python -m pytest tests/test_plan_protocol_rescue.py::test_workflow_stage_accepts_real_preflight_readiness_without_fabricated_storage tests/test_plan_protocol_rescue.py::test_workflow_readiness_authority_rejects_arithmetic_and_status_tampering tests/test_plan_protocol_rescue.py::test_workflow_readiness_authority_defines_zero_requirement_as_complete tests/test_plan_protocol_rescue.py::test_cached_workflow_preparation_projection_rejects_status_tampering -q` -> `9 passed`.
- Affected Cloud suite (plan/protocol/preflight/workflow/routes/storage/queue/connectors): `356 passed`.
- Full Cloud Offload suite: `1086 passed, 6 skipped in 184.95s`.
- Ruff: passed. Compileall: passed. `git diff --check`: passed.
- No provider, BWS, RunPod API, network install, or paid mutation was used; the RunPod-shaped connector was an in-process test double only.

Remaining concern: this commit requires Sol re-review of the authority/storage binding and cached status semantics. No release or real-provider proof is claimed.

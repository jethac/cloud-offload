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

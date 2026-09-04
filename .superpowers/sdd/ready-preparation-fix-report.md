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

## Round 3: prepared-volume binding fix

Base: `042b3b35b98af66cba7749ec3cb09b413dbdfdfb`

### Finding and TDD evidence

The production workflow builder selected a complete 1,024-byte prepared volume (`volume-1`), but the plan route normalized the provider offer as ephemeral because `_workflow_placement` omitted `prepared_volume_id` and `_bind_workflow_candidate` copied only preparation. The RED full-route regression (`test_real_prepared_workflow_volume_remains_bound_through_preflight_submit_and_replay`) observed `persistent: false`; the mismatched-volume binding regression also covered stale identity. The test uses the production builder, a read-only in-process provider double, local artifacts, and zero provider mutations.

### Implementation

- `cloud_offload/server.py`: include `prepared_volume_id` in workflow placement authority; derive canonical persistent storage (`region`, `persistent`, `storage_id`) from the authoritative selected volume; bind it into normalized candidates, recompute the candidate identity over storage, and reject independent or mismatched storage/volume facts. Cold candidates remain explicitly ephemeral. First submit continues live readiness/offer revalidation before queue mutation; idempotent replay retains the accepted binding.
- `cloud_offload/plan_protocol.py`: permit the private candidate authority to carry `prepared_volume_id`, validate its exact match to persistent storage, and hash storage identifiers in the public projection.
- `tests/test_plan_protocol_rescue.py`: real complete prepared-volume builder/full-route test covering public/private projections, successful submit/replay, stale first-submit rejection with zero jobs, and mismatched/missing authority cases.

### Verification

- RED: route regression returned HTTP 200 with ephemeral storage instead of the selected prepared volume (`1 failed, 1 passed` in the initial focused run).
- GREEN focused prepared-volume/authority gate: `python -m pytest tests/test_plan_protocol_rescue.py -q -k 'real_prepared_workflow_volume_remains_bound or rejects_mismatched_prepared_volume or workflow_readiness_authority_fails_closed_on_malformed_or_ambiguous_report'` -> `11 passed`.
- Affected Cloud suite: `266 passed in 19.13s`.
- Full Cloud Offload suite: `1089 passed, 6 skipped in 181.26s`.
- Ruff, MyPy, compileall, and `git diff --check`: passed.
- No provider, BWS, RunPod API, network install, or paid mutation was used.

Remaining concern: Sol must re-review the private/public storage identity projection and volume freshness semantics. No release or real-provider proof is claimed.

## Round 4: exact prepared provider-volume identity

Base: `081a3b09c666505ef870f2a1e1a7cf438bd83b57` (detached)

### Finding and TDD evidence

The previous round bound a prepared candidate to the stable local registry id, but not to the provider's physical volume id. A registry row could keep `volume-1` while its provider id changed from A to B; preflight would preserve only the local id and the dispatcher could attach B. The RED authority test accepted a hot report with the provider identity omitted, and the RED production-builder test showed that the provider identity was not carried through the candidate.

### Implementation

- `cloud_offload/preflight.py`: verify that the provider returns the exact requested physical id, provider, and datacenter before producing a prepared candidate. Bind both the local registry id and provider id into the candidate identity, candidate fields, execution plan, and deduplication key; cold candidates remain explicitly ephemeral.
- `cloud_offload/server.py`: require paired local/provider identities for hot workflow readiness; bind both through normalized candidate ids, submit authority, confirmed launch records, replay, and restart state. Legacy public preflight responses hash both identifiers; private authority retains the exact values. Revalidation compares the stored digest witness to a fresh raw report and rejects provider-id, registry, region, provider, persistence, or storage drift before queue mutation.
- `cloud_offload/plan_protocol.py`: allow, validate, and bind the paired identities in private candidates and cached/public projections, including the persistent storage binding, while keeping raw provider identifiers out of public responses.
- `cloud_offload/dispatcher.py`: require the exact confirmed provider id for new prepared confirmations, or re-prove the current registry/provider object for legacy confirmations. The provider response must match id, provider, and datacenter exactly. Identity failures return before launch events, leases, queue-state mutation, cold substitution, or provider mutation.
- `tests/test_plan_protocol_rescue.py` and `tests/test_prepared_storage.py`: add real production-builder/full-route regressions for provider-id projection, public redaction, submit/replay, stale first-submit rejection, registry replacement, provider-object replacement, and physical deletion. The adversarial dispatcher test verifies no launch, lease, or non-creation event on replacement/deletion.

### Verification

- RED identity gate: `2 failed` as expected before the authority/source changes; GREEN identity gate: `2 passed`.
- Focused prepared-volume/authority tests: `12 passed`.
- Affected Cloud suite (plan/protocol/preflight/routes/storage/dispatcher): `268 passed in 19.14s`.
- Full Cloud Offload suite: `1094 passed, 6 skipped in 182.81s`.
- Ruff on all changed source/tests: passed. Full-repository Ruff still reports 15 pre-existing errors in unrelated files.
- `python -m compileall -q cloud_offload tests`: passed. `git diff --check`: passed.
- Full MyPy remains a baseline failure (`108` errors across the repository); the changed-module no-incremental check reports `78` existing errors, with no new contract-specific failure isolated by the focused tests.
- No provider, BWS, RunPod API, network install, or paid mutation was used.

### Concerns

- Legacy hot queue records created before the paired field existed cannot prove their historical provider id; the dispatcher re-proves the current exact provider object and refuses any substitution, while all new preflight-produced records carry the physical id.
- This is a local/in-process provider-double proof only. No real provider or release proof is claimed. Full Ruff/MyPy baseline diagnostics remain for follow-up.

## Round 5: redact prepared-volume diagnostics

Base: `4d6dfeb80ad5aa50de5131646bc39f9fc4c3ff97` (detached)

### Finding and TDD evidence

The exact-identity checks correctly rejected missing, substituted, misplaced,
mis-provider, and unbound prepared resources, but their public warning text
included the private coordinator `volume.id`. The provider-read exception had
the same leak, and the neighboring prepared-capacity exception interpolated the
same local id. A real `/api/preflight` endpoint matrix was written first; RED
was seven failures, each showing a forbidden local id in serialized response
bytes while the expected diagnostic and cold fallback were otherwise present.

### Implementation

- `cloud_offload/preflight.py`: replace all prepared-resource diagnostic text
  that named `volume.id` with fixed, actionable storage-resource wording. Safe
  typed exception names remain available through `_safe_error`; provider
  payloads, paths, credentials, and registry values are never copied into the
  report.
- `tests/test_preflight.py`: add a real public endpoint matrix covering missing
  storage, provider-id substitution, region drift, provider drift, missing
  persistent identity, provider-read exceptions, and capacity-read exceptions.
  It checks HTTP status, diagnostic code/message/action, stable cold fallback,
  zero provider mutation, the persisted public report, and every serialized
  response/store byte for private IDs, paths, credentials, and registry data.

### Verification

- RED endpoint privacy matrix: `7 failed` for the expected raw local-id leak.
- GREEN endpoint privacy matrix: `7 passed`.
- Focused preflight file: `32 passed in 3.83s`.
- Affected Cloud suite (plan/protocol/preflight/routes/storage/dispatcher):
  `275 passed in 19.57s`.
- Full Cloud Offload suite: `1101 passed, 6 skipped in 184.48s`.
- Privacy scan found no remaining interpolated prepared-volume identities in
  `cloud_offload/preflight.py`; persisted public-report and HTTP-byte scans
  passed for all seven adversarial cases.
- Ruff on changed source/tests: passed. Full-repository Ruff still reports 15
  pre-existing errors in unrelated files. Compileall and `git diff --check`:
  passed.
- No provider, BWS, RunPod API, network install, or paid mutation was used.

### Concerns

- The public diagnostics intentionally no longer identify which local volume
  failed; operators can inspect typed internal authority/log evidence and run
  the repair action, while the response remains safe for users and logs.
- This remains a local/in-process provider-double proof only. No real-provider
  or release proof is claimed. Full Ruff/MyPy baseline diagnostics remain for
  follow-up.

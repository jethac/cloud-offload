# M7 Artifact Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (not used here because the operator explicitly prohibited subagents; this plan is executed inline with TDD checkpoints).

**Goal:** Stage only the declared M7 boundary artifacts into the isolated coordinator's configured content-addressed store, with digest/size verification, before campaign startup.

**Architecture:** A small bootstrap module will derive unique input-artifact declarations and roles from the private release plan, verify source files from a read-only root, and atomically import them into the configured local artifact store. The release CLI will expose a pre-coordinator bootstrap command; the existing production artifact endpoint remains the runtime registration boundary, and preflight receives the same stored bytes.

**Tech Stack:** Python 3.10+, pathlib, hashlib, pytest, existing `LocalStorage`, release-plan JSON, coordinator preflight.

**Spec:** Parent handoff for M7 systematic-debugging Phases 2-4 and `docs/m7-release-runbook.md`.

## Global Constraints

- No BWS, RunPod, provider calls, provider mutations, or paid runtime.
- Do not point the isolated coordinator at the prior mutable home.
- Import only content-addressed artifacts declared by the release plan.
- Verify source digest and size before publication; fail closed on missing, mismatch, partial, or conflicting destination bytes.
- Duplicate exact imports are idempotent.
- No secret values or private paths in public evidence.
- Use TDD: each production behavior has a failing test observed before implementation.

### Task 1: Verified artifact import and plan declaration extraction

**Files:**
- Create: `cloud_offload/artifact_bootstrap.py`
- Test: `tests/test_artifact_bootstrap.py`

**Interfaces:**
- Produces `DeclaredArtifact`, `ArtifactBootstrapError`, `declared_input_artifacts(benchmark_plans)`, and `import_declared_artifacts(source_root, destination_root, declarations)`.
- `DeclaredArtifact` carries digest, observed/expected size, and sorted input roles.
- Import returns redacted records containing digest, size, roles, and whether the destination was already present.

- [x] Write tests for successful isolated import, exact bytes, role aggregation, missing source, digest mismatch, wrong size, partial copy cleanup, conflicting destination, and duplicate idempotence.
- [x] Run the focused test file and observe failure because the module is absent.
- [x] Implement digest/size verification, content-addressed key derivation, temporary destination plus atomic replace, destination re-verification, and fail-closed cleanup.
- [x] Run focused tests to green.
- [x] Commit the helper and tests.

### Task 2: Preflight integration regression

**Files:**
- Modify: `tests/test_artifact_bootstrap.py`
- Test: `tests/test_artifact_bootstrap.py`

**Interfaces:**
- Uses the helper from Task 1 and `build_partition_preflight` with a fake connector/storage.

- [x] Add an integration test that begins with an empty isolated store, imports two declared bundles from a read-only source root, verifies exact bytes and metadata, then runs identical preflight with no input-artifact blockers and no real provider launch calls.
- [x] Run the new test and observe failure before the production integration is complete.
- [x] Wire the test to the production local artifact storage path and ensure the fake connector is used only for free capacity discovery.
- [x] Run the focused integration test to green.

### Task 3: Release bootstrap CLI and runbook

**Files:**
- Modify: `cloud_offload/__main__.py`
- Modify: `docs/m7-release-runbook.md`
- Test: `tests/test_artifact_bootstrap.py`

**Interfaces:**
- Adds `cloud-offload release bootstrap-artifacts --plan <release-plan> --source-root <read-only-root> --config <isolated-config>`.
- The command loads each private benchmark plan referenced by the release plan, aggregates only its input artifacts, and imports into `CloudConfig.storage_path` without starting a service.

- [x] Add CLI parsing/dispatch tests for safe summary and missing-source behavior in the helper.
- [x] Run them red.
- [x] Implement the smallest dispatch branch; keep output to counts/digests/sizes/roles and never print source paths or secrets.
- [x] Document required ordering: bootstrap, verify, then start coordinator; document fail-closed behavior and idempotence.
- [x] Run focused CLI tests to green.

### Task 4: Corruption ordering regression and validation

**Files:**
- Modify: `tests/test_benchmark.py`
- Modify: `cloud_offload/benchmark.py` only if the test demonstrates an ordering defect.

**Interfaces:**
- The test proves corruption preparation is invoked after the cold scenario's base manifest is created, and returns a clear dependent failure when cold fails.

- [x] Add a local fake-driver matrix-order test; no provider calls or false manifest seeding.
- [x] Run it red.
- [x] Make the smallest ordering/error-reporting adjustment only if required.
- [x] Run benchmark-focused tests to green.

### Task 5: Verification and handoff

**Files:**
- Create: `B:\lab\m7-real-run-20260901\m7-artifact-bootstrap-report.md` (private, outside Git)

- [x] Run artifact-bootstrap, preflight, benchmark, CLI, and full local suites.
- [x] Check both isolated worktrees for clean tracked state except intended changes and confirm no provider/network credentials were used.
- [x] Write private report with base/head commits, tests, and no secret/path public evidence.
- [x] Commit final changes and return base/head/results to parent; do not push or merge.

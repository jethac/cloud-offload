# Cloud Plan Protocol Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add strict, offline-testable `POST /api/plans/preflight` and `POST /api/plans` protocol foundations with durable bindings and safe public projections.

**Architecture:** A focused `cloud_offload.plan_protocol` module validates and canonicalizes the cloud plan, stores preflight and paid-submit records in SQLite, and exposes an injected connector interface. FastAPI routes call this module and reuse current capsule/preflight/queue/event/result code without changing existing workflow or partition semantics.

**Tech Stack:** Python 3.10+, FastAPI, Pydantic v2, SQLite, pytest, FastAPI TestClient.

**Spec:** `docs/task11-plan-protocol-design.md`

## Global Constraints

- No RunPod, BWS, paid provider, or external mutation in tests or offline proof.
- Provider access remains behind injected connectors.
- Preflight must mutate no provider state.
- Public projections must exclude workflows, prompts, paths, credentials, signed URLs, raw provider bodies, and private errors.
- Existing `/api/preflight` and `/api/workflows` behavior must remain unchanged.

---

### Task 1: Add strict cloud plan schema and canonical digest

**Files:**
- Create: `cloud_offload/plan_protocol.py`
- Test: `tests/test_plan_protocol.py`

**Interfaces:**
- `validate_cloud_plan(plan) -> dict`
- `canonical_plan_digest(plan) -> str`
- `public_plan_summary(plan) -> dict`

- [ ] **Step 1: Write failing tests** for exact root fields, unknown dependency, cycles, missing input/artifact identity, invalid storage/residency, and stable digest/public redaction.
- [ ] **Step 2: Run** `pytest tests/test_plan_protocol.py -q`; confirm failures are missing schema functions.
- [ ] **Step 3: Implement** strict JSON validation, bounded stages/fan-out/retry/checkpoint contracts, DAG and typed-link checks, residency policy, and `sha256:` canonical digest binding.
- [ ] **Step 4: Run** the focused tests and confirm all pass.

### Task 2: Add SQLite authority and deterministic offline connector

**Files:**
- Modify: `cloud_offload/plan_protocol.py`
- Test: `tests/test_plan_protocol.py`

**Interfaces:**
- `PlanProtocolStore(path)` with atomic preflight/submit/replay/cancel/closure methods.
- `OfflineConnector` implementing the existing connector interface with counters and zero network/provider mutations.

- [ ] **Step 1: Write failing direct DB/restart/concurrency tests** for one accepted preflight, exact replay, different-body conflict, and one closure receipt.
- [ ] **Step 2: Run** `pytest tests/test_plan_protocol.py -q`; confirm failures.
- [ ] **Step 3: Implement** SQLite tables, transactions, unique idempotency key, quote binding, lifecycle transitions, cancellation idempotency, unknown-submit reconciliation, and artifact size/SHA/media validation.
- [ ] **Step 4: Run** focused tests and confirm persistence after reopening the store.

### Task 3: Add HTTP plan routes and safe projections

**Files:**
- Modify: `cloud_offload/server.py`
- Test: `tests/test_plan_routes.py`

**Interfaces:**
- `POST /api/plans/preflight` accepts `plan`, `input_artifacts`, provider/policy/region limits.
- `POST /api/plans` accepts the same plan plus exact preflight binding and `client_request_id`; requires matching `Idempotency-Key`.

- [ ] **Step 1: Write failing TestClient tests** for valid preflight/submit, malformed plan rejection, provider-call ordering, replay/conflict, stale quote, and redacted response fields.
- [ ] **Step 2: Run** `pytest tests/test_plan_routes.py -q`; confirm failures.
- [ ] **Step 3: Implement** route models, validation errors, injected connector selection, durable binding, queue job creation, and safe response projections. Leave existing routes untouched.
- [ ] **Step 4: Run** focused HTTP tests and then existing preflight/workflow tests.

### Task 4: Add deterministic loopback proof and release documentation

**Files:**
- Create: `tests/test_plan_protocol_proof.py`
- Create: `docs/task11-plan-protocol-report.md`

- [ ] **Step 1: Write the offline end-to-end proof** asserting exactly one plan/job/accepted submit/closure receipt, monotonic cursor, and zero connector launches/terminations/network calls.
- [ ] **Step 2: Run** the proof and record its SHA-256 report hash.
- [ ] **Step 3: Run** focused tests, full pytest, Ruff, MyPy, compile, and repository release audits.
- [ ] **Step 4: Record exact base/head/tree, route/schema facts, counts, proof hash, and blockers in the report.

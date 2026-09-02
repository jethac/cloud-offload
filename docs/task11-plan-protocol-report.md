# Task 11 Plan Protocol Report

Base: `origin/main` at `343d905c49c992565718c336c2b6dceba11c3601`.

Head: `feat/task11-plan-protocol` (exact commit is reported by final
verification).
The worktree is `B:/lab/cloud-offload/.worktrees/task11-plan`.
PR: [#110](https://github.com/jethac/cloud-offload/pull/110), open and
non-draft, base `main`, head `feat/task11-plan-protocol`. The branch remains
unmerged; final verification records the exact local and remote commit state.

Routes added:

- `POST /api/plans/preflight`: strict `comfy.workflow.plan.v1`; returns
  `cloud-offload.plan-preflight.v1`, a bound `sha256:` plan digest, one
  candidate, bounded quote, region, storage facts, and expiry.
- `POST /api/plans`: requires exact plan/preflight/candidate binding and
  `Idempotency-Key == client_request_id`; same-key replay returns the first job
  identity; different-body conflict returns 409.

SQLite authority: `cloud_plans` stores only the safe plan summary, preflight
quote, opaque idempotency digest, job identity, lifecycle state, and closure
receipt. `cloud_plan_authority` is a separate private table for the complete
accepted plan, candidate, input, and request binding. Both tables carry the
explicit `cloud-offload.plan-authority.v2` schema marker. An older development
table fails closed; it is not altered or interpreted as the new schema. This
protocol has not shipped on `main`, so no production table migration is needed.
Acceptance and queue-job creation use one `BEGIN IMMEDIATE` transaction on the
same database, including the first journal event; injected `BaseException`
therefore rolls both writes back. Terminal result/failure/cancellation closure
is likewise synchronized with the queue row. The queue boundary independently
validates the already-redacted plan and preflight projections before opening a
write transaction. The offline connector returns one deterministic candidate
and records zero launch, termination, and network calls.

Verification:

- Focused protocol, route, review, rescue, and offline proof tests: `83 passed`.
- Full suite: `1,025 passed, 6 skipped`.
- Ruff: clean for changed files. Full-repository Ruff retains 16 pre-existing
  diagnostics outside this change.
- Compile: clean.
- MyPy: the exact base and changed `server.py` checks both report the same
  pre-existing server/utility diagnostics (including the six
  `providers/base.py` diagnostics when imports are followed); the new
  `plan_protocol.py` module reports no diagnostics. The repository has no
  configured MyPy baseline gate, so this remains a documented blocker.

Offline proof hash from the deterministic HTTP/TestClient loopback:
`sha256:91082b5aef02e454b368fc0779398ea2affe92bbcd0db19a35c749e409b3f8da`.
The independently recomputed proof records one job, one accepted submit, one
closure receipt, monotonic cursor `[1, 2, 3]`, and zero provider launches,
terminations, or network calls.

The review rescue binds every submit field and the idempotency header to a
canonical request digest, keeps full plan data out of every public queue/status/
event/support/result projection, refreshes only unused expired preflights, and
never deletes an authority row when a submission or terminal write fails.

Scope note: this foundation does not close a GitHub issue or a provider-
dependent Megumi item. It does not merge the pull request.

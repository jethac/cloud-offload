# Task 11 Plan Protocol Report

Base: `origin/main` at `343d905c49c992565718c336c2b6dceba11c3601`.

Head: `feat/task11-plan-protocol`; the exact commit is recorded in the final
PR metadata and verification comment.
The worktree is `B:/lab/cloud-offload/.worktrees/task11-plan`.
PR: [#110](https://github.com/jethac/cloud-offload/pull/110), open and
non-draft, base `main`, head `feat/task11-plan-protocol`. The branch remains
unmerged; final verification records the exact local and remote commit state.

Routes added:

- `POST /api/plans/preflight`: strict `comfy.workflow.plan.v1`; runs every
  workflow stage through the production readiness engine and compares every
  stage runner profile, GPU type/model, VRAM, and declared capability with the
  offer before returning `cloud-offload.plan-preflight.v1`.
- `POST /api/plans`: requires exactly one raw, valid `Idempotency-Key` header
  whose value equals `client_request_id`; same-key replay returns the first job
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
and records zero launch, termination, and network calls. The offline proof
reopens the SQLite store, replays the exact request, derives plan/job/submit/
closure counts and the event cursor from SQLite and HTTP, and independently
recomputes the proof hash; changing any fact fails verification. Cached public
preflights are strictly validated and re-projected on every lookup, so extra,
private, or corrupt fields fail closed.

Verification:

- Focused protocol, route, review, rescue, and offline proof tests: `97 passed`.
- Full suite: `1,039 passed, 6 skipped`.
- Ruff: clean for changed files. Full-repository Ruff retains 16 pre-existing
  diagnostics outside this change.
- Compile: clean.
- MyPy: changed `plan_protocol.py` and `queue.py` add no diagnostics; the
  changed server check retains the repository's existing imported-module
  diagnostics. Full Ruff reports the same 16 pre-existing diagnostics outside
  these files. The repository has no configured MyPy baseline gate.

The deterministic HTTP/TestClient proof derives all counts from the reopened
database and journal. It records zero provider launches, terminations, or
network calls; the test recomputes its hash independently and rejects changed
counts or cursors.

The review rescue binds every submit field and the idempotency header to a
canonical request digest, keeps full plan data out of every public queue/status/
event/support/result projection, refreshes only unused expired preflights, and
never deletes an authority row when a submission or terminal write fails.

Scope note: this foundation does not close a GitHub issue or a provider-
dependent Megumi item. It does not merge the pull request.

# Task 11 Plan Protocol Report

Base: `origin/main` at `343d905c49c992565718c336c2b6dceba11c3601`.

Head: `feat/task11-plan-protocol` (exact remote SHA is reported by the final
verification command below).
The worktree is `B:/lab/cloud-offload/.worktrees/task11-plan`.
PR: [#110](https://github.com/jethac/cloud-offload/pull/110), open and
non-draft, base `main`, head `feat/task11-plan-protocol`. At report time CI
checks `test (3.10)` and `test (3.12)` were in progress; GitHub reported
`UNSTABLE` until they finish. The branch is clean after commit.

Routes added:

- `POST /api/plans/preflight`: strict `comfy.workflow.plan.v1`; returns
  `cloud-offload.plan-preflight.v1`, a bound `sha256:` plan digest, one
  candidate, bounded quote, region, storage facts, and expiry.
- `POST /api/plans`: requires exact plan/preflight/candidate binding and
  `Idempotency-Key == client_request_id`; same-key replay returns the first job
  identity; different-body conflict returns 409.

SQLite authority: `cloud_plans` stores the safe plan summary, preflight quote,
job identity, idempotency key, lifecycle state, and closure receipt. The
offline connector returns one deterministic candidate and records zero launch,
termination, and network calls.

Verification:

- Focused protocol, route, and offline proof tests: `10 passed`.
- Existing preflight/workflow/visibility/queue tests: `144 passed`.
- Full suite: `952 passed, 6 skipped`.
- Ruff: clean for changed files.
- Compile: clean.
- MyPy: the exact base and changed `server.py` checks both report the same
  pre-existing server/utility diagnostics (including the six
  `providers/base.py` diagnostics when imports are followed); the new
  `plan_protocol.py` module reports no diagnostics. The repository has no
  configured MyPy baseline gate, so this remains a documented blocker.

Offline proof hash from the deterministic store loopback:
`sha256:cc781c4215fcdc43ac0e7e44d34452d73ee7e46389f5346017235db8b12d512e`.
The proof records one job, one accepted submit, one closure receipt, monotonic
cursor `[1, 2, 3]`, and zero provider launches, terminations, or network calls.

Scope note: this foundation does not close a GitHub issue or a provider-
dependent Megumi item. It does not merge, push, or open a PR.

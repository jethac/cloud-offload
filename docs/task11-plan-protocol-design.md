# Task 11 Plan Protocol Design

Cloud Offload accepts a strict `comfy.workflow.plan.v1` envelope at the new
plan routes. The coordinator normalizes and validates the plan, computes its
canonical digest, and stores only a redacted plan summary plus the accepted
preflight binding in the existing SQLite authority database. Provider access is
injected through a connector factory. Tests use a deterministic loopback
connector that never performs network, launch, termination, or storage calls.

The plan route delegates readiness checks to the existing partition preflight
engine for each workflow stage. A plan preflight has one candidate and one
short-lived quote. Submission requires an exact, unexpired accepted preflight,
an exact plan digest and candidate, and matching idempotency values before any
connector method is called. The existing workflow and partition routes are not
changed.

Public responses contain identifiers, safe state, bounded pricing facts,
regions, storage facts, event cursors, and validated result metadata only. They
do not contain workflow graphs, prompts, local paths, credentials, signed URLs,
provider response bodies, or private exception text.


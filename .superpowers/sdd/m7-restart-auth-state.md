# M7 restart authentication fix

## Root cause

The M7 coordinator started with anonymous loopback enabled. The restart fault
hook rebuilt the `serve` command without `--allow-anonymous-loopback`. The
replacement then enabled bearer authentication and created a token. The
already-running benchmark harness had no token, so its later requests returned
401.

The real restart log showed 401 responses for health, status, config, job
snapshot, job events, and support bundle after the replacement started.

## TDD result

The regression test models the unchanged harness session across a replacement
launch. Before the production change, it failed with `401 Unauthorized` on
`/api/config`. The test covers all six route groups seen in the real log.

The production change appends `--allow-anonymous-loopback` to the replacement
command only when the discovered service has `auth_required: false`.

## Storage-policy inspection

The storage-policy setup errors do not share this root cause. They happened
before the restart. A local diagnostic reproduced the separate behavior: the
config POST persisted policy `off`, while the isolated server's runtime config
continued to return policy `smart`. This PR does not change that separate path.

## Verification

- Focused restart and service tests: 21 passed.
- Full repository suite: 800 passed, 6 skipped.
- Python byte-code compile check: passed.
- Ruff: not installed and not configured by the repository.
- MyPy: the repository has no MyPy configuration; an informational full run
  reported 107 existing errors in 22 files. No reported error points to the
  changed restart command or regression test.
- No RunPod or BWS access occurred.

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

The replacement now gets exactly one explicit auth-mode flag. Required-auth
services get `--require-auth`; anonymous services get
`--allow-anonymous-loopback`. The child environment drops conflicting inherited
auth-policy flags. Tests prove both modes, including required auth under a
hostile inherited anonymous flag.

Before SIGTERM, the restart hook now validates the exact local HTTP host, the
integer port and URL match, the PID, the auth value and readable token, and the
existing client's URL/auth contract. Parameterized tests prove that 12 invalid
contracts cause zero SIGTERM calls and zero replacement launches. Strict
service discovery also rejects a raw string port before normalization.

## Storage-policy inspection

The storage-policy setup errors do not share this root cause. They happened
before the restart. A local diagnostic reproduced the separate behavior: the
config POST persisted policy `off`, while the isolated server's runtime config
continued to return policy `smart`. This PR does not change that separate path.

## Verification

- Focused restart and service tests: 35 passed.
- Full repository suite: 814 passed, 6 skipped.
- Python byte-code compile check: passed.
- Ruff: not installed and not configured by the repository.
- MyPy: the repository has no MyPy configuration; an informational full run
  reported 107 existing errors in 22 files. No reported error points to the
  changed restart command or regression test.
- No RunPod or BWS access occurred.

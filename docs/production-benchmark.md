# Production benchmark and scorecard

Cloud Offload's production benchmark exercises the same coordinator, dispatcher,
worker image, provider connector, and prepared storage as an ordinary job. It is
not a synthetic timing loop. Its output is a comparable, redacted JSON scorecard
and its first obligation is to leave no paid compute behind.

## Safety contract

Every plan must declare finite positive ceilings for:

- total estimated campaign compute cost;
- estimated compute cost per scenario;
- total campaign runtime;
- scenario runtime;
- cleanup verification time; and
- the time allowed for a terminated worker heartbeat to age out before the next
  fresh-Pod scenario.

`benchmark run` refuses to start without `--confirm-spend`. The default
`exclusive: true` mode also refuses to start while a provider account has an
active instance named `cloud-offload-worker-*`. Baseline resources are never
terminated. During a campaign the harness attributes resources from JobEventV2
and, in exclusive mode, from new provider inventory entries.

The harness cancels a job when a scenario, campaign-cost, campaign-runtime, or
scenario-runtime limit is reached. After every scenario it independently asks
the provider to terminate the exact attributable instance IDs, retries, and
records provider-absence receipts. It repeats the audit at campaign end. Any
remaining Cloud Offload instance makes the scorecard fail and stops subsequent
scenarios.

Cost is intentionally an upper estimate, accruing each observed Pod from scenario
submission through verified provider absence. Provider billing granularity and a
termination request's latency can create a small overshoot beyond a locally
observed threshold; the limit is a circuit breaker, not a prepaid provider
escrow. Use a short poll interval and leave headroom below the amount you can
actually spend.

## Plan

A plan uses `cloud-offload.benchmark-plan.v1`:

```json
{
  "schema": "cloud-offload.benchmark-plan.v1",
  "providers": ["runpod"],
  "exclusive": true,
  "limits": {
    "max_total_cost_usd": 1.00,
    "max_scenario_cost_usd": 0.25,
    "max_campaign_seconds": 3600,
    "poll_seconds": 2,
    "cleanup_timeout_seconds": 90,
    "fresh_worker_timeout_seconds": 120
  },
  "scenarios": [
    {
      "name": "fresh-pod-cold-1",
      "cache_state": "cold",
      "endpoint": "/api/partitions",
      "request": {
        "partition": { "...": "ordinary request body" },
        "force_execution": true
      },
      "timeout_seconds": 900,
      "expected_statuses": ["completed"],
      "fresh_instance": true
    },
    {
      "name": "fresh-pod-hot-1",
      "cache_state": "hot",
      "endpoint": "/api/partitions",
      "request": {
        "partition": { "...": "same manifest and inputs" },
        "force_execution": true
      },
      "timeout_seconds": 600,
      "expected_statuses": ["completed"],
      "fresh_instance": true
    }
  ]
}
```

Cold and hot scenarios must begin cold and alternate. Every fresh scenario waits
until the coordinator reports no active heartbeat for the selected providers;
this prevents a recently terminated Pod's stale worker record from suppressing
the next rental. The harness verifies that a fresh provider instance actually
appeared, so a result-cache hit cannot masquerade as a fresh-Pod hot restore.
Fresh partition scenarios are rejected at plan validation unless their ordinary
submission request explicitly sets `force_execution: true`; the coordinator then
bypasses only the completed-result cache while leaving prepared-state caching in
place. This flag exists for measurement and intentional recomputation, not as a
general performance setting.

The request body remains local in the plan. Validation and scorecards include
only its canonical SHA-256 digest, never the workflow, prompt, or input values.

## Failure injection

The five Milestone 0 failure classes are represented explicitly:

| `kind` | Action |
|---|---|
| `cancellation` | Calls the ordinary job cancellation API. |
| `provider` | Terminates the exact observed provider instance. |
| `storage` | Runs an operator-supplied hook at the selected phase/event. |
| `corruption` | Runs an operator-supplied hook at the selected phase/event. |
| `restart` | Runs an operator-supplied hook at the selected phase/event. |

An injection can declare `trigger_phase`, `trigger_event`, `after_seconds`, or a
combination. Storage, corruption, and restart hooks require `hook_argv` in the
plan and the run additionally requires `--allow-hooks`. Commands are executed as
an argument vector without a shell. The harness passes job, scenario, failure
kind, and observed instance IDs through `CLOUD_OFFLOAD_BENCHMARK_*` environment
variables. Hook arguments and output are deliberately absent from the scorecard
because either can contain object keys, URLs, or credentials. A non-zero hook
exit code fails the scenario.

Hooks are an explicit operator boundary: they may mutate external state. Keep
them narrow, idempotent, and reversible, and make the hook wait until its intended
failure or restart is observable before it exits.

## Commands

Validate without submitting work or loading provider credentials:

```bash
cloud-offload benchmark validate --plan benchmark.json
```

Run and atomically write a scorecard:

```bash
cloud-offload benchmark run \
  --plan benchmark.json \
  --output scorecards/production.json \
  --confirm-spend
```

Add `--allow-hooks` only for a reviewed plan that contains external failure-hook
commands. A passing process exits zero. A failed scenario, untriggered injection,
budget circuit breaker, missing fresh Pod, or orphaned provider resource writes a
failed scorecard and exits non-zero.

## Scorecard

`cloud-offload.benchmark-scorecard.v1` contains:

- safe plan summary and request digests;
- cold, hot, and failure scenario results;
- lifecycle/event cursor and redacted support bundle;
- phase durations from observed JobEventV2 timestamps;
- provider, Pod ID, rate, and attribution source;
- conservative compute-cost estimate;
- failure-trigger receipt;
- exact termination attempts and provider-absence receipt;
- cold/hot duration and cost distributions;
- phase distributions; and
- final orphan inventory.

The benchmark does not claim provider-final billed cost. That becomes possible
only after Milestone 3 defines and persists the provider's authoritative billing
closure receipt.

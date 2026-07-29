# Job visibility

`GET /api/job-visibility` is the safe projection used by the ComfyUI **Cloud
Jobs** panel. It is separate from worker and support APIs because a browser must
not receive a raw job request or raw event.

The response schema is `cloud-offload.job-visibility.v1`. Active jobs sort
first. The remaining rows sort by most recent update. `limit` is bounded to
1–200, and `active_only=true` removes terminal rows.

Each job includes only allow-listed fields:

- job and partition IDs;
- status, lifecycle stage, current operation, monotonic progress, and elapsed
  time;
- measured transfer bytes, smoothed throughput, and a confidence-bounded ETA;
- provider, GPU, region, Pod, resource-lease, and prepared-volume IDs;
- hourly rate, estimated accrued spend, and the confirmed preflight cost range;
- cache hit, miss, restored-byte, and saved-item counts;
- preflight confidence and matched-history count;
- cancellation and billing state; and
- the event count, resumable cursor, and recent safe event summaries.

The projection never returns the job request, workflow, prompt, raw parameters,
model path, signed URL, raw provider reply, raw event, artifact digest, or error
text. Event summaries use fixed messages. Legacy jobs can therefore show an
unknown GPU or region without exposing old private payloads.

## Progress and ETA

The stage order is readiness, provisioning, worker boot, dependency
preparation, execution, result transfer, and resource closure. Observed progress
never regresses. Between measurable events, an active stage can advance within
its fixed band from the preflight timing range. The response labels this basis
as `stage_time_estimate`; the UI marks the percentage as estimated.

Successful terminal jobs are always 100%. Failed and cancelled jobs keep the
highest observed progress. ETA is always a range. It uses measured transfer
throughput during result transfer, matched preflight history when available, and
an unavailable state when neither basis exists.

## Transfer telemetry

Workers report completed and total bytes when a download exposes a declared
size and a staging path whose growth can be observed. They report elapsed-only
heartbeats when the source does not expose measurable bytes. Cache population
already reports periodic completed and total bytes. The projection groups
repeated progress events by transfer operation so polling does not count the
same bytes more than once.

## Billing language

The browser can show these states:

- `not_started`: no paid Pod identity is known;
- `accruing`: a Pod exists and the job is not terminal;
- `termination_unconfirmed`: the job is terminal but no provider closure
  receipt exists; and
- `stopped`: a provider termination receipt exists.

The view does not infer that billing stopped from job completion, worker exit,
or a provider status of `stopped`. The dispatcher writes the closure receipt
only after the provider reports the exact resource absent or `terminated`.
Paid elapsed time then stops at the receipt time. See
[Job leases and provider closure](job-leases.md).

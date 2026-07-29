# JobEventV2 contract

`cloud-offload.job-event.v2` is the durable transport envelope for Cloud
Offload's append-only job flight recorder. It provides one coordinator-assigned
ordering while allowing dispatchers and workers to retry delivery safely.

## Envelope

```json
{
  "schema": "cloud-offload.job-event.v2",
  "sequence": 142,
  "job_id": "job-uuid",
  "created_at": "2026-07-29T00:00:01.000000",
  "occurred_at": "2026-07-29T00:00:00.900000",
  "observed_at": "2026-07-29T00:00:01.000000",
  "producer": {
    "id": "worker:worker-uuid:process-uuid",
    "sequence": 17
  },
  "type": "weight_download_progress",
  "phase": "dependency_preparation",
  "metrics": {
    "bytes": 1048576,
    "total_bytes": 4294967296,
    "overall_progress": 24
  },
  "resources": {
    "provider": "runpod",
    "region": "US-MD-1",
    "worker_instance_id": "pod-id"
  },
  "evidence": {},
  "event": {
    "type": "weight_download_progress",
    "phase": "dependency_preparation",
    "bytes": 1048576,
    "total_bytes": 4294967296,
    "overall_progress": 24,
    "provider": "runpod",
    "region": "US-MD-1",
    "worker_instance_id": "pod-id"
  }
}
```

`created_at` remains a compatibility alias for the coordinator observation time.
New clients should use `occurred_at` and `observed_at`. The original producer
event remains under `event`; normalized projections allow clients to migrate
incrementally without changing every producer at once.

## Ordering and idempotency

- `sequence` is assigned only after the coordinator durably inserts the event.
  It is strictly increasing for a job, although values can have gaps because the
  SQLite sequence is shared by all jobs.
- A producer ID identifies one process incarnation. Worker IDs use
  `worker:<claimed-worker-id>:<process-uuid>`; dispatcher IDs use
  `dispatcher:<process-uuid>`.
- `(job_id, producer.id, producer.sequence)` is unique when a producer sequence
  is present. Repeating the same tuple and exact event returns the original
  envelope. Reusing it for different event data is rejected.
- Migrated V1 rows use producer `legacy` with a null producer sequence and keep
  their original coordinator timestamp as both occurred and observed time.
- A terminal job ignores delayed worker events. A late producer therefore cannot
  alter the visible lifecycle after completion, failure, or dead-lettering.

## Replay and snapshot

`GET /api/jobs/{id}/events?after=N&limit=M` returns ordered envelopes and a
`next_after` cursor. `GET /api/jobs/{id}/snapshot` returns the current projected
job state, semantic progress, last event, event count, and event cursor. A polling
client can restore by loading a snapshot, applying events after its cursor, and
continuing from the returned `next_after` value. Progress is projected
monotonically and completed jobs project 100 percent.

The current snapshot combines the durable job row with the event journal. The
product goal's stronger requirement—deriving the entire lifecycle from journaled
state transitions—is tracked in Milestone 0 and is not implied by this first
transport slice.

## Support bundle boundary

`GET /api/jobs/{id}/support-bundle` returns at most 10,000 redacted events plus a
safe job/request summary. It keeps node-type counts, artifact digests and sizes,
phase metrics, resource identities, timestamps, and errors after redaction. It
omits workflow and prompt bodies, input/output values, base64 payloads, result
payloads, secret-bearing fields, URL credentials, and URL query or fragment
data. The bundle advertises truncation when more events exist.

The bundle is diagnostic evidence, not an exact workflow replay artifact. An
exact replay package requires a separate, explicitly authorized export because
workflow values and inputs may be private.

# Partition preflight

`POST /api/preflight` checks one compiled partition before Cloud Offload queues
it or creates a paid provider resource. The response schema is
`cloud-offload.preflight.v1`.

## Mutation boundary

Preflight can read:

- local configuration and immutable partition declarations;
- local boundary artifact presence;
- prepared-state registry data;
- provider storage identity and region; and
- current provider offers.

Preflight does not queue a job, create or stop a Pod, or create, change, or
delete provider storage.

## Request

The request contains:

| Field | Meaning |
| --- | --- |
| `partition` | A `comfy.partition.job.v1` compiled partition. |
| `input_artifacts` | Boundary key to immutable artifact digest. |
| `provider` | `auto` or one allowed provider. |
| `recommendation_policy` | Optional request override for `balanced`, `cheapest`, `fastest`, or `manual`. The configured policy is the default. |
| `max_hourly_rate` | Optional stricter hourly price limit. It cannot loosen the configured hard limit. |
| `max_total_job_cost` | Optional stricter limit applied to the current upper total-cost estimate. It cannot loosen the configured hard limit. |
| `allowed_regions` | Optional stricter region allowlist. It cannot add a region outside the configured allowlist. |

## Deterministic proof

The report blocks before a provider read when a deterministic requirement is
not satisfied. The proof checks:

- partition schema, workflow, runner profile, capability, GPU VRAM, and
  residency;
- a valid digest-pinned worker image;
- boundary artifact presence;
- declared asset identity, size, and eligible source;
- required pinned custom node packs;
- container disk plan and configured ceiling;
- provider, Hugging Face, worker, ingress, and prepared-storage credential
  presence when required; and
- strict prepared-storage bindings.

`blockers`, `warnings`, and `unknowns` are separate lists. Each item has a stable
code, a user message, and an action when Cloud Offload knows one.

## Volatile observations

After deterministic proof passes, preflight reads current offers. When prepared
storage is enabled, it also verifies each candidate volume and reads offers for
that volume's region. Provider errors are returned by safe error type. Provider
URLs, response bodies, raw offer data, and credentials are not returned.

The quote expires after 60 seconds. The response lists the facts that must be
read again before launch: offer availability, hourly price, region, and prepared
volume.

## Recommendation

Each safe candidate contains:

- provider, offer, GPU, VRAM, region, and hourly rate;
- prepared volume and byte coverage;
- required, cached, and missing bytes;
- startup, preparation, execution, and paid-lifetime ranges;
- a compute-cost range and confidence; and
- a stable candidate ID, rank, and score.

`balanced` uses normalized time and compute cost, with 65% weight on time and
35% weight on cost. `cheapest` uses the midpoint of the compute-cost range.
`fastest` uses the midpoint of the paid-lifetime range. `manual` returns ranked
compatible choices but does not select one.

The initial estimate has low confidence. Until comparable execution history is
available, it uses these explicit ranges:

- provider startup: 60 through 180 seconds;
- execution: 120 through 300 seconds;
- provider closure: 10 through 30 seconds;
- source download: 25 through 100 MiB/s; and
- prepared restore: 100 through 500 MiB/s.

The report identifies missing execution history and unmeasured incremental
transfer or storage charges as unknowns. It does not present these unknowns as
proof. Later M1 slices will use measured history and complete cost components.

## Rental confirmation

The safe report includes a `confirmation` object. It contains the configured
policy, the countdown duration, whether confirmation is required, whether the
interruption is mandatory, and the server-controlled `not_before` time.

The durable settings are:

| Setting | Values and default |
| --- | --- |
| `rental_confirmation` | `always` by default, `material_changes`, or `never`. |
| `confirmation_countdown_seconds` | 0 through 60; 10 by default. |
| `recommendation_policy` | `balanced` by default, `cheapest`, `fastest`, or `manual`. |
| `max_hourly_rate` | Hard positive hourly limit. |
| `max_total_job_cost` | Optional hard positive total-cost limit. |
| `allowed_regions` | Optional hard region allowlist. |
| `material_price_change_percent` | Price-change tolerance; 5% by default. |
| `material_cost_change_percent` | Estimated total-cost tolerance; 10% by default. |

`POST /api/config` validates and persists these non-secret settings. Skipping
normal confirmation does not disable price, total-cost, region, residency, GPU,
or provider constraints.

## Status

| Status | Meaning |
| --- | --- |
| `ready` | Deterministic proof passed and the selected candidate needs no missing data transfer. |
| `ready_with_preparation` | Deterministic proof passed and the selected candidate has cold work. |
| `blocked` | A deterministic requirement failed. Provider reads do not start. |
| `uncertain` | Proof passed, but no current offer satisfies all hard limits or provider facts cannot be read. |

The response includes a random `preflight_id` and a `manifest_digest`. The digest
binds the partition, boundary artifact identities, profile, image, GPU limits,
storage plan, residency, provider policy, price limits, region limits, asset
digests, and node pack digests. The report does not return the workflow body.

## Submission binding and revalidation

The coordinator stores the safe report projection in its SQLite database. It
does not store the workflow body in the preflight record.

A partition that needs paid execution must submit:

- `preflight_id`;
- `manifest_digest`; and
- one `candidate_id` from that report.

When confirmation is required, the request must also submit
`confirmation_action` as `start_now` or `countdown_elapsed`. `start_now` is the
explicit user action. The coordinator accepts `countdown_elapsed` only after
the report's server-controlled `not_before` time. When the active policy does
not require normal confirmation, the action can be omitted and the accepted
job records `policy_skip`.

A valid completed partition-cache hit stays free and does not require these
fields. Every cache miss requires them before the coordinator queues the job.

The submit route reads current facts again. It returns HTTP 409 without queuing
the job when the report is absent, blocked, expired, or does not match the
partition, or when the chosen offer, GPU, price, region, volume, preparation, or
estimate changed beyond the configured tolerances. A changed response includes
a new safe preflight report for a new user decision. That report has mandatory
confirmation even when normal confirmation is set to `material_changes` or
`never`.

The queued job contains only the safe confirmed launch projection. Immediately
before provider launch, the dispatcher reads the exact offer and prepared volume
again. It refuses launch when the quote expired or the provider, offer, GPU,
price, region, or volume changed. It does not select a replacement silently. A
confirmed prepared launch also cannot use the normal automatic cold fallback.

The worker sends its mounted prepared-volume identity when it claims work. A
job confirmed for one prepared volume cannot be claimed by a cold worker or a
worker on a different volume.

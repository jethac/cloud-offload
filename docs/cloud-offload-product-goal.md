# Cloud Offload product goal and delivery plan

> Status: **canonical product goal**
> Last updated: **2026-07-29**
> Scope: the end-to-end Cloud Offload product across the coordinator, dispatcher,
> workers, provider connectors, prepared storage, and ComfyUI extension.
>
> The detailed storage subsystem design remains in
> [Storage-aware Cloud Offload](storage-aware-cloud-offload.md). That PRD is a
> supporting design; this document defines the product outcome it serves.

## Product goal

Cloud Offload rents and operates the right cloud GPU for a ComfyUI workflow. It
checks everything knowable before the paid resource starts, explains what is
happening while it runs, proves that the resource stopped when the run ends, and
makes repeated runs materially faster.

GPU rental and execution are the center of the product. Readiness, visibility,
closure, and acceleration are additional promises that make that central promise
predictable and trustworthy; they do not replace it.

The complete product contract is:

> **Cloud Offload rents the right GPU, proves the workflow is ready before the
> meter starts, shows exactly what is happening while it runs, proves billing
> stopped when it ends or is cancelled, and accelerates the next compatible
> run.**

## Promise stack

| Promise | User expectation | Product definition of done |
| --- | --- | --- |
| **Execution** | “Rent and operate the right GPU for me.” | A compatible worker executes the workflow without the user managing provider infrastructure. |
| **Readiness** | “Check everything knowable before charging me.” | Deterministic blockers stop before Pod creation; preparation, cost, and uncertainty are disclosed. |
| **Visibility** | “Always tell me what is happening.” | Status survives reload and shows stage, elapsed time, measurable progress, ETA confidence, resource identity, and spend. |
| **Closure** | “Prove the paid resource stopped.” | Completion and cancellation end with provider-confirmed termination evidence, not only a local status change. |
| **Acceleration** | “Make compatible repeat runs faster.” | Verified prepared state, reproducible bundles, and storage-aware placement reduce measured time to result. |

## Current baseline

Cloud Offload already has the load-bearing execution path:

- partition compilation and authenticated coordinator submission;
- provider routing and immutable worker-image selection;
- RunPod provisioning and disposable worker lifecycle;
- authenticated Hugging Face model downloads;
- incremental worker and ComfyUI execution events;
- opt-in RunPod network-volume prepared storage;
- signed prepared-state manifests and content-addressed artifacts;
- storage-aware regional placement and explicit cold fallback;
- cache population, restore, verification, and quarantine primitives;
- visible startup, download, cache, verification, and execution feedback; and
- terminal cancellation semantics that prevent late callbacks from resurrecting
  a cancelled job.

A reference inpainting run has validated the connected path with five prepared
cache hits and one authenticated 4.23 GB model download. Durable copy completed
in approximately 7.5 seconds and read-back verification and publication in
approximately 15.3 seconds. This proves the direction, but one successful run is
not a production-readiness claim.

The remaining beta gaps are:

- job status is still too coupled to transient canvas state;
- reload and reconnect do not yet reconstruct one authoritative lifecycle;
- Hugging Face progress lacks reliable bytes, throughput, and ETA;
- overall percentages do not yet represent the real critical path;
- logical cancellation does not yet cooperatively abort every phase or prove
  provider billing closure;
- trusted cache hits may still pay expensive verification I/O;
- dependency failures can still be discovered after the rental boundary;
- custom-node prepared bundles are incomplete;
- regional replication is not yet demand- and budget-controlled; and
- validation is not yet a continuous cold/hot/failure-injection matrix.

## Product principles

1. **Execution remains central.** Preflight and confirmation must not turn a
   one-click offload into infrastructure administration.
2. **The happy path is automatic.** Healthy preflight proceeds through a short,
   understandable rental confirmation rather than a multi-page wizard.
3. **Proof, estimate, and unknown are distinct.** The UI must never present a
   volatile provider observation as a guarantee.
4. **Fail free when possible.** A deterministic blocker discovered after Pod
   creation is a product defect.
5. **Provider truth closes billing.** Local cancellation alone cannot justify a
   “billing stopped” claim.
6. **The coordinator journal is authoritative.** Canvas titles and transient
   notifications render state; they do not own it.
7. **Progress describes remaining time.** A percentage must be monotonic,
   phase-aware, and calibrated by measurements rather than arbitrary counters.
8. **Prepared state must prove value.** Cache admission, retention, and
   replication respond to measured time and cost saved.
9. **Storage automation is budgeted and reversible.** Every automatic replica
   needs a reason, spend ceiling, and expiry.
10. **Every paid lifecycle is auditable.** A support bundle should explain what
    was checked, rented, transferred, executed, and terminated without requiring
    raw server logs.

## Canonical user journey

```text
Queue workflow
  → Checking readiness
  → Recommendation: RunPod A100 80 GB, US-MD-1, $1.49/hour
  → Estimated total: $0.12–$0.24; 88% prepared; 4.2 GB to download
  → Starting automatically in 10 seconds
  → Revalidating price, availability, region, and storage
  → Renting GPU
  → Pulling image and starting ComfyUI
  → Restoring prepared state and downloading misses
  → Running graph
  → Uploading result
  → Terminating Pod
  → Complete — provider confirmed termination; billing stopped
```

If deterministic readiness fails, the journey stops before `Renting GPU` and
offers an actionable correction. If provider capacity, price, or storage changes
materially during the confirmation window, Cloud Offload presents the revised
recommendation instead of silently substituting it.

## Readiness and preflight

### Purpose

Preflight converts a submitted partition into a canonical, hash-addressed
execution plan before any paid provider mutation. It proves deterministic
requirements, reports volatile observations, estimates preparation and cost,
and supplies the facts used by the GPU recommendation.

### Proposed API

`POST /api/preflight` returns a versioned report such as:

```json
{
  "schema": "cloud-offload.preflight.v1",
  "preflight_id": "...",
  "manifest_digest": "sha256:...",
  "status": "ready_with_preparation",
  "blockers": [],
  "warnings": [],
  "unknowns": [],
  "execution_plan": {
    "profile": "comfyui",
    "image_digest": "sha256:...",
    "gpu_requirement": {"minimum_vram_gb": 80},
    "provider": "runpod",
    "region": "US-MD-1",
    "prepared_volume_id": "..."
  },
  "preparation": {
    "required_bytes": 36600000000,
    "cached_bytes": 32300000000,
    "missing_bytes": 4300000000
  },
  "estimate": {
    "startup_seconds": [80, 150],
    "execution_seconds": [150, 300],
    "hourly_rate": 1.49,
    "total_job_cost": [0.12, 0.24],
    "confidence": "medium"
  },
  "expires_at": "..."
}
```

### Deterministic proof

Preflight must establish, without renting a Pod:

- the runtime profile supports the submitted partition capability;
- the worker image is pinned by digest and passes policy;
- required node types map to known, pinned custom-node sources;
- required model artifacts have known digests, sizes, and eligible sources;
- necessary provider, Hugging Face, registry, and S3 credentials are configured;
- residency, privacy, tenant, and cacheability policies permit offload;
- the disk plan fits configured limits;
- the selected prepared volume exists in the required datacenter;
- cached manifests are signed, compatible, and internally consistent; and
- hard user price, provider, GPU, and region constraints can be satisfied by the
  proposed plan.

### Volatile observations and estimates

These facts may change between preflight and launch and must be labeled as such:

- current GPU capacity and hourly price;
- provider launch reliability;
- image-pull duration;
- Hugging Face and S3 throughput;
- estimated dependency-preparation time;
- historical execution duration; and
- expected total job cost.

Immediately before Pod creation, the coordinator revalidates price, availability,
region, volume attachment, and expiration. A material change invalidates the
recommendation and creates a revised confirmation unless policy explicitly
permits it within configured tolerances.

### Status behavior

| Status | Default behavior |
| --- | --- |
| `ready` | Show the rental recommendation and begin the confirmation countdown. |
| `ready_with_preparation` | Do the same, clearly showing cold work; interrupt only when time or cost exceeds policy. |
| `blocked` | Do not rent; show actionable blockers. |
| `uncertain` | Require confirmation when the uncertainty can plausibly cause a paid failure. |

The partition submission binds its `preflight_id` and `manifest_digest`, allowing
the coordinator to reject a stale or materially different plan.

## GPU recommendation and rental confirmation

### Recommendation objective

After removing incompatible offers, Cloud Offload recommends the viable option
that best satisfies the user's selected policy:

| Policy | Objective |
| --- | --- |
| `balanced` | Best expected time to result for reasonable total cost. Recommended default. |
| `cheapest` | Lowest estimated total job cost, not merely lowest hourly price. |
| `fastest` | Lowest estimated time to completed result. |
| `manual` | Present compatible choices without selecting one automatically. |

The ranking considers:

- minimum VRAM and required GPU capabilities;
- measured execution speed for comparable workflows;
- hourly price and expected total runtime;
- image-pull and worker-start history;
- prepared-state region and byte coverage;
- missing transfer volume and measured source throughput;
- provider and regional reliability; and
- user provider, region, hourly-rate, and per-job cost limits.

The primary cost estimate is:

```text
expected total job cost =
    hourly rate × expected paid lifetime
  + incremental transfer cost
  + attributable incremental storage cost
```

The recommendation must include an explanation record so the user and support
can see why it won over cheaper or faster alternatives.

### Confirmation experience

The default confirmation shows:

- recommended GPU, provider, and region;
- hourly price;
- estimated total job cost as a range;
- estimated startup and execution time;
- prepared-cache coverage and missing bytes;
- recommendation rationale;
- confidence and meaningful uncertainty; and
- a visible countdown, approximately ten seconds by default.

Example:

```text
Recommended: RunPod A100 80 GB in US-MD-1
$1.49/hour · estimated total $0.12–$0.24
Approximately 1–2 minutes of preparation; 88% already cached
Selected for fastest expected result under your $1.75/hour limit

Starting in 10 seconds…
```

Actions are:

- **Start now**;
- **Cancel**;
- **Choose another GPU**; and
- **Don't show this confirmation again**.

Untouched confirmation automatically starts when the countdown ends. Inspecting
or changing the GPU, provider, region, or cost details pauses the countdown.
“Don't show again” sets the persistent rental-confirmation setting to `never` and
explains where to restore it.

### Settings

The settings surface includes:

- rental confirmation: `always`, `material_changes`, or `never`;
- confirmation countdown duration;
- recommendation policy: `balanced`, `cheapest`, `fastest`, or `manual`;
- maximum hourly price;
- maximum estimated total job cost;
- preferred or allowed providers;
- preferred or allowed regions; and
- material-change tolerances for price and estimated cost.

Skipping confirmation never disables hard spend, residency, provider, or GPU
constraints.

### Mandatory interruption

Even when normal confirmation is disabled, Cloud Offload must interrupt automatic
launch when:

- hourly price or estimated total cost exceeds a hard limit;
- the recommendation changes materially after the quote;
- the required GPU class changes materially;
- prepared storage becomes unavailable and preparation unexpectedly becomes cold;
- preflight reports uncertainty likely to cause a paid failure;
- the quote expires; or
- provider capacity disappears and the replacement is outside allowed tolerance.

## Visibility and job journal

### Authoritative event model

Every job becomes an append-only flight recorder. The coordinator persists an
event before broadcasting it and assigns a monotonically increasing job sequence.
Dispatchers and workers submit idempotent events with producer IDs and local
sequence numbers so retries cannot duplicate or reorder the visible lifecycle.

`JobEventV2` contains at least:

- job and partition identity;
- coordinator sequence;
- producer identity and producer sequence;
- event type, lifecycle phase, and phase ownership;
- occurred-at and observed-at timestamps;
- completed and total work units;
- completed and total bytes;
- throughput and ETA inputs;
- provider, region, Pod, volume, and lease identities;
- cache decision and artifact identity;
- price and spend observations;
- provider acknowledgements and other evidence; and
- schema version.

On reload or reconnect, the client requests events after its last acknowledged
sequence, reconstructs current state, then joins the live stream without a gap.
Terminal-state precedence is explicit: a delayed producer cannot reopen a closed
job.

### User interface

The primary surface is a persistent **Cloud Jobs** drawer or panel, independent
of the canvas. It displays:

- preflight and recommendation result;
- current lifecycle stage;
- elapsed time;
- bytes transferred and total bytes;
- smoothed throughput;
- ETA range and confidence;
- GPU, provider, region, and Pod identity;
- hourly rate and estimated spend;
- cache hits, misses, and preparation work;
- cancellation, provider termination, and billing state; and
- resumable event history.

The canvas group title remains a compact secondary indicator rather than the only
place where state exists.

### Progress model

Overall progress is a semantic critical path:

1. readiness;
2. provisioning;
3. worker boot;
4. dependency preparation;
5. execution;
6. result transfer; and
7. resource closure.

Within a phase, use measurable work such as bytes, files, nodes, or sampler steps.
Across phases, use historical timings from comparable completed jobs. Progress is
monotonic, terminal success is 100%, and ETA is a range with confidence rather
than a falsely precise countdown.

For authenticated Hugging Face transfers, resolve expected size where possible
and observe temporary-file growth or instrument the transport. Emit bytes and
rolling throughput periodically. When total size is unknowable, preserve honest
elapsed-only feedback rather than manufacturing a percentage.

## Closure and billing assurance

Logical cancellation is necessary but insufficient. Paid execution is governed
by a persisted, revocable `JobLease`:

```text
ACTIVE
  → REVOKED
  → TERMINATING
  → PROVIDER_TERMINATED
  → CLOSED
```

A lease binds:

- job and worker profile;
- exact provider and Pod ID;
- expiry and renewal token/state;
- runtime and dollar budget;
- termination attempts and deadlines;
- provider state; and
- provider acknowledgement used as the billing-stop receipt.

Cancellation:

1. atomically revokes the lease;
2. stops future renewals;
3. emits `cancellation_requested`;
4. asks the worker to abort at download-chunk, cache-phase, and graph-node
   boundaries;
5. independently requests provider termination;
6. retries termination idempotently;
7. reconciles the Pod against provider inventory; and
8. closes only after recording provider acknowledgement.

The UI states are explicit:

```text
Cancellation requested
→ Aborting workflow
→ Terminating RunPod Pod
→ Pod terminated — billing stopped
```

Coordinator startup reconciles every non-closed lease against provider inventory
and terminates orphans. The product must verify which RunPod state transition is
the authoritative billing boundary before promising more precision than the
provider supplies.

## Repeat-run acceleration

### Fast cache trust

First-seen or changed objects receive full digest verification. A subsequent hot
restore may use a signed trust receipt that binds:

- manifest signature;
- artifact digest and size;
- provider volume and object identity/generation;
- runtime compatibility;
- verification timestamp; and
- scrub policy and expiry.

Recently attested immutable objects use metadata verification on the hot path.
Background sampling and scheduled full audits preserve integrity pressure. Any
mismatch quarantines the artifact, degrades the volume, and falls back safely.
Policy may still require full verification for sensitive assets.

Probabilistic verification must never be described as equivalent to full
verification without an explicit threat model and confidence semantics.

### Prepared-state capsules

A canonical preflight manifest becomes the identity of a reproducible workflow
closure. Frequently used or expensive cold manifests may be promoted into signed
prepared-state capsules containing:

- worker image digest;
- model digests and sources;
- custom-node repositories and pinned commits;
- locked Python dependencies;
- CUDA, Python, platform, and runtime ABI;
- deterministic setup and verification hooks;
- privacy and cache policy; and
- compatible storage locations.

Cloud Offload begins with its own capsule and bundle format rather than attempting
to standardize the whole ComfyUI ecosystem.

Custom-node authors may optionally provide a readiness manifest declaring node
types, models, runtime downloads, system packages, Python dependencies, ABI
constraints, and setup hooks. Undeclared dynamic behavior remains a visible
preflight uncertainty rather than being silently declared ready.

### Regional replication

Replication starts in shadow recommendation mode. The controller records demand,
misses, transfer cost, storage cost, avoided GPU-idle time, regional capacity,
and replica age. A recommendation includes source, destination, bytes, cost,
expected hits, expected time saved, expiry, and budget impact.

Automatic replication is enabled only after shadow recommendations demonstrate
value. Copies use provider object APIs such as RunPod S3 and do not rent a GPU.
Every replica has a spend ceiling, single-flight protection, and expiry or
eviction plan. Scheduling prefers compatible replicas while preserving explicit
cold fallback.

## Delivery milestones

### Milestone 0 — Measurement contract and production scorecard

- Define and persist `JobEventV2`.
- Add replay, snapshot, and support-bundle APIs.
- Build [spend-capped benchmark automation](production-benchmark.md).
- Run alternating fresh-Pod cold/hot jobs.
- Inject cancellation, provider, storage, corruption, and restart failures.
- Record startup, preparation, execution, closure, and cost distributions.

Exit:

- one job has an explainable critical path;
- reload reconstructs authoritative state;
- duplicate and reordered events do not regress it;
- failure results are comparable JSON; and
- no validation run leaves an orphaned Pod.

### Milestone 1 — Preflight, GPU recommendation, and rental confirmation

- Implement `PreflightReportV1` and `POST /api/preflight`.
- Produce deterministic blockers, warnings, unknowns, preparation plan, and
  volatile estimates.
- Rank viable GPUs under `balanced`, `cheapest`, `fastest`, and `manual` policy.
- Show provider, GPU, region, hourly price, total-cost range, timing range, cache
  coverage, and recommendation rationale.
- Add the default ten-second auto-start confirmation.
- Add Start now, Cancel, Choose another GPU, and Don't show again.
- Persist confirmation policy and related settings.
- Revalidate quote, capacity, region, and storage immediately before launch.
- Restart confirmation after a material recommendation change.
- Preserve hard spend and policy limits even when normal confirmation is hidden.

Exit:

- deterministic blockers never create a Pod;
- the reference workflow produces a stable manifest digest;
- recommendation decisions are explainable;
- no Pod is created before countdown completion or explicit Start now;
- revalidation catches material price, storage, or capacity changes; and
- ordinary healthy preflight adds little friction.

### Milestone 2 — Persistent visibility

- Ship the Cloud Jobs drawer.
- Reconstruct status after reload or reconnect.
- Add bytes, throughput, ETA range, spend, Pod, and billing state.
- Normalize phase-aware overall progress and terminal 100%.
- Show cold fallback and provider failures explicitly.

Exit:

- state returns within two seconds after reload;
- no active job appears frozen at a static early percentage;
- large downloads report measurable progress when the source exposes size; and
- canvas state is no longer the only source of truth.

### Milestone 3 — Revocable leases and billing closure

- Persist `JobLease` and renewal state.
- Add cooperative worker cancellation boundaries.
- Add independent idempotent provider termination.
- Record provider termination receipts.
- Reconcile non-closed leases on startup.
- Add runtime and dollar circuit breakers.

Exit:

- cancellation during every tested phase removes the exact Pod;
- a job cannot claim billing stopped without provider acknowledgement;
- worker or coordinator death creates no orphan beyond the reconciliation SLO;
- late callbacks cannot reopen terminal state; and
- cancelled work cannot publish unverified shared cache state.

### Milestone 4 — Fast trusted restore

- Add cache trust receipts.
- Skip full hot-path reads only for recently attested immutable objects.
- Add sampled background scrubbing and scheduled complete audits.
- Quarantine mismatches and preserve safe cold fallback.

Exit:

- trusted hot restore performs no complete artifact read;
- corruption canaries are detected and quarantined;
- first-seen integrity guarantees are unchanged; and
- median hot preparation is no more than 25% of median cold preparation.

### Milestone 5 — Prepared workflow capsules

- Define canonical `PreparedStateManifest` and capsule schema.
- Add optional custom-node readiness manifests.
- Build reproducible signed custom-node and environment bundles.
- Promote capsules using measured reuse and cold-start cost.

Exit:

- missing credentials, artifacts, disk, and incompatible nodes fail free;
- fresh Pods restore models and custom-node state;
- identical closures produce identical capsule digests; and
- undeclared dynamic behavior remains visible as uncertainty.

### Milestone 6 — Demand-weighted regional replication

- Collect regional demand and measured benefit.
- Produce shadow recommendations.
- Add budgets, TTLs, eviction, and single-flight copies.
- Replicate through provider object APIs without a GPU.
- Enable automation only after shadow accuracy is demonstrated.

Exit:

- every automatic replica has a reason, budget, and expiry;
- monthly storage spend cannot exceed policy;
- duplicate copies are suppressed;
- scheduling uses compatible replicas without hiding cold fallback; and
- regional loss has a rehearsed recovery path.

### Milestone 7 — Production release gate

Cloud Offload graduates from beta only after:

- thirty consecutive full canary matrices pass across supported images and
  regions;
- there are zero orphaned Pods;
- cancellation and provider-acknowledgement SLOs pass;
- reload, reconnect, and event-ordering tests pass;
- deterministic preflight false-readiness is zero in the validation matrix;
- hot/cold acceleration meets target;
- corrupt cache and stale-manifest recovery pass;
- regional failure and cold fallback pass;
- every failure can be explained from a redacted support bundle; and
- GPU and storage spending remain within configured budgets.

## Success metrics and SLO candidates

These are product targets to validate and tighten with baseline data:

| Area | Target |
| --- | --- |
| Preflight | No known deterministic blocker reaches Pod creation. |
| Recommendation | Every choice has a persisted explanation and cost estimate. |
| Confirmation | No launch before Start now or countdown completion; mandatory safety overrides always win. |
| Reload | Current state reconstructs within two seconds on a healthy local coordinator. |
| Progress | Monotonic; successful terminal state is 100%; ETA carries confidence. |
| Cancellation | Exact Pod reaches provider-confirmed termination within the provider-specific SLO. |
| Orphans | Zero Pods remain beyond two reconciliation intervals. |
| Hot restore | Median preparation is at most 25% of alternating cold baseline. |
| Integrity | Corrupt or incompatible prepared state never reaches ComfyUI silently. |
| Replication | Automated storage spend never exceeds configured budget. |
| Supportability | A redacted journal explains every failed canary without raw server logs. |

## Non-goals and rejected shortcuts

- Replacing GPU rental with a preflight or storage product. Rental and execution
  remain the core promise.
- Guaranteeing volatile provider capacity, price, boot time, or bandwidth.
- Making confirmation a mandatory modal on every run after the user deliberately
  disables it within safety limits.
- Treating hourly price as total job cost.
- Relying on worker cooperation alone to terminate paid resources.
- Claiming billing stopped from local state without provider evidence.
- Racing byte ranges across multiple sources before basic byte telemetry and
  integrity semantics are mature.
- Executing partial graphs while dependencies arrive if that requires a
  distributed scheduler rewrite.
- Presenting probabilistic cache verification as full verification.
- Standardizing the entire ComfyUI custom-node ecosystem before Cloud Offload can
  reproducibly bundle its own supported profiles.
- Automatically replicating state before demand, benefit, retention, and budget
  policies are proven in shadow mode.
- Persisting live VRAM, Python process objects, allocator state, or CUDA graphs
  across unrelated workers.

## Decisions recorded

- GPU rental and execution remain the central product promise.
- Readiness, visibility, closure, and acceleration are additive trust promises.
- Preflight runs before paid provider mutation.
- Healthy preflight flows automatically into a short rental confirmation.
- Balanced GPU recommendation is the proposed default.
- Default confirmation auto-starts after approximately ten seconds.
- The user can start immediately, cancel, choose another GPU, or disable future
  ordinary confirmations.
- The same confirmation behavior is controlled through persistent settings.
- Hard spend and policy limits remain active when confirmation is disabled.
- Material recommendation changes force a fresh decision when required by
  safety policy.
- Total estimated job cost matters more than hourly price alone.
- The coordinator event journal is the authoritative lifecycle record.
- Billing closure requires provider acknowledgement.
- Cache fast paths require explicit trust evidence and background integrity work.
- Prepared capsules begin as a Cloud Offload-owned format.
- Regional replication begins in shadow mode and is budget constrained.
- Production readiness is earned through continuous failure rehearsal, not one
  successful end-to-end run.

## Open questions

1. Which exact RunPod state and API response constitute authoritative billing
   closure, and what uncertainty must remain visible?
2. What price and estimated-cost deltas count as a material recommendation
   change?
3. Should `material_changes` rather than `always` become the long-term default
   confirmation setting after a user establishes history?
4. What minimum observation count and confidence are required before historical
   execution timings affect GPU recommendation?
5. How should per-job storage and transfer cost be attributed when a replica
   serves many future jobs?
6. Which custom nodes can provide complete readiness declarations, and how should
   undeclared runtime downloads be sandboxed or detected?
7. What cryptographic and object-generation evidence is sufficient to skip a
   complete hot-path read for each supported storage backend?
8. What provider-specific cancellation and orphan-reconciliation SLOs are
   realistic?
9. When should a frequently used preflight manifest be promoted into a capsule?
10. What measured avoided-GPU-idle threshold justifies a regional replica?

## Delivery shape

This plan should ship as narrow, reversible changes behind versioned schemas and
feature flags:

1. job journal and benchmark harness;
2. read-only preflight;
3. recommendation and confirmation UI;
4. persistent Cloud Jobs UI and transfer telemetry;
5. leases, termination receipts, and reconciliation;
6. cache trust receipts and scrubber;
7. custom-node bundles and prepared capsules;
8. replication shadow mode; and
9. budgeted automatic replication and the production release gate.

Each step preserves the existing execution path and stateless fallback. No phase
requires waiting for the complete roadmap before delivering user value.

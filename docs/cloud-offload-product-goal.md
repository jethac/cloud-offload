# Cloud Offload product goal and delivery plan

> Status: **canonical product goal**
> Last updated: **2026-07-29**
> Program status: **in progress — Milestone 0 production evidence is the current gate**
> Scope: the end-to-end Cloud Offload product across the coordinator, dispatcher,
> workers, provider connectors, prepared storage, and ComfyUI extension.
>
> The detailed storage subsystem design remains in
> [Storage-aware Cloud Offload](storage-aware-cloud-offload.md). That PRD is a
> supporting design; this document defines the product outcome it serves.

This is the single source of truth for the goal, product promises, requirements,
decisions, delivery sequence, current implementation state, and release evidence.
Supporting design documents may add implementation detail but may not weaken an
acceptance criterion here. A milestone is complete only when its exit criteria
have evidence; merged code alone is not completion.

## Goal control

This document is also the program control surface. It answers four questions
without requiring reconstruction from chat history, local logs, or merged pull
requests:

1. **What outcome are we pursuing?** The product contract and promise stack
   below.
2. **What has already been decided?** The decisions, non-goals, requirements,
   and milestone exits in this document.
3. **What has actually been proved?** The delivery ledger and validation record,
   which distinguish merged implementation from accepted production evidence.
4. **What happens next?** The current execution state and the first unmet exit
   criterion in milestone order.

The active program gate is **M0 production evidence**. M1 through M7 remain part
of this same goal; they are not a backlog that can be silently deferred or a new
goal that must be rediscovered later. The next implementation milestone may
start only after M0 evidence is durable, redacted, comparable, and orphan-free.

Raw benchmark plans, workflows, hooks, support bundles, and service logs remain
local under `.runlogs/`. The durable record committed to the repository contains
only safe request digests, aggregate timings and cost, opaque resource/job IDs,
test results, cleanup receipts, and explicit pass/fail conclusions. Credential
values, raw prompts/workflows, private paths, signed URLs, hook commands, and
secret endpoints are never goal-document evidence.

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

## Definition of program completion

The goal is complete only when all eight milestones in this document have met
their exits and the Milestone 7 production release gate has passed. In
particular, completion requires all of the following at the same time:

- a ComfyUI user can submit a supported workflow without managing provider
  infrastructure;
- deterministic readiness failures stop before paid compute is created;
- Cloud Offload recommends a compatible GPU and discloses expected total cost;
- the default ten-second confirmation behaves as specified and can be controlled
  by both “Don't show again” and persistent settings;
- the lifecycle survives reload and explains progress, uncertainty, spend, and
  resource identity from an authoritative journal;
- cancellation and normal completion end with provider-confirmed billing closure;
- compatible repeat runs are measurably faster without weakening integrity;
- storage placement, replication, retention, and spend stay within user policy;
- cold, hot, cancellation, provider, storage, corruption, restart, and regional
  fallback canaries pass continuously; and
- the coordinator and ComfyUI extension changes are released together wherever
  the user contract crosses both repositories.

Anything less is an intermediate delivery, not the completed goal.

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

## Why these requirements exist

The roadmap is grounded in observed end-to-end failures rather than hypothetical
polish work.

| Observation | What it revealed | Product consequence |
| --- | --- | --- |
| ComfyUI reported `Cloud Offload partition references missing node 70` for the `inpainting` workflow. | Visible boxed-subgraph node IDs differ from the executable IDs emitted by `graphToPrompt`; model declarations can also live inside subgraph definitions. | Compilation must expand nested boxed subgraphs and preflight must resolve the exact executable closure. This was fixed and proven by extension PR #2. |
| Prompt `4009468b-129a-4018-b5dd-d9e7fe1a2c13` existed in ComfyUI but returned 404 from the coordinator job APIs. | A local prompt ID can be mistaken for a cloud job ID, making support and cancellation ambiguous. | Every surface and support bundle must expose correlated prompt, job, partition, lease, and provider-resource identities. |
| ComfyUI was started without the dispatcher during an end-to-end debugging session. | A healthy UI and coordinator do not prove the rental path is operating. | Readiness and operator status must cover the dispatcher, ingress, provider credentials, worker profile, and ability to launch—not just HTTP health. |
| Healthy provisioning and model preparation appeared frozen at a static early percentage. | Canvas-only coarse progress makes long paid startup look hung. | Durable phase events, byte telemetry, elapsed time, ETA confidence, and a persistent Cloud Jobs surface are required. Extension PR #4 added an initial canvas improvement; it is not the final visibility milestone. |
| A large model download was slow and authentication was uncertain. | Hugging Face authentication affects throttling as well as gated-model access. | Preflight must verify credential presence, workers must receive authenticated download capability, and transfer telemetry must expose bytes and throughput without exposing tokens. |
| Prepared-state restore controls were hard to discover. | A capability is not complete when its control and current state are obscure. | Prepared storage needs an explicit settings entry, action-bar access, policy explanation, cache status, and visible cold fallback. |
| Local interruption did not originally guarantee cloud cancellation. | Stopping a prompt and stopping provider billing are different operations. | ComfyUI interruption must revoke the cloud job, and the coordinator must independently terminate and reconcile the exact paid resource. |
| The first prepared-storage run still performed a 4.23 GB authenticated download and complete verification. | Durable storage helps only after population, and a nominal cache hit can remain I/O-bound. | Cold and hot paths must be measured separately; trusted hot restore, capsules, and placement must prove time and cost saved. |

## System boundary and repositories

The product is one system delivered through multiple components:

| Component | Repository or artifact | Responsibility |
| --- | --- | --- |
| Coordinator, dispatcher, providers, worker, storage controller, benchmark | `jethac/cloud-offload` | Authoritative job state, preflight, recommendation, scheduling, paid lifecycle, prepared state, and evidence. |
| ComfyUI extension | `jethac/ComfyUI-Cloud-Offload` | Partition compilation, settings, confirmation, persistent job UX, cancellation intent, and local/cloud identity correlation. |
| Worker runtime | `ghcr.io/jethac/cloud-offload-worker-comfyui` | Reproducible ComfyUI environment, authenticated staging, prepared-state restore/population, execution, and cooperative abort. |
| RunPod | Provider API, Pods, network volumes, and S3-compatible object API | Paid compute truth, region-constrained volume attachment, durable prepared bytes, and termination evidence. |
| Hugging Face | Authenticated artifact source | Immutable model resolution and authenticated transfers for gated and rate-sensitive downloads. |

An end-to-end acceptance test uses the extension to submit through the real
coordinator and dispatcher to a real provider worker. Starting ComfyUI alone,
calling the coordinator directly, or passing backend tests does not by itself
validate the product journey.

## Requirement inventory and traceability

| ID | Requirement | Primary milestone | Current state |
| --- | --- | --- | --- |
| `EXEC-1` | Rent and operate a compatible GPU without user-managed infrastructure. | M1 | Working baseline; recommendation and preflight remain. |
| `READY-1` | Prove deterministic requirements before provider mutation. | M1 | Planned. |
| `RECOMMEND-1` | Recommend provider/GPU/region using expected total time and cost, including prepared-state locality. | M1 | Planned. |
| `CONFIRM-1` | Show recommendation, cost, rationale, and a default ten-second auto-start confirmation. | M1 | Planned. |
| `CONFIRM-2` | Provide Start now, Cancel, Choose another GPU, Don't show again, and equivalent persistent settings. | M1 | Planned. |
| `JOURNAL-1` | Persist an idempotent, replayable, lifecycle-authoritative `JobEventV2` journal. | M0 | Implemented and merged; production evidence pending. |
| `VISIBLE-1` | Reconstruct a persistent job surface with phases, bytes, throughput, ETA confidence, spend, and identities. | M2 | Initial canvas feedback merged; durable drawer pending. |
| `CLOSE-1` | Revoke work and prove provider termination before claiming billing stopped. | M3 | Logical cancellation baseline exists; leases and provider receipt pending. |
| `STORAGE-1` | Opt into or adopt RunPod storage before cached rental and attach it to compatible future Pods. | M4 foundation | Initial managed/adopted-volume MVP merged. |
| `STORAGE-2` | Track prepared contents and location, and prefer offers near compatible state with explicit cold fallback. | M4/M6 | Initial one-region placement merged; adaptive multi-region policy pending. |
| `ACCEL-1` | Make compatible repeat runs measurably faster with trusted restores and capsules. | M4/M5 | Durable population/restore baseline merged; fast trust and capsules pending. |
| `REPLICA-1` | Replicate prepared state only for measured benefit, within budget and TTL. | M6 | Planned; shadow mode first. |
| `EVIDENCE-1` | Produce redacted, comparable cold/hot/failure scorecards without orphaned resources. | M0 | Cold/hot plus cancellation, provider, storage, and restart canaries are accepted; corruption and the committed redacted projection remain. |
| `RELEASE-1` | Pass the continuous production matrix and budget gates. | M7 | Pending. |

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

## Delivery ledger

This ledger records merged implementation evidence across both repositories.
“Merged” means the code landed; it does not override the milestone exits below.

### Coordinator, dispatcher, worker, and provider repository

| PR | Delivered | Evidence status |
| --- | --- | --- |
| [#1](https://github.com/jethac/cloud-offload/pull/1) | Worker reliability, runner readiness, node-pack staging, VRAM/profile matching, and early worker status. | Merged as `4c3911a`. |
| [#2](https://github.com/jethac/cloud-offload/pull/2) | Storage-aware Cloud Offload PRD. | Merged as `60a2588`. |
| [#3](https://github.com/jethac/cloud-offload/pull/3) | RunPod volume lifecycle, prepared manifests/CAS, storage-aware placement, worker restore/population, and API foundations. | Merged as `efc8ceb`; live validation followed in #4. |
| [#4](https://github.com/jethac/cloud-offload/pull/4) | Durable publication and detailed prepared-storage/startup events. | Merged as `0059e31`; reference prepared run completed. |
| [#5](https://github.com/jethac/cloud-offload/pull/5) | Initial canonical product goal. | Merged as `5854bce`; this document now supersedes that initial snapshot. |
| [#6](https://github.com/jethac/cloud-offload/pull/6) | `JobEventV2`, producer idempotency, replay/snapshot/support-bundle APIs, redaction, anti-spoofing, and terminal precedence. | Merged as `92bbbf9`; 499 tests passed at merge. |
| [#7](https://github.com/jethac/cloud-offload/pull/7) | Atomic lifecycle journal, semantic phase protection, state seeding, duplicate collapse, and rollback proof. | Merged as `91864a9`; 504 tests passed at merge. |
| [#8](https://github.com/jethac/cloud-offload/pull/8) | Spend-capped production benchmark/scorecard, provider attribution and cleanup, cold/hot validation, and five-class failure injection. | Merged as `c1383fe`; 513 tests passed at merge. |
| [#9](https://github.com/jethac/cloud-offload/pull/9) | Explicit `force_execution` for fresh-Pod partition benchmarks without disabling prepared-state caching. | Merged as `781f212`; 513 tests passed at merge. |
| [#10](https://github.com/jethac/cloud-offload/pull/10) | Runtime `keyring` dependency required for Windows credential resolution. | Merged as `ce20543`; credential tests passed. |
| [#11](https://github.com/jethac/cloud-offload/pull/11) | Consolidated the full product goal, promise hierarchy, traceable requirements, M0–M7 exits, decisions, and release gate into this canonical document. | Merged as `3e42f61`; documentation is authoritative, but its milestones still require evidence. |
| [#12](https://github.com/jethac/cloud-offload/pull/12) | Made benchmark cache state authoritative: cold forces prepared storage off, hot requires an existing confirmed volume, full configuration is restored, and exact Pods are cleaned up. | Merged as `9675fdf`; 517 tests passed. |
| [#13](https://github.com/jethac/cloud-offload/pull/13) | Added reversible storage, corruption, and coordinator-restart production canaries plus health PID identity. | Merged as `429d405`; 520 tests passed. |
| [#14](https://github.com/jethac/cloud-offload/pull/14) | Added two-phase pre-submit failure hooks so reviewed corruption setup settles before job submission and Pod creation, with unconditional idempotent cleanup. | Merged as `4900aa6`; 522 tests passed. |
| [#15](https://github.com/jethac/cloud-offload/pull/15) | Consolidated the complete program record and replaced the unsafe Windows signal-zero PID probe with native process-state inspection. | Merged as `79faa25`; 525 tests and real live/absent PID probes passed. |
| [#16](https://github.com/jethac/cloud-offload/pull/16) | Made restart canaries prove journal replay and persist cancellation through the replacement coordinator instead of depending on unrelated image startup. | Merged as `7fdc37c`; 526 tests passed and the production restart canary passed. |
| [#17](https://github.com/jethac/cloud-offload/pull/17) | Made corruption canaries inject an isolated tiny artifact, publish a temporary signed manifest, provide a valid coordinator fallback, and remove all synthetic state during idempotent cleanup. | Merged as `0ea1754`; 527 tests passed. The first production run proved fresh-object isolation and cleanup but exposed stale manifest discovery, so corruption is not yet accepted. |
| [#18](https://github.com/jethac/cloud-offload/pull/18) | Recomputes the injected requirement profile, publishes signed manifests by immutable exact ID, falls back to that verified object when a mounted index is stale, and starts corruption observation at `cache_mount_ready`; also brings this goal record through the fresh-object campaign. | Merged as `f4d9cf9`; 529 tests passed. A bounded replay proved that the hook must fingerprint the configured launch profile name, not its requested capability name. |
| [#19](https://github.com/jethac/cloud-offload/pull/19) | Resolves the requested worker capability to the normalized configured launch profile before computing the corruption canary fingerprint and records the bounded failed exact-ID replay. | Merged as `3e43ff5`; 530 tests passed. The next replay proved exact selection and direct loading, then exposed first-write object caching. |
| [#20](https://github.com/jethac/cloud-offload/pull/20) | Writes corrupt bytes as the first and only value of the synthetic S3 key, keeps valid bytes only in coordinator fallback storage, and records the exact-selection replay. | Merged as `b1e6509`; 530 tests passed. Its bounded replay proved that a deterministic canary still reused the same digest and mounted object identity across campaigns. |

### ComfyUI extension repository

| PR | Delivered | Evidence status |
| --- | --- | --- |
| [#1](https://github.com/jethac/ComfyUI-Cloud-Offload/pull/1) | Cancel the associated cloud job when ComfyUI execution is interrupted. | Merged as `66b1814`; provider-confirmed closure remains M3. |
| [#2](https://github.com/jethac/ComfyUI-Cloud-Offload/pull/2) | Expand nested boxed subgraphs and resolve workflow-declared Hugging Face assets. | Merged as `d976769`; 73 Python tests passed, 3 skipped, 45 JavaScript tests passed, and live inpainting completed. |
| [#3](https://github.com/jethac/ComfyUI-Cloud-Offload/pull/3) | Prepared-storage opt-in and policy controls. | Merged as `21e66ca`; broader settings/visibility remain. |
| [#4](https://github.com/jethac/ComfyUI-Cloud-Offload/pull/4) | Action-bar discovery, write-only RunPod S3 credential setup, and monotonic startup/cache feedback in the partition title. | Merged as `40c3cf6`; 74 Python tests passed, 3 skipped, and 5 focused JavaScript tests passed. |

## Validation record

The following production observations are evidence for specific capabilities,
not blanket production-readiness claims:

- Inpainting job `f86f0bc6-860e-403f-aa1a-87b4b57505d4` completed at 100% on
  RunPod with six resolved assets after the boxed-subgraph/Hugging Face fix.
- Prepared-storage job `b9d44715-7161-4f7c-994a-bd7dc1792d3f` completed on
  RunPod using partition `fdf3fdd1-35b0-44bd-83ef-81dddbb87666` and the
  `comfyui-partition-v1` runtime profile. It recorded five prepared-cache hits
  and one authenticated 4.23 GB model download. Durable copy took approximately
  7.5 seconds; read-back verification and publication took approximately 15.3
  seconds.
- The validated worker artifact for that path is
  `ghcr.io/jethac/cloud-offload-worker-comfyui@sha256:ee3c8b1e4288509c5dd6e0b9d7640933d33be86a459826c23179265b82e2b705`.
- Replay, snapshot, support-bundle, journal-authority, concurrency, rollback,
  benchmark validation, force-execution, and credential-resolution tests pass in
  the merged backend history shown above.
- A spend-capped fresh-Pod cold/hot campaign used the same safe request digest,
  `db85d75dd5ee549db250d27fdf1679a7b1ddea16a01e0afcba4ade0a53afa527`,
  for jobs `34172fcd-4a6b-4ffb-aa6e-562ff709f841` and
  `c431bfb2-98df-490a-a7d5-bde3a950e485`. It passed configuration restoration,
  exact-Pod cleanup, final empty provider inventory, and redaction audit. Cold
  took 432.500 seconds at a conservative $0.179007; hot took 394.531 seconds at
  $0.163292, for a conservative campaign total of $0.342299.
- The cold run spent 80.864 seconds in weight download. The hot run produced six
  prepared-artifact hits with no misses or downloads, but still spent 64.425
  seconds reading and verifying prepared artifacts. This is an accepted M0
  baseline and evidence that current prepared state works. An 8.8% end-to-end
  improvement from one pair does **not** satisfy the M4 acceleration target.
- A spend-capped five-class failure campaign conservatively accounted for
  $0.920037 and ended with prepared-storage policy restored, empty provider
  inventory, and no orphan audit errors. It accepted three canaries:
  cancellation removed the exact Pod in 34.500 seconds; a terminated provider
  Pod was replaced and the job completed in 347.391 seconds; and a deliberately
  missing strict storage binding failed before provider launch, restored the
  valid binding, then launched and completed in 395.375 seconds.
- That same campaign did **not** accept its corruption or coordinator-restart
  cases. The corruption mutation reached object storage but the mounted path
  served previously cached valid bytes, so no quarantine event was observed and
  the original object was restored. The restart case encountered an unusually
  long worker startup and reached its $0.30 scenario ceiling before execution,
  so the restart hook never fired. Both exact Pods were removed. These outcomes
  are retained as failed evidence rather than rewritten as passes.
- A restart-only retry that still waited for `execution_start` remained in
  `runner_starting` for 1,012.250 seconds, reached its scenario timeout, and was
  cancelled. The exact Pod was absent after 9.328 seconds, policy was restored,
  provider inventory was empty, and the conservative cost was $0.418959. This
  is startup-latency evidence, not restart evidence.
- The corrected restart canary triggered immediately after provider creation,
  stopped coordinator PID `38032`, brought replacement PID `49336` healthy,
  proved job `05156c58-cff6-4164-915d-5273ec72519f` was replayable, and persisted
  its cancellation through that replacement. Exact Pod `tpoita6ieh2bq5` was
  provider-absent after one termination request. The case passed in 29.234
  seconds with a conservative $0.012100 compute upper bound, restored policy,
  empty final inventory, and no orphan or audit error.
- The PR #17 fresh-object corruption campaign used safe request digest
  `8c44c8ee4ddca337b148212dca24e2d9c6e8c068ea5300091f3b33c172310106`
  for job `d37b5054-d80f-4d59-87f4-dbd18e6d310c` and exact Pod
  `arlvyvhu8lm1we`. The pre-submit hook published a unique 31-byte corrupt blob,
  temporary signed generation, registry projection, and valid coordinator
  fallback. The run completed in 420.500 seconds with a conservative $0.174040
  compute upper bound, and cleanup removed the Pod, generation, blob, manifest,
  registry/invalidation/quarantine state, and coordinator fallback. Provider
  inventory was empty and prepared-storage policy was restored to `smart`.
- That fresh-object run is safe failed evidence, not an accepted corruption
  canary. Placement selected no prepared manifest and the worker emitted
  `cache_artifact_miss` with reason `manifest_not_found`; no
  `cache_artifact_quarantined` event occurred. The injected asset changed the
  actual requirement profile to
  `sha256:77ea27b92fac7d4b45f8e941c1a545d5368d985252453b0bf9d8419912f386f2`,
  while the canary manifest retained the prior profile. Cross-profile lookup
  then depended on the mounted mutable `indexes/latest`, which did not contain
  the newly published manifest. This proves the fresh blob removed cache aliasing
  and isolates the remaining defect to exact manifest selection and visibility.
- A worker image built from PR #18 was pushed as
  `ghcr.io/jethac/cloud-offload-worker-comfyui@sha256:2ecdf8b88ef758257a50aae9e24e90cea914dad731a16157bc5d84683d1db509`.
  Its in-image smoke test confirmed the exact manifest-by-ID code and source
  revision `f4d9cf9751a00d31e24f287140ec19993ee129a3`.
- The first PR #18 production replay created job
  `d66cf2c0-67d9-4272-b785-2b912f3e7327` and exact Pod `ni6c882lcryxyu`.
  The benchmark was stopped as soon as placement reported `complete: false` and
  `manifest_ids: []`; waiting for worker startup could not turn that state into
  an accepted exact-manifest canary. The exact Pod became provider-absent, the
  job was closed as failed, provider inventory returned empty, and the synthetic
  state, blob, coordinator fallback, and policy were clean. Because the operator
  stopped the command before scorecard completion, this run has no accepted cost
  figure; its configured scenario and campaign ceiling was $0.50.
- The replay isolated a second name-identity defect. The hook fingerprinted the
  requested capability `comfyui-partition-v1` as
  `sha256:fed17d04a6deb7117c56bd13256da11bed6b12d22088813bce89d5f8a3db5e24`.
  The dispatcher resolved that capability to configured launch profile
  `comfyui`, so the job fingerprint was
  `sha256:86a2004c166c05ec60f84d39c4fd0f3a1b565afd175ab3425689245b54185ed8`.
  The launch profile name is part of both the runtime identity and profile key.
  The canary must therefore use the normalized resolved profile's `name`, which
  is the same value used by `Dispatcher._launch_worker`.
- The PR #19 replay created job `e4c057bd-5cf1-4eff-90db-38f4e64e6637` and
  exact Pod `4qx520ec84h0lh`. Placement selected exact manifest
  `sha256:798a0d6878db984ed3b1804e823ec53619a3d2af0c4e9bf271d023e64966f664`;
  the worker emitted `cache_mount_ready`, verified the direct manifest, and
  found the synthetic digest
  `sha256:cec70f0e391431bde36301228ab7b83025c929f2ca61a549300fd55b30860311`.
  This directly proves the profile-name and immutable manifest-by-ID corrections.
- That PR #19 replay is still safe failed evidence. The worker reported the
  synthetic artifact as a verified cache hit instead of quarantining it. The
  canary had written valid bytes to the new S3 key to verify publication, then
  replaced them with corrupt bytes. The mounted RunPod volume retained the first
  valid object value. The operator stopped the case immediately; the exact Pod
  became provider-absent, the job closed as failed, provider inventory returned
  empty, storage policy returned `smart`, and all synthetic state was absent.
  The interrupted command produced no accepted cost figure; the configured
  scenario and campaign ceiling was $0.50.
- The merged PR #20 replay created job
  `c9dbd0d7-8628-4273-b87a-dfd87d1b0a33` and exact Pod `fqh9x6qm5xtn7c`.
  Placement selected exact manifest
  `sha256:e1af79787de255c0dbb7e288abfc540829451b1e3c5b83682a5d821f1dd9d477`,
  and the worker loaded it through the immutable direct path. The worker again
  reported synthetic digest
  `sha256:cec70f0e391431bde36301228ab7b83025c929f2ca61a549300fd55b30860311`
  as a valid hit instead of quarantining it.
- The third replay proved a second object-identity error. The canary derived its
  payload, digest, and object key only from the scenario name. All three recent
  campaigns therefore used the same synthetic digest. Deleting the control-plane
  object did not prove that a later mounted volume view had discarded valid
  bytes from an earlier campaign. The operator stopped the replay immediately.
  The job closed as failed, the exact Pod became provider-absent, provider
  inventory returned empty, storage policy returned `smart`, and the synthetic
  state, blob, and coordinator fallback were absent. The interrupted command has
  no accepted cost figure; its configured scenario and campaign ceiling was
  $0.50.

The accepted cold/hot scorecard and four accepted failure canaries prove a
larger part of M0, but M0 is not complete until the corruption canary passes and
a compact redacted evidence projection is committed.

### M0 evidence matrix

| Evidence | Result | Durable conclusion |
| --- | --- | --- |
| Fresh-Pod cold/hot pair | **Accepted** | Prepared state produces real hits, but the measured 8.8% end-to-end improvement is only a baseline and does not meet the M4 acceleration target. |
| Cancellation | **Accepted** | The attributable Pod was removed in 34.500 seconds and cleanup completed. |
| Provider interruption | **Accepted** | A terminated Pod was replaced and the job completed. |
| Strict storage failure | **Accepted** | Invalid storage failed before provider launch; restored configuration then completed. |
| Coordinator restart | **Accepted** | Replacement health, journal replay, replacement-owned cancellation, and exact provider cleanup passed. |
| Corruption, cached-object attempts | **Failed safely** | Mounted valid bytes defeated mutation; exact resources and configuration were cleaned up. |
| Corruption, fresh-object attempt | **Failed safely** | Unique-object isolation worked; stale mutable-index discovery prevented exact manifest restore and quarantine. Cleanup was complete. |
| Corruption, first exact-ID attempt | **Failed safely** | The immutable worker path was present, but capability-name fingerprinting did not match the dispatcher's configured launch-profile fingerprint. The run stopped at placement and cleanup was complete. |
| Corruption, first-write attempt | **Failed safely** | Exact manifest selection and loading passed, but writing valid bytes before corrupt bytes let the mount retain the valid first value. The run stopped after the synthetic verified hit and cleanup was complete. |
| Corruption, deterministic-campaign attempt | **Failed safely** | Corrupt-first publication worked, but each campaign reused the same digest and object key. A mounted volume view retained earlier valid bytes. The run stopped after the synthetic verified hit and cleanup was complete. |
| Compact redacted projection | **Required** | Accepted raw scorecards remain local under `.runlogs/`; a safe comparable projection still must be committed. |

## Current execution state and immediate next work

Status snapshot as of 2026-07-29:

- M0 journal transport and lifecycle authority are merged.
- M0 benchmark and scorecard automation are merged, including authoritative
  cold/hot policy orchestration, exact-Pod cleanup, and reversible failure
  hooks.
- The coordinator and dispatcher were restarted from merged `main`; coordinator
  health and `/api/active-workers` passed at restart, and provider inventory was
  empty. This is an operational handoff observation, not durable acceptance
  evidence.
- RunPod, Hugging Face, RunPod S3, and provider credentials resolve through the
  configured environment/keychain paths without logging their values.
- Local runtime plans, scorecards, and service logs belong under `.runlogs/` and
  must never be committed because they may contain workflow or operational data.
- The cold/hot campaign is accepted. Cancellation, provider recovery, and
  storage failure canaries are accepted. The first corruption and restart
  attempts failed safely and were cleaned up exactly.
- A focused corruption/restart rerun against merged PR #14 stayed within its
  $1.00 campaign and $0.50 scenario ceilings and conservatively accounted for
  $0.257885. It ended with empty provider inventory and both exact Pods absent,
  but both cases correctly remained failed. The fresh Pod again served valid
  cached bytes rather than the pre-submit corruption canary, so no quarantine
  event occurred. The restart canary stopped the exact healthy coordinator but
  a Windows process-liveness probe raised while that process was exiting, so the
  replacement was not launched. Manual recovery restored a healthy coordinator
  with a new PID; prepared-storage policy remained `smart`.
- The Windows restart probe and canary contract were corrected in PRs #15 and
  #16. A focused production rerun now directly proves replacement health,
  journal replay, replacement-owned cancellation, and exact provider cleanup.
- PR #17 removed cached-object aliasing from the corruption canary. Its first
  production run narrowed the failure to exact-profile and mutable-index
  identity. PR #18 merged the direct manifest path and produced the pinned
  worker image above. Its first replay showed that the hook and dispatcher used
  different names for the same profile. The launch-profile-name correction now
  passes 530 tests in merged PR #19. Its replay proved exact manifest selection
  and loading, then showed that a valid first write can remain cached after an
  object update. PR #20 made corrupt bytes the first and only S3 write. Its
  replay then proved that the deterministic scenario digest still reused one
  mounted object identity across campaigns. The campaign-nonce correction now
  passes 531 tests on its PR branch. Corruption remains unaccepted until that
  change merges and a bounded replay passes.
- The first unmet M0 work is therefore: compute the corruption manifest from the
  actual injected requirement profile; publish and resolve it through an
  immutable manifest-by-ID path when a mounted mutable index is stale; trigger
  observation from durable `cache_mount_ready`; prove the worker quarantines the
  synthetic artifact while the valid fallback lets the job continue; verify
  complete cleanup; commit a compact redacted evidence projection; and audit
  every M0 exit before starting M1.

### Active engineering handoff

The direct-manifest corruption fix is bounded to these contracts:

1. The benchmark hook receives only the safe requested worker capability and
   declared asset digests, never the workflow body or credentials.
2. The canary resolves that capability to the normalized configured launch
   profile and recomputes the same prepared-requirement fingerprint the
   dispatcher will use after synthetic-asset injection. The configured profile's
   `name`, not the requested capability, is the fingerprint identity.
3. The signed canary manifest is published under an immutable deterministic
   `manifests/by-id/sha256/...` key, and its registry entry references that key.
4. The synthetic S3 key receives corrupt bytes as its first and only value. The
   valid artifact exists only in coordinator fallback storage. The canary never
   depends on a mounted volume observing an update to an existing key.
5. Each campaign injects one safe random nonce. That nonce creates a new payload,
   digest, object key, file name, and state path. The same nonce is passed to the
   prepare, observe, and cleanup hooks. A campaign cannot reuse a synthetic
   mounted object identity from an earlier run.
6. `PreparedStateCAS.find_manifest(manifest_id=...)` may load and verify that
   exact immutable object if the mounted `indexes/latest` view is stale. A
   mismatched ID or bad signature remains a hard failure.
7. Corruption observation defaults to the durable `cache_mount_ready` event so
   a finite observation window is not consumed by unrelated image startup.
8. Existing manifest/index behavior remains the normal path; the direct object
   is a narrow exact-ID fallback, not a second source of unsigned truth.

Before this change can become accepted evidence it must pass focused and full
tests, merge through a reviewed PR, and pass one spend-capped production replay.
That replay must show a nonempty exact `manifest_ids` placement, a
`cache_artifact_quarantined` event for the synthetic digest, successful valid
fallback or explicit safe terminal behavior, hook cleanup success, exact Pod
absence, empty provider inventory, restored storage policy, and absence of every
synthetic manifest, blob, registry projection, invalidation, quarantine object,
and coordinator fallback. A completed job without quarantine is still a failed
canary.

### M0 evidence still required

1. **Proved:** a validated safe summary identifies the repeated workload by
   request digest while its workflow body stays local.
2. **Proved:** one alternating fresh-Pod cold/hot campaign ran within explicit
   campaign, scenario, runtime, spend, and cleanup ceilings.
3. **Proved for the accepted pair:** startup, preparation, execution, closure,
   and conservative compute cost are separately represented in its scorecard.
4. **Proved:** the full prepared-storage configuration was restored and verified
   after every completed scenario and campaign cleanup.
5. **Partial:** cancellation, provider, storage, and coordinator-restart canaries
   passed. Corruption still requires direct accepted evidence.
6. **Proved for completed campaigns:** final provider inventory was empty and
   attributable Pods had provider-absence receipts.
7. **Required:** commit a compact redacted scorecard projection or durable CI
   artifact reference that is comparable across runs and contains no prompts,
   raw workflow, asset paths, hook arguments/output, credentials, or secret
   endpoints.

### Operational safety rules

- A paid benchmark never starts without explicit spend confirmation and finite
  circuit breakers. Limits reduce risk but are not provider-side escrow.
- Attribute provider resources before terminating them; never delete an
  unverified baseline or unrelated Pod.
- Terminate attributable Pods after each scenario and audit provider inventory
  again at campaign end.
- Never persist or print credential values. Configuration and support surfaces
  expose presence, provenance category, and actionable absence only.
- Never include raw workflow bodies, prompts, private asset paths, signed URLs,
  hook arguments, or hook output in a scorecard or support bundle.
- Managed-volume deletion and adopted-volume detachment are distinct; destructive
  provider storage deletion always requires explicit user intent.
- Benchmark policy mutation must be reversible and restore the full prior object,
  not a reconstructed subset that could lose region, volume, tenant, or privacy
  settings.

## Delivery milestones

| Milestone | Status on 2026-07-29 | Gate |
| --- | --- | --- |
| M0 — measurement and scorecard | **In progress** | Cold/hot plus four failure classes accepted; corruption and durable redacted evidence remain. |
| M1 — preflight, recommendation, confirmation | **Not started** | Begins after M0 evidence is trustworthy. |
| M2 — persistent visibility | **Partial foundation** | Journal and initial canvas feedback exist; Cloud Jobs drawer and telemetry remain. |
| M3 — leases and billing closure | **Partial foundation** | Logical cancellation exists; persisted lease and provider receipt remain. |
| M4 — fast trusted restore | **Partial foundation** | Durable prepared storage exists; trust receipts/scrubbing and performance target remain. |
| M5 — workflow capsules | **Not started** | Schema and custom-node readiness contracts remain. |
| M6 — regional replication | **Not started** | Shadow recommendations precede automation. |
| M7 — production release gate | **Not started** | Requires all prior exits and continuous canaries. |

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
- cancellation, provider, storage, corruption, and restart canaries all have
  direct accepted production evidence within explicit spend/runtime ceilings;
- a compact redacted projection makes cold, hot, and failure results comparable
  without committing prompts, workflows, private paths, hook details, or
  secrets; and
- no validation run leaves an orphaned Pod or synthetic storage state.

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

# Storage-aware Cloud Offload — persistent prepared state

> **Product context:** This is the storage subsystem PRD. The canonical product
> goal, promise stack, preflight and GPU recommendation experience, lifecycle
> guarantees, and delivery milestones live in
> [Cloud Offload product goal and delivery plan](cloud-offload-product-goal.md).

> Status: **initial RunPod storage-aware MVP implemented** — opt-in volume
> lifecycle, signed prepared-state manifests/CAS, storage-aware placement,
> worker restore/population, status telemetry, and the ComfyUI settings flow are
> delivered. Adaptive admission/retention, full environment and compiler-cache
> layers, automated multi-region replication, and live performance validation
> remain follow-up work.
>
> Initial provider: **RunPod Secure Cloud**. The control-plane model is
> provider-neutral, but the first delivery uses RunPod network volumes and their
> S3-compatible API.

## Summary

Cloud Offload currently treats rented GPU workers as interchangeable and their
filesystems as disposable. That is correct for execution safety, but expensive
for startup: a fresh worker may pull the same pinned weights, install the same
custom nodes, rebuild the same dependency environment, and compile the same
kernels that a recently terminated worker already prepared.

Storage-aware Cloud Offload makes prepared state durable without making a GPU
durable. When the user opts in, Cloud Offload creates or adopts provider storage
before the first compatible worker is rented, records what that storage contains
and where it is located, prefers compute that can attach it, and gives every new
worker a verified restore plan. The GPU remains disposable; the prepared bytes
become a schedulable resource.

For RunPod, the durable primitive is a **network volume**, not a Pod's host-local
`mounts.persistent`. A host-local volume survives stop/restart only while the Pod
exists; the
dispatcher permanently deletes idle Pods. A network volume exists independently,
can be attached to later Pods in its datacenter, mounts at `/workspace`, and can
be populated through RunPod's S3-compatible API without renting compute.

The central product decision is:

> Cloud Offload chooses and attaches storage. The runner image consumes an
> already-mounted cache; it never attempts to mount provider storage itself.

## Problem

The current cold path has four avoidable properties:

1. Profile weights and declared job assets are staged into
   `/opt/ComfyUI/models`, on the disposable container disk.
2. Custom-node checkouts and their Python requirements are recreated for a new
   runner unless baked into its image.
3. The RunPod connector can mount host-local storage at `/workspace`, but no ComfyUI path
   or cache path uses that mount, and the volume is deleted with the Pod.
4. Offer selection sees GPU type and price but not datacenter, attached storage,
   cached contents, or expected hydration time.

This makes the nominally cheapest GPU a potentially expensive choice. A slightly
more expensive GPU beside a complete prepared-state cache may reach the first
sampler step minutes earlier and cost less for the completed job.

## Product goals

1. Let a user explicitly opt in to durable prepared-state storage, adopt an
   existing volume, or remain on the current stateless behavior.
2. Create or attach storage **before the first cached rent**, so the first worker
   populates the durable tier directly.
3. Track where cache volumes live and which compatible artifacts they contain.
4. Prefer compute placements that minimize expected time and cost to first useful
   execution, not hourly GPU price alone.
5. Restore only state proven compatible with the new worker.
6. Fall back safely to the current cold path whenever storage is unavailable,
   incomplete, incompatible, corrupt, or slower than the alternative.
7. Make every placement, cache hit, miss, refusal, transfer, verification, and
   fallback visible in job events and status APIs.
8. Keep the design usable by future providers with durable-volume or object-store
   capabilities.

## Non-goals

- Serializing or restoring live VRAM across arbitrary GPUs.
- Restoring pointer-rich Python process state, CUDA allocator arenas, or captured
  CUDA graphs across unrelated hosts.
- Replacing ComfyUI or requiring custom nodes to use a new execution runtime.
- Guaranteeing that every cached artifact is faster than recomputation; the
  system must measure and sometimes refuse its own cache.
- Building automatic multi-region replication in the first release.
- Making RunPod network volumes the authoritative backup for irreplaceable data.
- Caching assets whose license, tenant policy, or residency policy forbids it.

## Users and jobs to be done

### Default user

An individual repeatedly runs one or more large ComfyUI profiles on disposable
cloud GPUs and wants subsequent runs to start faster without manually managing a
long-lived Pod.

### Advanced user

A studio or regulated user wants to pin cache residency, adopt a pre-populated
volume, control cold fallbacks, or forbid particular weights from leaving an
approved location.

### Primary jobs

- “Keep these model and node artifacts ready even though I destroy the GPU.”
- “Prefer a cached region, but do not leave my job stuck forever when it has no
  compatible GPU.”
- “Tell me whether storage helped, what was restored, and why anything missed.”
- “Let two profiles share identical artifacts without pretending the profiles
  themselves are identical.”

## Product defaults and user experience

Storage remains opt-in. The settings surface exposes policies rather than forcing
most users to understand provider datacenter IDs.

| Policy | Behavior |
| --- | --- |
| `off` | Current stateless behavior; do not create or attach cache storage. |
| `smart` | **Recommended.** Prefer compatible cached regions; permit an explicitly configured cold fallback elsewhere. |
| `strict` | Rent only where an eligible cache volume can be attached. Wait or fail clearly if capacity is unavailable. |
| `pinned` | Use only the user-selected provider datacenter, primarily for residency or predictable locality. |

The default region mode is `auto`. A raw datacenter selector appears under
advanced settings and becomes required only for `pinned`. The first implementation
supports one managed region. The M6 shadow controller now records safe paid
placement demand and shows the cost and expected benefit of additional regions
before it can copy data.

Persisted configuration:

```json
{
  "prepared_storage": {
    "enabled": true,
    "provider": "runpod",
    "policy": "smart",
    "region": "auto",
    "cold_fallback": "allow",
    "managed_size_gb": 250,
    "existing_volume_id": null,
    "max_monthly_storage_cost": null,
    "confirmed": true,
    "tenant": "default",
    "cache_private_assets": false,
    "shadow_admission": true,
    "replication": {
      "mode": "shadow",
      "approved_regions": [],
      "monthly_budget_usd": null,
      "ttl_days": 30,
      "demand_window_days": 30,
      "min_hits": 3,
      "min_avoided_gpu_seconds": 600,
      "transfer_cost_per_gb_usd": null,
      "max_inflight": 1,
      "shadow_required_recommendations": 10,
      "shadow_validation_hours": 24,
      "shadow_min_precision": 0.8,
      "controller_interval_seconds": 300,
      "copy_timeout_seconds": 21600
    }
  }
}
```

`confirmed` is persisted only after the first-run disclosure is accepted; an
enabled hand-written configuration without it is rejected. RunPod managed
volume size is constrained to 1–4000 GB. Cost shown by the MVP is a published
estimate ($0.07/GB-month through 1000 GB, then $0.05/GB-month), not a live
provider quote.

Provider credentials and S3-compatible credentials remain in the environment or
OS keychain and must never be serialized into this object or returned by an API.
Automatic mode is rejected unless it has a finite monthly budget and at least
one approved region. Its budget cannot exceed the complete prepared-storage
monthly limit.

### First-run disclosure

Before creating managed storage, the UI shows:

- provider and selected/automatic datacenter;
- requested capacity and provider-reported storage price;
- that placement will be constrained to the storage datacenter;
- the configured behavior when no GPU is available there;
- whether licensed or private model bytes are eligible for caching;
- that deleting the managed volume is a separate destructive action.

No volume is created until the user confirms this disclosure.

## Experience flows

### First cached rent

1. Submission resolves the target runtime profile, declared assets, node packs,
   storage plan, and a canonical prepared-state requirement fingerprint.
2. The storage controller selects an adopted volume or creates a network volume
   in an eligible datacenter before the Pod is created.
   If that datacenter exposes RunPod's S3-compatible endpoint, the coordinator
   may prepopulate it without compute; otherwise the attached first worker is the
   population path.
3. The scheduler limits or prefers compute offers in that datacenter according
   to policy.
4. The connector creates the Pod with `mounts.network`; RunPod mounts the volume
   at the requested path (`/workspace`) before the entrypoint runs.
5. The runner reads the cache root and finds an empty or partial manifest.
6. Missing portable artifacts are downloaded into an immutable staging prefix on
   the volume and verified by digest.
7. A completed signed manifest is published atomically only after every referenced
   object exists and verifies.
8. Compatible artifacts are linked, copied, extracted, or configured into
   ComfyUI's expected paths; execution proceeds through the existing worker path.
9. Timings and observed file touches are reported to the coordinator.

The first rent may not be faster. Its requirement is to populate durable state
without an avoidable second upload or a silent change in execution semantics.

### Subsequent rent

1. The coordinator finds cache volumes whose manifest index overlaps the job's
   requirement fingerprint.
2. It estimates cold work for each eligible placement: missing bytes, materialize
   work, provider startup, and known setup costs.
3. It selects a datacenter/volume/offer according to the configured policy and
   passes the volume attachment when creating the Pod.
4. The runner fingerprints its actual runtime, verifies the signed manifest, and
   accepts or rejects each prepared layer independently.
5. It restores accepted state, performs normal staging for misses, and records a
   restore receipt.
6. Measured restore performance updates future scheduling, admission, replication,
   and retention decisions.

### No capacity in the cached region

- `smart` with `cold_fallback=allow`: try configured cached regions first, then
  rent cold elsewhere and say why.
- `smart` with `cold_fallback=ask`: leave the job queued in a decision state and
  ask whether to wait, expand price/GPU constraints, or run cold.
- `strict` or `pinned`: do not silently leave the allowed region; surface the
  unavailable placement and actionable alternatives.

## Prepared-state portability model

Prepared state is not one snapshot. Each class has different compatibility and
materialization rules.

| Tier | Examples | Compatibility key | Initial treatment |
| --- | --- | --- | --- |
| Portable blobs | model weights, node archives, wheels, boundary artifacts | content digest plus policy labels | Store in regional content-addressed storage. |
| Runtime-bound bundles | installed Python packages, native extensions, extracted custom-node trees | image digest, OS/architecture, Python ABI, dependency lock | Store as immutable bundles; extract or copy locally before import. |
| GPU-class-bound cache | compiled kernels and compiler caches | code digest, Torch/CUDA versions, driver constraint, GPU compute capability | Experimental, opt-in, verify and fall back. |
| Process-bound state | imported Python objects, pointer-rich decoded models, allocator state | exact process and addresses | Refuse; reconstruct. |
| GPU-resident state | VRAM tensors, CUDA graphs with live allocations | live GPU/process | Do not persist; use warm-worker affinity only. |

Unknown or omitted compatibility fields force a miss. A cache hit may be less
complete than the profile: weights can match while an environment or kernel does
not. Restoration is additive and never all-or-nothing.

## Architecture

```text
submission
    │
    ├── requirement resolver ──► prepared requirement fingerprint
    │
    └── storage-aware scheduler
            │
            ├── volume registry + manifest index
            ├── provider placement/price/availability
            └── restore cost model
                    │
                    ▼
             launch plan
             {offer, datacenter, volume, restore manifest}
                    │
                    ▼
             provider creates Pod
                    │
                    ▼
          /workspace/cloud-offload
                    │
          verify / select / materialize
                    │
                    ▼
                 ComfyUI
```

### Storage tiers

1. **Canonical object tier.** Existing Local/GCS/S3 storage remains the portable,
   provider-neutral artifact source and cross-region backup path.
2. **Regional prepared tier.** A RunPod network volume is the initial hot tier,
   populated through its S3-compatible API or through an attached worker.
3. **Worker-local materialized tier.** Container disk holds extracted bundles,
   temporary files, and any representation shown by measurement to perform better
   locally than directly from the network volume.
4. **Live residency tier.** RAM and VRAM are advertised for affinity routing while
   a worker lives, but are not represented as durable cache hits.

Large immutable objects should be read directly or copied in large sequential
transfers. Directory-heavy environments and custom-node trees should be packaged
as verified bundles rather than imported across a network filesystem one small
file at a time.

### Regional volume layout

```text
/workspace/cloud-offload/
  blobs/sha256/ab/<digest>
  bundles/sha256/cd/<digest>.tar.zst
  manifests/<profile-fingerprint>/<generation>.json
  indexes/<generation>.json
  staging/<writer-id>/
  quarantine/<digest>/
```

Objects are immutable. Writers upload to `staging`, verify bytes and policy, move
or copy to the digest key, then publish a small manifest pointer with a conditional
write. Readers never trust a filename, partial object, mutable directory, or
unpublished generation.

The coordinator must not discover inventory through recursive object listing.
It consumes compact generation indexes and keeps a queryable local projection.

## Manifest and compatibility contract

Every prepared artifact is described by a versioned signed manifest. The minimum
shape is:

```json
{
  "schema": "cloud-offload.prepared-state.v1",
  "manifest_id": "sha256:<canonical-json-digest>",
  "profile_fingerprint": "sha256:<digest>",
  "created_at": "2026-07-29T00:00:00Z",
  "producer": {
    "image_digest": "sha256:<digest>",
    "cloud_offload_version": "<version>",
    "python_abi": "cp312",
    "platform": "linux-x86_64",
    "torch": "<version>",
    "cuda": "<version>"
  },
  "artifacts": [
    {
      "digest": "sha256:<digest>",
      "kind": "model-weight",
      "size": 6938040714,
      "storage_key": "blobs/sha256/ab/<digest>",
      "portability": "portable",
      "requirements": {},
      "policy": {"tenant": "default", "cacheable": true}
    }
  ],
  "signature": {"algorithm": "<algorithm>", "key_id": "<id>", "value": "<value>"}
}
```

`manifest_id` is the digest of the canonical manifest payload excluding the
`signature` field; the signature covers that same unsigned payload or its digest.
The exact signing scheme is an implementation decision, but trust must anchor in
the coordinator or operator, not in an arbitrary worker that happened to write a
file. Canonical JSON and signature verification happen before any path is exposed
to ComfyUI.

### Hierarchical fingerprints

Fingerprint strata prevent both unsafe matches and unnecessary duplication:

```text
blob_key        = sha256(bytes)
environment_key = hash(image + platform + python_abi + dependency_lock)
kernel_key      = hash(code + torch + cuda + driver_constraint + gpu_capability)
profile_key     = hash(required artifact keys + runtime profile identity)
```

Nominally different profiles may share a blob or environment when their immutable
inputs match. Profile names alone are neither sufficient proof of compatibility
nor a reason to duplicate identical bytes.

## Control-plane data model

The coordinator needs an indexed projection of provider storage and known content.
The durable manifests remain authoritative.

### `cache_volumes`

| Field | Purpose |
| --- | --- |
| `id` | Coordinator identity. |
| `provider` | Connector name, initially `runpod`. |
| `provider_volume_id` | Opaque provider volume identity. |
| `datacenter_id` | Placement constraint and S3 endpoint selector. |
| `ownership` | `managed` or `adopted`. |
| `status` | `creating`, `ready`, `degraded`, `deleting`, `failed`. |
| `capacity_bytes` | Provider-reported capacity. |
| `inventory_generation` | Last indexed manifest generation. |
| `last_verified_at` | Successful control-plane verification. |
| `policy_json` | Residency, tenant, cacheability, and fallback policy. |

### `cache_artifacts`

| Field | Purpose |
| --- | --- |
| `volume_id`, `digest` | Physical presence identity. |
| `kind`, `size_bytes` | Planning and UI. |
| `compatibility_key` | Matching without loading every manifest. |
| `manifest_id` | Provenance and reachability. |
| `last_verified_at`, `last_used_at` | Health and lifecycle. |
| `restore_count`, `restore_ms` | Measured restore behavior. |
| `saved_ms` | Estimated benefit versus observed fallback. |
| `eligibility` | `eligible`, `quarantined`, `invalidated`, `unknown`. |

### `restore_observations`

Stores per-attempt lookup, transfer, verification, extraction, import, and fallback
timings keyed by datacenter, worker class, artifact topology, and strategy. These
observations begin as telemetry and later feed cache admission and scheduling.

## Provider contract changes

The existing `CloudConnector` contract cannot express storage-aware placement.
It needs typed placement and attachment inputs without making every provider
pretend it supports volumes.

Conceptual additions:

```python
@dataclass
class PlacementConstraints:
    datacenter_ids: list[str] | None = None
    storage_attachments: list["StorageAttachment"] | None = None

@dataclass
class StorageAttachment:
    provider_volume_id: str
    mount_path: str
    read_only: bool = False

connector.list_available(..., placement=constraints)
connector.launch(..., placement=constraints)
connector.list_storage()
connector.create_storage(...)
connector.get_storage(...)
connector.delete_storage(...)
```

Provider connectors may report storage management as unsupported while still
accepting an operator-provided attachment.

### RunPod requirements

1. Add network-volume list/get/create/delete operations through the official API.
2. Include datacenter identity and storage compatibility in normalized offers.
3. Pass `mounts.network[].volumeId` during Pod creation; do not confuse it with
   the host-local `mounts.persistent`.
4. Constrain launch to datacenters where the selected volume is attachable.
5. Discover whether a volume/datacenter exposes the S3-compatible API instead of
   assuming every network-volume location supports coordinator-side prepopulation.
6. Preserve the existing registry authentication, disk planning, and startup
   script behavior.
7. Return actionable failures for no capacity, incompatible cloud type, missing
   volume, and wrong datacenter.

RunPod network volumes are a Secure Cloud capability and are datacenter-bound.
The scheduler must treat that restriction as a first-class availability tradeoff,
not a launch-time surprise.

## Scheduling policy

### Phase 1: deterministic preference

The first release does not need a learned optimizer. It applies this order:

1. satisfy provider, profile, GPU type, VRAM, price ceiling, residency, and
   storage policy constraints;
2. prefer a candidate with a complete compatible manifest;
3. then prefer the candidate with the greatest compatible cached byte coverage;
4. then choose the cheapest remaining offer;
5. apply configured cold fallback only after cached candidates fail.

### Phase 2: estimated completion cost

When measurements exist, rank candidates by an explainable estimate:

```text
estimated_completion_cost =
    expected_provider_startup
  + expected_cache_lookup
  + expected_missing_bytes / measured_source_throughput
  + expected_materialization
  + expected_runtime_setup
  + expected_execution
  + monetary_cost_weight * expected_dollars
```

Every decision records its inputs and the chosen fallback. A learned or adaptive
policy may adjust estimates, but must not hide hard constraints or make the same
request irreproducible without an explanation record.

## Worker behavior

The provider mounts storage. The runner receives non-secret instructions:

```text
CLOUD_OFFLOAD_CACHE_ROOT=/workspace/cloud-offload
CLOUD_OFFLOAD_CACHE_VOLUME_ID=<coordinator volume id>
CLOUD_OFFLOAD_CACHE_MANIFEST=<manifest id or requirement fingerprint>
CLOUD_OFFLOAD_CACHE_MODE=restore-and-populate
```

At boot the runner:

1. verifies that the expected mount is present and identifies the volume;
2. fingerprints the actual image/runtime/GPU;
3. obtains the signed manifest or reports a miss;
4. verifies signature, policy, digest, size, and compatibility;
5. plans direct reads versus local materialization by artifact class;
6. makes accepted models visible to ComfyUI;
7. materializes required custom-node and environment bundles before ComfyUI
   imports them;
8. continues through the existing readiness and worker loop;
9. publishes a restore receipt and timing events.

The first release may use symlinks or a generated ComfyUI model-path configuration
for portable model files. It must not mutate the immutable runner image or import
unverified Python trees directly from shared storage.

### Population and multi-writer safety

- Content objects are immutable and keyed by verified digest.
- Duplicate production of identical bytes is harmless but wasted; use a bounded
  lease to reduce duplicate downloads.
- No worker overwrites a published object or manifest generation.
- Partial downloads remain under a writer-specific staging prefix.
- A manifest is published only after every referenced object verifies.
- Failed or abandoned staging entries are garbage-collected after a grace period.
- Concurrent writers never share a mutable Python environment directory.

Prepared state should be published as soon as it becomes reusable, not only during
worker shutdown; the dispatcher may terminate a failed or idle Pod before a
shutdown upload completes.

## Cache admission: storage must prove it helped

A cache hit is not automatically a performance win. Network-volume latency,
directory metadata, extraction, verification, or incompatible representations may
lose to a direct authenticated Hugging Face download or local reconstruction.

Every restore attempt records:

- artifact and manifest identities;
- datacenter, worker class, image, and restore strategy;
- lookup, transfer, verification, extraction, import, and total elapsed time;
- bytes and file count;
- hit, partial hit, miss, refusal, corruption, or fallback;
- observed or estimated fallback cost.

Admission starts in **shadow mode**: always follow the selected cache path and
record whether it won. Later, the worker may refuse an artifact when a
confidence-bounded restore estimate exceeds the fallback by a configured margin.
Negative observations expire, require a minimum sample count, use hysteresis, and
remain scoped to relevant datacenter/worker/artifact conditions.

## Inventory, replication, and retention

### Inventory

- Manifests and immutable objects are the durable truth.
- The coordinator indexes compact manifest generations; it does not recursively
  list a large volume for each scheduling decision.
- Workers verify selected objects at restore time even when the index says they
  exist.
- Inventory drift marks a volume degraded and removes questionable objects from
  scheduling until reconciliation.

### Replication

The MVP supports one managed region and manual prepopulation. Automatic
multi-region replication is deferred because RunPod volumes are datacenter-bound
and do not synchronize automatically.

A later replica controller may:

1. select hot artifacts using observed demand and saved latency;
2. copy immutable objects through S3-compatible endpoints;
3. publish a target-region manifest only after verification;
4. stop replication at the user's storage budget;
5. delete a replica without deleting the canonical object tier.

The M6 shadow controller is the first part of this controller. Each confirmed
paid placement records only a profile fingerprint, provider, region, prepared
coverage, conservative preparation estimate, hourly rate, and byte counts. It
does not store the workflow, prompt, input path, or job identity in its safe
output.

`POST /api/cache/replication/shadow` records a new local evaluation.
`GET /api/cache/replication/shadow` returns the safe evaluation history. A report
contains the source manifest and volume, target region and optional existing
target volume, bytes, expected hits, expected saved GPU time and cost, copy cost,
incremental monthly storage cost, expiry, budget, decision reasons, and explicit
cold-fallback visibility. Both endpoints make no provider mutation.

`POST /api/cache/replication/execute` copies one current recommendation through
the source and target provider object APIs. Shadow mode requires explicit user
confirmation. Automatic mode also requires enough mature unique recommendations
and the configured minimum observed precision. The copy claim is durable and
single-flight. It rechecks the finite monthly budget, copy cost, exact source,
exact target, approved region, and expiry before it transfers data. A repeated
request returns the same action and does not copy again. It never rents a GPU.

`POST /api/cache/replication/expire` unpublishes due target manifests and deletes
objects that no remaining target manifest uses. It does not delete source state
or rent a GPU. `GET /api/cache/replication/actions` returns the safe action and
shadow-accuracy state.

After the accuracy gate passes, automatic mode can create one approved managed
target volume. Creation has its own durable region single-flight lock and checks
the replication budget and complete prepared-storage budget before provider
mutation. A failed creation deletes its exact new provider volume. The primary
prepared-volume binding does not change.

The dispatcher starts a non-blocking authenticated controller cycle at the
configured interval. A cycle recovers stale copy claims, checks provider truth
for automatic targets, records a shadow evaluation, expires replicas, deletes an
empty automatic target after provider confirmation, and starts at most one new
copy. A lost target becomes ineligible for placement. Current preflight can use
compatible replicas in more than one region and still shows the cold fallback.
Production shadow accuracy, automatic copy, repeat-cycle safety, prepared and
cold placement, regional loss, controlled TTL expiry, source preservation, and
provider cleanup passed on 2026-07-30. The redacted evidence is in
`docs/evidence/m6-regional-replication-evidence-2026-07-30.json`. M6 is complete;
the continuous M7 production release matrix is now active.

### Retention

Eviction is based on measured value, not only least-recently-used:

```text
retention_value =
    restore_probability
  * expected_latency_saved
  / stored_gb_month
```

Policy constraints, explicit pins, recent failures, manifest reachability, and
minimum free-space reserves override the value score. Destructive volume deletion
always requires explicit confirmation and must distinguish managed from adopted
volumes.

## Security, policy, and integrity

1. Provider and S3-compatible credentials remain in the OS keychain or process
   environment; they never enter manifests, persisted config, logs, or job events.
2. Fine-grained Hugging Face tokens remain read-only and are not cached as state.
3. Every object is digest-verified before publication and before use when policy
   requires it.
4. Manifest signatures bind content, compatibility, producer, and policy.
5. Cache namespaces carry tenant, residency, license, and `cacheable` labels.
6. Derived artifacts inherit the strictest applicable policy from their inputs.
7. Revoked, vulnerable, corrupt, or license-ineligible objects are represented by
   explicit invalidation records and cannot silently become eligible again.
8. Custom-node bundles retain the existing traversal and symlink protections.
9. Adopted volumes are untrusted until their manifests and objects verify.
10. The UI warns before caching gated/private weights and allows profile- or
    asset-level cache refusal.

Application-level encryption for sensitive cached artifacts is a later capability;
its transfer and decryption cost must participate in admission measurements.

## API and status surface

Initial coordinator routes:

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/api/cache/status` | Policy, volumes, health, capacity, inventory generation, and recent benefit. |
| `POST` | `/api/cache/volumes` | Confirmed create or adopt operation. |
| `DELETE` | `/api/cache/volumes/{id}` | Delete coordinator metadata; provider deletion requires an explicit destructive flag and confirmation. |
| `POST` | `/api/cache/volumes/{id}/verify` | Reconcile provider state and manifest index. |
| `GET` | `/api/cache/manifests` | Query manifests by profile, compatibility, and region. |
| `POST` | `/api/cache/prepopulate` | Populate selected immutable artifacts without renting a GPU when the provider supports it. |

Configuration updates continue through the existing configuration route. Secrets
continue through provider credential routes.

### Job events

New event types:

- `cache_placement_considered`
- `cache_placement_selected`
- `cache_cold_fallback`
- `cache_manifest_verified`
- `cache_restore_started`
- `cache_artifact_hit`
- `cache_artifact_miss`
- `cache_artifact_refused`
- `cache_artifact_quarantined`
- `cache_restore_completed`
- `cache_population_started`
- `cache_population_completed`

Events carry stable identifiers and measured durations, never credentials or
provider-secret endpoints.

## Success metrics

The feature succeeds when all of the following are true:

1. Two fresh Pods can use one durable volume and the second Pod performs no
   third-party transfer for a fully cached pinned weight.
2. A cache hit never bypasses digest, manifest, compatibility, or policy checks.
3. `smart` fallback never leaves an otherwise runnable job indefinitely queued
   solely because the cached datacenter lacks capacity.
4. Every job explains its chosen region, attached volume, compatible byte coverage,
   cache result, and fallback reason.
5. For the reference profile, median cached preparation time is at most 25% of
   median cold preparation time over at least ten alternating fresh-Pod runs.
6. The scheduler's local cache/placement decision completes within one second,
   excluding provider API latency.
7. Concurrent population cannot publish a partial or digest-invalid object.
8. Disabling the feature preserves the current job, provider, and artifact behavior.

Metrics reported separately for cold, partial, and complete restores:

- time from submission to provider request;
- provider request to entrypoint start;
- manifest lookup and verification;
- cached and missing bytes;
- transfer throughput by source;
- materialization and import time;
- ComfyUI readiness and first sampler time;
- dollars and storage GB-month;
- measured milliseconds saved or lost.

## Delivery plan

### Phase 0 — measurement contract

- Versioned phase and restore-observation event schemas.
- Trace submission, placement, provider startup, mount availability, staging,
  ComfyUI readiness, first sampler, and result availability.
- Establish cold baselines for the reference profile.

Exit: a single job has an explainable critical path and artifact-level staging
times without enabling durable storage.

### Phase 1 — manually adopted RunPod volume

- Configure one existing network volume ID and datacenter.
- Pass the attachment during launch.
- Set `/workspace/cloud-offload` as the runner cache root.
- Prepopulate one pinned model through RunPod's S3-compatible API.
- Verify, expose, and consume that model on two successive fresh Pods.
- No automatic creation, replication, or learned scheduling.

Exit: the reference two-Pod experiment meets integrity requirements and shows a
measured preparation improvement.

### Phase 2 — manifests and managed lifecycle

- `PreparedArtifactManifest`, signatures, portability tiers, and compatibility
  explanations.
- Cache volume and artifact tables.
- Create/adopt/verify/delete status flows with explicit confirmation.
- Atomic population and compact inventory generations.
- Settings and status UI.

Exit: a user can opt in, understand cost/location, and manage one durable cache
without provider-console intervention.

### Phase 3 — storage-aware scheduling

- Datacenter-aware normalized offers.
- `smart`, `strict`, and `pinned` policies.
- Compatible-byte coverage and deterministic cached-region preference.
- Cold fallback with explicit events.
- Cache-affinity worker claims for live workers.

Exit: placement is explainable, obeys region/fallback policy, and handles no
capacity without silent drift.

### Phase 4 — prepared bundles and adaptive admission

- Runtime-bound custom-node/environment bundles.
- Experimental compiler caches with strict fingerprints.
- Restore receipts, shadow comparisons, refusal policy, canary restore checks,
  and value-based retention.

Exit: storage can refuse a losing representation and automatically retain state
that repeatedly saves time.

### Phase 5 — regional replicas

- Multiple managed RunPod volumes.
- S3-to-S3 immutable replication.
- Demand- and budget-aware placement.
- Replica health, drift reconciliation, and independent deletion.

Exit: hot profiles remain cacheable across approved regions without claiming that
RunPod synchronizes volumes for us.

## MVP acceptance criteria

The MVP is Phases 0 and 1, not the entire architecture.

1. Configuration accepts an operator-provided RunPod network volume ID and
   datacenter without persisting credentials.
2. The connector attaches that volume at Pod creation and refuses an incompatible
   placement before billing begins when possible.
3. Runner boot fails clearly when the expected mount is absent.
4. One pinned weight can be prepopulated through the provider's S3-compatible API,
   verified, and made available to ComfyUI.
5. Two independently created Pods consume the same verified bytes.
6. The second Pod does not call Hugging Face for the cached file.
7. Events distinguish provider startup, mount, manifest verification, restore,
   ComfyUI readiness, and first sampler.
8. Removing the configured network volume returns the system to current stateless
   behavior without a code or image change.
9. Corrupt bytes, a wrong digest, a missing mount, and an incompatible manifest
   each produce a named failure or safe cold fallback according to policy.
10. Tests cover attachment serialization, datacenter mismatch, manifest validation,
    first population, subsequent hit, concurrent writers, corruption, and fallback.

## Test strategy

### Unit

- Manifest canonicalization, signatures, compatibility tiers, and mismatch reasons.
- Requirement and profile fingerprints.
- Placement filtering and cache-coverage ranking.
- Retention values and cache-admission hysteresis.
- Path, archive, digest, policy, and staging protections.

### Connector contract

- Fake RunPod responses for storage list/create/get/delete.
- Pod creation includes `mounts.network` and datacenter constraints.
- A Pod's host-local `mounts.persistent` remains distinct from a network-volume
  attachment.
- No-capacity and wrong-datacenter errors are actionable.

### Integration

- Local directory emulates a network volume with multiple fresh worker roots.
- S3-compatible test service exercises object population and conditional manifest
  publication.
- Two writers race to publish the same digest.
- Coordinator inventory is rebuilt from a compact generation index.

### End to end

- Alternating cold and cached fresh-Pod runs of the same reference workflow.
- Partial cache, corrupt cache, stale manifest, unavailable cached region, cold
  fallback, and strict refusal.
- Gated/private asset policy and an explicitly non-cacheable asset.
- Restore from an adopted volume populated outside Cloud Offload.

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Network storage is slower than direct download or local disk. | Measure by artifact topology; support local materialization and cache refusal. |
| Volume locality reduces GPU availability. | Default `smart` policy, explicit fallback, later optional replicas. |
| Multi-writer corruption. | Immutable digest keys, writer staging, leases, conditional manifest publication. |
| Cache poisoning or ABI mismatch. | Signed manifests, strict compatibility, unknown-is-miss, canary restores, quarantine. |
| Directory-heavy Python imports are slow. | Package runtime-bound trees as large verified bundles and extract locally. |
| Storage costs grow without bound. | User-visible budgets, capacity reserves, reachability GC, measured value retention. |
| Licensed weights are cached inappropriately. | Inherited policy labels, per-asset refusal, first-run disclosure, explicit invalidation. |
| Provider API semantics change. | Connector contract tests, provider capability discovery, no provider fields in core manifests. |
| First managed volume is stranded in a capacity-poor region. | Show placement constraint, verify capacity before create where possible, permit adopt/delete, defer automatic replicas. |
| Coordinator index disagrees with the volume. | Manifests are authoritative; verify at use and reconcile degraded volumes. |

## Open questions

1. For `region=auto`, should first release choose from current GPU availability,
   ask once, or require an operator-selected datacenter until provider inventory is
   reliable enough?
2. Should `smart` default to running cold elsewhere or asking before abandoning a
   complete cache? The PRD recommends `allow` for unattended jobs and a visible
   warning in interactive mode.
3. Should managed storage be per account, per tenant, or per trust domain?
4. Which RunPod datacenters and volume sizes provide acceptable model-load
   throughput for direct reads versus local copying?
5. Can RunPod create-and-launch be made transactional enough to avoid a newly
   created but unused volume after a capacity race?
6. Which custom-node environments are safe and worthwhile to bundle instead of
   baking profile-specific images?
7. Which private or gated model licenses permit durable provider-side caching?
8. What key-management mechanism should sign manifests for a single-user local
   coordinator versus a shared coordinator?
9. When should Cloud Offload promote a regional cache to a second region, and how
   should it present the additional monthly cost?
10. Should the canonical object tier remain the existing configured Local/GCS/S3
    store, or may a RunPod network volume be canonical for explicitly disposable
    caches?

## Decisions recorded by this PRD

- Durable prepared state is opt-in.
- `smart` and `region=auto` are the recommended defaults.
- Advanced users may pin a region or require cached placement.
- Storage is created or adopted before the first cached Pod.
- RunPod attaches storage during Pod creation; the image never mounts it.
- Cloud Offload tracks both volume location and manifest contents.
- The coordinator uses compact inventories rather than recursive listings.
- Content-addressed immutable objects and signed manifests are foundational.
- Restore compatibility is artifact-specific and unknown means miss.
- Current stateless behavior remains the fallback and the disabled-state contract.
- The first proof is one adopted volume, one pinned model, and two fresh Pods.

## References

- RunPod, [Storage options](https://docs.runpod.io/pods/storage/types)
- RunPod, [Network volumes](https://docs.runpod.io/storage/network-volumes)
- RunPod, [S3-compatible API](https://docs.runpod.io/storage/s3-api)
- RunPod, [Create a Pod](https://docs.runpod.io/api-reference-v2/pods/create-a-pod)
- RunPod, [Migrate from API v1 to v2](https://docs.runpod.io/api-reference-v2/migrate-from-v1)
- Current connector: `cloud_offload/providers/runpod.py`
- Current storage abstraction: `cloud_offload/storage.py`
- Current worker staging: `cloud_offload/worker.py`
- Current profile and storage planning: `cloud_offload/profiles.py` and
  `cloud_offload/storage_plan.py`

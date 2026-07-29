# Job leases and provider closure

Every paid provider resource has a durable `JobLease` in the same SQLite
database as the job journal. The dispatcher creates the lease before it sends a
provider mutation. The lease is the authority for worker claims, cancellation,
hard runtime and cost limits, provider termination, and billing closure.

## Durable identity

A lease contains:

- an opaque lease ID and a provider resource name chosen before mutation;
- the provider, runtime profile, exact provider instance ID, and worker ID;
- attached job IDs;
- created, renewed, expiry, revocation, termination-request, and
  termination-confirmation times;
- hourly rate, runtime deadline, and optional dollar deadline; and
- bounded termination attempts, reason, and last error class or message.

The worker receives the lease ID in `CLOUD_OFFLOAD_LEASE_ID`. It sends that ID
and its worker ID on claims, status reports, and job callbacks. A callback for a
leased job must match both identities. Claims and active callbacks renew the
lease. A revoked, expired, or differently owned lease cannot claim more work.

## State machine

`provisioning` starts before provider mutation. `active` starts only after the
exact provider instance ID is bound. Cancellation, expiry, or a hard limit moves
the lease to `revocation_requested`. Each independent provider termination
attempt moves or keeps it in `terminating`. Only a provider observation of an
absent or `terminated` resource moves it to `closed` with a closure receipt.

A provider response that says only `stopped` is not proof that the resource was
removed. The dispatcher continues termination and keeps billing closure
unconfirmed.

## Restart reconciliation

The provider resource name is written before launch and sent to the provider.
If the dispatcher stops after provider creation but before it saves the returned
instance ID, the replacement dispatcher lists provider resources and finds the
exact name. It binds the recovered ID instead of renting a second GPU.

Each dispatcher poll reads all open leases before it makes a new launch
decision. It restores exact active resources, closes provider-confirmed absent
resources, revokes expired leases, applies hard limits, and retries termination.
An open lease also blocks a duplicate launch for the same provider and runtime
profile.

## Cancellation and hard limits

User cancellation first journals `cancellation_requested`, revokes every open
lease attached to the job, and makes the job terminal. The worker checks this
state at staging, cache-publication, execution, and result-transfer boundaries.
It cannot publish a shared prepared manifest after cancellation. The dispatcher
terminates the exact provider instance without depending on worker cooperation.

`max_job_runtime_seconds` is a finite paid-resource runtime limit. Its default is
7200 seconds. `max_total_job_cost` is optional. When it is set, the lease derives
a dollar deadline from the confirmed hourly rate. The earlier job or configured
limit wins. Either deadline cancels active jobs, revokes the lease, and starts
exact provider termination.

`lease_ttl_seconds` controls worker renewal time and defaults to 300 seconds. It
must be at least 30 seconds. A provisioning lease uses the longer runner-start
window. These settings can also be supplied as
`CLOUD_OFFLOAD_MAX_JOB_RUNTIME_SECONDS` and `CLOUD_OFFLOAD_LEASE_TTL`.

## Billing proof

Job completion, worker exit, a successful termination request, and provider
status `stopped` do not claim that billing stopped. The job journal records
`provider_termination_completed` only after the provider reports the exact
resource absent or `terminated`. The Cloud Jobs projection then changes billing
from `termination_unconfirmed` to `stopped`, freezes paid elapsed time at the
receipt time, and shows the provider-confirmed closure time.

Lease events contain only opaque IDs, timestamps, prices, states, and bounded
reasons. They never contain provider credentials, raw requests, workflows,
prompts, private paths, signed URLs, or raw provider replies.

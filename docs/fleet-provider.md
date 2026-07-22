# Fleet provider — your own machines as a backend

> Status: **designed, not yet implemented.** This document is the build plan.
> The internet-scale sibling of this feature is described in
> [compute-pool.md](compute-pool.md).

Cloud Offload's providers rent GPUs. The fleet provider does something better
when you already own them: it turns the machines on your network into offload
workers, and the provider itself acts as the load balancer. A studio runs the
agent on every workstation and the office becomes a render farm after hours —
the Deadline/OpenCue model applied to ComfyUI graphs. A home user with a
second GPU box gets cloud-style offload with no cloud account at all.

## How it fits the existing system

The fleet is deliberately *just another provider*. It implements the same
`CloudConnector` contract RunPod and Vast.ai do, with rental semantics
replaced by leases:

| Connector call | Cloud meaning | Fleet meaning |
| --- | --- | --- |
| `list_available` | offers on the marketplace | enrolled machines currently idle and in-schedule |
| `launch` | rent a pod | **lease** a machine (VRAM-fit, then least-recently-leased) |
| `terminate` | destroy the pod | release the lease |
| `account_balance` | dollars | machine counts; `hourly_rate` is always `0.0` |

The dispatcher, queue, retry machinery, progress relay, and artifact transport
are untouched: to the rest of the coordinator, a fleet machine is a provider
instance that appears fast and costs nothing. When both fleet and paid cloud
could serve a job, `provider_order` with `fleet` first is the intended default.

## The agent

`cloud-offload agent` runs as a user-level service (Windows scheduled task,
systemd user unit) on each machine you want in the fleet.

**Agents are outbound-only.** They enroll, then heartbeat the coordinator
every few seconds; commands ride back on the heartbeat *response*
(`activate`, `release`, `yield`). No listening port, no firewall rules, no
inbound anything on the workstation. This is also what makes the
[internet pool](compute-pool.md) the same system rather than a rewrite.

Lifecycle: `enroll -> idle -> leased -> active -> yielding -> idle`.

- **Enroll** exchanges an operator-generated enrollment token for a per-agent
  credential (stored in the OS keychain). The enrollment token itself never
  persists on the workstation, and revoking an agent is deleting a row.
- **Heartbeat** reports GPU model, free/total VRAM, input-idle seconds, and
  schedule state.
- **Activate** starts the standard worker loop, pinned to its lease so it only
  claims the jobs routed to it.
- Agents hold no provider API keys — the fleet has none. Nothing worth
  stealing lands on a workstation.

## Idle-yield

The feature that makes "install it on every artist's machine" acceptable
rather than infuriating: the fleet never fights the human at the keyboard.

- Availability schedules (`weekday evenings + weekends` style) plus a
  per-machine toggle decide when a machine is offerable at all.
- If a human starts using a leased machine, the agent reports it and the
  coordinator yields the lease. **Soft yield** (default) finishes the
  in-flight job and claims nothing more. **Hard yield** cancels immediately;
  the job's retry counter requeues it on another machine — safe, because a
  partition is a pure function of its bundle.

## If you already have a load balancer

Studios running Deadline, OpenCue, or an in-house allocator don't need the
fleet provider's scheduler. Two integration paths, neither requiring a fork:

1. Describe your allocator's REST API as a
   [declarative provider spec](declarative-providers.md) — its `offers`,
   `select`, and `wait_for` primitives were shaped by exactly this pattern.
   Your balancer becomes a provider; the fleet provider is simply the built-in
   balancer for everyone who doesn't have one.
2. Run the agent under your scheduler's control: it is just a process, so a
   farm job that starts and stops it inherits your farm's own policies.

## Delivery plan

1. Core lease loop: `providers/fleet.py`, the agent subcommand, enroll and
   heartbeat routes, lease-pinned job routing. Proven by two loopback agents
   against a live coordinator, differential-tested against a cloud worker run.
2. Idle-yield: input-idle sampling (Windows first), schedule windows, soft and
   hard yield with requeue tests.
3. Operator UX: fleet section in the node-pack provider dialog, enrollment
   token management, agent installer scripts.
4. [Compute pool](compute-pool.md): trust tiers, transfer budgets, the ledger.
5. Farm bridge: a worked declarative-spec example against a mock allocator;
   a coded OpenCue connector only if demand shows up.

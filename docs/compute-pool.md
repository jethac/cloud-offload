# Compute pool — shared GPUs across the internet

> Status: **designed, not yet implemented**, and sequenced after the
> [fleet provider](fleet-provider.md), which it extends. Read that first;
> this document only describes what changes when the fleet leaves the LAN.

The fleet provider assumes one building. The compute pool assumes one *group*:
an indie team or a circle of friends pooling their GPUs so that whoever needs
compute tonight can borrow whoever's card is idle. Same agent, same enrollment,
same lease protocol — different transport, trust, and bookkeeping posture.

## Why it's the same system

Fleet agents are outbound-only: they dial the coordinator and receive commands
on heartbeat responses. That decision was made for studio firewalls, but it is
exactly what a home network needs — a pool member installs the agent, pastes an
enrollment token, and never configures port forwarding. Only the coordinator
must be reachable, and it already knows how to arrange that: give it a real
address, or let the built-in ingress open a verified Cloudflare tunnel.

What genuinely changes:

## Transport

TLS becomes mandatory rather than recommended. Partition bundles cross home
uplinks, so the pool gets a per-pool **bundle size budget** and compression on
the artifact channel, with transfer progress surfaced through the normal event
relay instead of silent stalls.

## Trust

Enrollment tokens gain a tier: `org` tokens behave like the LAN fleet;
`member` tokens mark semi-trusted participants. Member-submitted partitions
are validated against the worker's declared capability and profile manifest —
a worker only ever runs node types its operator shipped in its profile — and
job outputs are confined to the job workspace.

Said honestly: joining a pool means running your friends' workloads on your
GPU. Capability manifests *bound* that trust decision; they do not eliminate
it. Pool operators should say this out loud when they hand out tokens, and the
docs will ship a short plain-language agreement template for exactly that.

## Fairness

A per-member ledger of contributed versus consumed GPU-hours, visible to the
whole pool. Advisory first — a dashboard nobody can argue with is usually
enough among friends. Queue-priority enforcement keyed to ledger balance is
designed but deliberately deferred until a real pool asks for it.

## Failure model

Nothing new. A member's machine going to sleep mid-job is the same event as a
cloud pod dying or a studio workstation hard-yielding: the heartbeat times
out, the lease is revoked, and the job's retry counter requeues it on the next
available machine. Pools inherit the whole fleet failure model unchanged.

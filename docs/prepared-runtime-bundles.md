# Prepared runtime bundles

Cloud Offload can preserve custom-node code and its Python packages on an
opted-in prepared volume. This state is part of the same profile closure as
model files. It is not a mutable shared virtual environment.

## First rent

Before ComfyUI starts, the worker installs each profile-pinned custom node. A
Git source must use a full commit SHA. A registry source must use an exact
release version. Python requirements install into
`/opt/cloud-offload/environment`, not the base image environment.

When the worker claims the authorized job, it builds:

- one portable `custom-node-bundle` for each pinned pack; and
- one `environment-bundle` bound to the image digest, platform, Python ABI,
  and complete dependency lock.

The archive builder orders every member. It fixes time and ownership. It
normalizes permissions. It excludes Git state, Python bytecode, and test cache
files. It refuses links and special files. The same directory closure produces
the same archive digest.

The worker publishes each archive to the mounted content-addressed store. The
coordinator signs the manifest only when the pack source exactly matches the
job's configured profile pin. It also checks the environment dependency lock,
image digest, platform, and Python ABI. The signed policy is tenant-bound and
does not permit private data or credentials.

## Later rents

The scheduler counts the pack and environment requirement keys when it measures
prepared coverage. A compatible Pod restores the environment and every pack
before ComfyUI starts. The entrypoint puts the restored environment first on
`PYTHONPATH`. ComfyUI therefore builds its node registry from the restored code
and packages. It does not repeat Git, registry, or pip downloads.

The boot process writes a small container-local restore report. The first
claimed job accepts it only when its worker ID, profile fingerprint, manifest
ID, artifact digests, sizes, types, and destinations match the selected signed
manifest. The job then emits the cache-hit events and restore receipt. It does
not publish the same bundles again.

Each restore verifies the signed manifest and compatibility contract. Bundle
extraction permits only regular files and directories. A corrupt environment
bundle is quarantined. The configured cold-fallback policy then decides whether
the worker can rebuild it or must stop.

The configured worker profile declares the platform and Python ABI that its
pinned image digest provides. Preflight uses these values with the image digest
and dependency lock. An omitted value stays unknown and makes the
runtime-bound bundle a cache miss.

## Boundaries

The bundle contains only declared profile code and packages. A custom node that
downloads undeclared models, code, or packages while it runs remains dynamic
behavior. A workflow capsule must report that behavior as uncertainty or
declare the resulting immutable artifact separately.

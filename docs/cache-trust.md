# Prepared cache trust receipts

Cloud Offload performs a complete SHA-256 read when it first sees an artifact or
when any trust condition changes. A later restore can use a signed trust receipt
to avoid another complete read.

## Receipt boundary

The coordinator signs `cloud-offload.cache-trust-receipt.v1`. A worker cannot
create a valid receipt by itself. The signing route accepts a claim only from the
worker that owns an active job on the exact prepared volume in the launch plan.
The signed receipt binds:

- the signed manifest ID and the digest of its exact authority signature;
- the artifact digest, size, and canonical storage key;
- the provider volume identity;
- the signed portability and runtime-requirement contract;
- the mounted object's size and modification generation;
- the complete-verification time and receipt expiry; and
- five fixed byte-sample hashes plus the next complete-audit time.

The default receipt life is seven days. A complete audit becomes due after one
day. The coordinator owns these times. A worker cannot extend them.

## Restore decision

A hot restore must verify the receipt signature and every binding above. It then
reads one rotating 1 MiB sample. For an object larger than 1 MiB, this is not a
complete artifact read. The job event and safe visibility projection distinguish
`trusted_metadata_sample` from `full_digest` and report verification bytes.

Cloud Offload performs a complete digest read when:

- a receipt is absent, malformed, expired, or due for a complete audit;
- the manifest, signature, artifact, volume, compatibility contract, size, or
  object generation changed;
- the artifact is private, sensitive, or has a full-verification policy; or
- the coordinator receipt service is not available.

A successful complete read renews the receipt. A signed sample mismatch is
corruption. The worker quarantines the object, removes its receipt, and uses the
configured safe cold fallback. A denied cold fallback fails the job.

## Threat model and honest language

The fast path is sampled verification. It is not equivalent to a complete digest
read. It protects against normal object replacement through the signed metadata
generation and against sampled byte corruption through signed sample hashes. An
actor that can rewrite both object bytes and provider metadata, and can avoid all
sampled ranges, can remain undetected until a later sample or complete audit.

Private and explicitly sensitive artifacts do not use this risk tradeoff. They
always receive complete digest verification.

The current slice enforces the scheduled complete audit on the next restore.
Background sampling, volume degradation state, and the paid cold/hot performance
gate remain in Milestone 4.

"""Signed, content-addressed prepared state shared by disposable workers.

The provider attaches storage.  This module only consumes an existing mount or
an injected object client; it deliberately contains no cloud mount operation.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import platform as platform_module
import shutil
import sys
import tarfile
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable


MANIFEST_SCHEMA = "cloud-offload.prepared-state.v1"
INDEX_SCHEMA = "cloud-offload.prepared-state.index.v1"
RECEIPT_SCHEMA = "cloud-offload.restore-receipt.v1"
OBSERVATION_SCHEMA = "cloud-offload.restore-observation.v1"
TRUST_RECEIPT_SCHEMA = "cloud-offload.cache-trust-receipt.v1"
DEFAULT_TRUST_RECEIPT_TTL_SECONDS = 7 * 24 * 3600
DEFAULT_FULL_AUDIT_INTERVAL_SECONDS = 24 * 3600
DEFAULT_TRUST_SAMPLE_BYTES = 1024 * 1024
DEFAULT_TRUST_SAMPLE_COUNT = 5
PORTABILITY_TIERS = {
    "portable",
    "runtime-bound",
    "gpu-class-bound",
    "process-bound",
    "gpu-resident",
}

_S3_PUBLICATION_LOCKS: dict[tuple[str, str], threading.RLock] = {}
_S3_PUBLICATION_LOCKS_GUARD = threading.Lock()
_LOCAL_PUBLICATION_LOCKS: dict[str, threading.RLock] = {}
_LOCAL_PUBLICATION_LOCKS_GUARD = threading.Lock()


def _s3_publication_lock(endpoint_url: str, volume_id: str) -> threading.RLock:
    key = (str(endpoint_url).rstrip("/"), str(volume_id))
    with _S3_PUBLICATION_LOCKS_GUARD:
        return _S3_PUBLICATION_LOCKS.setdefault(key, threading.RLock())


def _local_publication_lock(root: Path) -> threading.RLock:
    key = os.path.normcase(str(root.resolve()))
    with _LOCAL_PUBLICATION_LOCKS_GUARD:
        return _LOCAL_PUBLICATION_LOCKS.setdefault(key, threading.RLock())


class ManifestError(RuntimeError):
    """A prepared-state manifest is unauthentic or malformed."""


class CacheMountError(RuntimeError):
    """The provider did not mount the storage selected by the coordinator."""


class CacheCorruptionError(RuntimeError):
    """A published content object does not match its immutable identity."""


class CachePolicyError(RuntimeError):
    """An artifact is not eligible under the current tenant/license policy."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json(value: Any) -> bytes:
    """RFC-8785-compatible for the JSON types emitted by Cloud Offload."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_digest(value: str) -> str:
    digest = str(value).lower().removeprefix("sha256:")
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise ValueError(f"Invalid sha256 digest: {value!r}")
    return digest


def digest_id(value: str) -> str:
    return "sha256:" + normalize_digest(value)


def fingerprint(value: Any) -> str:
    return digest_id(sha256_bytes(canonical_json(value)))


def manifest_signature_digest(manifest: dict[str, Any]) -> str:
    """Bind a receipt to the exact authority signature, not only its payload."""
    signature = manifest.get("signature")
    if not isinstance(signature, dict):
        raise ManifestError("Manifest has no signature")
    return digest_id(sha256_bytes(canonical_json(signature)))


def artifact_runtime_compatibility_key(artifact: dict[str, Any]) -> str:
    """Bind a receipt to the signed portability and runtime requirements."""
    return fingerprint(
        {
            "portability": artifact.get("portability"),
            "requirements": artifact.get("requirements"),
        }
    )


def trust_receipt_key(digest: str) -> str:
    normalized = normalize_digest(digest)
    return f"trust-receipts/sha256/{normalized[:2]}/{normalized}.json"


class CoordinatorManifestAuthority:
    """Worker facade: proposals and verification go to the coordinator.

    The worker possesses only its channel credential, never the signing key.
    """

    def __init__(self, channel: Any):
        if not callable(getattr(channel, "sign_prepared_manifest", None)):
            raise RuntimeError("Coordinator channel cannot sign prepared manifests")
        if not callable(getattr(channel, "verify_prepared_manifest", None)):
            raise RuntimeError("Coordinator channel cannot verify prepared manifests")
        if not callable(getattr(channel, "announce_prepared_manifest", None)):
            raise RuntimeError("Coordinator channel cannot announce prepared manifests")
        if not callable(getattr(channel, "fetch_prepared_manifest", None)):
            raise RuntimeError("Coordinator channel cannot fetch prepared manifests")
        self.channel = channel
        self.job_id: str | None = None
        self.volume_id: str | None = None

    def set_context(self, *, job_id: str | None, volume_id: str | None) -> None:
        self.job_id = job_id
        self.volume_id = volume_id

    def sign(self, proposal: dict[str, Any]) -> dict[str, Any]:
        if not self.job_id or not self.volume_id:
            raise RuntimeError(
                "Prepared manifest proposals require an active coordinator job and volume"
            )
        return self.channel.sign_prepared_manifest(
            proposal, job_id=self.job_id, volume_id=self.volume_id
        )

    def verify(self, manifest: dict[str, Any]) -> dict[str, Any]:
        return self.channel.verify_prepared_manifest(manifest)

    def fetch(self, manifest_id: str) -> dict[str, Any]:
        if not self.job_id or not self.volume_id:
            raise RuntimeError(
                "Prepared manifest fetches require an active coordinator job and volume"
            )
        return self.channel.fetch_prepared_manifest(
            manifest_id, job_id=self.job_id, volume_id=self.volume_id
        )

    def announce(self, manifest: dict[str, Any], *, generation: str) -> dict[str, Any]:
        if not self.job_id or not self.volume_id:
            raise RuntimeError(
                "Prepared manifest announcements require an active coordinator job and volume"
            )
        return self.channel.announce_prepared_manifest(
            manifest,
            job_id=self.job_id,
            volume_id=self.volume_id,
            generation=generation,
        )

    def sign_trust_receipt(
        self, proposal: dict[str, Any], *, manifest: dict[str, Any]
    ) -> dict[str, Any]:
        signer = getattr(self.channel, "sign_cache_trust_receipt", None)
        if not callable(signer):
            raise RuntimeError("Coordinator channel cannot sign cache trust receipts")
        if not self.job_id or not self.volume_id:
            raise RuntimeError(
                "Cache trust receipts require an active coordinator job and volume"
            )
        return signer(
            proposal,
            manifest=manifest,
            job_id=self.job_id,
            volume_id=self.volume_id,
        )

    def verify_trust_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        verifier = getattr(self.channel, "verify_cache_trust_receipt", None)
        if not callable(verifier):
            raise RuntimeError("Coordinator channel cannot verify cache trust receipts")
        if not self.job_id or not self.volume_id:
            raise RuntimeError(
                "Cache trust receipts require an active coordinator job and volume"
            )
        return verifier(
            receipt,
            job_id=self.job_id,
            volume_id=self.volume_id,
        )


def blob_key(digest: str) -> str:
    normalized = normalize_digest(digest)
    return f"blobs/sha256/{normalized[:2]}/{normalized}"


def bundle_key(digest: str) -> str:
    normalized = normalize_digest(digest)
    return f"bundles/sha256/{normalized[:2]}/{normalized}.tar.zst"


def manifest_by_id_key(manifest_id: str) -> str:
    normalized = normalize_digest(manifest_id)
    return f"manifests/by-id/sha256/{normalized[:2]}/{normalized}.json"


def environment_key(runtime: dict[str, Any]) -> str:
    required = ("image_digest", "platform", "python_abi", "dependency_lock")
    return fingerprint({key: runtime.get(key) for key in required})


def kernel_key(runtime: dict[str, Any]) -> str:
    required = ("code_digest", "torch", "cuda", "driver_constraint", "gpu_capability")
    return fingerprint({key: runtime.get(key) for key in required})


def profile_key(runtime_profile: str, required_artifact_keys: Iterable[str]) -> str:
    return fingerprint(
        {
            "runtime_profile": str(runtime_profile),
            "artifacts": sorted(str(item) for item in required_artifact_keys),
        }
    )


def profile_weight_requirement_key(
    repo_id: str, revision: str, filename: str | None = None
) -> str:
    member = str(filename) if filename else "<snapshot>"
    return f"profile-weight:{repo_id}@{revision}:{member}"


def custom_node_requirement_key(pack_id: str) -> str:
    return f"custom-node:{pack_id}"


def environment_requirement_key(dependency_lock: str) -> str:
    return f"environment:{digest_id(dependency_lock)}"


def artifact_requirement_key(artifact: dict[str, Any]) -> str | None:
    kind = str(artifact.get("kind") or "")
    if kind == "profile-weight":
        source = artifact.get("source") or {}
        return profile_weight_requirement_key(
            str(source.get("repo_id") or ""),
            str(source.get("revision") or ""),
            None
            if source.get("snapshot") is True
            else str(source.get("filename") or ""),
        )
    if kind == "custom-node-bundle":
        return custom_node_requirement_key(
            str((artifact.get("destination") or {}).get("pack_id") or "")
        )
    if kind == "environment-bundle":
        return environment_requirement_key(
            str((artifact.get("destination") or {}).get("dependency_lock") or "")
        )
    return None


def runtime_fingerprint(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Fingerprint the actual worker. Unknown fields remain absent and miss."""
    result: dict[str, Any] = {
        "python_abi": f"cp{sys.version_info.major}{sys.version_info.minor}",
        "platform": f"{sys.platform}-{platform_module.machine().lower()}",
    }
    try:
        import torch

        result["torch"] = str(torch.__version__)
        result["cuda"] = str(torch.version.cuda or "")
        if torch.cuda.is_available():
            major, minor = torch.cuda.get_device_capability()
            result["gpu_capability"] = f"{major}.{minor}"
    except ImportError:
        pass
    image_digest = os.environ.get("CLOUD_OFFLOAD_IMAGE_DIGEST", "").strip()
    if image_digest:
        result["image_digest"] = image_digest
    driver = os.environ.get("NVIDIA_DRIVER_VERSION", "").strip()
    if driver:
        result["driver_constraint"] = driver
    result.update(overrides or {})
    return result


@dataclass(frozen=True)
class CompatibilityDecision:
    accepted: bool
    reason: str
    mismatches: tuple[str, ...] = ()


_TIER_FIELDS = {
    "portable": (),
    "runtime-bound": ("image_digest", "platform", "python_abi", "dependency_lock"),
    "gpu-class-bound": (
        "code_digest",
        "torch",
        "cuda",
        "driver_constraint",
        "gpu_capability",
    ),
}


def artifact_compatibility(
    artifact: dict[str, Any], runtime: dict[str, Any]
) -> CompatibilityDecision:
    tier = str(artifact.get("portability") or "")
    if tier not in PORTABILITY_TIERS:
        return CompatibilityDecision(False, "unknown_portability", ("portability",))
    if tier in {"process-bound", "gpu-resident"}:
        return CompatibilityDecision(False, f"{tier}_is_not_durable")
    requirements = artifact.get("requirements")
    if not isinstance(requirements, dict):
        return CompatibilityDecision(False, "missing_requirements", ("requirements",))
    missing: list[str] = []
    mismatched: list[str] = []
    for key in _TIER_FIELDS[tier]:
        if key not in requirements or key not in runtime:
            missing.append(key)
        elif requirements[key] != runtime[key]:
            mismatched.append(key)
    if missing:
        return CompatibilityDecision(False, "unknown_compatibility", tuple(missing))
    if mismatched:
        return CompatibilityDecision(False, "runtime_mismatch", tuple(mismatched))
    return CompatibilityDecision(True, "compatible")


def artifact_policy(
    artifact: dict[str, Any], *, tenant: str, allow_private: bool = False
) -> CompatibilityDecision:
    policy = artifact.get("policy")
    if not isinstance(policy, dict):
        return CompatibilityDecision(False, "missing_policy", ("policy",))
    if policy.get("cacheable") is not True:
        return CompatibilityDecision(False, "not_cacheable")
    if str(policy.get("tenant") or "") != str(tenant):
        return CompatibilityDecision(False, "tenant_mismatch", ("tenant",))
    if policy.get("invalidated"):
        return CompatibilityDecision(False, "invalidated")
    if policy.get("private") and not allow_private:
        return CompatibilityDecision(False, "private_cache_refused")
    if policy.get("residency") and policy.get("datacenter_id"):
        if policy["residency"] != policy["datacenter_id"]:
            return CompatibilityDecision(False, "residency_mismatch")
    return CompatibilityDecision(True, "eligible")


class ManifestSigner:
    """Coordinator-anchored HMAC signer; the key never enters a manifest."""

    algorithm = "hmac-sha256"

    def __init__(self, key: bytes | str, key_id: str = "coordinator"):
        self.key = key.encode("utf-8") if isinstance(key, str) else bytes(key)
        if len(self.key) < 32:
            raise ValueError("Manifest signing key must be at least 32 bytes")
        self.key_id = str(key_id)

    @staticmethod
    def _payload(manifest: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in manifest.items()
            if key not in {"signature", "manifest_id"}
        }

    def sign(self, manifest: dict[str, Any]) -> dict[str, Any]:
        payload = self._payload(manifest)
        if payload.get("schema") != MANIFEST_SCHEMA:
            raise ManifestError(f"Unsupported manifest schema: {payload.get('schema')}")
        encoded = canonical_json(payload)
        manifest_id = digest_id(sha256_bytes(encoded))
        signature = hmac.new(self.key, encoded, hashlib.sha256).hexdigest()
        return {
            **payload,
            "manifest_id": manifest_id,
            "signature": {
                "algorithm": self.algorithm,
                "key_id": self.key_id,
                "value": signature,
            },
        }

    def verify(self, manifest: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(manifest, dict):
            raise ManifestError("Manifest must be an object")
        signature = manifest.get("signature")
        if not isinstance(signature, dict):
            raise ManifestError("Manifest has no signature")
        if signature.get("algorithm") != self.algorithm:
            raise ManifestError("Manifest signature algorithm is not trusted")
        if signature.get("key_id") != self.key_id:
            raise ManifestError("Manifest signing key is not trusted")
        payload = self._payload(manifest)
        encoded = canonical_json(payload)
        expected_id = digest_id(sha256_bytes(encoded))
        if not hmac.compare_digest(str(manifest.get("manifest_id")), expected_id):
            raise ManifestError("Manifest ID does not match its canonical payload")
        expected_signature = hmac.new(self.key, encoded, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(
            str(signature.get("value") or ""), expected_signature
        ):
            raise ManifestError("Manifest signature verification failed")
        validate_manifest_shape(manifest)
        return manifest

    @staticmethod
    def _trust_payload(receipt: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in receipt.items()
            if key not in {"signature", "receipt_id"}
        }

    def _trust_key(self) -> bytes:
        return hmac.new(
            self.key,
            b"cloud-offload.cache-trust-receipt.v1",
            hashlib.sha256,
        ).digest()

    def sign_trust_receipt(
        self,
        proposal: dict[str, Any],
        *,
        manifest: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = self._trust_payload(proposal)
        if payload.get("schema") != TRUST_RECEIPT_SCHEMA:
            raise ManifestError(
                f"Unsupported trust receipt schema: {payload.get('schema')}"
            )
        encoded = canonical_json({"domain": TRUST_RECEIPT_SCHEMA, "payload": payload})
        receipt_id = digest_id(sha256_bytes(encoded))
        signature = hmac.new(self._trust_key(), encoded, hashlib.sha256).hexdigest()
        signed = {
            **payload,
            "receipt_id": receipt_id,
            "signature": {
                "algorithm": self.algorithm,
                "key_id": self.key_id,
                "value": signature,
            },
        }
        validate_trust_receipt_shape(signed)
        return signed

    def verify_trust_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(receipt, dict):
            raise ManifestError("Cache trust receipt must be an object")
        signature = receipt.get("signature")
        if not isinstance(signature, dict):
            raise ManifestError("Cache trust receipt has no signature")
        if signature.get("algorithm") != self.algorithm:
            raise ManifestError("Cache trust receipt signature is not trusted")
        if signature.get("key_id") != self.key_id:
            raise ManifestError("Cache trust receipt signing key is not trusted")
        payload = self._trust_payload(receipt)
        encoded = canonical_json({"domain": TRUST_RECEIPT_SCHEMA, "payload": payload})
        expected_id = digest_id(sha256_bytes(encoded))
        if not hmac.compare_digest(str(receipt.get("receipt_id")), expected_id):
            raise ManifestError("Cache trust receipt ID does not match its payload")
        expected_signature = hmac.new(
            self._trust_key(), encoded, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(
            str(signature.get("value") or ""), expected_signature
        ):
            raise ManifestError("Cache trust receipt signature verification failed")
        validate_trust_receipt_shape(receipt)
        return receipt


def load_or_create_manifest_signer(
    key_path: str | Path, *, key_id: str = "coordinator-v1"
) -> ManifestSigner:
    """Load the coordinator-only manifest authority from an owner-only file."""
    path = Path(key_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        key = path.read_bytes()
    except FileNotFoundError:
        key = os.urandom(32)
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(key)
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError:
            key = path.read_bytes()
    if len(key) != 32:
        raise RuntimeError(f"Prepared manifest signing key is invalid: {path}")
    try:
        path.chmod(0o600)
    except OSError:
        pass
    return ManifestSigner(key, key_id=key_id)


def validate_manifest_shape(manifest: dict[str, Any]) -> None:
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise ManifestError(f"Unsupported manifest schema: {manifest.get('schema')}")
    if not str(manifest.get("profile_fingerprint") or "").startswith("sha256:"):
        raise ManifestError("Manifest profile_fingerprint is missing")
    if not isinstance(manifest.get("producer"), dict):
        raise ManifestError("Manifest producer is missing")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list):
        raise ManifestError("Manifest artifacts must be a list")
    seen: set[tuple[str, str, str, str]] = set()
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            raise ManifestError("Manifest artifact must be an object")
        digest = digest_id(str(artifact.get("digest") or ""))
        identity = (
            digest,
            str(artifact.get("kind") or ""),
            json.dumps(artifact.get("source") or {}, sort_keys=True),
            json.dumps(artifact.get("destination") or {}, sort_keys=True),
        )
        if identity in seen:
            raise ManifestError(f"Manifest repeats artifact identity {digest}")
        seen.add(identity)
        size = int(artifact.get("size") or -1)
        if size < 0:
            raise ManifestError(f"Artifact {digest} has invalid size")
        is_bundle = (
            artifact.get("portability") == "runtime-bound"
            or artifact.get("materialization") == "extract"
        )
        expected_key = bundle_key(digest) if is_bundle else blob_key(digest)
        key = str(artifact.get("storage_key") or "")
        if key != expected_key:
            raise ManifestError(f"Artifact {digest} has non-canonical storage key")
        if artifact.get("portability") not in PORTABILITY_TIERS:
            raise ManifestError(f"Artifact {digest} has unknown portability")


def _parse_utc_timestamp(value: Any, field_name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ManifestError(f"Cache trust receipt {field_name} is invalid") from exc
    if parsed.tzinfo is None:
        raise ManifestError(f"Cache trust receipt {field_name} has no timezone")
    return parsed.astimezone(timezone.utc)


def validate_trust_receipt_shape(receipt: dict[str, Any]) -> None:
    if receipt.get("schema") != TRUST_RECEIPT_SCHEMA:
        raise ManifestError(
            f"Unsupported trust receipt schema: {receipt.get('schema')}"
        )
    digest = digest_id(str(receipt.get("artifact_digest") or ""))
    digest_id(str(receipt.get("manifest_id") or ""))
    digest_id(str(receipt.get("manifest_signature_digest") or ""))
    digest_id(str(receipt.get("runtime_compatibility") or ""))
    size = int(receipt.get("artifact_size") or -1)
    if size < 0:
        raise ManifestError(f"Cache trust receipt {digest} has invalid size")
    storage_key = str(receipt.get("storage_key") or "")
    if storage_key not in {blob_key(digest), bundle_key(digest)}:
        raise ManifestError(
            f"Cache trust receipt {digest} has non-canonical storage key"
        )
    if not str(receipt.get("volume_id") or ""):
        raise ManifestError("Cache trust receipt has no volume identity")
    if not str(receipt.get("provider_volume_id") or ""):
        raise ManifestError("Cache trust receipt has no provider volume identity")
    generation = receipt.get("object_generation")
    if not isinstance(generation, dict):
        raise ManifestError("Cache trust receipt has no object generation")
    if str(generation.get("storage_key") or "") != storage_key:
        raise ManifestError("Cache trust receipt object identity is inconsistent")
    if int(generation.get("size") or -1) != size:
        raise ManifestError("Cache trust receipt object size is inconsistent")
    if int(generation.get("modified_ns") or -1) < 0:
        raise ManifestError("Cache trust receipt object generation is invalid")
    verified_at = _parse_utc_timestamp(receipt.get("verified_at"), "verified_at")
    expires_at = _parse_utc_timestamp(receipt.get("expires_at"), "expires_at")
    if expires_at <= verified_at:
        raise ManifestError("Cache trust receipt expiry is not after verification")
    scrub = receipt.get("scrub")
    if not isinstance(scrub, dict):
        raise ManifestError("Cache trust receipt has no scrub policy")
    audit_due = _parse_utc_timestamp(
        scrub.get("full_audit_due_at"), "full_audit_due_at"
    )
    if audit_due < verified_at:
        raise ManifestError("Cache trust receipt full audit predates verification")
    samples = scrub.get("samples")
    if not isinstance(samples, list) or not samples:
        raise ManifestError("Cache trust receipt has no byte samples")
    previous = -1
    for sample in samples:
        if not isinstance(sample, dict):
            raise ManifestError("Cache trust receipt sample is not an object")
        offset = int(sample.get("offset") or 0)
        sample_size = int(sample.get("size") or 0)
        if offset < 0 or sample_size <= 0 or offset + sample_size > size:
            raise ManifestError("Cache trust receipt sample is outside the object")
        if offset <= previous:
            raise ManifestError("Cache trust receipt samples are not ordered")
        previous = offset
        digest_id(str(sample.get("sha256") or ""))


def build_manifest(
    *,
    profile_fingerprint: str,
    producer: dict[str, Any],
    artifacts: list[dict[str, Any]],
    signer: ManifestSigner,
    created_at: str | None = None,
    claims: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return signer.sign(
        {
            "schema": MANIFEST_SCHEMA,
            "profile_fingerprint": digest_id(profile_fingerprint),
            "created_at": created_at or utc_now(),
            "producer": dict(producer),
            "artifacts": artifacts,
            **(claims or {}),
        }
    )


@dataclass
class RestoreReceipt:
    manifest_id: str | None
    volume_id: str
    datacenter_id: str
    worker_class: str
    started_at: str = field(default_factory=utc_now)
    artifacts: list[dict[str, Any]] = field(default_factory=list)
    fallback_reason: str | None = None

    def record(self, **entry: Any) -> None:
        self.artifacts.append(dict(entry))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": RECEIPT_SCHEMA,
            "manifest_id": self.manifest_id,
            "volume_id": self.volume_id,
            "datacenter_id": self.datacenter_id,
            "worker_class": self.worker_class,
            "started_at": self.started_at,
            "completed_at": utc_now(),
            "artifacts": self.artifacts,
            "fallback_reason": self.fallback_reason,
            "cached_bytes": sum(
                int(item.get("bytes") or 0)
                for item in self.artifacts
                if item.get("result") == "hit"
            ),
        }


class PreparedStateCAS:
    """Attached-volume CAS using staging, immutable targets and atomic pointers."""

    def __init__(self, root: str | Path, signer: ManifestSigner):
        self.root = Path(root).resolve()
        self.signer = signer
        for name in (
            "blobs",
            "bundles",
            "manifests",
            "indexes",
            "staging",
            "quarantine",
            "leases",
            "pending-announcements",
            "trust-receipts",
        ):
            (self.root / name).mkdir(parents=True, exist_ok=True)
        self._index_thread_lock = _local_publication_lock(self.root)
        self._full_verified_objects: dict[
            tuple[str, str], dict[str, Any]
        ] = {}

    @contextmanager
    def _index_publication_lock(self):
        """Serialize inventory read/merge/publish across Linux worker processes.

        RunPod workers are Linux, where ``flock`` is volume-wide. Platforms
        without ``fcntl`` retain a process-wide lock for local development;
        they must not be used for multi-process prepared-volume publication.
        """
        lock_path = self._resolve("leases/index-publication.lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self._index_thread_lock:
            with lock_path.open("a+b") as handle:
                try:
                    import fcntl
                except ImportError:  # pragma: no cover - Windows development only
                    yield
                else:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                    try:
                        yield
                    finally:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _resolve(self, key: str) -> Path:
        path = (self.root / PurePosixPath(str(key))).resolve()
        try:
            path.relative_to(self.root)
        except ValueError as exc:
            raise ValueError(f"Prepared-state key escapes cache root: {key!r}") from exc
        return path

    @staticmethod
    def _object_generation(path: Path, storage_key: str) -> dict[str, Any]:
        stat = path.stat()
        return {
            "storage_key": str(storage_key),
            "size": int(stat.st_size),
            "modified_ns": int(stat.st_mtime_ns),
        }

    @staticmethod
    def _sample_layout(
        path: Path,
        *,
        sample_bytes: int = DEFAULT_TRUST_SAMPLE_BYTES,
        sample_count: int = DEFAULT_TRUST_SAMPLE_COUNT,
    ) -> list[dict[str, Any]]:
        size = int(path.stat().st_size)
        if size <= 0:
            return []
        width = min(size, max(4096, int(sample_bytes)))
        maximum_offset = size - width
        count = max(1, min(int(sample_count), 16))
        offsets = sorted(
            {
                round(maximum_offset * index / max(1, count - 1))
                for index in range(count)
            }
        )
        samples: list[dict[str, Any]] = []
        with path.open("rb") as handle:
            for offset in offsets:
                handle.seek(offset)
                value = handle.read(width)
                if len(value) != width:
                    raise CacheCorruptionError(
                        "Prepared object changed while trust samples were read"
                    )
                samples.append(
                    {
                        "offset": int(offset),
                        "size": len(value),
                        "sha256": digest_id(sha256_bytes(value)),
                    }
                )
        return samples

    @staticmethod
    def _receipt_eligible(artifact: dict[str, Any]) -> bool:
        policy = artifact.get("policy") or {}
        return not bool(
            policy.get("private")
            or policy.get("sensitive")
            or policy.get("verification") == "full"
        )

    def _receipt_path(self, digest: str) -> Path:
        return self._resolve(trust_receipt_key(digest))

    def _write_trust_receipt(self, receipt: dict[str, Any]) -> None:
        target = self._receipt_path(receipt["artifact_digest"])
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._resolve(
            f"staging/trust-receipt-{uuid.uuid4().hex}.partial"
        )
        temporary.write_bytes(canonical_json(receipt))
        os.replace(temporary, target)

    def _issue_trust_receipt(
        self,
        manifest: dict[str, Any],
        artifact: dict[str, Any],
        *,
        volume_id: str,
        provider_volume_id: str,
    ) -> dict[str, Any] | None:
        if not self._receipt_eligible(artifact):
            return None
        size = int(artifact.get("size") or 0)
        if size <= 0:
            return None
        digest = normalize_digest(artifact["digest"])
        storage_key = str(artifact["storage_key"])
        source = self._resolve(storage_key)
        generation = self._object_generation(source, storage_key)
        verified_generation = self._full_verified_objects.get((digest, storage_key))
        if verified_generation != generation:
            return None
        samples = self._sample_layout(source)
        verified_at = datetime.now(timezone.utc)
        proposal = {
            "schema": TRUST_RECEIPT_SCHEMA,
            "manifest_id": manifest["manifest_id"],
            "manifest_signature_digest": manifest_signature_digest(manifest),
            "artifact_digest": digest_id(digest),
            "artifact_size": size,
            "storage_key": storage_key,
            "volume_id": str(volume_id),
            "provider_volume_id": str(provider_volume_id),
            "runtime_compatibility": artifact_runtime_compatibility_key(artifact),
            "object_generation": generation,
            "verified_at": verified_at.isoformat().replace("+00:00", "Z"),
            "expires_at": (
                verified_at + timedelta(seconds=DEFAULT_TRUST_RECEIPT_TTL_SECONDS)
            ).isoformat().replace("+00:00", "Z"),
            "scrub": {
                "mode": "rotating-sample-and-scheduled-full-audit",
                "full_audit_due_at": (
                    verified_at
                    + timedelta(seconds=DEFAULT_FULL_AUDIT_INTERVAL_SECONDS)
                ).isoformat().replace("+00:00", "Z"),
                "samples": samples,
            },
        }
        signer = getattr(self.signer, "sign_trust_receipt", None)
        if not callable(signer):
            return None
        signed = signer(proposal, manifest=manifest)
        self._write_trust_receipt(signed)
        return signed

    def _load_trust_receipt(self, digest: str) -> dict[str, Any] | None:
        path = self._receipt_path(digest)
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
            return None
        return value if isinstance(value, dict) else None

    def _verify_trust_receipt(
        self,
        receipt: dict[str, Any],
        manifest: dict[str, Any],
        artifact: dict[str, Any],
        *,
        volume_id: str,
        provider_volume_id: str,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        verifier = getattr(self.signer, "verify_trust_receipt", None)
        if not callable(verifier):
            return None
        try:
            verified = verifier(receipt)
        except (ManifestError, RuntimeError, ValueError):
            return None
        digest = digest_id(str(artifact["digest"]))
        storage_key = str(artifact["storage_key"])
        if any(
            (
                verified.get("manifest_id") != manifest.get("manifest_id"),
                verified.get("manifest_signature_digest")
                != manifest_signature_digest(manifest),
                verified.get("artifact_digest") != digest,
                int(verified.get("artifact_size") or -1)
                != int(artifact.get("size") or -2),
                verified.get("storage_key") != storage_key,
                verified.get("volume_id") != str(volume_id),
                verified.get("provider_volume_id") != str(provider_volume_id),
                verified.get("runtime_compatibility")
                != artifact_runtime_compatibility_key(artifact),
            )
        ):
            return None
        current_time = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        if _parse_utc_timestamp(verified["expires_at"], "expires_at") <= current_time:
            return None
        audit_due = _parse_utc_timestamp(
            (verified.get("scrub") or {}).get("full_audit_due_at"),
            "full_audit_due_at",
        )
        if audit_due <= current_time:
            return None
        source = self._resolve(storage_key)
        try:
            generation = self._object_generation(source, storage_key)
        except (FileNotFoundError, OSError):
            return None
        if generation != verified.get("object_generation"):
            return None
        samples = list((verified.get("scrub") or {}).get("samples") or [])
        if not samples:
            return None
        bucket = int(current_time.timestamp() // 3600)
        selector = sha256_bytes(
            f"{verified['receipt_id']}:{bucket}".encode("utf-8")
        )
        sample_index = int(selector[:8], 16) % len(samples)
        sample = samples[sample_index]
        offset = int(sample["offset"])
        size = int(sample["size"])
        with source.open("rb") as handle:
            handle.seek(offset)
            value = handle.read(size)
        if len(value) != size or digest_id(sha256_bytes(value)) != sample["sha256"]:
            raise CacheCorruptionError(
                f"Prepared object {digest} failed a signed trust sample"
            )
        result: dict[str, Any] = {
            "mode": "trusted_metadata_sample",
            "bytes_read": size,
            "receipt_id": verified["receipt_id"],
            "full_audit_due_at": (verified.get("scrub") or {}).get(
                "full_audit_due_at"
            ),
        }
        if len(samples) > 1:
            background_sample = samples[(sample_index + 1) % len(samples)]
            outcome: dict[str, Any] = {}

            def scrub() -> None:
                try:
                    background_offset = int(background_sample["offset"])
                    background_size = int(background_sample["size"])
                    with source.open("rb") as handle:
                        handle.seek(background_offset)
                        background_value = handle.read(background_size)
                    if (
                        len(background_value) != background_size
                        or digest_id(sha256_bytes(background_value))
                        != background_sample["sha256"]
                    ):
                        raise CacheCorruptionError(
                            f"Prepared object {digest} failed a background trust sample"
                        )
                    outcome["bytes_read"] = background_size
                except BaseException as exc:
                    outcome["error"] = exc

            thread = threading.Thread(
                target=scrub,
                name=f"cache-scrub-{normalize_digest(digest)[:12]}",
                daemon=True,
            )
            thread.start()
            result["_background_thread"] = thread
            result["_background_outcome"] = outcome
        return result

    @staticmethod
    def _finish_background_scrub(verification: dict[str, Any]) -> None:
        thread = verification.pop("_background_thread", None)
        outcome = verification.pop("_background_outcome", None)
        if thread is None or not isinstance(outcome, dict):
            if verification.get("mode") == "trusted_metadata_sample":
                verification["background_sampled"] = False
            return
        thread.join()
        error = outcome.get("error")
        if error is not None:
            if isinstance(error, CacheCorruptionError):
                raise error
            raise CacheCorruptionError(
                "Prepared object background trust sample could not complete"
            ) from error
        sampled = int(outcome.get("bytes_read") or 0)
        verification["bytes_read"] = int(verification.get("bytes_read") or 0) + sampled
        verification["background_sampled"] = sampled > 0

    @staticmethod
    def _remove_materialized_destination(destination: Path) -> None:
        try:
            if destination.is_symlink() or destination.is_file():
                destination.unlink(missing_ok=True)
            elif destination.is_dir():
                shutil.rmtree(destination)
        except OSError:
            pass

    def _remember_full_verification(self, digest: str, path: Path) -> None:
        try:
            storage_key = path.resolve().relative_to(self.root).as_posix()
        except ValueError:
            return
        self._full_verified_objects[(normalize_digest(digest), storage_key)] = (
            self._object_generation(path, storage_key)
        )

    def verify_mount(self, expected_volume_id: str | None = None) -> None:
        if not self.root.is_dir():
            raise CacheMountError(f"Prepared cache mount is absent: {self.root}")
        marker = self.root / ".volume-id"
        actual = os.environ.get("RUNPOD_VOLUME_ID", "").strip()
        if marker.is_file():
            marker_value = marker.read_text(encoding="utf-8").strip()
            if actual and marker_value and marker_value != actual:
                raise CacheMountError(
                    f"Cache mount marker {marker_value} disagrees with provider volume {actual}"
                )
            actual = actual or marker_value
        if expected_volume_id and actual and expected_volume_id != actual:
            raise CacheMountError(
                f"Expected cache volume {expected_volume_id}, provider mounted {actual}"
            )
        if expected_volume_id and not actual:
            raise CacheMountError(
                f"Cannot identify expected cache volume {expected_volume_id} at {self.root}"
            )

    def _lease(
        self, digest: str, writer_id: str, ttl_seconds: int = 900
    ) -> Path | None:
        lease = self._resolve(f"leases/{normalize_digest(digest)}.json")
        lease.parent.mkdir(parents=True, exist_ok=True)
        payload = canonical_json(
            {"writer_id": writer_id, "expires_at": time.time() + ttl_seconds}
        )
        try:
            descriptor = os.open(lease, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
            return lease
        except FileExistsError:
            try:
                current = json.loads(lease.read_text(encoding="utf-8"))
                if float(current.get("expires_at") or 0) < time.time():
                    lease.unlink(missing_ok=True)
                    return self._lease(digest, writer_id, ttl_seconds)
            except (OSError, ValueError, json.JSONDecodeError):
                pass
            return None

    def publish_blob(
        self,
        source: str | Path,
        expected_digest: str,
        *,
        writer_id: str | None = None,
        bundle: bool = False,
        source_verified: bool = False,
        progress_callback: Callable[[int, int], None] | None = None,
        commit_callback: Callable[[str], None] | None = None,
    ) -> Path:
        source = Path(source)
        digest = normalize_digest(expected_digest)
        if not source_verified and sha256_file(source) != digest:
            raise CacheCorruptionError(
                f"Population source does not match sha256:{digest}"
            )
        target = self._resolve(bundle_key(digest) if bundle else blob_key(digest))
        if target.is_file():
            self.verify_object(digest, target=target)
            return target
        writer_id = writer_id or uuid.uuid4().hex
        lease = self._lease(digest, writer_id)
        staging = self._resolve(f"staging/{writer_id}/{target.name}.partial")
        staging.parent.mkdir(parents=True, exist_ok=False)
        try:
            total = source.stat().st_size
            copied = 0
            copied_digest = hashlib.sha256()
            with source.open("rb") as reader, staging.open("xb") as writer:
                for chunk in iter(lambda: reader.read(8 * 1024 * 1024), b""):
                    writer.write(chunk)
                    copied_digest.update(chunk)
                    copied += len(chunk)
                    if progress_callback:
                        progress_callback(copied, total)
                writer.flush()
                if commit_callback:
                    commit_callback("flushing")
            # RunPod network volumes are object-backed FUSE mounts. fsync can
            # hang for minutes or fail with EIO even though close+readback is
            # healthy. Closing the writer above hands the object to the mount;
            # a full digest readback below is the stronger portable durability
            # check we actually need.
            if copied != total or copied_digest.hexdigest() != digest:
                raise CacheCorruptionError(
                    f"Prepared cache copy does not match sha256:{digest}"
                )
            if commit_callback:
                commit_callback("verifying")
            self.verify_object(digest, target=staging, expected_size=total)
            target.parent.mkdir(parents=True, exist_ok=True)
            if commit_callback:
                commit_callback("publishing")
            try:
                # Hard-link is an atomic create-if-absent. Another valid writer
                # winning the race is harmless because the name is the digest.
                os.link(staging, target)
            except FileExistsError:
                self.verify_object(digest, target=target)
            except OSError as exc:
                # Never stream into the immutable target: a dead writer would
                # publish a partial object. The mounted filesystem must support
                # atomic link creation for safe multi-writer population.
                raise RuntimeError(
                    "Prepared cache filesystem cannot atomically publish objects"
                ) from exc
            # When our hard-link won, target and the already verified staging
            # file are the same inode. Re-reading a 20 GB object here provides
            # no additional integrity and doubles first-run publication time.
            if not target.samefile(staging):
                self.verify_object(digest, target=target)
            else:
                self._remember_full_verification(digest, target)
            if commit_callback:
                commit_callback("committed")
            return target
        finally:
            shutil.rmtree(staging.parent, ignore_errors=True)
            if lease:
                # A losing Windows test writer may still have the advisory
                # lease open for its expiry check. Retry the owner cleanup;
                # Linux volume workers do not need this, but the semantics are
                # the same on both platforms.
                for attempt in range(5):
                    try:
                        lease.unlink(missing_ok=True)
                        break
                    except PermissionError:
                        if attempt == 4:
                            raise
                        time.sleep(0.01)

    def verify_object(
        self,
        digest: str,
        *,
        target: Path | None = None,
        expected_size: int | None = None,
    ) -> Path:
        normalized = normalize_digest(digest)
        path = target or self._resolve(blob_key(normalized))
        if not path.is_file():
            raise CacheCorruptionError(
                f"Prepared object sha256:{normalized} is missing"
            )
        if expected_size is not None and path.stat().st_size != int(expected_size):
            raise CacheCorruptionError(
                f"Prepared object sha256:{normalized} has the wrong size"
            )
        actual = sha256_file(path)
        if actual != normalized:
            raise CacheCorruptionError(
                f"Prepared object sha256:{normalized} hashes as sha256:{actual}"
            )
        self._remember_full_verification(normalized, path)
        return path

    def quarantine(
        self, digest: str, reason: str, *, storage_key: str | None = None
    ) -> Path | None:
        normalized = normalize_digest(digest)
        self._receipt_path(normalized).unlink(missing_ok=True)
        for key in [item for item in self._full_verified_objects if item[0] == normalized]:
            self._full_verified_objects.pop(key, None)
        candidates = (
            [self._resolve(storage_key)]
            if storage_key
            else [
                self._resolve(blob_key(normalized)),
                self._resolve(bundle_key(normalized)),
            ]
        )
        source = next(
            (candidate for candidate in candidates if candidate.exists()), None
        )
        if source is None:
            return None
        target = self._resolve(
            f"quarantine/{normalized}/{int(time.time())}-{uuid.uuid4().hex[:8]}"
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(source, target)
        (target.with_suffix(".reason")).write_text(str(reason), encoding="utf-8")
        return target

    def publish_manifest(
        self,
        manifest: dict[str, Any],
        *,
        verified_digests: set[str] | None = None,
    ) -> Path:
        verified = self.signer.verify(manifest)
        already_verified = {
            normalize_digest(item) for item in (verified_digests or set())
        }
        for artifact in verified["artifacts"]:
            if normalize_digest(artifact["digest"]) in already_verified:
                continue
            self.verify_object(
                artifact["digest"],
                target=self._resolve(artifact["storage_key"]),
                expected_size=int(artifact["size"]),
            )
        profile = normalize_digest(verified["profile_fingerprint"])
        generation = f"{time.time_ns()}-{uuid.uuid4().hex}"
        key = f"manifests/{profile}/{generation}.json"
        target = self._resolve(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._resolve(f"staging/manifest-{uuid.uuid4().hex}.partial")
        temporary.write_bytes(canonical_json(verified))
        os.replace(temporary, target)
        self.publish_index(
            [verified],
            generation=generation,
            manifest_keys={verified["manifest_id"]: key},
        )
        announcer = getattr(self.signer, "announce", None)
        if callable(announcer):
            # Publication is authoritative only after both the immutable
            # manifest and the volume's atomic index pointer are visible.
            try:
                announcer(verified, generation=generation)
            except Exception:
                pending = self._resolve(
                    f"pending-announcements/{verified['manifest_id'].removeprefix('sha256:')}.json"
                )
                temporary_pending = self._resolve(
                    f"staging/announcement-{uuid.uuid4().hex}.partial"
                )
                temporary_pending.write_bytes(
                    canonical_json({"manifest": verified, "generation": generation})
                )
                os.replace(temporary_pending, pending)
        volume_id = str(verified.get("cache_volume_id") or "")
        provider_volume_id = str(
            verified.get("cache_provider_volume_id") or ""
        )
        if volume_id and provider_volume_id:
            for artifact in verified["artifacts"]:
                try:
                    self._issue_trust_receipt(
                        verified,
                        artifact,
                        volume_id=volume_id,
                        provider_volume_id=provider_volume_id,
                    )
                except (ManifestError, RuntimeError, ValueError, OSError):
                    # Trust receipts are an optimization. A missing receipt
                    # always returns the next restore to complete verification.
                    pass
        return target

    def retry_pending_announcements(self) -> tuple[int, int]:
        """Best-effort heal coordinator projection after a prior HTTP failure."""
        announcer = getattr(self.signer, "announce", None)
        if not callable(announcer):
            return (0, 0)
        succeeded = 0
        failed = 0
        for marker in sorted(self._resolve("pending-announcements").glob("*.json")):
            try:
                payload = json.loads(marker.read_text(encoding="utf-8"))
                manifest = self.signer.verify(payload["manifest"])
                announcer(manifest, generation=str(payload["generation"]))
                marker.unlink(missing_ok=True)
                succeeded += 1
            except Exception:
                failed += 1
        return succeeded, failed

    def publish_index(
        self,
        manifests: list[dict[str, Any]],
        *,
        generation: str | None = None,
        manifest_keys: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        generation = generation or f"{time.time_ns()}-{uuid.uuid4().hex}"
        with self._index_publication_lock():
            try:
                previous = self.load_index().get("manifests") or []
            except (OSError, ValueError, ManifestError, json.JSONDecodeError):
                previous = []
            entries_by_id = {entry["manifest_id"]: entry for entry in previous}
            for manifest in manifests:
                self.signer.verify(manifest)
                entries_by_id[manifest["manifest_id"]] = {
                    "manifest_id": manifest["manifest_id"],
                    "storage_key": (manifest_keys or {}).get(manifest["manifest_id"]),
                    "profile_fingerprint": manifest["profile_fingerprint"],
                    "created_at": manifest["created_at"],
                    "generation": generation,
                    "artifacts": [
                        {
                            "digest": item["digest"],
                            "kind": item["kind"],
                            "size": item["size"],
                            "portability": item["portability"],
                            "requirements": item["requirements"],
                            "policy": item["policy"],
                        }
                        for item in manifest["artifacts"]
                    ],
                }
            entries = sorted(
                entries_by_id.values(),
                key=lambda item: (item.get("created_at") or "", item["manifest_id"]),
            )
            index = {
                "schema": INDEX_SCHEMA,
                "generation": generation,
                "created_at": utc_now(),
                "manifests": entries,
            }
            target = self._resolve(f"indexes/{generation}.json")
            target.write_bytes(canonical_json(index))
            pointer_tmp = self._resolve(f"staging/index-pointer-{uuid.uuid4().hex}")
            pointer_tmp.write_text(generation, encoding="utf-8")
            os.replace(pointer_tmp, self._resolve("indexes/latest"))
            return index

    def load_index(self, generation: str | None = None) -> dict[str, Any]:
        if generation is None:
            pointer = self._resolve("indexes/latest")
            if not pointer.is_file():
                return {"schema": INDEX_SCHEMA, "generation": None, "manifests": []}
            generation = pointer.read_text(encoding="utf-8").strip()
        if not generation or "/" in generation or "\\" in generation:
            raise ManifestError("Invalid inventory generation")
        index = json.loads(
            self._resolve(f"indexes/{generation}.json").read_text(encoding="utf-8")
        )
        if index.get("schema") != INDEX_SCHEMA or index.get("generation") != generation:
            raise ManifestError("Inventory generation is malformed")
        return index

    def find_manifest(
        self, *, profile_fingerprint: str | None = None, manifest_id: str | None = None
    ) -> dict[str, Any] | None:
        index = self.load_index()
        entries = index.get("manifests") or []
        candidates = [
            entry
            for entry in entries
            if (
                not profile_fingerprint
                or entry["profile_fingerprint"] == profile_fingerprint
            )
            and (not manifest_id or entry["manifest_id"] == manifest_id)
        ]
        if manifest_id:
            document = None
            if candidates:
                newest = max(
                    candidates,
                    key=lambda item: (
                        item.get("created_at") or "",
                        item.get("generation") or "",
                    ),
                )
                key = newest.get("storage_key")
                path = self._resolve(key) if key else None
                if path and path.is_file():
                    document = json.loads(path.read_text(encoding="utf-8"))
            if document is None:
                direct = self._resolve(manifest_by_id_key(manifest_id))
                if direct.is_file():
                    document = json.loads(direct.read_text(encoding="utf-8"))
            if document is None:
                fetch = getattr(self.signer, "fetch", None)
                if not callable(fetch):
                    return None
                document = fetch(manifest_id)
            verified = self.signer.verify(document)
            if verified["manifest_id"] != digest_id(manifest_id):
                raise ManifestError(
                    "Exact manifest source contains a different manifest"
                )
            if (
                profile_fingerprint
                and verified["profile_fingerprint"] != profile_fingerprint
            ):
                return None
            return verified
        if not candidates:
            return None
        newest = max(
            candidates,
            key=lambda item: (
                item.get("created_at") or "",
                item.get("generation") or "",
            ),
        )
        key = newest.get("storage_key")
        if not key:
            raise ManifestError("Inventory entry has no manifest storage key")
        document = json.loads(self._resolve(key).read_text(encoding="utf-8"))
        return self.signer.verify(document)

    def find_portable_artifact(self, digest: str) -> tuple[dict[str, Any], str] | None:
        """Find a verified portable blob across profiles on this same volume."""
        wanted = digest_id(digest)
        candidates = [
            entry
            for entry in self.load_index().get("manifests") or []
            if any(
                item.get("digest") == wanted and item.get("portability") == "portable"
                for item in entry.get("artifacts") or []
            )
        ]
        for entry in sorted(
            candidates,
            key=lambda item: (
                item.get("created_at") or "",
                item.get("generation") or "",
            ),
            reverse=True,
        ):
            manifest = self.find_manifest(manifest_id=entry["manifest_id"])
            if not manifest:
                continue
            artifact = next(
                (
                    item
                    for item in manifest["artifacts"]
                    if item.get("digest") == wanted
                    and item.get("portability") == "portable"
                ),
                None,
            )
            if artifact:
                return artifact, manifest["manifest_id"]
        return None

    def restore_artifact(
        self,
        artifact: dict[str, Any],
        destination: str | Path,
        *,
        runtime: dict[str, Any],
        tenant: str,
        allow_private: bool = False,
        symlink_portable: bool = True,
        manifest: dict[str, Any] | None = None,
        volume_id: str | None = None,
        provider_volume_id: str | None = None,
        trust_receipts: bool = True,
        verification_callback: Callable[[dict[str, Any]], None] | None = None,
        now: datetime | None = None,
    ) -> Path:
        compatible = artifact_compatibility(artifact, runtime)
        if not compatible.accepted:
            raise ManifestError(
                f"Prepared artifact refused: {compatible.reason} "
                f"({', '.join(compatible.mismatches)})"
            )
        policy = artifact_policy(artifact, tenant=tenant, allow_private=allow_private)
        if not policy.accepted:
            raise CachePolicyError(f"Prepared artifact refused: {policy.reason}")
        source = self._resolve(artifact["storage_key"])
        selected_volume = str(
            volume_id or (manifest or {}).get("cache_volume_id") or ""
        )
        selected_provider_volume = str(
            provider_volume_id
            or (manifest or {}).get("cache_provider_volume_id")
            or ""
        )
        verification: dict[str, Any] | None = None
        can_trust = bool(
            trust_receipts
            and manifest
            and selected_volume
            and selected_provider_volume
            and self._receipt_eligible(artifact)
        )
        if can_trust:
            receipt = self._load_trust_receipt(artifact["digest"])
            if receipt:
                verification = self._verify_trust_receipt(
                    receipt,
                    manifest or {},
                    artifact,
                    volume_id=selected_volume,
                    provider_volume_id=selected_provider_volume,
                    now=now,
                )
        if verification is None:
            source = self.verify_object(
                artifact["digest"],
                target=source,
                expected_size=int(artifact["size"]),
            )
            verification = {
                "mode": "full_digest",
                "bytes_read": int(artifact["size"]),
                "receipt_issued": False,
            }
            if can_trust:
                try:
                    issued = self._issue_trust_receipt(
                        manifest or {},
                        artifact,
                        volume_id=selected_volume,
                        provider_volume_id=selected_provider_volume,
                    )
                    verification["receipt_issued"] = issued is not None
                except (ManifestError, RuntimeError, ValueError, OSError):
                    pass
        destination = Path(destination).resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        if (
            artifact["portability"] == "runtime-bound"
            or artifact.get("materialization") == "extract"
        ):
            self._extract_verified_bundle(source, destination)
        elif symlink_portable:
            temporary = destination.with_name(
                destination.name + f".{uuid.uuid4().hex}.tmp"
            )
            temporary.symlink_to(source)
            os.replace(temporary, destination)
        else:
            temporary = destination.with_name(
                destination.name + f".{uuid.uuid4().hex}.tmp"
            )
            shutil.copyfile(source, temporary)
            os.replace(temporary, destination)
        try:
            self._finish_background_scrub(verification)
        except CacheCorruptionError:
            self._remove_materialized_destination(destination)
            raise
        if verification_callback:
            verification_callback(dict(verification))
        return destination

    @staticmethod
    def _extract_verified_bundle(archive: Path, destination: Path) -> None:
        temporary = destination.with_name(destination.name + f".{uuid.uuid4().hex}.tmp")
        temporary.mkdir(parents=True, exist_ok=False)
        try:
            with tarfile.open(archive, mode="r:*") as bundle:
                for member in bundle.getmembers():
                    relative = PurePosixPath(member.name)
                    if relative.is_absolute() or ".." in relative.parts:
                        raise CacheCorruptionError(
                            "Prepared bundle contains path traversal"
                        )
                    if not member.isdir() and not member.isfile():
                        raise CacheCorruptionError(
                            "Prepared bundle contains a link or special filesystem member"
                        )
                    target = (temporary / relative).resolve()
                    try:
                        target.relative_to(temporary.resolve())
                    except ValueError as exc:
                        raise CacheCorruptionError(
                            "Prepared bundle contains path traversal"
                        ) from exc
                    if member.isdir():
                        target.mkdir(parents=True, exist_ok=True)
                        continue
                    target.parent.mkdir(parents=True, exist_ok=True)
                    source = bundle.extractfile(member)
                    if source is None:
                        raise CacheCorruptionError(
                            "Prepared bundle regular file has no content"
                        )
                    with source, target.open("wb") as handle:
                        shutil.copyfileobj(source, handle)
            if destination.exists():
                shutil.rmtree(destination)
            os.replace(temporary, destination)
        finally:
            shutil.rmtree(temporary, ignore_errors=True)

    def garbage_collect_staging(self, grace_seconds: int = 3600) -> int:
        cutoff = time.time() - max(1, int(grace_seconds))
        removed = 0
        for entry in self._resolve("staging").iterdir():
            try:
                if entry.stat().st_mtime < cutoff:
                    if entry.is_dir():
                        shutil.rmtree(entry)
                    else:
                        entry.unlink()
                    removed += 1
            except FileNotFoundError:
                pass
        return removed


class RunPodS3PreparedStore:
    """Injectable RunPod S3 facade for coordinator-side prepopulation.

    RunPod's S3 implementation has no conditional writes or object versioning.
    Objects and generations therefore use immutable names; a manifest is copied
    from staging only after every referenced digest object verifies.
    """

    def __init__(
        self,
        *,
        volume_id: str,
        datacenter_id: str,
        client: Any,
        endpoint_url: str,
        prefix: str = "",
        publication_lock: Any | None = None,
    ):
        self.volume_id = str(volume_id)
        self.datacenter_id = str(datacenter_id).upper()
        self.client = client
        self.endpoint_url = str(endpoint_url)
        self.prefix = str(prefix).strip("/")
        if any(part in {".", ".."} for part in self.prefix.split("/") if part):
            raise ValueError("RunPod S3 prefix cannot contain relative path segments")
        lock_namespace = (
            f"{self.volume_id}/{self.prefix}" if self.prefix else self.volume_id
        )
        self.publication_lock = publication_lock or _s3_publication_lock(
            self.endpoint_url, lock_namespace
        )

    def _key(self, key: str) -> str:
        logical_key = str(key).lstrip("/")
        if not logical_key:
            raise ValueError("RunPod S3 object key cannot be empty")
        return f"{self.prefix}/{logical_key}" if self.prefix else logical_key

    @classmethod
    def from_environment(
        cls,
        *,
        volume_id: str,
        datacenter_id: str,
        endpoint_url: str,
        prefix: str = "",
        client_factory: Callable[..., Any] | None = None,
    ) -> "RunPodS3PreparedStore":
        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
        if not access_key or not secret_key:
            from cloud_offload.credentials import get_credential

            access_key = access_key or get_credential("runpod-s3-access-key")
            secret_key = secret_key or get_credential("runpod-s3-secret-key")
        if not access_key or not secret_key:
            raise RuntimeError(
                "RunPod S3 prepopulation needs AWS_ACCESS_KEY_ID and "
                "AWS_SECRET_ACCESS_KEY outside Cloud Offload configuration"
            )
        if client_factory is None:
            try:
                import boto3
            except ImportError as exc:
                raise ImportError(
                    "boto3 is required for RunPod S3 prepopulation"
                ) from exc
            client_factory = boto3.client
        client = client_factory(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=str(datacenter_id),
            endpoint_url=str(endpoint_url),
        )
        return cls(
            volume_id=volume_id,
            datacenter_id=datacenter_id,
            client=client,
            endpoint_url=endpoint_url,
            prefix=prefix,
        )

    def probe(self) -> bool:
        self.client.head_bucket(Bucket=self.volume_id)
        # A bucket HEAD only proves that the credential can discover the
        # volume.  Prepared-state verification needs the exact permissions it
        # will use later, so exercise a tiny write/read/delete lifecycle under
        # the staging namespace and always clean it up.
        key = self._key(f"staging/probes/{uuid.uuid4().hex}")
        payload = os.urandom(32)
        try:
            self.client.put_object(Bucket=self.volume_id, Key=key, Body=payload)
            response = self.client.get_object(Bucket=self.volume_id, Key=key)
            if response["Body"].read() != payload:
                raise CacheCorruptionError("RunPod S3 probe returned different bytes")
            head = self.client.head_object(Bucket=self.volume_id, Key=key)
            if int(head.get("ContentLength") or -1) != len(payload):
                raise CacheCorruptionError("RunPod S3 probe returned the wrong size")
            return True
        finally:
            self.client.delete_object(Bucket=self.volume_id, Key=key)

    def exists(self, key: str, size: int | None = None) -> bool:
        try:
            head = self.client.head_object(Bucket=self.volume_id, Key=self._key(key))
        except Exception:
            return False
        return size is None or int(head.get("ContentLength") or -1) == int(size)

    def upload_verified(
        self,
        source: str | Path,
        expected_digest: str,
        *,
        writer_id: str | None = None,
        storage_key: str | None = None,
    ) -> str:
        source = Path(source)
        digest = normalize_digest(expected_digest)
        if sha256_file(source) != digest:
            raise CacheCorruptionError(
                f"Prepopulation source does not match sha256:{digest}"
            )
        key = str(storage_key or blob_key(digest))
        if self.exists(key, source.stat().st_size):
            return key
        # boto's upload_file performs multipart upload directly to the
        # canonical key. A single CopyObject promotion would fail for common
        # >5GB weights; duplicate digest writers are safe because their bytes
        # were verified locally and are identical by definition.
        self.client.upload_file(str(source), self.volume_id, self._key(key))
        if not self.exists(key, source.stat().st_size):
            raise CacheCorruptionError("RunPod S3 published object has the wrong size")
        self._download_verify(key, digest, source.stat().st_size)
        return key

    def publish_manifest(self, manifest: dict[str, Any], signer: ManifestSigner) -> str:
        signer.verify(manifest)
        for artifact in manifest["artifacts"]:
            if not self.exists(artifact["storage_key"], int(artifact["size"])):
                raise CacheCorruptionError(
                    f"Cannot publish manifest; {artifact['digest']} is absent"
                )
            self._download_verify(
                artifact["storage_key"],
                normalize_digest(artifact["digest"]),
                int(artifact["size"]),
            )
        with self.publication_lock:
            profile = normalize_digest(manifest["profile_fingerprint"])
            generation = f"{time.time_ns()}-{uuid.uuid4().hex}"
            key = f"manifests/{profile}/{generation}.json"
            payload = canonical_json(manifest)
            self.client.put_object(
                Bucket=self.volume_id, Key=self._key(key), Body=payload
            )
            if not self.exists(key, len(payload)):
                raise CacheCorruptionError(
                    "RunPod S3 manifest publication was incomplete"
                )
            entries = self._load_latest_index_entries()
            entries = [
                item
                for item in entries
                if item.get("manifest_id") != manifest["manifest_id"]
            ]
            entries.append(
                {
                    "manifest_id": manifest["manifest_id"],
                    "storage_key": key,
                    "profile_fingerprint": manifest["profile_fingerprint"],
                    "created_at": manifest["created_at"],
                    "generation": generation,
                    "artifacts": [
                        {
                            "digest": item["digest"],
                            "kind": item["kind"],
                            "size": item["size"],
                            "portability": item["portability"],
                            "requirements": item["requirements"],
                            "policy": item["policy"],
                        }
                        for item in manifest["artifacts"]
                    ],
                }
            )
            index = {
                "schema": INDEX_SCHEMA,
                "generation": generation,
                "created_at": utc_now(),
                "manifests": sorted(entries, key=lambda item: item["manifest_id"]),
            }
            index_payload = canonical_json(index)
            index_key = f"indexes/{generation}.json"
            self.client.put_object(
                Bucket=self.volume_id,
                Key=self._key(index_key),
                Body=index_payload,
            )
            if not self.exists(index_key, len(index_payload)):
                raise CacheCorruptionError(
                    "RunPod S3 inventory publication was incomplete"
                )
            pointer = generation.encode("utf-8")
            # RunPod S3 offers no conditional writes. This small pointer is
            # published last under a coordinator-side critical section; every
            # referenced generation remains immutable and independently valid.
            self.client.put_object(
                Bucket=self.volume_id, Key=self._key("indexes/latest"), Body=pointer
            )
            return key

    def _load_latest_index_entries(self) -> list[dict[str, Any]]:
        return list(self.load_index().get("manifests") or [])

    def load_index(self) -> dict[str, Any]:
        try:
            response = self.client.get_object(
                Bucket=self.volume_id, Key=self._key("indexes/latest")
            )
            generation = response["Body"].read().decode("utf-8").strip()
            response = self.client.get_object(
                Bucket=self.volume_id,
                Key=self._key(f"indexes/{generation}.json"),
            )
            index = json.loads(response["Body"].read())
            if index.get("schema") != INDEX_SCHEMA:
                raise ManifestError("RunPod S3 inventory schema is invalid")
            return index
        except Exception as exc:
            error = getattr(exc, "response", {}).get("Error", {})
            status = error.get("Code")
            message = str(error.get("Message") or exc).lower()
            if status in {"NoSuchKey", "404", "NotFound"}:
                return {"schema": INDEX_SCHEMA, "generation": None, "manifests": []}
            # RunPod's S3 gateway reports a missing object as
            # InvalidArgument/"object not found" rather than NoSuchKey.
            if status == "InvalidArgument" and "object not found" in message:
                return {"schema": INDEX_SCHEMA, "generation": None, "manifests": []}
            # Injected fakes commonly use KeyError for a missing first pointer.
            if isinstance(exc, KeyError):
                return {"schema": INDEX_SCHEMA, "generation": None, "manifests": []}
            raise

    def read_json(self, key: str) -> dict[str, Any]:
        response = self.client.get_object(Bucket=self.volume_id, Key=self._key(key))
        return json.loads(response["Body"].read())

    def download_verified(self, key: str, digest: str, destination: str | Path) -> Path:
        destination = Path(destination)
        self.client.download_file(self.volume_id, self._key(key), str(destination))
        if sha256_file(destination) != normalize_digest(digest):
            destination.unlink(missing_ok=True)
            raise CacheCorruptionError(f"RunPod S3 object {key} failed verification")
        return destination

    def _download_verify(self, key: str, digest: str, size: int) -> None:
        descriptor, temporary_name = tempfile.mkstemp(prefix="cloud-offload-s3-verify-")
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            self.client.download_file(self.volume_id, self._key(key), str(temporary))
            if (
                temporary.stat().st_size != int(size)
                or sha256_file(temporary) != digest
            ):
                raise CacheCorruptionError(
                    f"RunPod S3 object {key} failed digest verification"
                )
        finally:
            temporary.unlink(missing_ok=True)


@dataclass
class AdmissionState:
    samples: int = 0
    wins: int = 0
    losses: int = 0
    consecutive_wins: int = 0
    consecutive_losses: int = 0
    last_observed_at: float = 0.0


class CacheAdmissionController:
    """Shadow-first, hysteretic refusal hook scoped by caller-provided key."""

    def __init__(
        self,
        *,
        shadow: bool = True,
        minimum_samples: int = 5,
        hysteresis_samples: int = 3,
        loss_margin: float = 0.10,
        expiry_seconds: int = 7 * 24 * 3600,
    ):
        self.shadow = shadow
        self.minimum_samples = minimum_samples
        self.hysteresis_samples = hysteresis_samples
        self.loss_margin = loss_margin
        self.expiry_seconds = expiry_seconds
        self.states: dict[str, AdmissionState] = {}

    def observe(self, key: str, restore_ms: float, fallback_ms: float) -> None:
        state = self.states.setdefault(key, AdmissionState())
        won = restore_ms <= fallback_ms * (1 + self.loss_margin)
        state.samples += 1
        state.wins += int(won)
        state.losses += int(not won)
        state.consecutive_wins = state.consecutive_wins + 1 if won else 0
        state.consecutive_losses = state.consecutive_losses + 1 if not won else 0
        state.last_observed_at = time.time()

    def admit(self, key: str) -> tuple[bool, str]:
        if self.shadow:
            return True, "shadow_mode"
        state = self.states.get(key)
        if not state or time.time() - state.last_observed_at > self.expiry_seconds:
            return True, "insufficient_observations"
        if state.samples < self.minimum_samples:
            return True, "insufficient_observations"
        if state.consecutive_losses >= self.hysteresis_samples:
            return False, "measured_cache_slower_than_fallback"
        if state.consecutive_wins >= self.hysteresis_samples:
            return True, "measured_cache_faster"
        return True, "hysteresis_hold"


def retention_value(
    restore_probability: float, expected_latency_saved_ms: float, stored_gb_month: float
) -> float:
    if stored_gb_month <= 0:
        return float("inf") if expected_latency_saved_ms > 0 else 0.0
    return max(0.0, restore_probability) * expected_latency_saved_ms / stored_gb_month

"""Cloud Offload coordinator - HTTP API for the provider-neutral offload service.

The coordinator never loads a model. It accepts ComfyUI workflows and compiled
partition jobs, schedules them onto cloud GPU workers, stores content-addressed
boundary artifacts, and relays resumable execution events.
"""

import asyncio
import hashlib
import json
import logging
import math
import os
import shutil
import tempfile
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Optional

from fastapi import Body, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask
from starlette.exceptions import HTTPException as StarletteHTTPException
import uvicorn

from cloud_offload.service_config import (
    API_VERSION,
    SERVICE_NAME,
    VERSION,
    ServiceConfigError,
    choose_service_port,
    get_or_create_service_token,
    is_local_host,
    local_service_url,
    validate_bind_host,
    write_service_info,
)

logger = logging.getLogger("cloud_offload.server")

WORKER_PATH_PREFIX = "/api/workers/"
PARTITION_MEDIA_TYPE = "application/vnd.comfy.partition+zip"
PARTITION_JOB_SCHEMA = "comfy.partition.job.v1"

# A worker's self-reported detail carries the tail of its runner log, which the
# runner already truncates. Bound it again here: what a worker sends is not
# something the coordinator gets to trust the size of.
MAX_WORKER_DETAIL_CHARS = 8000

MAX_UPLOAD_BYTES = int(
    os.environ.get("CLOUD_OFFLOAD_MAX_UPLOAD_BYTES", str(32 * 1024 * 1024))
)
MAX_PARTITION_ARTIFACT_BYTES = int(
    os.environ.get(
        "CLOUD_OFFLOAD_MAX_PARTITION_ARTIFACT_BYTES", str(2 * 1024 * 1024 * 1024)
    )
)

# Runtime auth state, set by ``serve`` when binding to a LAN address.
auth_required = False
auth_token: str | None = None
last_error: str | None = None
_config_write_lock = threading.RLock()
_ANY_VOLUME_BINDING = object()
_HF_SOURCE_DIGESTS: dict[tuple[str, str, str], str] = {}
_PREFLIGHT_POLICY_CONFIG_FIELDS = {
    "max_hourly_rate",
    "max_total_job_cost",
    "max_job_runtime_seconds",
    "recommendation_policy",
    "rental_confirmation",
    "confirmation_countdown_seconds",
    "allowed_regions",
    "material_price_change_percent",
    "material_cost_change_percent",
    "lease_ttl_seconds",
}


# === Request/Response Models ===


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    error: ErrorDetail


class WorkflowSubmitRequest(BaseModel):
    capsule: dict[str, Any]
    input_artifacts: dict[str, str] = Field(default_factory=dict)
    provider: str = "auto"
    timeout_seconds: int = Field(default=3600, ge=1, le=86400)
    preflight_id: str | None = None
    manifest_digest: str | None = None
    candidate_id: str | None = None
    confirmation_action: (
        Literal["start_now", "countdown_elapsed", "policy_skip"] | None
    ) = None
    force_execution: bool = False


class PartitionSubmitRequest(BaseModel):
    partition: dict[str, Any]
    input_artifacts: dict[str, str] = Field(default_factory=dict)
    provider: str = "auto"
    timeout_seconds: int = Field(default=3600, ge=1, le=86400)
    preflight_id: str | None = None
    manifest_digest: str | None = None
    candidate_id: str | None = None
    confirmation_action: (
        Literal["start_now", "countdown_elapsed", "policy_skip"] | None
    ) = None
    # Production benchmarking must exercise a fresh Pod rather than silently
    # accepting an already-computed partition result.
    force_execution: bool = False


class PreflightRequest(BaseModel):
    partition: dict[str, Any] | None = None
    capsule: dict[str, Any] | None = None
    input_artifacts: dict[str, str] = Field(default_factory=dict)
    provider: str = "auto"
    recommendation_policy: str | None = None
    max_hourly_rate: float | None = Field(default=None, gt=0)
    max_total_job_cost: float | None = Field(default=None, gt=0)
    allowed_regions: list[str] | None = None


# === App Setup ===

app = FastAPI(
    title="Cloud Offload",
    description="Provider-neutral cloud offload coordinator for ComfyUI workflows",
    version=VERSION,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        origin.strip()
        for origin in os.environ.get(
            "CLOUD_OFFLOAD_CORS_ORIGINS",
            "http://127.0.0.1,http://127.0.0.1:8188,http://localhost,http://localhost:8188",
        ).split(",")
        if origin.strip()
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def error_response(
    status_code: int, code: str, message: str, details: dict[str, Any] | None = None
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "message": message, "details": details or {}}},
    )


@app.exception_handler(HTTPException)
async def handle_http_exception(request: Request, exc: HTTPException):
    global last_error
    detail = exc.detail
    message = detail if isinstance(detail, str) else json.dumps(detail)
    if exc.status_code >= 500:
        last_error = message
    return error_response(
        exc.status_code, "cloud_offload.http_error", message, {"detail": detail}
    )


@app.exception_handler(StarletteHTTPException)
async def handle_starlette_http_exception(
    request: Request, exc: StarletteHTTPException
):
    global last_error
    detail = exc.detail
    message = detail if isinstance(detail, str) else json.dumps(detail)
    if exc.status_code >= 500:
        last_error = message
    return error_response(
        exc.status_code, "cloud_offload.http_error", message, {"detail": detail}
    )


@app.exception_handler(RequestValidationError)
async def handle_validation_error(request: Request, exc: RequestValidationError):
    return error_response(
        422,
        "cloud_offload.validation_error",
        "Request validation failed",
        {"errors": exc.errors()},
    )


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _resolve_auth_required(host: str, require_auth: bool = False) -> bool:
    """Decide whether the global bearer token is enforced. Default: yes.

    The bearer token is required everywhere, including loopback. Binding to
    127.0.0.1 keeps other *hosts* out, but says nothing about other *processes*
    on this machine — any of which can open a socket to the port and drive the
    coordinator, spending money on rented GPUs. TLS would not help there;
    authentication does. It is also what makes tunneling the service safe by
    default rather than by remembering a flag.

    ``CLOUD_OFFLOAD_ALLOW_ANONYMOUS_LOOPBACK`` opts a loopback bind back out,
    for a single-user desktop that would rather not manage a token. It is
    ignored for non-loopback binds, which are never anonymous.
    """
    if not is_local_host(host):
        return True
    if require_auth or _env_flag("CLOUD_OFFLOAD_REQUIRE_AUTH"):
        return True
    return not _env_flag("CLOUD_OFFLOAD_ALLOW_ANONYMOUS_LOOPBACK")


@app.middleware("http")
async def require_bearer_when_enabled(request: Request, call_next):
    # The worker channel carries its own ``Bearer <worker_token>`` credential and
    # is exempt from the global LAN bearer token.
    worker_path = request.url.path.startswith(WORKER_PATH_PREFIX)
    if auth_required and request.method != "OPTIONS" and not worker_path:
        expected = f"Bearer {auth_token}"
        if request.headers.get("Authorization") != expected:
            return error_response(
                401,
                "cloud_offload.auth_required",
                "Cloud Offload service token is required for this request",
            )
    return await call_next(request)


@app.middleware("http")
async def add_request_id(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Cloud-Offload-Request-ID"] = request_id
    return response


# === Helpers ===


def _config(*, resolve_secrets: bool = True):
    from cloud_offload.config import CloudConfig

    return CloudConfig.load(resolve_secrets=resolve_secrets)


def _queue():
    from cloud_offload.queue import JobQueue

    config = _config()
    return config, JobQueue(config.queue_db_path)


def _cache_registry(config=None):
    from cloud_offload.cache_registry import CacheRegistry

    config = config or _config(resolve_secrets=False)
    return CacheRegistry(config.queue_db_path)


def _record_regional_demand(config, report, candidate, job_id: str) -> None:
    """Record safe paid demand and refresh the local shadow recommendation."""

    prepared = config.prepared_storage or {}
    replication = prepared.get("replication") or {}
    if not prepared.get("enabled") or replication.get("mode") == "off":
        return
    execution_plan = report.get("execution_plan") or {}
    profile_fingerprint = str(execution_plan.get("profile_fingerprint") or "")
    region = str(candidate.get("region") or "")
    if not profile_fingerprint or not region:
        return
    preparation = candidate.get("preparation") or {}
    preparation_range = (candidate.get("estimate") or {}).get(
        "preparation_seconds"
    )
    if isinstance(preparation_range, list):
        preparation_seconds = preparation_range[0] if preparation_range else 0
    else:
        preparation_seconds = preparation_range or 0
    registry = _cache_registry(config)
    registry.record_regional_demand(
        job_id=job_id,
        profile_fingerprint=profile_fingerprint,
        provider=str(candidate.get("provider") or ""),
        datacenter_id=region,
        prepared_volume_id=candidate.get("prepared_volume_id"),
        required_bytes=int(preparation.get("required_bytes") or 0),
        cached_bytes=int(preparation.get("cached_bytes") or 0),
        missing_bytes=int(preparation.get("missing_bytes") or 0),
        preparation_seconds=float(preparation_seconds or 0),
        hourly_rate=float(candidate.get("hourly_rate") or 0),
    )
    from cloud_offload.regional_replication import build_shadow_report

    shadow = build_shadow_report(registry, prepared)
    registry.record_shadow_evaluation(shadow)


def _preflight_store(config=None):
    from cloud_offload.preflight_store import PreflightStore

    config = config or _config(resolve_secrets=False)
    return PreflightStore(config.queue_db_path)


def _worker_auth_configured(config) -> bool:
    """Include the stable token that the dispatcher stores in the shared queue."""
    from cloud_offload.queue import JobQueue

    return (
        bool(config.worker_token)
        or JobQueue(config.queue_db_path).worker_auth_configured()
    )


def _cache_connector(config, provider: str):
    from cloud_offload.providers import create_connector

    return create_connector(provider, config)


def _runpod_s3_store(volume, connector):
    from cloud_offload.prepared_state import RunPodS3PreparedStore

    endpoint = connector.s3_endpoint(volume.datacenter_id)
    if not endpoint:
        raise RuntimeError(
            f"RunPod datacenter {volume.datacenter_id} has no published S3 endpoint"
        )
    return RunPodS3PreparedStore.from_environment(
        volume_id=volume.provider_volume_id,
        datacenter_id=volume.datacenter_id,
        endpoint_url=endpoint,
        prefix="cloud-offload",
    )


def _worker_token(request: Request) -> str | None:
    authorization = request.headers.get("Authorization", "")
    return authorization[7:] if authorization.startswith("Bearer ") else None


def _worker_identity(request: Request) -> tuple[str | None, str | None]:
    worker_id = request.headers.get("X-Cloud-Offload-Worker-ID", "").strip() or None
    lease_id = request.headers.get("X-Cloud-Offload-Lease-ID", "").strip() or None
    return worker_id, lease_id


def _partition_artifact_key(digest: str) -> str:
    normalized = str(digest).lower()
    if len(normalized) != 64 or any(
        char not in "0123456789abcdef" for char in normalized
    ):
        raise HTTPException(status_code=400, detail="Invalid partition artifact digest")
    return f"partition-artifacts/{normalized[:2]}/{normalized}.part"


def _prepared_manifest_signer(config):
    from cloud_offload.prepared_state import load_or_create_manifest_signer

    return load_or_create_manifest_signer(
        Path(config.queue_db_path).with_name("prepared-manifest-key")
    )


def _validate_manifest_proposal(
    config,
    proposal: dict[str, Any],
    *,
    job,
    volume_id: str,
) -> None:
    """Apply coordinator policy before an untrusted worker proposal is signed."""
    if proposal.get("schema") != "cloud-offload.prepared-state.v1":
        raise ValueError("Unsupported prepared manifest schema")
    if not isinstance(proposal.get("artifacts"), list):
        raise ValueError("Prepared manifest artifacts must be a list")
    allowed_top_level = {
        "schema",
        "profile_fingerprint",
        "created_at",
        "producer",
        "artifacts",
    }
    unknown_claims = set(proposal) - allowed_top_level
    if unknown_claims:
        raise ValueError(
            "Prepared manifest has unknown authority claims: "
            + ", ".join(sorted(unknown_claims))
        )
    policy = config.prepared_storage
    tenant = str(policy.get("tenant") or "default")
    forbidden = ("secret", "token", "api_key", "access_key", "password")

    def walk(value: Any, path: str = "manifest") -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if any(fragment in str(key).lower() for fragment in forbidden):
                    raise ValueError(
                        f"Prepared manifest contains credential field {path}.{key}"
                    )
                walk(child, f"{path}.{key}")
        elif isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, f"{path}[{index}]")

    walk(proposal)
    requirement = job.params.get("prepared_requirement") or {}
    if proposal.get("profile_fingerprint") != requirement.get("profile_fingerprint"):
        raise ValueError("Prepared manifest profile is outside the job launch plan")
    if str(job.params.get("cache_volume_id") or "") != str(volume_id):
        raise ValueError("Prepared manifest volume is outside the job launch plan")

    from cloud_offload.prepared_state import fingerprint
    from cloud_offload.profiles import (
        configured_worker_profiles,
        profile_pack_identifier,
    )
    from cloud_offload.service_config import VERSION

    profile_name = str(job.params.get("runtime_profile") or "")
    profiles = configured_worker_profiles(config)
    profile = profiles.get(profile_name)
    if profile is None:
        from cloud_offload.profiles import profile_providing

        profile = profile_providing(profiles, profile_name)
    if profile is None:
        raise ValueError("Prepared manifest job profile is no longer configured")
    declared_digests = {
        str(item.get("digest") or "")
        for item in (requirement.get("artifacts") or [])
        if isinstance(item, dict) and item.get("digest")
    }
    declared_policies = {
        str(item.get("digest") or ""): dict(item.get("policy") or {})
        for item in (requirement.get("artifacts") or [])
        if isinstance(item, dict) and item.get("digest")
    }
    weight_entries = list(profile.get("weights") or [])
    pack_entries = {
        profile_pack_identifier(item): item
        for item in (profile.get("custom_nodes") or [])
    }
    runtime_identity = requirement.get("runtime_identity") or {}
    dependency_lock = fingerprint(
        {
            "custom_nodes": runtime_identity.get("custom_nodes") or [],
            "wheelhouse_sha256": runtime_identity.get("wheelhouse_sha256"),
        }
    )
    for artifact in proposal["artifacts"]:
        portability = artifact.get("portability")
        if portability in {"process-bound", "gpu-resident"}:
            raise ValueError(f"Coordinator refuses durable {portability} state")
        artifact_policy = artifact.get("policy") or {}
        if artifact_policy.get("cacheable") is not True:
            raise ValueError("Coordinator refuses a non-cacheable artifact")
        if str(artifact_policy.get("tenant") or "") != tenant:
            raise ValueError("Prepared artifact tenant is outside coordinator policy")
        if artifact_policy.get("private") and not policy.get("cache_private_assets"):
            raise ValueError("Coordinator policy refuses private prepared artifacts")
        kind = str(artifact.get("kind") or "")
        if kind == "model-weight":
            if str(artifact.get("digest") or "") not in declared_digests:
                raise ValueError("Prepared model is not declared by the authorized job")
            artifact_policy["private"] = bool(
                declared_policies.get(str(artifact.get("digest") or ""), {}).get(
                    "private"
                )
            )
        elif kind == "profile-weight":
            source = artifact.get("source") or {}
            identity = (
                str(source.get("repo_id") or ""),
                str(source.get("revision") or ""),
                str(source.get("filename") or ""),
            )
            relative = PurePosixPath(identity[2])
            matching_weights = [
                item
                for item in weight_entries
                if str(item.get("repo_id") or "") == identity[0]
                and str(item.get("revision") or "") == identity[1]
                and (
                    (
                        item.get("files")
                        and identity[2] in (item.get("files") or [])
                        and source.get("snapshot") is not True
                    )
                    or (
                        not item.get("files")
                        and source.get("snapshot") is True
                        and bool(identity[2])
                        and not relative.is_absolute()
                        and ".." not in relative.parts
                    )
                )
            ]
            if not matching_weights:
                raise ValueError(
                    "Prepared weight is not pinned by the authorized profile"
                )
            if len(identity[1]) != 40 or any(
                character not in "0123456789abcdefABCDEF" for character in identity[1]
            ):
                raise ValueError(
                    "Prepared weight revision must be an immutable 40-character commit"
                )
            trusted_digest = _trusted_huggingface_digest(*identity)
            if str(artifact.get("digest") or "") != trusted_digest:
                raise ValueError(
                    "Prepared weight digest does not match its coordinator-verified source"
                )
            artifact_policy["private"] = any(
                bool(item.get("gated")) for item in matching_weights
            )
        elif kind == "custom-node-bundle":
            destination = artifact.get("destination") or {}
            pack_id = str(destination.get("pack_id") or "")
            declared_pack = pack_entries.get(pack_id)
            if declared_pack is None:
                raise ValueError(
                    "Prepared custom-node bundle is not pinned by the authorized profile"
                )
            expected_source = {"pack_id": pack_id, **declared_pack}
            if artifact.get("source") != expected_source:
                raise ValueError(
                    "Prepared custom-node bundle source does not match the profile pin"
                )
            if artifact.get("materialization") != "extract":
                raise ValueError("Prepared custom-node bundle must use safe extraction")
            if portability != "portable" or artifact.get("requirements") != {}:
                raise ValueError(
                    "Prepared custom-node source bundle has an invalid portability contract"
                )
            artifact_policy["private"] = False
        elif kind == "environment-bundle":
            destination = artifact.get("destination") or {}
            if destination.get("dependency_lock") != dependency_lock:
                raise ValueError(
                    "Prepared environment bundle does not match the profile dependency lock"
                )
            if artifact.get("source") != {"dependency_lock": dependency_lock}:
                raise ValueError(
                    "Prepared environment bundle source does not match the dependency lock"
                )
            if artifact.get("materialization") != "extract":
                raise ValueError("Prepared environment bundle must use safe extraction")
            requirements = artifact.get("requirements") or {}
            if portability != "runtime-bound":
                raise ValueError("Prepared environment bundle must be runtime-bound")
            if requirements.get("dependency_lock") != dependency_lock:
                raise ValueError(
                    "Prepared environment bundle has the wrong dependency lock"
                )
            if not str(requirements.get("platform") or "") or not str(
                requirements.get("python_abi") or ""
            ):
                raise ValueError(
                    "Prepared environment bundle has incomplete runtime requirements"
                )
            artifact_policy["private"] = False
        else:
            raise ValueError(f"Prepared artifact kind is not authorized: {kind}")
        if artifact_policy.get("private") and not policy.get("cache_private_assets"):
            raise ValueError("Coordinator policy refuses private prepared artifacts")
        artifact["policy"] = {
            **artifact_policy,
            "tenant": tenant,
            "cacheable": True,
        }
    image = str(profile.get("image") or "")
    image_digest = (
        "sha256:" + image.rsplit("@sha256:", 1)[1] if "@sha256:" in image else ""
    )
    for artifact in proposal["artifacts"]:
        if artifact.get("kind") == "environment-bundle" and (
            artifact.get("requirements") or {}
        ).get("image_digest") != image_digest:
            raise ValueError(
                "Prepared environment bundle does not match the pinned worker image"
            )
    from cloud_offload.prepared_state import utc_now

    proposal["created_at"] = utc_now()
    proposal["producer"] = {
        "image_digest": image_digest,
        "cloud_offload_version": VERSION,
    }
    proposal["cache_volume_id"] = str(volume_id)
    proposal["cache_provider_volume_id"] = str(
        job.params.get("cache_provider_volume_id") or ""
    )


def _validate_trust_receipt_proposal(
    config,
    proposal: dict[str, Any],
    manifest: dict[str, Any],
    *,
    job,
    volume_id: str,
) -> dict[str, Any]:
    """Bind a worker verification claim to signed manifest and launch state."""
    from datetime import datetime, timedelta, timezone

    from cloud_offload.prepared_state import (
        DEFAULT_FULL_AUDIT_INTERVAL_SECONDS,
        DEFAULT_TRUST_RECEIPT_TTL_SECONDS,
        DEFAULT_TRUST_SAMPLE_BYTES,
        TRUST_RECEIPT_SCHEMA,
        artifact_runtime_compatibility_key,
        digest_id,
        manifest_signature_digest,
    )

    allowed = {
        "schema",
        "manifest_id",
        "manifest_signature_digest",
        "artifact_digest",
        "artifact_size",
        "storage_key",
        "volume_id",
        "provider_volume_id",
        "runtime_compatibility",
        "object_generation",
        "verified_at",
        "expires_at",
        "scrub",
    }
    unknown = set(proposal) - allowed
    if unknown:
        raise ValueError(
            "Cache trust receipt has unknown claims: "
            + ", ".join(sorted(unknown))
        )
    signer = _prepared_manifest_signer(config)
    verified_manifest = signer.verify(manifest)
    if str(job.params.get("cache_volume_id") or "") != str(volume_id):
        raise ValueError("Cache trust receipt volume is outside the job launch plan")
    if str(verified_manifest.get("cache_volume_id") or "") != str(volume_id):
        raise ValueError("Cache trust receipt manifest belongs to another volume")
    provider_volume_id = str(job.params.get("cache_provider_volume_id") or "")
    if not provider_volume_id:
        raise ValueError("Cache trust receipt has no provider volume identity")
    if (
        str(verified_manifest.get("cache_provider_volume_id") or "")
        != provider_volume_id
    ):
        raise ValueError("Cache trust receipt manifest provider volume changed")
    requested_digest = digest_id(str(proposal.get("artifact_digest") or ""))
    artifact = next(
        (
            item
            for item in verified_manifest.get("artifacts") or []
            if item.get("digest") == requested_digest
        ),
        None,
    )
    if not artifact:
        raise ValueError("Cache trust receipt artifact is outside the signed manifest")
    policy = artifact.get("policy") or {}
    if (
        policy.get("private")
        or policy.get("sensitive")
        or policy.get("verification") == "full"
    ):
        raise ValueError("Cache trust receipt policy requires full verification")
    artifact_size = int(artifact.get("size") or -1)
    if artifact_size <= 0:
        raise ValueError("Cache trust receipt artifact size is invalid")
    storage_key = str(artifact.get("storage_key") or "")
    generation = proposal.get("object_generation")
    if not isinstance(generation, dict):
        raise ValueError("Cache trust receipt has no object generation")
    if str(generation.get("storage_key") or "") != storage_key:
        raise ValueError("Cache trust receipt object identity is not signed")
    if int(generation.get("size") or -1) != artifact_size:
        raise ValueError("Cache trust receipt object size is not signed")
    if int(generation.get("modified_ns") or -1) < 0:
        raise ValueError("Cache trust receipt object generation is invalid")
    scrub = proposal.get("scrub")
    samples = (scrub or {}).get("samples") if isinstance(scrub, dict) else None
    if not isinstance(samples, list) or not 1 <= len(samples) <= 16:
        raise ValueError("Cache trust receipt sample set is invalid")
    sample_bytes = 0
    previous = -1
    for sample in samples:
        if not isinstance(sample, dict):
            raise ValueError("Cache trust receipt sample is invalid")
        offset = int(sample.get("offset") or 0)
        size = int(sample.get("size") or 0)
        if offset < 0 or size <= 0 or offset + size > artifact_size:
            raise ValueError("Cache trust receipt sample is outside the artifact")
        if offset <= previous:
            raise ValueError("Cache trust receipt samples are not ordered")
        previous = offset
        sample_bytes += size
        digest_id(str(sample.get("sha256") or ""))
    if sample_bytes > DEFAULT_TRUST_SAMPLE_BYTES * 16:
        raise ValueError("Cache trust receipt sample set is too large")
    verified_at = datetime.now(timezone.utc)
    return {
        "schema": TRUST_RECEIPT_SCHEMA,
        "manifest_id": verified_manifest["manifest_id"],
        "manifest_signature_digest": manifest_signature_digest(verified_manifest),
        "artifact_digest": artifact["digest"],
        "artifact_size": artifact_size,
        "storage_key": storage_key,
        "volume_id": str(volume_id),
        "provider_volume_id": provider_volume_id,
        "runtime_compatibility": artifact_runtime_compatibility_key(artifact),
        "object_generation": {
            "storage_key": storage_key,
            "size": artifact_size,
            "modified_ns": int(generation["modified_ns"]),
        },
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


def _trusted_huggingface_digest(repo_id: str, revision: str, filename: str) -> str:
    """Resolve pinned HF source bytes to a coordinator-trusted sha256 digest."""
    identity = (str(repo_id), str(revision), str(filename))
    cached = _HF_SOURCE_DIGESTS.get(identity)
    if cached:
        return cached
    import huggingface_hub

    from cloud_offload.credentials import huggingface_token
    from cloud_offload.prepared_state import sha256_file

    token = huggingface_token() or None
    url = huggingface_hub.hf_hub_url(
        repo_id=identity[0], filename=identity[2], revision=identity[1]
    )
    metadata = huggingface_hub.get_hf_file_metadata(url, token=token)
    etag = str(getattr(metadata, "etag", "") or "").strip('"').lower()
    is_lfs_sha256 = (
        getattr(metadata, "xet_file_data", None) is None
        and len(etag) == 64
        and all(character in "0123456789abcdef" for character in etag)
    )
    if is_lfs_sha256:
        digest = "sha256:" + etag
    else:
        downloaded = huggingface_hub.hf_hub_download(
            repo_id=identity[0],
            filename=identity[2],
            revision=identity[1],
            token=token,
        )
        digest = "sha256:" + sha256_file(downloaded)
    _HF_SOURCE_DIGESTS[identity] = digest
    return digest


async def _store_partition_artifact(
    upload: UploadFile, expected_digest: str | None = None
) -> dict[str, Any]:
    """Stream an immutable partition artifact into configured storage."""
    from cloud_offload.storage import create_storage

    digest = hashlib.sha256()
    size = 0
    temporary = tempfile.NamedTemporaryFile(delete=False, suffix=".part")
    temporary_path = Path(temporary.name)
    try:
        with temporary:
            while True:
                chunk = await upload.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_PARTITION_ARTIFACT_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail=f"Partition artifact exceeds {MAX_PARTITION_ARTIFACT_BYTES} bytes",
                    )
                digest.update(chunk)
                temporary.write(chunk)
        actual = digest.hexdigest()
        if expected_digest and actual != expected_digest.lower():
            raise HTTPException(
                status_code=400, detail="Partition artifact digest mismatch"
            )
        config = _config()
        storage = create_storage(config)
        key = _partition_artifact_key(actual)
        if not storage.exists(key):
            storage.upload(temporary_path, key)
        return {"artifact_id": actual, "sha256": actual, "size": size}
    finally:
        temporary_path.unlink(missing_ok=True)


def _partition_artifact_response(digest: str):
    """Materialize a stored artifact as a bounded temporary download."""
    from cloud_offload.storage import create_storage

    key = _partition_artifact_key(digest)
    storage = create_storage(_config())
    if not storage.exists(key):
        raise HTTPException(status_code=404, detail="Partition artifact not found")
    handle = tempfile.NamedTemporaryFile(delete=False, suffix=".part")
    path = Path(handle.name)
    handle.close()
    try:
        storage.download(key, path)
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return FileResponse(
        path,
        media_type=PARTITION_MEDIA_TYPE,
        filename=f"{digest}.part",
        background=BackgroundTask(path.unlink, missing_ok=True),
    )


def _asset_warnings(assets: list[dict[str, Any]]) -> dict[str, Any]:
    """Surface assets resolved on the legacy name-matched path.

    A digest-keyed source or a stored artifact proves the runner gets these
    exact bytes; a profile weights entry only proves something with that name is
    expected to arrive. The submitter is told which, so "same filename,
    different weights" is visible before the output is trusted.
    """
    warnings = [
        {
            "category": asset["category"],
            "filename": asset["filename"],
            "sha256": asset["sha256"],
            "warning": asset["warning"],
        }
        for asset in assets
        if asset.get("warning")
    ]
    return {"asset_warnings": warnings} if warnings else {}


def _node_pack_warnings(warnings: list[dict[str, Any]]) -> dict[str, Any]:
    """Surface node packs whose pinned version disagrees with the client's."""
    return {"node_pack_warnings": warnings} if warnings else {}


def _provider_statuses(config) -> list[dict[str, Any]]:
    from cloud_offload.providers import (
        connector_names,
        create_connector,
        connector_metadata,
    )
    from cloud_offload.profiles import configured_worker_profiles

    profiles = configured_worker_profiles(config)
    # Union the user's ordered list with every registered connector, so a newly
    # registered plugin is visible before it has been added to provider_order.
    ordered = list(config.provider_order)
    known = ordered + [name for name in connector_names() if name not in ordered]
    providers = []
    for name in known:
        position = ordered.index(name) if name in ordered else len(ordered)
        configured = bool(config.api_key_for(name))
        metadata = connector_metadata(name)
        entry: dict[str, Any] = {
            "provider": name,
            "display_name": metadata.get("display_name") or name,
            "kind": metadata.get("kind", "builtin"),
            "residency_class": metadata.get("residency_class", "cloud"),
            "registered": metadata.get("registered", False),
            "settings_schema": metadata.get("settings_schema", []),
            "settings": config.settings_for(name),
            "priority": position,
            "in_provider_order": name in ordered,
            "configured": configured,
            "balance": {"available": False, "currency": "USD"},
            "runtime_profiles": [
                {
                    "name": profile["name"],
                    "models": profile["models"],
                    "image": profile["image"],
                }
                for profile in profiles.values()
                if name in profile["providers"]
            ],
        }
        if configured:
            try:
                entry["balance"] = create_connector(name, config).account_balance()
            except Exception as exc:
                entry["error"] = str(exc)
        providers.append(entry)
    return providers


def _authorize_worker_job(request: Request, job_id: str):
    config, queue = _queue()
    try:
        queue.authorize_worker(_worker_token(request))
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    worker_id, lease_id = _worker_identity(request)
    try:
        job = queue.authorize_worker_job(
            job_id,
            worker_id=worker_id,
            lease_id=lease_id,
            lease_ttl_seconds=config.lease_ttl_seconds,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found")
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return queue, job


def _is_active_worker_job(job, worker_id: str) -> bool:
    from cloud_offload.queue import JobStatus

    return bool(
        job
        and worker_id
        and job.worker_id == worker_id
        and job.status in {JobStatus.DISPATCHED, JobStatus.RUNNING}
    )


# === Public routes ===


@app.get("/")
async def root():
    return {
        "name": SERVICE_NAME,
        "version": VERSION,
        "api_version": API_VERSION,
        "status": "ok",
    }


@app.get("/api/health")
async def health():
    return {
        "name": SERVICE_NAME,
        "status": "ok",
        "version": VERSION,
        "api_version": API_VERSION,
        # Local restart canaries compare this with the service-discovery file
        # before signaling a process. It is operational identity, not a secret.
        "pid": os.getpid(),
    }


@app.get("/api/status")
async def status():
    """Queue counts, active workers and provider balances."""
    from cloud_offload.queue import JobStatus

    config, queue = _queue()
    workers = queue.list_active_workers()
    return {
        "queued_jobs": queue.count_by_status(JobStatus.QUEUED),
        "running_jobs": queue.count_by_status(JobStatus.RUNNING, JobStatus.DISPATCHED),
        "pending_jobs": queue.count_by_status(JobStatus.PENDING),
        "completed_jobs": queue.count_by_status(JobStatus.COMPLETED),
        "failed_jobs": queue.count_by_status(JobStatus.FAILED),
        "dead_letter_jobs": queue.count_by_status(JobStatus.DEAD_LETTER),
        "active_workers": len(workers),
        "workers": workers,
        # A runner that failed to start is not an active worker, but it is the
        # answer to "where did my pod go", so it is reported rather than dropped.
        "failed_workers": [
            worker
            for worker in queue.list_recent_workers()
            if worker["status"] == "failed"
        ],
        "providers": await asyncio.to_thread(_provider_statuses, config),
        "config": config.to_dict(),
    }


@app.get("/api/active-workers")
async def active_workers():
    """Return only fresh worker heartbeats for fresh-Pod orchestration."""
    _, queue = _queue()
    workers = queue.list_active_workers()
    return {"active_workers": len(workers), "workers": workers}


@app.get("/api/config")
async def get_config():
    """Return the current non-secret configuration."""
    return _config().to_dict()


def _read_persisted_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError("Persisted config must be a JSON object")
    return value


def _atomic_write_persisted_config(path: Path, data: dict[str, Any]) -> None:
    """Replace config.json atomically so dispatcher reloads never see a partial file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp"
    )
    temporary_path = Path(temporary.name)
    try:
        with temporary:
            json.dump(data, temporary, indent=2)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _persist_config_updates(payload: dict[str, Any]) -> None:
    from cloud_offload.config import CONFIG_DIR

    config_path = CONFIG_DIR / "config.json"
    with _config_write_lock:
        data = _read_persisted_config(config_path)
        if "cloud" in data and isinstance(data["cloud"], dict):
            data["cloud"].update(payload)
        else:
            data.update(payload)
        _atomic_write_persisted_config(config_path, data)


def _persist_prepared_volume_binding(
    config,
    provider_volume_id: str | None,
    *,
    expected_provider_volume_id: object = _ANY_VOLUME_BINDING,
) -> bool:
    """Atomically bind or clear the one configured prepared-state volume.

    A conditional clear prevents a delayed detach request for volume A from
    erasing a newer user choice of volume B.
    """
    from cloud_offload.config import CONFIG_DIR, normalized_prepared_storage

    config_path = CONFIG_DIR / "config.json"
    with _config_write_lock:
        data = _read_persisted_config(config_path)
        target = (
            data["cloud"]
            if "cloud" in data and isinstance(data["cloud"], dict)
            else data
        )
        prepared = normalized_prepared_storage(
            target.get("prepared_storage", config.prepared_storage)
        )
        if (
            expected_provider_volume_id is not _ANY_VOLUME_BINDING
            and prepared.get("existing_volume_id") != expected_provider_volume_id
        ):
            return False
        prepared["existing_volume_id"] = provider_volume_id
        target["prepared_storage"] = normalized_prepared_storage(prepared)
        _atomic_write_persisted_config(config_path, data)
        return True


@app.post("/api/config")
async def update_config(updates: dict[str, Any] = Body(...)):
    """Persist non-secret configuration. Secrets come from the environment only."""
    secret_fields = {
        "vast_api_key",
        "runpod_api_key",
        "worker_token",
        "gcs_credentials",
        # Connector credentials are written through /api/providers/{name}/credentials,
        # which stores them outside config.json.
        "provider_credentials",
    }
    payload = updates.get("cloud", updates) if isinstance(updates, dict) else {}
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Config body must be a JSON object")
    rejected = sorted(secret_fields.intersection(payload))
    if rejected:
        raise HTTPException(
            status_code=400,
            detail=f"Secrets must be supplied through environment variables: {', '.join(rejected)}",
        )
    if "prepared_storage" in payload:
        from cloud_offload.config import normalized_prepared_storage

        try:
            payload["prepared_storage"] = normalized_prepared_storage(
                payload["prepared_storage"]
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    if _PREFLIGHT_POLICY_CONFIG_FIELDS.intersection(payload):
        try:
            validated = _config(resolve_secrets=False)
            for field_name in _PREFLIGHT_POLICY_CONFIG_FIELDS.intersection(payload):
                setattr(validated, field_name, payload[field_name])
            validated.__post_init__()
            for field_name in _PREFLIGHT_POLICY_CONFIG_FIELDS.intersection(payload):
                payload[field_name] = getattr(validated, field_name)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    _persist_config_updates(payload)

    return {"status": "updated", "config": _config().to_dict()}


@app.get("/api/providers")
async def get_providers():
    """Return selectable providers, routing choices and live balances."""
    config = _config()
    return {
        "enabled": config.enabled,
        "routing_policy": config.routing_policy,
        "default_provider": config.provider,
        "providers": await asyncio.to_thread(_provider_statuses, config),
    }


@app.get("/api/cache/status")
async def cache_status():
    """Prepared-storage policy, regional volumes, health and measured benefit."""
    from cloud_offload.credentials import (
        RUNPOD_S3_ACCESS_CREDENTIAL,
        RUNPOD_S3_SECRET_CREDENTIAL,
        get_credential,
    )

    config = _config(resolve_secrets=False)
    status = _cache_registry(config).status(config.prepared_storage)
    status["s3_credentials_configured"] = bool(
        get_credential(RUNPOD_S3_ACCESS_CREDENTIAL)
        and get_credential(RUNPOD_S3_SECRET_CREDENTIAL)
    )
    return status


@app.get("/api/cache/replication/shadow")
async def replication_shadow_history(limit: int = 20):
    """Return safe recorded recommendations without provider mutation."""

    config = _config(resolve_secrets=False)
    return {
        "schema": "cloud-offload.replication-shadow-history.v1",
        "evaluations": _cache_registry(config).list_shadow_evaluations(limit=limit),
        "provider_mutation": False,
    }


@app.post("/api/cache/replication/shadow")
async def evaluate_replication_shadow():
    """Record one read-only regional replication recommendation report."""

    from cloud_offload.regional_replication import build_shadow_report

    config = _config(resolve_secrets=False)
    registry = _cache_registry(config)
    report = build_shadow_report(registry, config.prepared_storage)
    registry.record_shadow_evaluation(report)
    return report


@app.post("/api/cache/s3-credentials")
async def set_cache_s3_credentials(body: dict[str, Any] = Body(...)):
    """Store the dedicated RunPod S3 credential pair in the OS keychain.

    Both values are write-only and must arrive together.  The endpoint never
    echoes either half and rolls back the first write if the second fails.
    Environment-owned credentials remain authoritative and cannot be replaced
    through the API.
    """
    from cloud_offload.credentials import (
        RUNPOD_S3_ACCESS_CREDENTIAL,
        RUNPOD_S3_SECRET_CREDENTIAL,
        get_credential,
        provider_env_var,
        set_credential,
    )

    access_key = body.get("access_key")
    secret_key = body.get("secret_key")
    if not isinstance(access_key, str) or not isinstance(secret_key, str):
        raise HTTPException(
            status_code=400,
            detail="access_key and secret_key must both be strings",
        )
    access_key = access_key.strip()
    secret_key = secret_key.strip()
    if not access_key or not secret_key:
        raise HTTPException(
            status_code=400,
            detail="access_key and secret_key are both required",
        )
    for credential in (
        RUNPOD_S3_ACCESS_CREDENTIAL,
        RUNPOD_S3_SECRET_CREDENTIAL,
    ):
        env_name = provider_env_var(credential)
        if os.environ.get(env_name, "").strip():
            raise HTTPException(
                status_code=409,
                detail=f"RunPod S3 credentials come from {env_name}",
            )

    previous_access = get_credential(RUNPOD_S3_ACCESS_CREDENTIAL)
    previous_secret = get_credential(RUNPOD_S3_SECRET_CREDENTIAL)
    try:
        await asyncio.to_thread(set_credential, RUNPOD_S3_ACCESS_CREDENTIAL, access_key)
        await asyncio.to_thread(set_credential, RUNPOD_S3_SECRET_CREDENTIAL, secret_key)
    except Exception as exc:
        # Best-effort compare-and-restore keeps a half-written pair from being
        # mistaken for usable credentials by the status surface.
        try:
            await asyncio.to_thread(
                set_credential, RUNPOD_S3_ACCESS_CREDENTIAL, previous_access
            )
            await asyncio.to_thread(
                set_credential, RUNPOD_S3_SECRET_CREDENTIAL, previous_secret
            )
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"provider": "runpod", "s3_credentials_configured": True}


@app.post("/api/cache/volumes")
async def create_or_adopt_cache_volume(body: dict[str, Any] = Body(...)):
    """Perform a confirmed managed create or untrusted-volume adoption."""
    config = _config()
    policy = config.prepared_storage
    if not policy.get("enabled") or not policy.get("confirmed"):
        from cloud_offload.config import estimate_runpod_storage_monthly

        raise HTTPException(
            status_code=409,
            detail="Prepared storage must be enabled after first-run disclosure confirmation",
        )
    if body.get("confirmed") is not True:
        size_gb = int(
            body["size_gb"]
            if "size_gb" in body
            else policy.get("managed_size_gb") or 250
        )
        region = str(body.get("datacenter_id") or policy.get("region") or "auto")
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Confirm prepared storage creation or adoption",
                "provider": "runpod",
                "datacenter_id": region,
                "size_gb": size_gb,
                "published_estimated_monthly_usd": estimate_runpod_storage_monthly(
                    size_gb
                ),
                "placement_constrained": True,
                "cold_fallback": policy.get("cold_fallback"),
                "cache_private_assets": policy.get("cache_private_assets"),
                "provider_deletion_is_separate": True,
            },
        )
    operation = str(body.get("operation") or "adopt")
    connector = _cache_connector(config, "runpod")
    registry = _cache_registry(config)
    if operation == "adopt":
        provider_id = str(body.get("provider_volume_id") or "")
        if not provider_id:
            raise HTTPException(
                status_code=400, detail="provider_volume_id is required"
            )
        volume = await asyncio.to_thread(connector.get_storage, provider_id)
        if volume is None:
            raise HTTPException(
                status_code=404, detail="RunPod network volume not found"
            )
        expected_region = str(body.get("datacenter_id") or "")
        if expected_region and expected_region != volume.datacenter_id:
            raise HTTPException(
                status_code=409,
                detail=f"Volume is in {volume.datacenter_id}, not {expected_region}",
            )
        ownership = "adopted"
    elif operation == "create":
        region = str(body.get("datacenter_id") or policy.get("region") or "auto")
        if region == "auto":
            raise HTTPException(
                status_code=409,
                detail="Select a concrete RunPod datacenter before managed volume creation",
            )
        size_gb = int(
            body["size_gb"]
            if "size_gb" in body
            else policy.get("managed_size_gb") or 250
        )
        from cloud_offload.config import (
            RUNPOD_NETWORK_VOLUME_MAX_GB,
            estimate_runpod_storage_monthly,
        )

        if size_gb < 1 or size_gb > RUNPOD_NETWORK_VOLUME_MAX_GB:
            raise HTTPException(
                status_code=409,
                detail=f"RunPod network volume size must be 1-{RUNPOD_NETWORK_VOLUME_MAX_GB} GB",
            )
        budget = policy.get("max_monthly_storage_cost")
        if budget is not None and estimate_runpod_storage_monthly(size_gb) > float(
            budget
        ):
            raise HTTPException(
                status_code=409, detail="Managed volume exceeds storage budget"
            )
        volume = await asyncio.to_thread(
            connector.create_storage,
            name=str(body.get("name") or f"cloud-offload-{region.lower()}"),
            size_gb=size_gb,
            datacenter_id=region,
        )
        ownership = "managed"
    else:
        raise HTTPException(status_code=400, detail="operation must be create or adopt")
    registered = registry.upsert_volume(
        provider="runpod",
        provider_volume_id=volume.id,
        datacenter_id=volume.datacenter_id,
        ownership=ownership,
        capacity_bytes=volume.size_gb * 1024**3,
        policy={**policy, "existing_volume_id": volume.id},
        status="ready",
        s3_compatible=volume.s3_compatible,
    )
    _persist_prepared_volume_binding(config, volume.id)
    return registered.__dict__


@app.delete("/api/cache/volumes/{volume_id}")
async def delete_cache_volume(
    volume_id: str,
    delete_provider: bool = False,
    confirm_provider_volume_id: str | None = None,
):
    """Delete metadata; provider deletion is a separate, explicit action."""
    config = _config()
    registry = _cache_registry(config)
    volume = registry.get_volume(volume_id)
    if not volume:
        raise HTTPException(status_code=404, detail="Cache volume not found")
    if delete_provider:
        if volume.ownership != "managed":
            raise HTTPException(
                status_code=409,
                detail="Adopted volumes can only be detached; delete them in the provider console",
            )
        if confirm_provider_volume_id != volume.provider_volume_id:
            raise HTTPException(
                status_code=409,
                detail="Provider volume ID confirmation does not match",
            )
    provider_deleted = False
    binding_cleared = _persist_prepared_volume_binding(
        config,
        None,
        expected_provider_volume_id=volume.provider_volume_id,
    )
    if delete_provider:
        registry.mark_volume(volume.id, "deleting")
        connector = _cache_connector(config, volume.provider)
        provider_deleted = await asyncio.to_thread(
            connector.delete_storage, volume.provider_volume_id
        )
        if not provider_deleted:
            registry.mark_volume(volume.id, "failed")
            if binding_cleared:
                _persist_prepared_volume_binding(
                    config,
                    volume.provider_volume_id,
                    expected_provider_volume_id=None,
                )
            raise HTTPException(
                status_code=502, detail="Provider volume deletion failed"
            )
    try:
        metadata_deleted = registry.delete_metadata(volume.id)
    except Exception:
        if binding_cleared and not provider_deleted:
            _persist_prepared_volume_binding(
                config,
                volume.provider_volume_id,
                expected_provider_volume_id=None,
            )
        raise
    if not metadata_deleted:
        if binding_cleared and not provider_deleted:
            _persist_prepared_volume_binding(
                config,
                volume.provider_volume_id,
                expected_provider_volume_id=None,
            )
        raise HTTPException(
            status_code=409, detail="Cache volume metadata changed concurrently"
        )
    return {
        "deleted_metadata": True,
        "deleted_provider_volume": provider_deleted,
        "ownership": volume.ownership,
        "cleared_existing_volume_id": binding_cleared,
    }


@app.post("/api/cache/volumes/{volume_id}/verify")
async def verify_cache_volume(volume_id: str):
    """Reconcile provider truth and a compact signed inventory generation."""
    config = _config()
    registry = _cache_registry(config)
    volume = registry.get_volume(volume_id)
    if not volume:
        raise HTTPException(status_code=404, detail="Cache volume not found")
    connector = _cache_connector(config, volume.provider)
    actual = await asyncio.to_thread(connector.get_storage, volume.provider_volume_id)
    if not actual or actual.datacenter_id != volume.datacenter_id:
        registry.mark_volume(volume.id, "degraded")
        raise HTTPException(
            status_code=409,
            detail="Provider volume is missing or no longer in the recorded datacenter",
        )
    if not actual.s3_compatible:
        registry.mark_volume(volume.id, "ready")
        return {"volume_id": volume.id, "provider_verified": True, "inventory": None}
    try:
        store = _runpod_s3_store(volume, connector)
        await asyncio.to_thread(store.probe)
        index = await asyncio.to_thread(store.load_index)
        if index.get("generation") is None:
            registry.mark_volume(volume.id, "ready")
            return {
                "volume_id": volume.id,
                "provider_verified": True,
                "inventory": None,
            }
        signer = _prepared_manifest_signer(config)
        documents = {}
        for entry in index.get("manifests") or []:
            document = await asyncio.to_thread(store.read_json, entry["storage_key"])
            documents[entry["manifest_id"]] = signer.verify(document)
        result = registry.reconcile_index(
            volume.id, index, manifest_documents=documents
        )
        return {
            "volume_id": volume.id,
            "provider_verified": True,
            "generation": index["generation"],
            **result,
        }
    except Exception as exc:
        registry.mark_volume(volume.id, "degraded")
        raise HTTPException(status_code=409, detail=f"Cache verification failed: {exc}")


@app.get("/api/cache/manifests")
async def get_cache_manifests(
    profile_fingerprint: str | None = None,
    datacenter_id: str | None = None,
):
    return {
        "manifests": _cache_registry().query_manifests(
            profile_fingerprint=profile_fingerprint,
            datacenter_id=datacenter_id,
        )
    }


@app.post("/api/cache/prepopulate")
async def prepopulate_cache(body: dict[str, Any] = Body(...)):
    """Resolve pinned sources and copy verified bytes through RunPod S3."""
    from types import SimpleNamespace

    from cloud_offload.cache_scheduler import resolve_prepared_requirements
    from cloud_offload.credentials import huggingface_token
    from cloud_offload.prepared_state import (
        blob_key,
        build_manifest,
        normalize_digest,
        sha256_file as prepared_sha256_file,
    )
    from cloud_offload.profiles import configured_worker_profiles
    from cloud_offload.storage import create_storage, partition_artifact_key

    config = _config()
    registry = _cache_registry(config)
    volume = registry.get_volume(str(body.get("volume_id") or ""))
    if not volume:
        raise HTTPException(status_code=404, detail="Cache volume not found")
    if not volume.s3_compatible:
        raise HTTPException(
            status_code=409,
            detail="This RunPod datacenter has no coordinator-side S3 prepopulation",
        )
    profile_name = str(body.get("profile") or "")
    profile = configured_worker_profiles(config).get(profile_name)
    if not profile:
        raise HTTPException(status_code=400, detail="A configured profile is required")
    artifacts = body.get("artifacts") or []
    if not isinstance(artifacts, list) or not artifacts:
        raise HTTPException(
            status_code=400,
            detail="A non-empty artifacts list is required",
        )
    connector = _cache_connector(config, volume.provider)
    store = _runpod_s3_store(volume, connector)
    canonical = create_storage(config)
    manifest_artifacts = []
    seeded_requirement_assets = []
    token = huggingface_token() or None
    with tempfile.TemporaryDirectory(prefix="cloud-offload-prepopulate-") as directory:
        for item in artifacts:
            try:
                digest = normalize_digest(str(item.get("sha256") or ""))
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            source = Path(directory) / digest
            source_key = partition_artifact_key(digest)
            registered_source = config.asset_sources.get(digest)
            profile_source = None
            requested_source = item.get("source") or {}
            if requested_source:
                identity = (
                    str(requested_source.get("repo_id") or ""),
                    str(requested_source.get("revision") or ""),
                    str(requested_source.get("filename") or ""),
                )
                for candidate in profile.get("weights") or []:
                    if (
                        str(candidate.get("repo_id") or "") == identity[0]
                        and str(candidate.get("revision") or "") == identity[1]
                        and identity[2] in (candidate.get("files") or [])
                    ):
                        profile_source = {
                            **requested_source,
                            "gated": candidate.get("gated"),
                        }
                        break
                if profile_source is None:
                    raise HTTPException(
                        status_code=403,
                        detail="Requested source is not pinned by the configured profile",
                    )
                if len(identity[1]) != 40 or any(
                    character not in "0123456789abcdefABCDEF"
                    for character in identity[1]
                ):
                    raise HTTPException(
                        status_code=409,
                        detail="Prepared weight revision must be an immutable 40-character commit",
                    )
            if canonical.exists(source_key):
                canonical.download(source_key, source)
            elif registered_source or profile_source:
                resolved = registered_source or profile_source
                if resolved.get("repo_id"):
                    try:
                        import huggingface_hub
                    except ImportError as exc:
                        raise HTTPException(
                            status_code=409,
                            detail="huggingface_hub is required for HF prepopulation",
                        ) from exc
                    downloaded = await asyncio.to_thread(
                        huggingface_hub.hf_hub_download,
                        repo_id=str(resolved["repo_id"]),
                        filename=str(resolved["filename"]),
                        revision=str(resolved["revision"]),
                        local_dir=directory,
                        token=token,
                    )
                    shutil.copyfile(downloaded, source)
                elif resolved.get("url"):
                    import requests

                    with requests.get(
                        str(resolved["url"]), stream=True, timeout=60
                    ) as response:
                        response.raise_for_status()
                        with source.open("wb") as handle:
                            for chunk in response.iter_content(1024 * 1024):
                                if chunk:
                                    handle.write(chunk)
                else:
                    raise HTTPException(
                        status_code=400, detail="Pinned source is malformed"
                    )
            else:
                raise HTTPException(
                    status_code=404,
                    detail=f"No canonical or immutable configured source for sha256:{digest}",
                )
            if prepared_sha256_file(source) != digest:
                raise HTTPException(
                    status_code=409,
                    detail=f"Prepopulation source failed sha256 verification: {digest}",
                )
            private = bool(
                (registered_source or {}).get("private")
                or (profile_source or {}).get("gated")
            )
            cacheable = bool((registered_source or {}).get("cacheable", True))
            if not cacheable:
                raise HTTPException(
                    status_code=403, detail="Artifact policy forbids caching"
                )
            if private and not config.prepared_storage.get("cache_private_assets"):
                raise HTTPException(
                    status_code=403,
                    detail="Prepared storage policy refuses private/gated weights",
                )
            await asyncio.to_thread(store.upload_verified, source, digest)
            kind = "profile-weight" if profile_source else "model-weight"
            category = str(item.get("category") or "")
            filename = str(item.get("filename") or source.name)
            seeded_requirement_assets.append(
                {
                    "sha256": digest,
                    "size": source.stat().st_size,
                    "category": category,
                    "filename": filename,
                    "private": private,
                    "cacheable": cacheable,
                }
            )
            manifest_artifacts.append(
                {
                    "digest": "sha256:" + digest,
                    "kind": kind,
                    "size": source.stat().st_size,
                    "storage_key": blob_key(digest),
                    "portability": "portable",
                    "requirements": {},
                    "policy": {
                        "tenant": str(
                            config.prepared_storage.get("tenant") or "default"
                        ),
                        "cacheable": cacheable,
                        "private": private,
                    },
                    **(
                        {
                            "source": {
                                "repo_id": profile_source["repo_id"],
                                "revision": profile_source["revision"],
                                "filename": profile_source["filename"],
                            }
                        }
                        if profile_source
                        else {}
                    ),
                    "destination": {"category": category, "filename": filename},
                }
            )
    # A caller may seed only the highest-value subset while still binding its
    # signed manifest to the complete workflow requirement. Without this
    # separate identity list, a one-model seed gets a different fingerprint
    # from the six-model job and can never be selected by that job's worker.
    requirement_assets = seeded_requirement_assets
    declared_requirement = body.get("requirement_artifacts")
    if declared_requirement is not None:
        if not isinstance(declared_requirement, list) or not declared_requirement:
            raise HTTPException(
                status_code=400,
                detail="requirement_artifacts must be a non-empty list",
            )
        requirement_assets = []
        for item in declared_requirement:
            if not isinstance(item, dict):
                raise HTTPException(
                    status_code=400,
                    detail="requirement_artifacts entries must be objects",
                )
            try:
                digest = normalize_digest(str(item.get("sha256") or ""))
                size = int(item.get("size") or 0)
            except (TypeError, ValueError) as exc:
                raise HTTPException(
                    status_code=400,
                    detail="requirement_artifacts need valid sha256 and size fields",
                ) from exc
            if size < 0:
                raise HTTPException(
                    status_code=400,
                    detail="requirement_artifacts size cannot be negative",
                )
            requirement_assets.append(
                {
                    "sha256": digest,
                    "size": size,
                    "category": str(item.get("category") or ""),
                    "filename": str(item.get("filename") or ""),
                    "private": bool(item.get("private") or item.get("gated")),
                    "cacheable": bool(item.get("cacheable", True)),
                }
            )
        complete_digests = {item["sha256"] for item in requirement_assets}
        missing = sorted(
            item["sha256"]
            for item in seeded_requirement_assets
            if item["sha256"] not in complete_digests
        )
        if missing:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Every seeded artifact must appear in requirement_artifacts: "
                    + ", ".join(missing)
                ),
            )
    requirement = resolve_prepared_requirements(
        profile_name,
        profile,
        [SimpleNamespace(request={"assets": requirement_assets})],
    )
    profile_fingerprint = requirement["profile_fingerprint"]
    signer = _prepared_manifest_signer(config)
    manifest = build_manifest(
        profile_fingerprint=profile_fingerprint,
        producer={
            "image_digest": "sha256:" + profile["image"].rsplit("@sha256:", 1)[1],
            "cloud_offload_version": VERSION,
            "python_abi": "coordinator-prepopulation",
            "platform": "portable",
            "torch": "",
            "cuda": "",
        },
        artifacts=manifest_artifacts,
        signer=signer,
        claims={"cache_volume_id": volume.id},
    )
    await asyncio.to_thread(store.publish_manifest, manifest, signer)
    index = await asyncio.to_thread(store.load_index)
    registry.reconcile_index(
        volume.id, index, manifest_documents={manifest["manifest_id"]: manifest}
    )
    return {
        "volume_id": volume.id,
        "manifest_id": manifest["manifest_id"],
        "generation": index["generation"],
        "artifacts": len(manifest_artifacts),
    }


async def _copy_cache_manifest(config, registry, source, target, manifest_id: str):
    """Copy one exact signed manifest through provider object APIs."""

    source_connector = _cache_connector(config, source.provider)
    target_connector = _cache_connector(config, target.provider)
    source_store = _runpod_s3_store(source, source_connector)
    target_store = _runpod_s3_store(target, target_connector)
    source_index = await asyncio.to_thread(source_store.load_index)
    entry = next(
        (
            item
            for item in source_index.get("manifests") or []
            if item.get("manifest_id") == manifest_id
        ),
        None,
    )
    if not entry:
        raise KeyError("Source manifest not found")
    signer = _prepared_manifest_signer(config)
    document = signer.verify(
        await asyncio.to_thread(source_store.read_json, entry["storage_key"])
    )
    with tempfile.TemporaryDirectory(prefix="cloud-offload-replica-") as directory:
        for artifact in document["artifacts"]:
            if await asyncio.to_thread(
                target_store.exists,
                artifact["storage_key"],
                int(artifact["size"]),
            ):
                continue
            path = Path(directory) / normalize_cache_filename(artifact["digest"])
            try:
                await asyncio.to_thread(
                    source_store.download_verified,
                    artifact["storage_key"],
                    artifact["digest"],
                    path,
                )
                await asyncio.to_thread(
                    target_store.upload_verified,
                    path,
                    artifact["digest"],
                    storage_key=artifact["storage_key"],
                )
            finally:
                path.unlink(missing_ok=True)
    proposal = {
        key: value
        for key, value in document.items()
        if key not in {"manifest_id", "signature", "cache_volume_id"}
    }
    proposal["cache_volume_id"] = target.id
    replica_manifest = signer.sign(proposal)
    signer.verify(replica_manifest)
    await asyncio.to_thread(target_store.publish_manifest, replica_manifest, signer)
    target_index = await asyncio.to_thread(target_store.load_index)
    registry.reconcile_index(
        target.id,
        target_index,
        manifest_documents={replica_manifest["manifest_id"]: replica_manifest},
    )
    return {
        "source_manifest_id": document["manifest_id"],
        "target_manifest_id": replica_manifest["manifest_id"],
        "target_generation": target_index["generation"],
        "bytes": sum(int(item["size"]) for item in document["artifacts"]),
        "artifact_count": len(document["artifacts"]),
    }


@app.post("/api/cache/replicate")
async def replicate_cache_manifest(body: dict[str, Any] = Body(...)):
    """Manually copy one verified immutable manifest to an approved replica."""
    if body.get("confirmed") is not True:
        raise HTTPException(status_code=409, detail="Replication requires confirmation")
    config = _config()
    registry = _cache_registry(config)
    source = registry.get_volume(str(body.get("source_volume_id") or ""))
    target = registry.get_volume(str(body.get("target_volume_id") or ""))
    if not source or not target:
        raise HTTPException(status_code=404, detail="Replication volume not found")
    if not source.s3_compatible or not target.s3_compatible:
        raise HTTPException(
            status_code=409, detail="Both replica volumes need RunPod S3"
        )
    manifest_id = str(body.get("manifest_id") or "")
    document = registry.get_manifest(source.id, manifest_id)
    if not document:
        raise HTTPException(status_code=404, detail="Source manifest not found")
    sizes = {item["digest"]: int(item["size"]) for item in document["artifacts"]}
    plan = registry.create_replication(source.id, target.id, list(sizes), sizes)
    try:
        result = await _copy_cache_manifest(
            config, registry, source, target, manifest_id
        )
        registry.complete_replication(plan["id"])
        return {
            **plan,
            "status": "completed",
            **result,
        }
    except Exception as exc:  # noqa: BLE001 - persisted as bounded action failure
        registry.complete_replication(
            plan["id"], failed_reason=type(exc).__name__
        )
        raise HTTPException(
            status_code=409,
            detail=f"Replication failed: {type(exc).__name__}",
        )


@app.get("/api/cache/replication/actions")
async def regional_replica_actions():
    """Return safe durable copy, completion, failure, and expiry state."""

    config = _config(resolve_secrets=False)
    registry = _cache_registry(config)
    from cloud_offload.regional_replication import shadow_accuracy

    return {
        "schema": "cloud-offload.regional-replica-actions.v1",
        "actions": registry.list_replica_actions(),
        "targets": registry.list_replica_targets(),
        "shadow_accuracy": shadow_accuracy(
            registry, config.prepared_storage
        ),
    }


def _registered_storage_monthly_cost(registry) -> float:
    from cloud_offload.config import estimate_runpod_storage_monthly

    return sum(
        estimate_runpod_storage_monthly(volume.capacity_bytes / (1024**3))
        for volume in registry.list_volumes()
        if volume.status in {"creating", "ready", "deleting"}
    )


async def _ensure_automatic_replica_target(
    config, registry, recommendation: dict[str, Any]
):
    """Create one approved target volume under both storage budgets."""

    target_id = str(recommendation.get("target_volume_id") or "")
    if target_id:
        target = registry.get_volume(target_id)
        if target:
            return target
    policy = config.prepared_storage
    replication = policy.get("replication") or {}
    region = str(recommendation.get("target_region") or "")
    approved = {str(item) for item in replication.get("approved_regions") or []}
    if not region or region not in approved:
        raise RuntimeError("Replica target region is not approved")
    budget = replication.get("monthly_budget_usd")
    if budget is None:
        raise RuntimeError("Automatic replica target needs a monthly budget")
    size_gb = int(policy.get("managed_size_gb") or 0)
    from cloud_offload.config import estimate_runpod_storage_monthly

    monthly_cost = estimate_runpod_storage_monthly(size_gb)
    total_budget = policy.get("max_monthly_storage_cost")
    if total_budget is None:
        raise RuntimeError("Automatic replica target needs a total storage budget")
    if _registered_storage_monthly_cost(registry) + monthly_cost > float(total_budget):
        raise RuntimeError("Replica target exceeds the total storage budget")
    claim = registry.claim_replica_target(
        provider=str(recommendation.get("provider") or ""),
        datacenter_id=region,
        size_gb=size_gb,
        monthly_cost_usd=monthly_cost,
        monthly_budget_usd=float(budget),
    )
    if claim["duplicate_suppressed"]:
        if claim["status"] == "ready" and claim.get("cache_volume_id"):
            target = registry.get_volume(claim["cache_volume_id"])
            if target:
                return target
            registry.update_replica_target(
                claim["id"], "lost", error="cache_volume_missing"
            )
        raise RuntimeError("Replica target creation is already in progress")
    connector = _cache_connector(config, claim["provider"])
    provider_volume = None
    registered_target = None
    try:
        provider_volume = await asyncio.to_thread(
            connector.create_storage,
            name=f"cloud-offload-replica-{region.lower()}",
            size_gb=size_gb,
            datacenter_id=region,
        )
        registry.bind_replica_target_provider(
            claim["id"], provider_volume_id=provider_volume.id
        )
        if not provider_volume.s3_compatible:
            raise RuntimeError("Replica target does not support RunPod S3")
        target = registry.upsert_volume(
            provider=claim["provider"],
            provider_volume_id=provider_volume.id,
            datacenter_id=provider_volume.datacenter_id,
            ownership="managed",
            capacity_bytes=provider_volume.size_gb * 1024**3,
            policy={**policy, "existing_volume_id": provider_volume.id},
            status="ready",
            s3_compatible=True,
        )
        registered_target = target
        registry.complete_replica_target(
            claim["id"],
            provider_volume_id=provider_volume.id,
            cache_volume_id=target.id,
        )
        return target
    except Exception as exc:
        if registered_target is not None:
            registry.mark_volume(registered_target.id, "failed")
        cleanup_confirmed = False
        if provider_volume is not None:
            try:
                cleanup_confirmed = bool(
                    await asyncio.to_thread(
                        connector.delete_storage, provider_volume.id
                    )
                )
            except Exception:  # noqa: BLE001 - exact failed target remains visible
                logger.exception("Automatic replica target cleanup failed")
        registry.update_replica_target(
            claim["id"],
            (
                "deleted"
                if cleanup_confirmed
                else "deleting" if provider_volume is not None else "failed"
            ),
            error=type(exc).__name__,
        )
        raise


async def _reconcile_automatic_replica_targets(config, registry) -> dict[str, Any]:
    """Remove lost automatic targets from placement without assuming deletion."""

    lost: list[str] = []
    unknown: list[str] = []
    cleaned: list[str] = []
    for record in registry.list_replica_targets(status="deleting"):
        provider_volume_id = str(record.get("provider_volume_id") or "")
        if not provider_volume_id:
            continue
        try:
            connector = _cache_connector(config, record["provider"])
            provider_volume = await asyncio.to_thread(
                connector.get_storage, provider_volume_id
            )
            removed = provider_volume is None or bool(
                await asyncio.to_thread(
                    connector.delete_storage, provider_volume_id
                )
            )
        except Exception:  # noqa: BLE001 - later cycles retry exact ownership
            unknown.append(record["id"])
            continue
        if removed:
            cache_volume_id = str(record.get("cache_volume_id") or "")
            if cache_volume_id and registry.get_volume(cache_volume_id):
                registry.mark_volume(cache_volume_id, "failed")
            registry.update_replica_target(record["id"], "deleted")
            cleaned.append(record["id"])
    for record in registry.list_replica_targets(status="ready"):
        cache_volume_id = str(record.get("cache_volume_id") or "")
        volume = registry.get_volume(cache_volume_id)
        if not volume:
            registry.update_replica_target(
                record["id"], "lost", error="cache_volume_missing"
            )
            if cache_volume_id:
                registry.lose_replica_actions_for_target(cache_volume_id)
            lost.append(record["id"])
            continue
        try:
            connector = _cache_connector(config, record["provider"])
            provider_volume = await asyncio.to_thread(
                connector.get_storage, record["provider_volume_id"]
            )
        except Exception:  # noqa: BLE001 - provider uncertainty is not loss
            unknown.append(record["id"])
            continue
        if (
            provider_volume is None
            or provider_volume.datacenter_id != record["datacenter_id"]
        ):
            registry.update_replica_target(
                record["id"], "lost", error="provider_volume_absent"
            )
            registry.mark_volume(volume.id, "failed")
            registry.lose_replica_actions_for_target(volume.id)
            lost.append(record["id"])
    return {"lost": lost, "cleaned": cleaned, "unknown": unknown}


async def _delete_empty_automatic_replica_targets(config, registry) -> list[dict[str, Any]]:
    """Delete exact empty automatic volumes after their last replica expires."""

    deleted: list[dict[str, Any]] = []
    active_actions = registry.list_replica_actions()
    for record in registry.list_replica_targets(status="ready"):
        cache_volume_id = str(record.get("cache_volume_id") or "")
        volume = registry.get_volume(cache_volume_id)
        if not volume or volume.ownership != "managed":
            continue
        if any(
            action.get("target_volume_id") == cache_volume_id
            and action.get("status") in {"copying", "completed"}
            for action in active_actions
        ):
            continue
        if any(
            item.get("volume_id") == cache_volume_id
            for item in registry.query_manifests()
        ):
            continue
        registry.update_replica_target(record["id"], "deleting")
        registry.mark_volume(cache_volume_id, "deleting")
        try:
            connector = _cache_connector(config, record["provider"])
            removed = await asyncio.to_thread(
                connector.delete_storage, record["provider_volume_id"]
            )
            if not removed:
                raise RuntimeError("Provider did not confirm target deletion")
            registry.mark_volume(cache_volume_id, "failed")
            state = registry.update_replica_target(record["id"], "deleted")
            deleted.append(state)
        except Exception as exc:
            registry.mark_volume(cache_volume_id, "ready")
            registry.update_replica_target(
                record["id"], "ready", error=type(exc).__name__
            )
    return deleted


@app.post("/api/cache/replication/execute")
async def execute_regional_replica(body: dict[str, Any] = Body(...)):
    """Execute one current recommendation without renting a GPU."""

    from cloud_offload.regional_replication import (
        build_shadow_report,
        shadow_accuracy,
    )

    config = _config()
    policy = config.prepared_storage
    replication = policy.get("replication") or {}
    mode = str(replication.get("mode") or "shadow")
    if mode == "off":
        raise HTTPException(status_code=409, detail="Regional replication is off")
    if mode == "shadow" and body.get("confirmed") is not True:
        raise HTTPException(
            status_code=409,
            detail="Shadow-mode replication requires explicit confirmation",
        )
    registry = _cache_registry(config)
    report = build_shadow_report(registry, policy)
    recommendation_id = str(body.get("recommendation_id") or "")
    recommendation = next(
        (
            item
            for item in report.get("recommendations") or []
            if item.get("recommendation_id") == recommendation_id
        ),
        None,
    )
    if not recommendation:
        raise HTTPException(
            status_code=409,
            detail="Replication recommendation is absent, stale, or no longer useful",
        )
    if mode == "automatic":
        accuracy = shadow_accuracy(registry, policy)
        if not accuracy["automation_gate_passed"]:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "cloud_offload.replication_shadow_gate",
                    "message": "Shadow accuracy has not reached the automatic-copy gate.",
                    "accuracy": accuracy,
                },
            )
        blockers = set(recommendation.get("automatic_blockers") or [])
        if blockers - {"target_volume_missing"}:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "cloud_offload.replication_policy_blocked",
                    "reasons": sorted(blockers),
                },
            )
        if not recommendation.get("target_volume_id"):
            try:
                await _ensure_automatic_replica_target(
                    config, registry, recommendation
                )
            except (ValueError, RuntimeError) as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            refreshed = build_shadow_report(registry, policy)
            recommendation = next(
                (
                    item
                    for item in refreshed.get("recommendations") or []
                    if item.get("recommendation_id") == recommendation_id
                ),
                None,
            )
            if not recommendation or not recommendation.get("eligible_for_automatic"):
                raise HTTPException(
                    status_code=409,
                    detail="Created replica target did not satisfy the current policy",
                )
    source = registry.get_volume(str(recommendation.get("source_volume_id") or ""))
    target = registry.get_volume(str(recommendation.get("target_volume_id") or ""))
    if not source or not target:
        raise HTTPException(
            status_code=409,
            detail="The approved source and target volumes must exist before copy",
        )
    if not source.s3_compatible or not target.s3_compatible:
        raise HTTPException(status_code=409, detail="Replica volumes need RunPod S3")
    budget = replication.get("monthly_budget_usd")
    if budget is None:
        if mode == "automatic":
            raise HTTPException(
                status_code=409, detail="Automatic replication needs a monthly budget"
            )
        budget = 0.0
    try:
        action = registry.claim_replica_action(
            recommendation,
            monthly_budget_usd=float(budget),
            max_inflight=int(replication.get("max_inflight") or 1),
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if action["duplicate_suppressed"]:
        return action
    try:
        result = await _copy_cache_manifest(
            config,
            registry,
            source,
            target,
            str(recommendation["source_manifest_id"]),
        )
        completed = registry.complete_replica_action(
            action["id"], target_manifest_id=result["target_manifest_id"]
        )
        return {**completed, "duplicate_suppressed": False, **result}
    except Exception as exc:  # noqa: BLE001 - exact action remains auditable
        registry.fail_replica_action(action["id"], type(exc).__name__)
        logger.exception("Regional replica action failed")
        raise HTTPException(
            status_code=409,
            detail="Regional replication failed; inspect the safe action status",
        ) from exc


@app.post("/api/cache/replication/expire")
async def expire_regional_replicas():
    """Unpublish expired replicas without deleting their source state."""

    config = _config()
    registry = _cache_registry(config)
    signer = _prepared_manifest_signer(config)
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    due = registry.list_replica_actions(status="completed", due_before=now)
    expired: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for action in due:
        target = registry.get_volume(action["target_volume_id"])
        manifest = registry.get_manifest(
            action["target_volume_id"], action["target_manifest_id"]
        )
        if not target or not manifest:
            failures.append(
                {"action_id": action["id"], "reason": "replica_projection_missing"}
            )
            continue
        try:
            connector = _cache_connector(config, target.provider)
            store = _runpod_s3_store(target, connector)
            removed = await asyncio.to_thread(
                store.remove_manifest,
                action["target_manifest_id"],
                signer,
                manifest=manifest,
            )
            index = await asyncio.to_thread(store.load_index)
            registry.remove_manifest(
                target.id,
                action["target_manifest_id"],
                inventory_generation=index.get("generation"),
            )
            state = registry.expire_replica_action(action["id"])
            expired.append({**state, "removed": removed})
        except Exception as exc:  # noqa: BLE001 - retry keeps the action active
            logger.exception("Regional replica expiry failed")
            failures.append(
                {"action_id": action["id"], "reason": type(exc).__name__}
            )
    deleted_targets = await _delete_empty_automatic_replica_targets(
        config, registry
    )
    return {
        "schema": "cloud-offload.regional-replica-expiry.v1",
        "expired": expired,
        "failures": failures,
        "deleted_targets": deleted_targets,
        "source_state_deleted": False,
        "provider_gpu_mutation": False,
    }


@app.post("/api/cache/replication/controller/tick")
async def regional_replication_controller_tick():
    """Run one scheduled reconcile, shadow, expiry, and optional copy cycle."""

    from cloud_offload.regional_replication import (
        build_shadow_report,
        shadow_accuracy,
    )

    config = _config()
    policy = config.prepared_storage
    replication = policy.get("replication") or {}
    if replication.get("mode") != "automatic":
        return {
            "schema": "cloud-offload.replication-controller-tick.v1",
            "status": "idle",
            "reason": "automatic_mode_off",
            "provider_gpu_mutation": False,
        }
    registry = _cache_registry(config)
    now = datetime.now(timezone.utc)
    timeout = int(replication.get("copy_timeout_seconds") or 21600)
    stale_before = (now - timedelta(seconds=timeout)).isoformat().replace(
        "+00:00", "Z"
    )
    recovered = registry.recover_stale_replica_actions(
        stale_before=stale_before
    )
    target_recovery = await _reconcile_automatic_replica_targets(config, registry)
    shadow = build_shadow_report(registry, policy, now=now)
    registry.record_shadow_evaluation(shadow)
    expiry = await expire_regional_replicas()
    accuracy = shadow_accuracy(registry, policy, now=now)
    action: dict[str, Any] | None = None
    blocked: Any = None
    if accuracy["automation_gate_passed"] and shadow["recommendations"]:
        recommendation = shadow["recommendations"][0]
        try:
            action = await execute_regional_replica(
                {"recommendation_id": recommendation["recommendation_id"]}
            )
        except HTTPException as exc:
            blocked = exc.detail
    return {
        "schema": "cloud-offload.replication-controller-tick.v1",
        "status": "copied" if action else "observed",
        "recovered_stale_actions": recovered,
        "target_recovery": target_recovery,
        "shadow_evaluation_id": shadow["evaluation_id"],
        "shadow_accuracy": accuracy,
        "expiry": expiry,
        "action": action,
        "blocked": blocked,
        "provider_gpu_mutation": False,
    }


def normalize_cache_filename(digest: str) -> str:
    from cloud_offload.prepared_state import normalize_digest

    return normalize_digest(digest)


@app.post("/api/providers/{provider}/credentials")
async def set_provider_credentials(provider: str, body: dict[str, Any] = Body(...)):
    """Store one connector credential outside config.json.

    The key is written to the credential file with owner-only permissions and is
    never echoed back. Send an empty ``api_key`` to clear it.

    ``huggingface`` is accepted alongside the connectors: it is not a provider,
    but the Hub token workers use for gated weight downloads rides the same
    keychain storage.
    """
    from cloud_offload.config import (
        normalize_provider_name,
        provider_env_var,
        save_provider_credential,
    )
    from cloud_offload.credentials import HUGGINGFACE_CREDENTIAL
    from cloud_offload.providers import connector_names

    name = normalize_provider_name(provider)
    if name not in connector_names() and name != HUGGINGFACE_CREDENTIAL:
        raise HTTPException(
            status_code=404, detail=f"Unknown cloud connector: {provider}"
        )
    api_key = body.get("api_key")
    if not isinstance(api_key, str):
        raise HTTPException(status_code=400, detail="api_key must be a string")
    # HF_TOKEN is the canonical Hugging Face variable and outranks the keychain.
    env_names = ["HF_TOKEN"] if name == HUGGINGFACE_CREDENTIAL else []
    env_names.append(provider_env_var(name))
    for env_name in env_names:
        if os.environ.get(env_name, "").strip():
            raise HTTPException(
                status_code=409,
                detail=(
                    f"{name} credentials come from {env_name}; "
                    "unset it to manage the credential here"
                ),
            )
    try:
        await asyncio.to_thread(save_provider_credential, name, api_key)
    except (OSError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"provider": name, "configured": bool(api_key.strip())}


@app.post("/api/providers/{provider}/settings")
async def set_provider_settings(provider: str, body: dict[str, Any] = Body(...)):
    """Persist non-secret per-connector settings into ``connector_options``."""
    from cloud_offload.config import CONFIG_DIR, normalize_provider_name
    from cloud_offload.providers import connector_names

    name = normalize_provider_name(provider)
    if name not in connector_names():
        raise HTTPException(
            status_code=404, detail=f"Unknown cloud connector: {provider}"
        )
    settings = body.get("settings", body)
    if not isinstance(settings, dict):
        raise HTTPException(status_code=400, detail="settings must be a JSON object")
    if any(key.endswith("api_key") or key == "token" for key in settings):
        raise HTTPException(
            status_code=400,
            detail="Credentials belong to /api/providers/{provider}/credentials",
        )

    config_path = CONFIG_DIR / "config.json"
    data = json.loads(config_path.read_text()) if config_path.exists() else {}
    cloud = data.setdefault("cloud", {})
    options = cloud.setdefault("connector_options", {})
    options[name] = {**options.get(name, {}), **settings}
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps(data, indent=2, sort_keys=True))
    return {"provider": name, "settings": options[name]}


@app.post("/api/providers/{provider}/test")
async def test_provider(provider: str):
    """Check a connector's credentials without provisioning anything."""
    from cloud_offload.config import normalize_provider_name
    from cloud_offload.providers import connector_names, create_connector

    name = normalize_provider_name(provider)
    if name not in connector_names():
        raise HTTPException(
            status_code=404, detail=f"Unknown cloud connector: {provider}"
        )
    config = _config()
    if not config.api_key_for(name):
        return {"provider": name, "ok": False, "error": "No credentials configured"}

    def _probe() -> dict[str, Any]:
        connector = create_connector(name, config)
        balance = connector.account_balance()
        offers = connector.list_available(min_gpu_ram=1)
        return {"balance": balance, "offer_count": len(offers)}

    try:
        result = await asyncio.to_thread(_probe)
    except Exception as exc:
        return {"provider": name, "ok": False, "error": str(exc)}
    return {"provider": name, "ok": True, **result}


# === Declarative provider specs ===
#
# A REST/JSON provider can be added without writing Python by dropping a spec
# into the user spec directory. These routes make that directory editable over
# HTTP so the settings UI can author a spec, validate it, dry-run it against the
# provider's offers endpoint, and save it — without a coordinator restart.
#
# Specs never contain credentials: the declarative connector takes its API key
# from ``config.api_key_for()`` and no template variable exposes it. ``validate_spec``
# enforces that, so validate, save and load all give the same answer; the dry-run
# route treats the key it is handed as probe-only — never stored, never echoed.
#
# Names arrive from the wire, so they go through ``normalize_provider_name`` (the
# same canonicalization the credentials/settings/test routes use, which folds the
# ``vast`` alias) and then ``spec_file_path``, which refuses anything that is not
# a bare file stem.


def _spec_body(body: dict[str, Any]) -> dict[str, Any]:
    """Accept either a bare spec or ``{"spec": {...}}`` as a request body."""
    return body["spec"] if isinstance(body.get("spec"), dict) else body


def _dry_run(spec: dict[str, Any], api_key: str) -> dict[str, Any]:
    """Probe a spec's offers endpoint. Seam for injecting a fake client in tests."""
    from cloud_offload.providers.declarative import dry_run_spec

    return dry_run_spec(spec, api_key=api_key)


@app.get("/api/providers/specs")
async def list_provider_specs():
    """List the declarative provider specs in the user spec directory."""
    from cloud_offload.providers import connector_metadata
    from cloud_offload.providers.declarative import (
        AUTH_TYPES,
        describe_spec_files,
        spec_directory,
    )

    specs = await asyncio.to_thread(describe_spec_files)
    for entry in specs:
        metadata = connector_metadata(entry["name"] or "")
        entry["registered"] = bool(metadata.get("registered")) and (
            metadata.get("kind") == "declarative"
        )
    return {
        "directory": str(spec_directory()),
        "specs": specs,
        # The engine owns which auth styles exist; an authoring UI that hardcoded
        # them would drift the moment a new one is supported.
        "auth_types": list(AUTH_TYPES),
    }


@app.post("/api/providers/specs/validate")
async def validate_provider_spec(body: dict[str, Any] = Body(...)):
    """Check a spec without writing anything or contacting the provider."""
    from cloud_offload.providers.declarative import validate_spec

    spec = _spec_body(body)
    problems = validate_spec(spec)
    return {"valid": not problems, "problems": problems}


@app.post("/api/providers/specs/dry-run")
async def dry_run_provider_spec(body: dict[str, Any] = Body(...)):
    """Exercise a spec's offers endpoint so it can be debugged before saving.

    Read-only: nothing is provisioned and no money is spent. ``api_key`` is used
    for this probe alone — it is never persisted and never echoed back. Omit it
    to reuse whatever credential the provider name already resolves to.
    """
    from cloud_offload.config import normalize_provider_name

    spec = _spec_body(body)
    api_key = body.get("api_key")
    if api_key is not None and not isinstance(api_key, str):
        raise HTTPException(status_code=400, detail="api_key must be a string")
    api_key = (api_key or "").strip()
    if not api_key:
        name = normalize_provider_name(spec.get("name") or "")
        api_key = _config().api_key_for(name) if name else ""

    result = await asyncio.to_thread(_dry_run, spec, api_key)
    if api_key and isinstance(result.get("error"), str):
        # A transport error can quote the request it failed to send; make sure a
        # credential cannot ride back out through the message.
        result["error"] = result["error"].replace(api_key, "***")
    return result


def _spec_response(name: str, spec: dict, path: Path | None, problems: list) -> dict:
    """One response shape for a spec, whether it came from the user dir or the package."""
    spec.pop("_source", None)
    return {
        "name": name,
        "source": str(path) if path else None,
        "builtin": path is None,
        "editable": path is not None,
        "valid": not problems,
        "problems": problems,
        "spec": spec,
    }


@app.get("/api/providers/specs/{name}")
async def get_provider_spec(name: str):
    """Return one user spec's JSON, or the built-in spec of that name."""
    from cloud_offload.config import normalize_provider_name
    from cloud_offload.providers.declarative import (
        builtin_provider_spec,
        spec_file_path,
        validate_spec,
    )

    normalized = normalize_provider_name(name)
    try:
        path = spec_file_path(normalized)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not path.exists():
        builtin = builtin_provider_spec(normalized)
        if builtin is None:
            raise HTTPException(
                status_code=404, detail=f"No provider spec named {name!r}"
            )
        return _spec_response(normalized, builtin, None, [])

    try:
        spec = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(
            status_code=400, detail=f"{path.name} could not be read as JSON: {exc}"
        ) from exc
    return _spec_response(normalized, spec, path, validate_spec(spec))


@app.put("/api/providers/specs/{name}")
async def put_provider_spec(name: str, body: dict[str, Any] = Body(...)):
    """Create or replace a user provider spec, then register it.

    The spec is validated *before* anything touches disk, so an invalid spec can
    never be persisted, and refused outright if it would shadow a connector that
    already exists in code. On success the provider is re-registered, which makes
    it routable immediately rather than after a restart.
    """
    from cloud_offload.config import normalize_provider_name
    from cloud_offload.providers import connector_metadata
    from cloud_offload.providers.declarative import (
        register_declarative_providers,
        shadow_conflict,
        spec_file_path,
        validate_spec,
    )

    spec = _spec_body(body)
    try:
        path = spec_file_path(normalize_provider_name(name))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    stem = path.stem
    declared = normalize_provider_name(spec.get("name") or "")
    if not declared:
        spec = {**spec, "name": stem}
    elif declared != stem:
        raise HTTPException(
            status_code=400,
            detail=f"Spec 'name' is {declared!r} but the URL names {stem!r}",
        )
    spec.pop("_source", None)

    # validate_spec also rejects credential-shaped keys, so this is the same
    # answer /validate gives and the same one the loader gives at startup.
    problems = validate_spec(spec)
    if problems:
        return error_response(
            400,
            "cloud_offload.invalid_provider_spec",
            f"Provider spec {stem!r} is invalid",
            {"problems": problems},
        )

    conflict = shadow_conflict(stem, spec.get("aliases") or ())
    if conflict is not None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"{conflict[0]!r} is already served by a {conflict[1]} connector; "
                "rename the spec"
            ),
        )

    existed = path.exists()

    def _write_and_register() -> dict:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(spec, indent=2, sort_keys=True), encoding="utf-8")
        # User specs only: writing one cannot change what the in-package specs
        # register, and rescanning them would double the work for nothing.
        return register_declarative_providers(include_builtin=False)

    try:
        report = await asyncio.to_thread(_write_and_register)
    except OSError as exc:
        raise HTTPException(
            status_code=400, detail=f"Could not write {path.name}: {exc}"
        ) from exc

    metadata = connector_metadata(stem)
    return {
        "name": stem,
        "display_name": str(spec.get("display_name") or stem),
        "source": str(path),
        "created": not existed,
        "registered": bool(metadata.get("registered"))
        and metadata.get("kind") == "declarative",
        "errors": [
            problem
            for entry in report.get("failed", []) + report.get("skipped", [])
            if entry.get("name") == stem
            for problem in entry.get("errors", [])
        ],
    }


@app.delete("/api/providers/specs/{name}")
async def delete_provider_spec(name: str):
    """Delete a user provider spec. Built-in specs are not deletable."""
    from cloud_offload.config import normalize_provider_name
    from cloud_offload.providers import connector_metadata
    from cloud_offload.providers.declarative import (
        builtin_provider_spec,
        spec_file_path,
    )

    normalized = normalize_provider_name(name)
    try:
        path = spec_file_path(normalized)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    builtin = builtin_provider_spec(normalized) is not None
    if not path.exists():
        if builtin:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"{normalized!r} is a built-in spec shipped with Cloud Offload "
                    "and cannot be deleted"
                ),
            )
        raise HTTPException(status_code=404, detail=f"No provider spec named {name!r}")

    try:
        path.unlink()
    except OSError as exc:
        raise HTTPException(
            status_code=400, detail=f"Could not delete {path.name}: {exc}"
        ) from exc

    metadata = connector_metadata(normalized)
    still_registered = bool(metadata.get("registered"))
    return {
        "name": normalized,
        "deleted": True,
        "source": str(path),
        # The registry has no unregister: a deleted spec keeps serving from
        # memory until the coordinator restarts. Say so rather than imply the
        # provider vanished.
        "restart_required": still_registered and not builtin,
    }


@app.get("/api/jobs")
async def list_jobs(status: Optional[str] = None, limit: int = 50):
    """List jobs in the queue (convenience endpoint)."""
    from cloud_offload.queue import JobStatus

    _, queue = _queue()
    statuses = [JobStatus(status)] if status else list(JobStatus)
    jobs = queue.list_by_status(*statuses)[:limit]
    return [job.to_dict() for job in jobs]


@app.get("/api/job-visibility")
async def job_visibility(limit: int = 50, active_only: bool = False):
    """Return a safe, reloadable view for the Cloud Jobs user interface."""
    from cloud_offload.job_visibility import visibility_page

    _, queue = _queue()
    return visibility_page(queue, limit=limit, active_only=active_only)


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    """Get job status (+ result when completed)."""
    _, queue = _queue()
    job = queue.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job.to_dict()


@app.get("/api/jobs/{job_id}/snapshot")
async def get_job_snapshot(job_id: str):
    """Return current lifecycle state plus the resumable event cursor."""
    _, queue = _queue()
    snapshot = queue.event_snapshot(job_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return snapshot


@app.get("/api/jobs/{job_id}/support-bundle")
async def get_job_support_bundle(job_id: str):
    """Return bounded, redacted evidence for diagnostics and replay."""
    from cloud_offload.support_bundle import build_support_bundle

    _, queue = _queue()
    job = queue.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return build_support_bundle(queue, job)


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    """Cancel a job."""
    from cloud_offload.queue import JobStatus

    _, queue = _queue()
    current = queue.get(job_id)
    if not current:
        raise HTTPException(status_code=404, detail="Job not found")
    if current.status == JobStatus.COMPLETED:
        raise HTTPException(
            status_code=409, detail="Completed jobs cannot be cancelled"
        )
    if current.status in {JobStatus.FAILED, JobStatus.DEAD_LETTER}:
        return current.to_dict()
    queue.append_event(
        job_id,
        {
            "type": "cancellation_requested",
            "partition_id": (current.request.get("partition") or {}).get(
                "partition_id"
            ),
        },
    )
    queue.request_job_lease_revocation(job_id, "user_cancelled")
    job = queue.update_status(job_id, JobStatus.FAILED, error="Cancelled")
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job.to_dict()


@app.get("/api/jobs/{job_id}/events")
async def job_events(job_id: str, after: int = 0, limit: int = 250):
    """Return a resumable page of node-level cloud execution events."""
    _, queue = _queue()
    if not queue.get(job_id):
        raise HTTPException(status_code=404, detail="Job not found")
    events = queue.list_events(job_id, after=after, limit=limit)
    return {
        "events": events,
        "next_after": events[-1]["sequence"] if events else max(0, int(after)),
    }


@app.post("/api/preflight")
async def preflight_partition(request: PreflightRequest):
    """Prove readiness and recommend a current offer without paid mutation."""
    from cloud_offload.preflight import (
        build_partition_preflight,
        build_workflow_preflight,
        finite_report,
    )
    from cloud_offload.recommendation_history import RecommendationHistory
    from cloud_offload.storage import create_storage

    config = _config()
    history = RecommendationHistory(config.queue_db_path)
    if (request.partition is None) == (request.capsule is None):
        raise HTTPException(
            status_code=400,
            detail="Preflight requires exactly one partition or workflow capsule",
        )
    builder = (
        build_workflow_preflight
        if request.capsule is not None
        else build_partition_preflight
    )
    workload = (
        {"capsule": request.capsule}
        if request.capsule is not None
        else {"partition": request.partition}
    )
    try:
        report = await asyncio.to_thread(
            builder,
            config=config,
            **workload,
            input_artifacts=request.input_artifacts,
            provider=request.provider,
            recommendation_policy=request.recommendation_policy,
            max_hourly_rate=request.max_hourly_rate,
            max_total_job_cost=request.max_total_job_cost,
            allowed_regions=request.allowed_regions,
            storage=create_storage(config),
            cache_registry=_cache_registry(config),
            worker_auth_configured=_worker_auth_configured(config),
            history_lookup=history.lookup,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not finite_report(report):
        raise HTTPException(
            status_code=500,
            detail="Preflight produced a non-finite numeric value",
        )
    await asyncio.to_thread(_preflight_store(config).put, report)
    return report


def _report_candidate(
    report: dict[str, Any], candidate_id: str
) -> dict[str, Any] | None:
    return next(
        (
            item
            for item in report.get("candidates") or []
            if item.get("candidate_id") == candidate_id
        ),
        None,
    )


def _preflight_changes(
    previous: dict[str, Any],
    current: dict[str, Any],
    policy: dict[str, Any],
) -> list[str]:
    fields = (
        "provider",
        "offer_id",
        "gpu_type",
        "gpu_ram_gb",
        "region",
        "prepared_volume_id",
    )
    changes = [field for field in fields if previous.get(field) != current.get(field)]
    if _relative_change_exceeds(
        previous.get("hourly_rate"),
        current.get("hourly_rate"),
        policy.get("material_price_change_percent", 0),
    ):
        changes.append("hourly_rate")
    if previous.get("preparation") != current.get("preparation"):
        changes.append("preparation")
    previous_cost = _upper_estimated_cost(previous)
    current_cost = _upper_estimated_cost(current)
    if _relative_change_exceeds(
        previous_cost,
        current_cost,
        policy.get("material_cost_change_percent", 0),
    ):
        changes.append("estimate")
    return changes


def _upper_estimated_cost(candidate: dict[str, Any]) -> Any:
    values = (candidate.get("estimate") or {}).get("total_job_cost_usd") or []
    return values[-1] if values else None


def _relative_change_exceeds(previous: Any, current: Any, percent: Any) -> bool:
    try:
        before = float(previous)
        after = float(current)
        tolerance = max(0.0, float(percent)) / 100.0
    except (TypeError, ValueError):
        return previous != current
    if not all(math.isfinite(item) for item in (before, after, tolerance)):
        return True
    if before == after:
        return False
    if before == 0:
        return True
    return abs(after - before) / abs(before) > tolerance


def _expired(timestamp: str) -> bool:
    try:
        parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    except ValueError:
        return True
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed <= datetime.now(timezone.utc)


def _confirmation_gate(report: dict[str, Any], action: str | None) -> dict[str, Any]:
    confirmation = report.get("confirmation") or {
        "policy": "always",
        "required": True,
        "mandatory": True,
        "reason": "missing_confirmation_contract",
        "countdown_seconds": 10,
        "not_before": report.get("created_at"),
    }
    required = bool(confirmation.get("required"))
    if action == "start_now":
        return {
            "accepted": True,
            "action": action,
            "policy": confirmation.get("policy"),
            "mandatory": bool(confirmation.get("mandatory")),
        }
    if action == "countdown_elapsed":
        not_before = str(confirmation.get("not_before") or "")
        if not not_before or _expired(not_before):
            return {
                "accepted": True,
                "action": action,
                "policy": confirmation.get("policy"),
                "mandatory": bool(confirmation.get("mandatory")),
            }
        try:
            start = datetime.fromisoformat(not_before.replace("Z", "+00:00"))
            remaining = max(
                1,
                math.ceil((start - datetime.now(timezone.utc)).total_seconds()),
            )
        except ValueError:
            remaining = int(confirmation.get("countdown_seconds") or 1)
        return {
            "accepted": False,
            "code": "cloud_offload.confirmation_countdown_active",
            "message": "The rental confirmation countdown is still active.",
            "details": {
                "action": "Wait for the countdown or select Start now.",
                "remaining_seconds": remaining,
                "confirmation": confirmation,
            },
        }
    if required:
        return {
            "accepted": False,
            "code": "cloud_offload.confirmation_required",
            "message": "Confirm the recommended rental before paid launch.",
            "details": {
                "action": "Select Start now or wait for the countdown.",
                "confirmation": confirmation,
            },
        }
    return {
        "accepted": True,
        "action": "policy_skip",
        "policy": confirmation.get("policy"),
        "mandatory": bool(confirmation.get("mandatory")),
    }


def _iso_confirmation_not_before(report: dict[str, Any]) -> str:
    confirmation = report.get("confirmation") or {}
    try:
        created = datetime.fromisoformat(
            str(report.get("created_at") or "").replace("Z", "+00:00")
        )
    except ValueError:
        created = datetime.now(timezone.utc)
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    countdown = max(0, int(confirmation.get("countdown_seconds") or 0))
    value = created + timedelta(seconds=countdown)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _revalidate_partition_preflight(
    *,
    request: PartitionSubmitRequest | WorkflowSubmitRequest,
    config: Any,
    storage: Any,
) -> dict[str, Any]:
    """Rebuild volatile facts and accept only the exact confirmed candidate."""
    from cloud_offload.preflight import (
        build_partition_preflight,
        build_workflow_preflight,
        finite_report,
    )
    from cloud_offload.recommendation_history import RecommendationHistory

    if (
        not request.preflight_id
        or not request.manifest_digest
        or not request.candidate_id
    ):
        return {
            "accepted": False,
            "code": "cloud_offload.preflight_required",
            "message": "A current preflight report and confirmed GPU choice are required before paid launch.",
            "details": {"action": "Run preflight and confirm one candidate."},
        }
    store = _preflight_store(config)
    previous = store.get(request.preflight_id)
    if previous is None:
        return {
            "accepted": False,
            "code": "cloud_offload.preflight_not_found",
            "message": "The preflight report was not found.",
            "details": {"action": "Run preflight again."},
        }
    if request.manifest_digest != previous.get("manifest_digest"):
        return {
            "accepted": False,
            "code": "cloud_offload.preflight_manifest_mismatch",
            "message": "The submitted partition does not match the confirmed preflight identity.",
            "details": {"action": "Run preflight again for this partition."},
        }
    previous_candidate = _report_candidate(previous, request.candidate_id)
    if previous_candidate is None:
        return {
            "accepted": False,
            "code": "cloud_offload.preflight_candidate_mismatch",
            "message": "The confirmed GPU choice is not in the preflight report.",
            "details": {"action": "Choose one candidate from the current report."},
        }
    if previous.get("status") not in {"ready", "ready_with_preparation"}:
        return {
            "accepted": False,
            "code": "cloud_offload.preflight_not_ready",
            "message": "The preflight report is not ready for paid launch.",
            "details": {"action": "Resolve its blockers or unknown provider state."},
        }
    expected_workload_type = (
        "workflow_capsule"
        if isinstance(request, WorkflowSubmitRequest)
        else None
    )
    if previous.get("workload_type") != expected_workload_type:
        return {
            "accepted": False,
            "code": "cloud_offload.preflight_workload_mismatch",
            "message": "The preflight report is for a different workload type.",
            "details": {"action": "Run preflight again for this workload."},
        }

    confirmation = _confirmation_gate(previous, request.confirmation_action)
    if not confirmation["accepted"]:
        return confirmation

    policy = previous.get("request_policy") or {}
    history = RecommendationHistory(config.queue_db_path)
    common = {
        "config": config,
        "input_artifacts": request.input_artifacts,
        "provider": str(policy.get("provider") or request.provider),
        "recommendation_policy": str(
            policy.get("recommendation_policy") or "balanced"
        ),
        "max_hourly_rate": policy.get("max_hourly_rate"),
        "max_total_job_cost": policy.get("max_total_job_cost"),
        "allowed_regions": list(policy.get("allowed_regions") or []),
        "storage": storage,
        "cache_registry": _cache_registry(config),
        "worker_auth_configured": _worker_auth_configured(config),
        "history_lookup": history.lookup,
    }
    current = (
        build_workflow_preflight(capsule=request.capsule, **common)
        if isinstance(request, WorkflowSubmitRequest)
        else build_partition_preflight(partition=request.partition, **common)
    )
    if not finite_report(current):
        return {
            "accepted": False,
            "code": "cloud_offload.preflight_invalid",
            "message": "Preflight revalidation produced an invalid report.",
            "details": {"action": "Check coordinator logs and run preflight again."},
        }
    current_candidate = _report_candidate(current, request.candidate_id)
    changes = (
        ["manifest_digest"]
        if current.get("manifest_digest") != previous.get("manifest_digest")
        else []
    )
    if current_candidate is None:
        changes.append("candidate_availability")
    else:
        changes.extend(
            _preflight_changes(previous_candidate, current_candidate, policy)
        )
    if _expired(str(previous.get("expires_at") or "")):
        changes.append("quote_expired")
    if current.get("status") not in {"ready", "ready_with_preparation"}:
        changes.append("readiness")
    previous_confirmation = previous.get("confirmation") or {}
    current_confirmation = current.get("confirmation") or {}
    for field_name in ("policy", "countdown_seconds"):
        if previous_confirmation.get(field_name) != current_confirmation.get(
            field_name
        ):
            changes.append("confirmation_policy")
    changes = list(dict.fromkeys(changes))
    if changes:
        current_confirmation.update(
            {
                "required": True,
                "mandatory": True,
                "reason": "material_change",
                "not_before": _iso_confirmation_not_before(current),
            }
        )
        current["confirmation"] = current_confirmation
        store.put(current)
        return {
            "accepted": False,
            "code": "cloud_offload.preflight_changed",
            "message": "The confirmed plan changed before launch.",
            "details": {
                "action": "Review and confirm the revised preflight report.",
                "changes": changes,
                "revised_preflight": current,
            },
        }
    store.put(current)
    return {
        "accepted": True,
        "report": current,
        "candidate": current_candidate,
        "confirmation": confirmation,
    }


@app.post("/api/workflows", status_code=202)
async def submit_workflow(request: WorkflowSubmitRequest):
    """Queue one confirmed, preflighted whole-workflow capsule."""
    from cloud_offload.assets import (
        resolve_partition_assets,
        unresolved_assets_message,
    )
    from cloud_offload.node_packs import (
        missing_node_packs,
        missing_node_packs_message,
        node_pack_version_warnings,
    )
    from cloud_offload.queue import JobStatus
    from cloud_offload.router import resolve_worker_profile
    from cloud_offload.storage import create_storage
    from cloud_offload.storage_plan import (
        GIB,
        exceeds_ceiling_message,
        plan_disk_gb,
        plan_storage,
        plan_summary,
    )
    from cloud_offload.weight_sizes import cached_weight_sizes
    from cloud_offload.workflow_capsule import normalize_workflow_capsule

    config, queue = _queue()
    try:
        capsule = normalize_workflow_capsule(request.capsule)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    runner = capsule["runner"]
    profile_name = runner["profile"]
    profile = resolve_worker_profile(config, profile_name)
    storage = create_storage(config)
    for filename, artifact_id in request.input_artifacts.items():
        try:
            exists = storage.exists(_partition_artifact_key(str(artifact_id)))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not exists:
            raise HTTPException(
                status_code=404,
                detail=f"Workflow input artifact was not found: {filename}",
            )

    assets, unresolved = resolve_partition_assets(
        config, capsule["assets"], profile, storage
    )
    if unresolved:
        return error_response(
            409,
            "cloud_offload.unresolved_assets",
            unresolved_assets_message(unresolved),
            {"unresolved": unresolved},
        )
    missing_packs = missing_node_packs(capsule["node_packs"], profile)
    if missing_packs:
        return error_response(
            409,
            "cloud_offload.missing_node_packs",
            missing_node_packs_message(missing_packs),
            {"missing": missing_packs},
        )
    pack_warnings = node_pack_version_warnings(capsule["node_packs"], profile)
    image_bytes = int(float((profile or {}).get("image_size_gb") or 0) * GIB) or None
    plan = plan_storage(
        assets,
        profile,
        image_bytes=image_bytes,
        weight_bytes=cached_weight_sizes(config, profile),
    )
    disk_gb = plan_disk_gb(plan)
    storage_summary = plan_summary(plan)
    if disk_gb > config.max_container_disk_gb:
        return error_response(
            409,
            "cloud_offload.storage_plan_exceeds_ceiling",
            exceeds_ceiling_message(plan, disk_gb, config.max_container_disk_gb),
            {"storage": storage_summary},
        )

    preflight_binding = await asyncio.to_thread(
        _revalidate_partition_preflight,
        request=request,
        config=config,
        storage=storage,
    )
    if not preflight_binding["accepted"]:
        return error_response(
            409,
            preflight_binding["code"],
            preflight_binding["message"],
            preflight_binding["details"],
        )
    confirmed_report = preflight_binding["report"]
    confirmed_candidate = preflight_binding["candidate"]
    confirmation_evidence = preflight_binding["confirmation"]
    confirmed_offer = {
        "id": confirmed_candidate["offer_id"],
        "provider": confirmed_candidate["provider"],
        "gpu_type": confirmed_candidate["gpu_type"],
        "gpu_count": confirmed_candidate["gpu_count"],
        "gpu_ram_gb": confirmed_candidate["gpu_ram_gb"],
        "hourly_rate": confirmed_candidate["hourly_rate"],
    }
    confirmed_launch = {
        "preflight_id": confirmed_report["preflight_id"],
        "manifest_digest": confirmed_report["manifest_digest"],
        "capsule_digest": confirmed_report["capsule_digest"],
        "workload_digest": confirmed_report.get("workload_digest"),
        "candidate_id": confirmed_candidate["candidate_id"],
        "expires_at": confirmed_report["expires_at"],
        "provider": confirmed_candidate["provider"],
        "offer_id": confirmed_candidate["offer_id"],
        "gpu_type": confirmed_candidate["gpu_type"],
        "gpu_ram_gb": confirmed_candidate["gpu_ram_gb"],
        "hourly_rate": confirmed_candidate["hourly_rate"],
        "region": confirmed_candidate.get("region"),
        "prepared_volume_id": confirmed_candidate.get("prepared_volume_id"),
        "preparation_class": confirmed_candidate.get("preparation_class"),
        "estimate": confirmed_candidate["estimate"],
        "request_policy": confirmed_report["request_policy"],
        "confirmation": confirmation_evidence,
    }
    job = queue.create(
        model="comfyui-workflow",
        input_path="artifacts://comfyui-workflow-capsule",
        params={
            "offer": confirmed_offer,
            "preflight": confirmed_launch,
            "runtime_profile": profile_name,
            "gpu_type": confirmed_candidate["gpu_type"],
            "min_gpu_ram_gb": runner["min_gpu_ram_gb"],
            "container_disk_gb": disk_gb,
            "keep_warm": bool(config.keep_warm),
        },
        request={
            "kind": "comfyui-workflow-capsule",
            "capsule": capsule,
            "input_artifacts": request.input_artifacts,
            "timeout_seconds": request.timeout_seconds,
            **({"assets": assets} if assets else {}),
        },
        provider=confirmed_candidate["provider"],
        status=JobStatus.QUEUED,
    )
    try:
        _record_regional_demand(
            config, confirmed_report, confirmed_candidate, job.id
        )
    except Exception:  # noqa: BLE001 - telemetry cannot hide a queued paid job
        logger.exception("Could not record regional demand for queued workflow")
    return {
        "job_id": job.id,
        "status": job.status.value,
        "status_url": f"/api/jobs/{job.id}",
        "preflight_id": confirmed_report["preflight_id"],
        "manifest_digest": confirmed_report["manifest_digest"],
        "capsule_digest": confirmed_report["capsule_digest"],
        "candidate_id": confirmed_candidate["candidate_id"],
        "confirmation_action": confirmation_evidence["action"],
        "storage": storage_summary,
        **_asset_warnings(assets),
        **_node_pack_warnings(pack_warnings),
    }


@app.post("/api/partitions", status_code=202)
async def submit_partition(request: PartitionSubmitRequest):
    """Queue a compiled subgraph with immutable typed boundary artifacts."""
    from cloud_offload.assets import (
        normalized_partition_assets,
        resolve_partition_assets,
        unresolved_assets_message,
    )
    from cloud_offload.node_packs import (
        missing_node_packs,
        missing_node_packs_message,
        node_pack_version_warnings,
        normalized_partition_node_packs,
    )
    from cloud_offload.queue import JobStatus
    from cloud_offload.router import resolve_worker_profile, select_profile_provider
    from cloud_offload.storage import create_storage
    from cloud_offload.storage_plan import (
        GIB,
        exceeds_ceiling_message,
        plan_disk_gb,
        plan_storage,
        plan_summary,
    )
    from cloud_offload.weight_sizes import cached_weight_sizes

    if request.partition.get("schema") != PARTITION_JOB_SCHEMA:
        raise HTTPException(status_code=400, detail="Unsupported partition job schema")
    if (
        not isinstance(request.partition.get("workflow"), dict)
        or not request.partition["workflow"]
    ):
        raise HTTPException(status_code=400, detail="Partition workflow is required")
    runner = request.partition.get("runner") or {}
    profile_name = str(runner.get("profile") or "comfyui").strip()[:100]
    if not profile_name.startswith("comfyui"):
        raise HTTPException(status_code=400, detail="Invalid ComfyUI runner profile")
    try:
        min_gpu_ram_gb = max(1, min(256, int(runner.get("min_gpu_ram_gb") or 16)))
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=400, detail="Invalid partition GPU VRAM requirement"
        )
    gpu_type = str(runner.get("gpu_type") or "any").strip()[:100] or "any"
    # The compiler stamps residency from its taint analysis; refusing an
    # on-prem job here when only cloud backends exist is the server-side
    # guarantee behind that client-side check.
    residency = request.partition.get("residency", "cloud")
    if residency not in {"cloud", "on-prem"}:
        raise HTTPException(
            status_code=400,
            detail="Invalid partition residency (expected 'cloud' or 'on-prem')",
        )
    try:
        declared_assets = normalized_partition_assets(request.partition.get("assets"))
        declared_packs = normalized_partition_node_packs(
            request.partition.get("node_packs")
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    config, queue = _queue()
    storage = create_storage(config)
    for boundary_key, artifact_id in request.input_artifacts.items():
        if not boundary_key.startswith("input_"):
            raise HTTPException(
                status_code=400, detail=f"Invalid input boundary key: {boundary_key}"
            )
        if not storage.exists(_partition_artifact_key(artifact_id)):
            raise HTTPException(
                status_code=404, detail=f"Input artifact not found: {artifact_id}"
            )
    # Declared assets and node packs resolve before routing, let alone
    # provisioning: a model file nobody can supply, or a node type that will not
    # exist on the runner, must cost a 409 rather than a rented GPU that fails on
    # its first prompt. The profile is read directly rather than taken from the
    # route, because it is the same profile whichever provider wins.
    profile = resolve_worker_profile(config, profile_name)
    assets, unresolved = resolve_partition_assets(
        config, declared_assets, profile, storage
    )
    if unresolved:
        return error_response(
            409,
            "cloud_offload.unresolved_assets",
            unresolved_assets_message(unresolved),
            {"unresolved": unresolved},
        )
    missing_packs = missing_node_packs(declared_packs, profile)
    if missing_packs:
        return error_response(
            409,
            "cloud_offload.missing_node_packs",
            missing_node_packs_message(missing_packs),
            {"missing": missing_packs},
        )
    # Divergence is not a refusal: the coordinator cannot know what code the
    # runner will actually hold until the runner reports its own digest, and a
    # version match would not have proven a code match either — a pack can ship
    # a security fix under the version number of the unpatched release.
    pack_warnings = node_pack_version_warnings(declared_packs, profile)
    # Size the pod's disk before anything is rented, for the same reason the
    # checks above run here: a worker that runs out of space mid-job has already
    # started the meter. Weight sizes come from the on-disk cache only — a
    # submission must not wait on, or fail because of, the Hugging Face API, and
    # anything unresolved is charged a conservative default and reported.
    image_bytes = int(float((profile or {}).get("image_size_gb") or 0) * GIB) or None
    plan = plan_storage(
        assets,
        profile,
        image_bytes=image_bytes,
        weight_bytes=cached_weight_sizes(config, profile),
    )
    disk_gb = plan_disk_gb(plan)
    storage_summary = plan_summary(plan)
    if disk_gb > config.max_container_disk_gb:
        return error_response(
            409,
            "cloud_offload.storage_plan_exceeds_ceiling",
            exceeds_ceiling_message(plan, disk_gb, config.max_container_disk_gb),
            {"storage": storage_summary},
        )
    try:
        route = await asyncio.to_thread(
            select_profile_provider,
            config,
            profile_name,
            request.provider,
            residency=residency,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    cache_identity = {
        "schema": "comfy.partition.cache.v1",
        "partition": request.partition,
        "input_artifacts": request.input_artifacts,
        "runner_image": (route.profile or {}).get("image"),
    }
    cache_key = hashlib.sha256(
        json.dumps(cache_identity, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    cached = None if request.force_execution else queue.get_partition_cache(cache_key)
    if cached:
        output_ids = (cached.get("output_artifacts") or {}).values()
        if all(
            storage.exists(_partition_artifact_key(str(item))) for item in output_ids
        ):
            job = queue.create(
                model="comfyui-partition-v1",
                input_path="artifacts://comfyui-partition-cache",
                params={
                    "runtime_profile": profile_name,
                    "partition_cache_key": cache_key,
                    "cache_hit": True,
                    "gpu_type": gpu_type,
                    "min_gpu_ram_gb": min_gpu_ram_gb,
                    "container_disk_gb": disk_gb,
                },
                request={
                    "kind": "comfyui-partition",
                    "partition": request.partition,
                    "input_artifacts": request.input_artifacts,
                    "timeout_seconds": request.timeout_seconds,
                    **({"assets": assets} if assets else {}),
                },
                provider=route.provider,
                status=JobStatus.QUEUED,
            )
            queue.append_event(
                job.id,
                {
                    "type": "partition_cache_hit",
                    "partition_id": request.partition.get("partition_id"),
                    "overall_progress": 100,
                },
            )
            job = queue.complete_job(job.id, cached)
            return {
                "job_id": job.id,
                "status": job.status.value,
                "status_url": f"/api/jobs/{job.id}",
                "cache_hit": True,
                "storage": storage_summary,
                **_asset_warnings(assets),
                **_node_pack_warnings(pack_warnings),
            }

    preflight_binding = await asyncio.to_thread(
        _revalidate_partition_preflight,
        request=request,
        config=config,
        storage=storage,
    )
    if not preflight_binding["accepted"]:
        return error_response(
            409,
            preflight_binding["code"],
            preflight_binding["message"],
            preflight_binding["details"],
        )
    confirmed_report = preflight_binding["report"]
    confirmed_candidate = preflight_binding["candidate"]
    confirmation_evidence = preflight_binding.get("confirmation") or {
        "accepted": True,
        "action": request.confirmation_action or "policy_skip",
        "policy": "test_or_legacy_binding",
        "mandatory": False,
    }
    confirmed_offer = {
        "id": confirmed_candidate["offer_id"],
        "provider": confirmed_candidate["provider"],
        "gpu_type": confirmed_candidate["gpu_type"],
        "gpu_count": confirmed_candidate["gpu_count"],
        "gpu_ram_gb": confirmed_candidate["gpu_ram_gb"],
        "hourly_rate": confirmed_candidate["hourly_rate"],
    }
    confirmed_launch = {
        "preflight_id": confirmed_report["preflight_id"],
        "manifest_digest": confirmed_report["manifest_digest"],
        "workload_digest": confirmed_report.get("workload_digest"),
        "candidate_id": confirmed_candidate["candidate_id"],
        "expires_at": confirmed_report["expires_at"],
        "provider": confirmed_candidate["provider"],
        "offer_id": confirmed_candidate["offer_id"],
        "gpu_type": confirmed_candidate["gpu_type"],
        "gpu_ram_gb": confirmed_candidate["gpu_ram_gb"],
        "hourly_rate": confirmed_candidate["hourly_rate"],
        "region": confirmed_candidate.get("region"),
        "prepared_volume_id": confirmed_candidate.get("prepared_volume_id"),
        "preparation_class": confirmed_candidate.get("preparation_class"),
        "estimate": confirmed_candidate["estimate"],
        "request_policy": confirmed_report["request_policy"],
        "confirmation": confirmation_evidence,
    }
    job = queue.create(
        model="comfyui-partition-v1",
        input_path="artifacts://comfyui-partition",
        params={
            "offer": confirmed_offer,
            "preflight": confirmed_launch,
            "runtime_profile": profile_name,
            "partition_cache_key": cache_key,
            "gpu_type": confirmed_candidate["gpu_type"],
            "min_gpu_ram_gb": min_gpu_ram_gb,
            # The dispatcher rents at least this much container disk, so the
            # pod that stages these bytes has somewhere to put them.
            "container_disk_gb": disk_gb,
            "keep_warm": bool(config.keep_warm),
        },
        request={
            "kind": "comfyui-partition",
            "partition": request.partition,
            "input_artifacts": request.input_artifacts,
            "timeout_seconds": request.timeout_seconds,
            # Only present for a partition that declared assets: a manifest-less
            # job carries exactly the request it carried before this existed, so
            # the worker falls back to its profile's static weights list.
            **({"assets": assets} if assets else {}),
        },
        provider=confirmed_candidate["provider"],
        status=JobStatus.QUEUED,
    )
    try:
        _record_regional_demand(
            config, confirmed_report, confirmed_candidate, job.id
        )
    except Exception:  # noqa: BLE001 - telemetry cannot hide a queued paid job
        logger.exception("Could not record regional demand for queued partition")
    return {
        "job_id": job.id,
        "status": job.status.value,
        "status_url": f"/api/jobs/{job.id}",
        "preflight_id": confirmed_report["preflight_id"],
        "manifest_digest": confirmed_report["manifest_digest"],
        "candidate_id": confirmed_candidate["candidate_id"],
        "confirmation_action": confirmation_evidence["action"],
        **({"cache_bypassed": True} if request.force_execution else {}),
        "storage": storage_summary,
        **_asset_warnings(assets),
        **_node_pack_warnings(pack_warnings),
    }


@app.post("/api/artifacts")
async def upload_artifact(
    file: UploadFile = File(...), sha256: str | None = Form(None)
):
    """Upload a content-addressed .part boundary bundle from local ComfyUI."""
    return await _store_partition_artifact(file, sha256)


@app.get("/api/artifacts/{artifact_id}")
async def download_artifact(artifact_id: str):
    """Download a completed boundary artifact to local ComfyUI."""
    return _partition_artifact_response(artifact_id)


# === Worker channel (separate Bearer <worker_token>, exempt from global auth) ===


@app.post("/api/workers/claim")
async def worker_claim(request: Request, payload: dict[str, Any] = Body(...)):
    """Claim provider-scoped jobs from a remote worker."""
    config, queue = _queue()
    token = _worker_token(request)
    try:
        queue.authorize_worker(token)
        jobs = queue.claim_jobs(
            str(payload["worker_id"]),
            limit=max(1, min(10, int(payload.get("limit", 5)))),
            token=token,
            provider=str(payload["provider"]),
            models=[str(item) for item in payload.get("models", [])],
            runtime_profile=str(payload.get("runtime_profile") or ""),
            gpu_vram_gb=float(payload.get("gpu_vram_gb") or 0),
            gpu_name=str(payload.get("gpu_name") or ""),
            cache_volume_id=str(payload.get("cache_volume_id") or ""),
            lease_id=str(payload.get("lease_id") or "") or None,
            lease_ttl_seconds=config.lease_ttl_seconds,
        )
        queue.record_worker(
            str(payload["worker_id"]),
            str(payload["provider"]),
            runtime_profile=str(payload.get("runtime_profile") or ""),
            capabilities=[str(item) for item in payload.get("models", [])],
            idle=not jobs,
            lease_id=str(payload.get("lease_id") or "") or None,
            lease_ttl_seconds=config.lease_ttl_seconds,
        )
    except (KeyError, PermissionError, ValueError) as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    return [job.to_dict() for job in jobs]


@app.post("/api/workers/status")
async def worker_status(request: Request, payload: dict[str, Any] = Body(...)):
    """Accept a worker's report of its own state, including why it never started.

    A runner that dies during startup has no job to hang an error on — nothing
    has been claimed, and the container's logs go with the container — so it
    reports against its own worker id here instead. Registering as ``starting``
    uses the same route: it tells the dispatcher a pod is already coming up for
    this profile, and it deliberately claims nothing, because readiness is
    proved by ComfyUI answering rather than by a worker asserting it.
    """
    from cloud_offload.queue import WORKER_STATUSES

    config, queue = _queue()
    try:
        queue.authorize_worker(_worker_token(request))
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc))

    worker_id = str(payload.get("worker_id") or "").strip()
    provider = str(payload.get("provider") or "").strip()
    if not worker_id or not provider:
        raise HTTPException(
            status_code=400, detail="worker_id and provider are required"
        )
    status = str(payload.get("status") or "active")
    if status not in WORKER_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown worker status {status!r}; expected one of "
            + ", ".join(WORKER_STATUSES),
        )
    detail = payload.get("detail")
    try:
        queue.record_worker(
            worker_id,
            provider,
            status=status,
            runtime_profile=str(payload.get("runtime_profile") or ""),
            capabilities=[str(item) for item in payload.get("models", [])],
            idle=bool(payload.get("idle", False)),
            detail=str(detail)[:MAX_WORKER_DETAIL_CHARS] if detail else None,
            lease_id=str(payload.get("lease_id") or "") or None,
            lease_ttl_seconds=config.lease_ttl_seconds,
        )
    except (KeyError, PermissionError) as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return {"worker_id": worker_id, "status": status}


@app.get("/api/workers/policy")
async def worker_policy(request: Request):
    """Return the live coordinator-owned worker lifetime policy."""
    _, queue = _queue()
    try:
        queue.authorize_worker(_worker_token(request))
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    config = _config(resolve_secrets=False)
    return {
        "keep_warm": config.keep_warm,
        "idle_shutdown_seconds": config.idle_shutdown_seconds,
        "keep_warm_warning_seconds": config.keep_warm_warning_seconds,
        "lease_ttl_seconds": config.lease_ttl_seconds,
    }


@app.get("/api/workers/artifacts/{artifact_id}")
async def worker_download_artifact(artifact_id: str, request: Request):
    """Download an input artifact over the authenticated worker channel."""
    _, queue = _queue()
    try:
        queue.authorize_worker(_worker_token(request))
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    return _partition_artifact_response(artifact_id)


@app.post("/api/workers/cache/manifests/sign")
async def worker_sign_prepared_manifest(
    request: Request, payload: dict[str, Any] = Body(...)
):
    """Validate and sign a worker proposal without disclosing authority key."""
    config, queue = _queue()
    try:
        queue.authorize_worker(_worker_token(request))
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    proposal = payload.get("manifest")
    if not isinstance(proposal, dict):
        raise HTTPException(status_code=400, detail="manifest must be an object")
    job_id = str(payload.get("job_id") or "")
    volume_id = str(payload.get("volume_id") or "")
    worker_id = str(payload.get("worker_id") or "")
    job = queue.get(job_id)
    if not _is_active_worker_job(job, worker_id):
        raise HTTPException(
            status_code=403,
            detail="Prepared manifest proposal is not bound to this worker's active job",
        )
    try:
        await asyncio.to_thread(
            _validate_manifest_proposal,
            config,
            proposal,
            job=job,
            volume_id=volume_id,
        )
        signer = _prepared_manifest_signer(config)
        signed = signer.sign(proposal)
        # Full canonical key, size, tier and signature validation before return.
        return signer.verify(signed)
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/workers/cache/manifests/verify")
async def worker_verify_prepared_manifest(
    request: Request, payload: dict[str, Any] = Body(...)
):
    """Verify a manifest against coordinator authority for an authenticated worker."""
    config, queue = _queue()
    try:
        queue.authorize_worker(_worker_token(request))
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    manifest = payload.get("manifest")
    if not isinstance(manifest, dict):
        raise HTTPException(status_code=400, detail="manifest must be an object")
    try:
        return _prepared_manifest_signer(config).verify(manifest)
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/workers/cache/trust-receipts/sign")
async def worker_sign_cache_trust_receipt(
    request: Request, payload: dict[str, Any] = Body(...)
):
    """Sign a bounded full-verification claim for an active worker and volume."""
    config, queue = _queue()
    try:
        queue.authorize_worker(_worker_token(request))
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    job = queue.get(str(payload.get("job_id") or ""))
    worker_id = str(payload.get("worker_id") or "")
    volume_id = str(payload.get("volume_id") or "")
    proposal = payload.get("receipt")
    manifest = payload.get("manifest")
    if not _is_active_worker_job(job, worker_id):
        raise HTTPException(
            status_code=403,
            detail="Cache trust receipt is not bound to this worker's active job",
        )
    if not isinstance(proposal, dict) or not isinstance(manifest, dict):
        raise HTTPException(
            status_code=400, detail="receipt and manifest must be objects"
        )
    try:
        validated = _validate_trust_receipt_proposal(
            config,
            proposal,
            manifest,
            job=job,
            volume_id=volume_id,
        )
        signer = _prepared_manifest_signer(config)
        signed = signer.verify_trust_receipt(
            signer.sign_trust_receipt(validated, manifest=manifest)
        )
        _cache_registry(config).mark_verified(
            volume_id, str(signed["artifact_digest"])
        )
        return signed
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/workers/cache/trust-receipts/verify")
async def worker_verify_cache_trust_receipt(
    request: Request, payload: dict[str, Any] = Body(...)
):
    """Verify a receipt only for the active worker's exact launch volume."""
    config, queue = _queue()
    try:
        queue.authorize_worker(_worker_token(request))
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    job = queue.get(str(payload.get("job_id") or ""))
    worker_id = str(payload.get("worker_id") or "")
    volume_id = str(payload.get("volume_id") or "")
    receipt = payload.get("receipt")
    if not _is_active_worker_job(job, worker_id):
        raise HTTPException(
            status_code=403,
            detail="Cache trust receipt is not bound to this worker's active job",
        )
    if str(job.params.get("cache_volume_id") or "") != volume_id:
        raise HTTPException(
            status_code=403, detail="Cache trust receipt volume is outside launch plan"
        )
    if not isinstance(receipt, dict):
        raise HTTPException(status_code=400, detail="receipt must be an object")
    try:
        verified = _prepared_manifest_signer(config).verify_trust_receipt(receipt)
        if str(verified.get("volume_id") or "") != volume_id:
            raise ValueError("Cache trust receipt belongs to another volume")
        if str(verified.get("provider_volume_id") or "") != str(
            job.params.get("cache_provider_volume_id") or ""
        ):
            raise ValueError("Cache trust receipt provider volume changed")
        return verified
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/workers/cache/manifests/fetch")
async def worker_fetch_prepared_manifest(
    request: Request, payload: dict[str, Any] = Body(...)
):
    """Return only the exact signed manifest assigned to an active worker job."""

    config, queue = _queue()
    try:
        queue.authorize_worker(_worker_token(request))
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    job = queue.get(str(payload.get("job_id") or ""))
    worker_id = str(payload.get("worker_id") or "")
    volume_id = str(payload.get("volume_id") or "")
    manifest_id = str(payload.get("manifest_id") or "")
    if not _is_active_worker_job(job, worker_id):
        raise HTTPException(
            status_code=403,
            detail="Prepared manifest fetch is not bound to this worker's active job",
        )
    if str(job.params.get("cache_volume_id") or "") != volume_id:
        raise HTTPException(
            status_code=403, detail="Fetch volume is outside launch plan"
        )
    if str(job.params.get("cache_manifest_id") or "") != manifest_id:
        raise HTTPException(
            status_code=403, detail="Manifest ID is outside the exact launch plan"
        )
    manifest = _cache_registry(config).get_manifest(volume_id, manifest_id)
    if not manifest:
        raise HTTPException(status_code=404, detail="Prepared manifest not found")
    try:
        verified = _prepared_manifest_signer(config).verify(manifest)
        if str(verified.get("cache_volume_id") or "") != volume_id:
            raise ValueError("Signed manifest volume claim does not match launch plan")
        return verified
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/workers/cache/manifests/announce")
async def worker_announce_prepared_manifest(
    request: Request, payload: dict[str, Any] = Body(...)
):
    """Project a manifest only after an active worker atomically published it."""
    config, queue = _queue()
    try:
        queue.authorize_worker(_worker_token(request))
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    job = queue.get(str(payload.get("job_id") or ""))
    worker_id = str(payload.get("worker_id") or "")
    volume_id = str(payload.get("volume_id") or "")
    generation = str(payload.get("generation") or "")
    manifest = payload.get("manifest")
    if not _is_active_worker_job(job, worker_id):
        raise HTTPException(
            status_code=403,
            detail="Prepared manifest announcement is not bound to this worker's active job",
        )
    if str(job.params.get("cache_volume_id") or "") != volume_id:
        raise HTTPException(
            status_code=403, detail="Announcement volume is outside launch plan"
        )
    if not generation or "/" in generation or "\\" in generation:
        raise HTTPException(
            status_code=400, detail="Announcement generation is invalid"
        )
    if not isinstance(manifest, dict):
        raise HTTPException(status_code=400, detail="manifest must be an object")
    try:
        verified = _prepared_manifest_signer(config).verify(manifest)
        if str(verified.get("cache_volume_id") or "") != volume_id:
            raise ValueError("Signed manifest volume claim does not match announcement")
        volume = _cache_registry(config).get_volume(volume_id)
        if not volume:
            raise ValueError("Announcement cache volume is not registered")
        result = _cache_registry(config).announce_manifest(
            volume_id, generation, verified
        )
    except (ValueError, RuntimeError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "volume_id": volume_id,
        "manifest_id": verified["manifest_id"],
        "generation": generation,
        **result,
    }


@app.post("/api/workers/cache/observations")
async def worker_record_cache_observation(
    request: Request, payload: dict[str, Any] = Body(...)
):
    config, queue = _queue()
    try:
        queue.authorize_worker(_worker_token(request))
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    job = queue.get(str(payload.get("job_id") or ""))
    worker_id = str(payload.get("worker_id") or "")
    if not _is_active_worker_job(job, worker_id):
        raise HTTPException(
            status_code=403, detail="Observation is not bound to this worker job"
        )
    observation = payload.get("observation")
    if not isinstance(observation, dict):
        raise HTTPException(status_code=400, detail="observation must be an object")
    allowed_volume = str(job.params.get("cache_volume_id") or "")
    if str(observation.get("volume_id") or "") != allowed_volume:
        raise HTTPException(
            status_code=403, detail="Observation volume is outside launch plan"
        )
    try:
        observation_id = _cache_registry(config).record_observation(observation)
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"id": observation_id}


@app.post("/api/workers/artifacts")
async def worker_upload_artifact(
    request: Request,
    file: UploadFile = File(...),
    sha256: str | None = Form(None),
):
    """Upload an output artifact over the authenticated worker channel."""
    _, queue = _queue()
    try:
        queue.authorize_worker(_worker_token(request))
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    return await _store_partition_artifact(file, sha256)


@app.get("/api/workers/jobs/{job_id}")
async def worker_job_status(job_id: str, request: Request):
    """Allow a worker to observe cancellation without exposing its ComfyUI port."""
    _, job = _authorize_worker_job(request, job_id)
    return job.to_dict()


@app.post("/api/workers/jobs/{job_id}/running")
async def worker_running(job_id: str, request: Request):
    from cloud_offload.queue import JobStatus

    queue, job = _authorize_worker_job(request, job_id)
    return queue.update_status(job.id, JobStatus.RUNNING, progress=10).to_dict()


@app.post("/api/workers/jobs/{job_id}/progress")
async def worker_progress(
    job_id: str, request: Request, payload: dict[str, Any] = Body(...)
):
    queue, job = _authorize_worker_job(request, job_id)
    return queue.set_progress(job.id, int(payload.get("progress", 0))).to_dict()


@app.post("/api/workers/jobs/{job_id}/events")
async def worker_event(
    job_id: str, request: Request, payload: dict[str, Any] = Body(...)
):
    """Accept an authenticated incremental event from a remote runner."""
    from cloud_offload.queue import JobStatus

    queue, job = _authorize_worker_job(request, job_id)
    if job.status in {
        JobStatus.COMPLETED,
        JobStatus.FAILED,
        JobStatus.DEAD_LETTER,
    }:
        return {"job_id": job.id, "ignored": True, "status": job.status.value}
    event = payload.get("event")
    if not isinstance(event, dict):
        raise HTTPException(status_code=400, detail="event object is required")
    producer_id = payload.get("producer_id")
    if producer_id and job.worker_id:
        expected_prefix = f"worker:{job.worker_id}:"
        if not str(producer_id).startswith(expected_prefix):
            raise HTTPException(
                status_code=403,
                detail="Worker event producer does not match the claimed job",
            )
    try:
        return queue.append_event(
            job.id,
            event,
            producer_id=str(producer_id or f"worker:{job.worker_id or 'legacy'}"),
            producer_sequence=payload.get("producer_sequence"),
            occurred_at=payload.get("occurred_at"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/workers/jobs/{job_id}/complete")
async def worker_complete(
    job_id: str, request: Request, payload: dict[str, Any] = Body(...)
):
    queue, job = _authorize_worker_job(request, job_id)
    result = payload.get("result")
    if not isinstance(result, dict):
        raise HTTPException(status_code=400, detail="result object is required")
    return queue.complete_job(job.id, result).to_dict()


@app.post("/api/workers/jobs/{job_id}/fail")
async def worker_fail(
    job_id: str, request: Request, payload: dict[str, Any] = Body(...)
):
    queue, job = _authorize_worker_job(request, job_id)
    return queue.fail_job(job.id, str(payload.get("error", "Worker failed"))).to_dict()


# === Main ===


def _resolve_tls(
    tls_cert: str | None, tls_key: str | None
) -> tuple[str | None, str | None]:
    """Resolve and check the TLS material, falling back to the environment."""
    cert = tls_cert or os.environ.get("CLOUD_OFFLOAD_TLS_CERT", "").strip() or None
    key = tls_key or os.environ.get("CLOUD_OFFLOAD_TLS_KEY", "").strip() or None
    if bool(cert) != bool(key):
        raise ServiceConfigError("TLS needs both a certificate and a private key")
    for label, path in (("certificate", cert), ("private key", key)):
        if path and not Path(path).is_file():
            raise ServiceConfigError(f"TLS {label} not found: {path}")
    return cert, key


def serve(
    host: str = "127.0.0.1",
    port: int | None = None,
    allow_lan: bool = False,
    require_auth: bool = False,
    tls_cert: str | None = None,
    tls_key: str | None = None,
):
    """Start the Cloud Offload coordinator.

    The bearer token is required by default, including on loopback: the local
    threat is another process on this machine, not another host, and a token is
    what addresses that. Pass ``--allow-anonymous-loopback`` to opt out on a
    single-user desktop.

    Supply ``tls_cert``/``tls_key`` (or ``CLOUD_OFFLOAD_TLS_CERT``/``_KEY``) to
    terminate TLS here. A non-loopback bind without TLS warns loudly: workers
    and clients then send their bearer tokens in the clear unless something in
    front — a tunnel or reverse proxy — is terminating TLS for you.
    """
    validate_bind_host(host, allow_lan=allow_lan)
    cert, key = _resolve_tls(tls_cert, tls_key)
    port = choose_service_port(host, port)
    scheme = "https" if cert else "http"
    service_url = local_service_url(host, port).replace("http://", f"{scheme}://", 1)
    global auth_required, auth_token
    auth_required = _resolve_auth_required(host, require_auth)
    token_path = None
    if auth_required:
        auth_token, token_path = get_or_create_service_token()
    else:
        auth_token = None
    service_file = write_service_info(
        host, port, auth_required=auth_required, token_path=token_path, scheme=scheme
    )
    logger.info("Cloud Offload coordinator listening on %s", service_url)
    logger.info("Service discovery written to %s", service_file)
    if auth_required:
        logger.info("Bearer token required (token at %s)", token_path)
    else:
        logger.warning(
            "Anonymous loopback enabled: any local process can drive this "
            "coordinator and spend money on rented GPUs"
        )
    if not cert and not is_local_host(host):
        logger.warning(
            "Serving %s without TLS. Bearer tokens will cross the network in "
            "the clear unless a tunnel or reverse proxy terminates TLS.",
            host,
        )
    uvicorn.run(app, host=host, port=port, ssl_certfile=cert, ssl_keyfile=key)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Cloud Offload coordinator")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--port", type=int, help="Port to bind. Omit or pass 0 to auto-select."
    )
    parser.add_argument(
        "--allow-lan",
        action="store_true",
        help="Allow binding to a non-localhost address",
    )
    parser.add_argument(
        "--require-auth",
        action="store_true",
        help="Force a bearer token even on a loopback bind (use when tunneling)",
    )
    args = parser.parse_args()
    try:
        serve(
            args.host,
            args.port,
            allow_lan=args.allow_lan,
            require_auth=args.require_auth,
        )
    except ServiceConfigError as exc:
        raise SystemExit(str(exc)) from exc

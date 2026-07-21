"""Cloud Offload coordinator - HTTP API for the provider-neutral offload service.

The coordinator never loads a model. It accepts ComfyUI workflows and compiled
partition jobs, schedules them onto cloud GPU workers, stores content-addressed
boundary artifacts, and relays resumable execution events.
"""

import asyncio
import hashlib
import json
import logging
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any, Optional

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

MAX_UPLOAD_BYTES = int(os.environ.get("CLOUD_OFFLOAD_MAX_UPLOAD_BYTES", str(32 * 1024 * 1024)))
MAX_PARTITION_ARTIFACT_BYTES = int(
    os.environ.get(
        "CLOUD_OFFLOAD_MAX_PARTITION_ARTIFACT_BYTES", str(2 * 1024 * 1024 * 1024)
    )
)

# Runtime auth state, set by ``serve`` when binding to a LAN address.
auth_required = False
auth_token: str | None = None
last_error: str | None = None


# === Request/Response Models ===

class ErrorDetail(BaseModel):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    error: ErrorDetail


class WorkflowSubmitRequest(BaseModel):
    workflow: dict[str, Any]
    inputs: dict[str, str] = Field(default_factory=dict)
    provider: str = "auto"
    timeout_seconds: int = Field(default=3600, ge=1, le=86400)


class PartitionSubmitRequest(BaseModel):
    partition: dict[str, Any]
    input_artifacts: dict[str, str] = Field(default_factory=dict)
    provider: str = "auto"
    timeout_seconds: int = Field(default=3600, ge=1, le=86400)


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
async def handle_starlette_http_exception(request: Request, exc: StarletteHTTPException):
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


def _resolve_auth_required(host: str, require_auth: bool = False) -> bool:
    """Decide whether the global bearer token is enforced.

    Enforced automatically for non-loopback binds. Also enforced whenever
    ``require_auth`` or the ``CLOUD_OFFLOAD_REQUIRE_AUTH`` env var is set — a
    loopback bind is otherwise treated as private, which is unsafe when the
    service is fronted by a tunnel and thus publicly reachable.
    """
    env_force = os.environ.get("CLOUD_OFFLOAD_REQUIRE_AUTH", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    return require_auth or env_force or not is_local_host(host)


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


def _worker_token(request: Request) -> str | None:
    authorization = request.headers.get("Authorization", "")
    return authorization[7:] if authorization.startswith("Bearer ") else None


def _partition_artifact_key(digest: str) -> str:
    normalized = str(digest).lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise HTTPException(status_code=400, detail="Invalid partition artifact digest")
    return f"partition-artifacts/{normalized[:2]}/{normalized}.part"


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
            raise HTTPException(status_code=400, detail="Partition artifact digest mismatch")
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


def _provider_statuses(config) -> list[dict[str, Any]]:
    from cloud_offload.providers import connector_names, create_connector, connector_metadata
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
    _, queue = _queue()
    try:
        queue.authorize_worker(_worker_token(request))
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    job = queue.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return queue, job


# === Public routes ===

@app.get("/")
async def root():
    return {"name": SERVICE_NAME, "version": VERSION, "api_version": API_VERSION, "status": "ok"}


@app.get("/api/health")
async def health():
    return {"name": SERVICE_NAME, "status": "ok", "version": VERSION, "api_version": API_VERSION}


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
        "providers": await asyncio.to_thread(_provider_statuses, config),
        "config": config.to_dict(),
    }


@app.get("/api/config")
async def get_config():
    """Return the current non-secret configuration."""
    return _config().to_dict()


@app.post("/api/config")
async def update_config(updates: dict[str, Any] = Body(...)):
    """Persist non-secret configuration. Secrets come from the environment only."""
    from cloud_offload.config import CONFIG_DIR

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

    config_path = CONFIG_DIR / "config.json"
    if config_path.exists():
        with open(config_path) as f:
            data = json.load(f)
    else:
        data = {}
    if "cloud" in data and isinstance(data["cloud"], dict):
        data["cloud"].update(payload)
    else:
        data.update(payload)

    config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config_path, "w") as f:
        json.dump(data, f, indent=2)

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


@app.post("/api/providers/{provider}/credentials")
async def set_provider_credentials(provider: str, body: dict[str, Any] = Body(...)):
    """Store one connector credential outside config.json.

    The key is written to the credential file with owner-only permissions and is
    never echoed back. Send an empty ``api_key`` to clear it.
    """
    from cloud_offload.config import (
        normalize_provider_name,
        provider_env_var,
        save_provider_credential,
    )
    from cloud_offload.providers import connector_names

    name = normalize_provider_name(provider)
    if name not in connector_names():
        raise HTTPException(
            status_code=404, detail=f"Unknown cloud connector: {provider}"
        )
    api_key = body.get("api_key")
    if not isinstance(api_key, str):
        raise HTTPException(status_code=400, detail="api_key must be a string")
    if os.environ.get(provider_env_var(name), "").strip():
        raise HTTPException(
            status_code=409,
            detail=(
                f"{name} credentials come from {provider_env_var(name)}; "
                "unset it to manage the credential here"
            ),
        )
    try:
        await asyncio.to_thread(save_provider_credential, name, api_key)
    except (OSError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"provider": name, "configured": bool(api_key.strip())}


@app.get("/api/jobs")
async def list_jobs(status: Optional[str] = None, limit: int = 50):
    """List jobs in the queue (convenience endpoint)."""
    from cloud_offload.queue import JobStatus

    _, queue = _queue()
    statuses = [JobStatus(status)] if status else list(JobStatus)
    jobs = queue.list_by_status(*statuses)[:limit]
    return [job.to_dict() for job in jobs]


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    """Get job status (+ result when completed)."""
    _, queue = _queue()
    job = queue.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job.to_dict()


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    """Cancel a job."""
    from cloud_offload.queue import JobStatus

    _, queue = _queue()
    current = queue.get(job_id)
    if not current:
        raise HTTPException(status_code=404, detail="Job not found")
    if current.status == JobStatus.COMPLETED:
        raise HTTPException(status_code=409, detail="Completed jobs cannot be cancelled")
    if current.status in {JobStatus.FAILED, JobStatus.DEAD_LETTER}:
        return current.to_dict()
    queue.append_event(
        job_id,
        {
            "type": "cancellation_requested",
            "partition_id": (current.request.get("partition") or {}).get("partition_id"),
        },
    )
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


@app.post("/api/workflows", status_code=202)
async def submit_workflow(request: WorkflowSubmitRequest):
    """Queue an API-format ComfyUI workflow on the dedicated cloud profile."""
    from cloud_offload.queue import JobStatus
    from cloud_offload.router import select_profile_provider

    config, queue = _queue()
    try:
        route = await asyncio.to_thread(
            select_profile_provider, config, "comfyui", request.provider
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    job = queue.create(
        model="comfyui-workflow",
        input_path="inline://comfyui-workflow",
        params={
            **({"offer": route.offer} if route.offer else {}),
            "runtime_profile": "comfyui",
        },
        request=request.model_dump(),
        provider=route.provider,
        status=JobStatus.QUEUED,
    )
    return {
        "job_id": job.id,
        "status": job.status.value,
        "status_url": f"/api/jobs/{job.id}",
    }


@app.post("/api/partitions", status_code=202)
async def submit_partition(request: PartitionSubmitRequest):
    """Queue a compiled subgraph with immutable typed boundary artifacts."""
    from cloud_offload.queue import JobStatus
    from cloud_offload.router import select_profile_provider
    from cloud_offload.storage import create_storage

    if request.partition.get("schema") != PARTITION_JOB_SCHEMA:
        raise HTTPException(status_code=400, detail="Unsupported partition job schema")
    if not isinstance(request.partition.get("workflow"), dict) or not request.partition["workflow"]:
        raise HTTPException(status_code=400, detail="Partition workflow is required")
    runner = request.partition.get("runner") or {}
    profile_name = str(runner.get("profile") or "comfyui").strip()[:100]
    if not profile_name.startswith("comfyui"):
        raise HTTPException(status_code=400, detail="Invalid ComfyUI runner profile")
    try:
        min_gpu_ram_gb = max(1, min(256, int(runner.get("min_gpu_ram_gb") or 16)))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid partition GPU VRAM requirement")
    gpu_type = str(runner.get("gpu_type") or "any").strip()[:100] or "any"
    config, queue = _queue()
    storage = create_storage(config)
    for boundary_key, artifact_id in request.input_artifacts.items():
        if not boundary_key.startswith("input_"):
            raise HTTPException(status_code=400, detail=f"Invalid input boundary key: {boundary_key}")
        if not storage.exists(_partition_artifact_key(artifact_id)):
            raise HTTPException(status_code=404, detail=f"Input artifact not found: {artifact_id}")
    try:
        route = await asyncio.to_thread(
            select_profile_provider, config, profile_name, request.provider
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
        json.dumps(cache_identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    cached = queue.get_partition_cache(cache_key)
    if cached:
        output_ids = (cached.get("output_artifacts") or {}).values()
        if all(storage.exists(_partition_artifact_key(str(item))) for item in output_ids):
            job = queue.create(
                model="comfyui-partition-v1",
                input_path="artifacts://comfyui-partition-cache",
                params={
                    "runtime_profile": profile_name,
                    "partition_cache_key": cache_key,
                    "cache_hit": True,
                    "gpu_type": gpu_type,
                    "min_gpu_ram_gb": min_gpu_ram_gb,
                },
                request={
                    "kind": "comfyui-partition",
                    "partition": request.partition,
                    "input_artifacts": request.input_artifacts,
                    "timeout_seconds": request.timeout_seconds,
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
            }
    job = queue.create(
        model="comfyui-partition-v1",
        input_path="artifacts://comfyui-partition",
        params={
            **({"offer": route.offer} if route.offer else {}),
            "runtime_profile": profile_name,
            "partition_cache_key": cache_key,
            "gpu_type": gpu_type,
            "min_gpu_ram_gb": min_gpu_ram_gb,
            "keep_warm": bool(runner.get("keep_warm", False)),
        },
        request={
            "kind": "comfyui-partition",
            "partition": request.partition,
            "input_artifacts": request.input_artifacts,
            "timeout_seconds": request.timeout_seconds,
        },
        provider=route.provider,
        status=JobStatus.QUEUED,
    )
    return {
        "job_id": job.id,
        "status": job.status.value,
        "status_url": f"/api/jobs/{job.id}",
    }


@app.post("/api/artifacts")
async def upload_artifact(file: UploadFile = File(...), sha256: str | None = Form(None)):
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
    _, queue = _queue()
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
        )
        queue.record_worker(
            str(payload["worker_id"]),
            str(payload["provider"]),
            runtime_profile=str(payload.get("runtime_profile") or ""),
            capabilities=[str(item) for item in payload.get("models", [])],
            idle=not jobs,
        )
    except (KeyError, PermissionError, ValueError) as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    return [job.to_dict() for job in jobs]


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
async def worker_progress(job_id: str, request: Request, payload: dict[str, Any] = Body(...)):
    queue, job = _authorize_worker_job(request, job_id)
    return queue.set_progress(job.id, int(payload.get("progress", 0))).to_dict()


@app.post("/api/workers/jobs/{job_id}/events")
async def worker_event(job_id: str, request: Request, payload: dict[str, Any] = Body(...)):
    """Accept an authenticated incremental event from a remote runner."""
    queue, job = _authorize_worker_job(request, job_id)
    event = payload.get("event")
    if not isinstance(event, dict):
        raise HTTPException(status_code=400, detail="event object is required")
    try:
        return queue.append_event(job.id, event)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/workers/jobs/{job_id}/complete")
async def worker_complete(job_id: str, request: Request, payload: dict[str, Any] = Body(...)):
    queue, job = _authorize_worker_job(request, job_id)
    result = payload.get("result")
    if not isinstance(result, dict):
        raise HTTPException(status_code=400, detail="result object is required")
    return queue.complete_job(job.id, result).to_dict()


@app.post("/api/workers/jobs/{job_id}/fail")
async def worker_fail(job_id: str, request: Request, payload: dict[str, Any] = Body(...)):
    queue, job = _authorize_worker_job(request, job_id)
    return queue.fail_job(job.id, str(payload.get("error", "Worker failed"))).to_dict()


# === Main ===

def serve(
    host: str = "127.0.0.1",
    port: int | None = None,
    allow_lan: bool = False,
    require_auth: bool = False,
):
    """Start the Cloud Offload coordinator.

    Bearer auth is enforced automatically for non-loopback binds. A loopback
    bind is treated as private, but that assumption breaks when the loopback
    service is fronted by a tunnel (e.g. Cloudflare) — the routes are then
    publicly reachable yet unauthenticated. Set ``require_auth`` (or the
    ``CLOUD_OFFLOAD_REQUIRE_AUTH`` env var) to force a bearer token regardless
    of bind host, so a tunneled deployment stays authenticated.
    """
    validate_bind_host(host, allow_lan=allow_lan)
    port = choose_service_port(host, port)
    service_url = local_service_url(host, port)
    global auth_required, auth_token
    auth_required = _resolve_auth_required(host, require_auth)
    token_path = None
    if auth_required:
        auth_token, token_path = get_or_create_service_token()
    else:
        auth_token = None
    service_file = write_service_info(
        host, port, auth_required=auth_required, token_path=token_path
    )
    logger.info("Cloud Offload coordinator listening on %s", service_url)
    logger.info("Service discovery written to %s", service_file)
    if auth_required:
        reason = "LAN bind" if not is_local_host(host) else "require_auth"
        logger.info("%s: bearer token required (token at %s)", reason, token_path)
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Cloud Offload coordinator")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, help="Port to bind. Omit or pass 0 to auto-select.")
    parser.add_argument("--allow-lan", action="store_true", help="Allow binding to a non-localhost address")
    parser.add_argument(
        "--require-auth",
        action="store_true",
        help="Force a bearer token even on a loopback bind (use when tunneling)",
    )
    args = parser.parse_args()
    try:
        serve(args.host, args.port, allow_lan=args.allow_lan, require_auth=args.require_auth)
    except ServiceConfigError as exc:
        raise SystemExit(str(exc)) from exc

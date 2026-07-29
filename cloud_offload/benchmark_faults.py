"""Reviewed, reversible production canaries for the benchmark harness.

These commands are deliberately unavailable as ordinary product operations.
They require the environment injected by ``BenchmarkRunner.run_hook`` and are
still gated by the campaign's explicit ``--allow-hooks`` acknowledgement.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import requests

from cloud_offload.cache_registry import CacheRegistry
from cloud_offload.config import CONFIG_DIR, CloudConfig
from cloud_offload.prepared_state import (
    INDEX_SCHEMA,
    RunPodS3PreparedStore,
    blob_key,
    build_manifest,
    canonical_json,
    load_or_create_manifest_signer,
    normalize_digest,
    utc_now,
)
from cloud_offload.providers import create_connector
from cloud_offload.service_config import (
    ServiceConfigError,
    discover_service_info,
    is_local_host,
    read_service_info,
)
from cloud_offload.storage import create_storage, partition_artifact_key


FAULT_KINDS = {"storage", "corruption", "restart"}


def _benchmark_context(expected_kind: str) -> tuple[str, str, str]:
    kind = os.environ.get("CLOUD_OFFLOAD_BENCHMARK_FAILURE_KIND", "").strip()
    job_id = os.environ.get("CLOUD_OFFLOAD_BENCHMARK_JOB_ID", "").strip()
    scenario = os.environ.get("CLOUD_OFFLOAD_BENCHMARK_SCENARIO", "").strip()
    stage = (
        os.environ.get("CLOUD_OFFLOAD_BENCHMARK_HOOK_STAGE", "observe").strip().lower()
    )
    if (
        kind != expected_kind
        or not scenario
        or stage not in {"prepare", "observe", "cleanup"}
        or (stage == "observe" and not job_id)
    ):
        raise RuntimeError(
            "Benchmark fault canaries run only inside a matching benchmark hook"
        )
    return job_id, scenario, stage


class CoordinatorFaultClient:
    def __init__(self, service: dict[str, Any] | None = None):
        self.service = service or discover_service_info(require_healthy=True)
        self.base_url = str(self.service["url"]).rstrip("/")
        self.session = requests.Session()
        if self.service.get("token"):
            self.session.headers["Authorization"] = f"Bearer {self.service['token']}"

    def get(self, path: str) -> Any:
        response = self.session.get(f"{self.base_url}{path}", timeout=30)
        response.raise_for_status()
        return response.json()

    def post(self, path: str, body: dict[str, Any]) -> Any:
        response = self.session.post(f"{self.base_url}{path}", json=body, timeout=30)
        response.raise_for_status()
        return response.json()

    def prepared_storage(self) -> dict[str, Any]:
        prepared = self.get("/api/config").get("prepared_storage")
        if not isinstance(prepared, dict):
            raise RuntimeError("Coordinator returned no prepared-storage config")
        return json.loads(json.dumps(prepared, allow_nan=False))

    def set_prepared_storage(self, prepared: dict[str, Any]) -> None:
        self.post("/api/config", {"prepared_storage": prepared})

    def job(self, job_id: str) -> dict[str, Any]:
        value = self.get(f"/api/jobs/{job_id}")
        if not isinstance(value, dict):
            raise RuntimeError("Coordinator returned no benchmark job")
        return value

    def events(self, job_id: str, after: int) -> list[dict[str, Any]]:
        payload = self.get(f"/api/jobs/{job_id}/events?after={after}&limit=1000")
        return list(payload.get("events") or [])


def _wait_for_event(
    client: CoordinatorFaultClient,
    job_id: str,
    wanted: set[str],
    *,
    timeout_seconds: float,
    forbidden: set[str] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    deadline = time.monotonic() + timeout_seconds
    cursor = 0
    forbidden = forbidden or set()
    while time.monotonic() < deadline:
        events = client.events(job_id, cursor)
        if events:
            cursor = max(int(item.get("sequence") or 0) for item in events)
        event_types = {str(item.get("type") or "") for item in events}
        unexpected = sorted(event_types & forbidden)
        if unexpected:
            raise RuntimeError(
                "Fault canary lost its pre-mutation race: " + ", ".join(unexpected)
            )
        matched = sorted(event_types & wanted)
        if matched:
            return matched[0]
        status = str(client.job(job_id).get("status") or "")
        if status in {"completed", "failed", "dead_letter"}:
            raise RuntimeError(
                f"Benchmark job became {status} before the fault was observed"
            )
        sleep(1)
    raise TimeoutError("Timed out waiting for benchmark fault evidence")


def inject_storage_unavailable(
    client: CoordinatorFaultClient, job_id: str
) -> dict[str, Any]:
    """Temporarily point strict placement at a nonexistent volume.

    No provider storage is created, detached, or deleted. The full scenario
    config is restored before this hook exits, and a concurrent edit is never
    overwritten.
    """

    original = client.prepared_storage()
    if not original.get("enabled") or original.get("policy") not in {
        "strict",
        "pinned",
    }:
        raise RuntimeError(
            "Storage-unavailable canary requires strict prepared storage"
        )
    if not original.get("existing_volume_id"):
        raise RuntimeError("Storage-unavailable canary requires a bound volume")
    target = json.loads(json.dumps(original, allow_nan=False))
    target["existing_volume_id"] = f"benchmark-missing-{job_id[:12]}"
    applied = False
    observed = None
    try:
        client.set_prepared_storage(target)
        applied = True
        if client.prepared_storage() != target:
            raise RuntimeError("Coordinator did not apply storage fault config")
        observed = _wait_for_event(
            client,
            job_id,
            {"provisioning_failed"},
            forbidden={"provisioning_started", "runner_starting"},
            timeout_seconds=90,
        )
    finally:
        if applied:
            current = client.prepared_storage()
            if current == target:
                client.set_prepared_storage(original)
            elif current != original:
                raise RuntimeError(
                    "Prepared-storage config changed during fault; refusing to overwrite it"
                )
            if client.prepared_storage() != original:
                raise RuntimeError("Storage fault did not restore scenario config")
    return {
        "kind": "storage",
        "observed_event": observed,
        "provider_storage_mutated": False,
        "config_restored": True,
    }


CORRUPTION_STATE_SCHEMA = "cloud-offload.benchmark-corruption-state.v1"


def corruption_canary_asset(scenario: str) -> dict[str, Any]:
    """Return the synthetic asset unique to one corruption scenario."""

    scenario_tag = hashlib.sha256(scenario.encode("utf-8")).hexdigest()[:16]
    payload = f"cloud-offload-benchmark-valid-v1:{scenario_tag}".encode("utf-8")
    return {
        "category": "vae",
        "filename": f"cloud_offload_benchmark_canary_{scenario_tag}.bin",
        "format": "other",
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size": len(payload),
        "cacheable": True,
        "private": False,
    }


def _corruption_valid_payload(scenario: str) -> bytes:
    scenario_tag = hashlib.sha256(scenario.encode("utf-8")).hexdigest()[:16]
    return f"cloud-offload-benchmark-valid-v1:{scenario_tag}".encode("utf-8")


def _corruption_target(
    client: CoordinatorFaultClient,
    scenario: str,
    *,
    declared_digests: set[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    prepared = client.prepared_storage()
    provider_volume_id = str(prepared.get("existing_volume_id") or "")
    if not prepared.get("enabled") or not provider_volume_id:
        raise RuntimeError("Corruption canary requires enabled, bound prepared storage")
    volumes = list(client.get("/api/cache/status").get("volumes") or [])
    volume = next(
        (
            item
            for item in volumes
            if str(item.get("provider_volume_id") or "") == provider_volume_id
        ),
        None,
    )
    if not volume or not volume.get("s3_compatible"):
        raise RuntimeError("Bound prepared volume has no S3 canary path")

    declared = {"sha256:" + normalize_digest(item) for item in declared_digests}
    canary_digest = "sha256:" + corruption_canary_asset(scenario)["sha256"]
    if canary_digest not in declared:
        raise RuntimeError("Benchmark request is missing its fresh corruption asset")
    ordinary = declared - {canary_digest}
    manifests = list(client.get("/api/cache/manifests").get("manifests") or [])
    candidates = [
        manifest
        for manifest in manifests
        if str(manifest.get("volume_id") or "") == str(volume.get("id") or "")
        and canary_digest
        not in {
            str(artifact.get("digest") or "")
            for artifact in manifest.get("artifacts") or []
        }
    ]
    if not candidates:
        raise RuntimeError("Bound prepared volume has no base manifest for the canary")
    manifest = max(
        candidates,
        key=lambda item: (
            len(
                ordinary
                & {
                    str(artifact.get("digest") or "")
                    for artifact in item.get("artifacts") or []
                }
            ),
            str(item.get("created_at") or ""),
            str(item.get("manifest_id") or ""),
        ),
    )
    overlap = ordinary & {
        str(item.get("digest") or "") for item in manifest.get("artifacts") or []
    }
    if not overlap:
        raise RuntimeError("No prepared base artifact matches the benchmark request")
    return volume, manifest


def _prepared_store(volume: dict[str, Any]) -> RunPodS3PreparedStore:
    connector = create_connector("runpod", CloudConfig.load())
    endpoint = connector.s3_endpoint(str(volume["datacenter_id"]))
    if not endpoint:
        raise RuntimeError("RunPod volume datacenter has no S3 endpoint")
    return RunPodS3PreparedStore.from_environment(
        volume_id=str(volume["provider_volume_id"]),
        datacenter_id=str(volume["datacenter_id"]),
        endpoint_url=endpoint,
    )


def _manifest_signer():
    config = CloudConfig.load()
    return load_or_create_manifest_signer(
        Path(config.queue_db_path).with_name("prepared-manifest-key")
    )


def _cache_registry() -> CacheRegistry:
    return CacheRegistry(CloudConfig.load(resolve_secrets=False).queue_db_path)


def _coordinator_storage():
    return create_storage(CloudConfig.load())


def _corruption_keys(scenario: str) -> dict[str, str]:
    digest = normalize_digest(str(corruption_canary_asset(scenario)["sha256"]))
    scenario_tag = hashlib.sha256(scenario.encode("utf-8")).hexdigest()[:16]
    prefix = f"staging/benchmark-corruption/{scenario_tag}"
    return {
        "digest": digest,
        "blob_key": blob_key(digest),
        "state_key": f"{prefix}/state.json",
        "quarantine_prefix": f"quarantine/{digest}/",
    }


def _object_size(store: RunPodS3PreparedStore, key: str) -> int | None:
    try:
        head = store.client.head_object(Bucket=store.volume_id, Key=key)
    except Exception:
        return None
    return int(head.get("ContentLength") or -1)


def _object_bytes(store: RunPodS3PreparedStore, key: str) -> bytes | None:
    try:
        response = store.client.get_object(Bucket=store.volume_id, Key=key)
    except Exception:
        return None
    return bytes(response["Body"].read())


def _manifest_index_entry(
    manifest: dict[str, Any], generation: str, storage_key: str
) -> dict[str, Any]:
    return {
        "manifest_id": manifest["manifest_id"],
        "storage_key": storage_key,
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


def _delete_quarantine_objects(store: RunPodS3PreparedStore, prefix: str) -> int:
    deleted = 0
    continuation = None
    while True:
        request: dict[str, Any] = {"Bucket": store.volume_id, "Prefix": prefix}
        if continuation:
            request["ContinuationToken"] = continuation
        response = store.client.list_objects_v2(**request)
        for item in response.get("Contents") or []:
            key = str(item.get("Key") or "")
            if key.startswith(prefix):
                store.client.delete_object(Bucket=store.volume_id, Key=key)
                deleted += 1
        if not response.get("IsTruncated"):
            break
        continuation = response.get("NextContinuationToken")
        if not continuation:
            raise RuntimeError("Corruption quarantine listing has no cursor")
    return deleted


def prepare_corruption(
    client: CoordinatorFaultClient,
    scenario: str,
    declared_digests: set[str],
    *,
    store_factory: Callable[[dict[str, Any]], RunPodS3PreparedStore] = _prepared_store,
    signer_factory: Callable[[], Any] = _manifest_signer,
    registry_factory: Callable[[], CacheRegistry] = _cache_registry,
    coordinator_storage_factory: Callable[[], Any] = _coordinator_storage,
    settle_seconds: float = 5,
) -> dict[str, Any]:
    """Publish then corrupt an object no prior mount could have cached."""

    volume, base_manifest = _corruption_target(
        client, scenario, declared_digests=declared_digests
    )
    store = store_factory(volume)
    keys = _corruption_keys(scenario)
    if _object_size(store, keys["state_key"]) is not None:
        raise RuntimeError("A stale fresh-object corruption canary is still active")
    if _object_size(store, keys["blob_key"]) is not None:
        raise RuntimeError("Fresh corruption canary digest already exists")
    original_index = store.load_index()
    original_generation = str(original_index.get("generation") or "")
    if not original_generation:
        raise RuntimeError("Prepared storage has no restorable inventory generation")
    asset = corruption_canary_asset(scenario)
    valid_payload = _corruption_valid_payload(scenario)
    coordinator_storage = coordinator_storage_factory()
    coordinator_artifact_key = partition_artifact_key(keys["digest"])
    if coordinator_storage.exists(coordinator_artifact_key):
        raise RuntimeError("Fresh corruption coordinator artifact already exists")
    with tempfile.NamedTemporaryFile(delete=False, suffix=".part") as temporary:
        temporary.write(valid_payload)
        temporary_path = Path(temporary.name)
    try:
        coordinator_storage.upload(temporary_path, coordinator_artifact_key)
    finally:
        temporary_path.unlink(missing_ok=True)
    if not coordinator_storage.exists(coordinator_artifact_key):
        raise RuntimeError("Fresh corruption coordinator artifact was not published")
    artifact = {
        "digest": "sha256:" + keys["digest"],
        "kind": "model-weight",
        "size": len(valid_payload),
        "storage_key": keys["blob_key"],
        "portability": "portable",
        "requirements": {},
        "policy": {
            "tenant": str(client.prepared_storage().get("tenant") or "default"),
            "cacheable": True,
            "private": False,
        },
        "destination": {
            "category": asset["category"],
            "filename": asset["filename"],
        },
    }
    canary_manifest = build_manifest(
        profile_fingerprint=str(base_manifest["profile_fingerprint"]),
        producer=dict(base_manifest.get("producer") or {}),
        artifacts=[
            item
            for item in base_manifest.get("artifacts") or []
            if str(item.get("digest") or "") != artifact["digest"]
        ]
        + [artifact],
        signer=signer_factory(),
        claims={"cache_volume_id": str(volume["id"])},
    )
    scenario_tag = hashlib.sha256(scenario.encode("utf-8")).hexdigest()[:16]
    generation = f"{time.time_ns()}-benchmark-{scenario_tag}"
    profile = normalize_digest(str(canary_manifest["profile_fingerprint"]))
    manifest_key = f"manifests/{profile}/{generation}.json"
    index_key = f"indexes/{generation}.json"
    state = {
        "schema": CORRUPTION_STATE_SCHEMA,
        "volume_id": str(volume["id"]),
        "provider_volume_id": str(volume["provider_volume_id"]),
        "original_generation": original_generation,
        "canary_generation": generation,
        "manifest_id": canary_manifest["manifest_id"],
        "manifest_key": manifest_key,
        "index_key": index_key,
        "coordinator_artifact_key": coordinator_artifact_key,
        "coordinator_artifact_created": True,
        **keys,
    }
    try:
        store.client.put_object(
            Bucket=store.volume_id,
            Key=keys["state_key"],
            Body=canonical_json(state),
        )
    except Exception:
        coordinator_storage.delete(coordinator_artifact_key)
        raise
    try:
        store.client.put_object(
            Bucket=store.volume_id,
            Key=keys["blob_key"],
            Body=valid_payload,
        )
        if _object_bytes(store, keys["blob_key"]) != valid_payload:
            raise RuntimeError("Fresh corruption canary valid object was not published")
        manifest_payload = canonical_json(canary_manifest)
        store.client.put_object(
            Bucket=store.volume_id, Key=manifest_key, Body=manifest_payload
        )
        if _object_size(store, manifest_key) != len(manifest_payload):
            raise RuntimeError("Fresh corruption manifest publication was incomplete")
        entries = [
            item
            for item in original_index.get("manifests") or []
            if str(item.get("manifest_id") or "") != canary_manifest["manifest_id"]
        ]
        entries.append(_manifest_index_entry(canary_manifest, generation, manifest_key))
        canary_index = {
            "schema": INDEX_SCHEMA,
            "generation": generation,
            "created_at": utc_now(),
            "manifests": sorted(
                entries,
                key=lambda item: (
                    str(item.get("created_at") or ""),
                    str(item.get("manifest_id") or ""),
                ),
            ),
        }
        index_payload = canonical_json(canary_index)
        store.client.put_object(
            Bucket=store.volume_id, Key=index_key, Body=index_payload
        )
        if _object_size(store, index_key) != len(index_payload):
            raise RuntimeError("Fresh corruption index publication was incomplete")
        registry_factory().announce_manifest(
            str(volume["id"]), generation, canary_manifest
        )
        store.client.put_object(
            Bucket=store.volume_id,
            Key="indexes/latest",
            Body=generation.encode("utf-8"),
        )
        corrupt_payload = b"cloud-offload-benchmark-corrupt"
        store.client.put_object(
            Bucket=store.volume_id,
            Key=keys["blob_key"],
            Body=corrupt_payload,
        )
        if _object_bytes(store, keys["blob_key"]) != corrupt_payload:
            raise RuntimeError("Fresh corruption object did not replace valid bytes")
        time.sleep(max(0.0, settle_seconds))
        if _object_bytes(store, keys["blob_key"]) != corrupt_payload:
            raise RuntimeError(
                "Fresh corruption object was not stable before submission"
            )
    except Exception:
        cleanup_corruption(
            client,
            scenario,
            declared_digests,
            store_factory=store_factory,
            registry_factory=registry_factory,
            coordinator_storage_factory=coordinator_storage_factory,
        )
        raise
    return {
        "kind": "corruption",
        "stage": "prepare",
        "fresh_object": True,
        "canary_manifest_published": True,
        "canary_verified": True,
        "artifact_size": len(valid_payload),
    }


def cleanup_corruption(
    client: CoordinatorFaultClient,
    scenario: str,
    declared_digests: set[str],
    *,
    store_factory: Callable[[dict[str, Any]], RunPodS3PreparedStore] = _prepared_store,
    registry_factory: Callable[[], CacheRegistry] = _cache_registry,
    coordinator_storage_factory: Callable[[], Any] = _coordinator_storage,
) -> dict[str, Any]:
    """Remove the synthetic generation without touching ordinary artifacts."""

    volume, _ = _corruption_target(client, scenario, declared_digests=declared_digests)
    store = store_factory(volume)
    keys = _corruption_keys(scenario)
    state_payload = _object_bytes(store, keys["state_key"])
    if state_payload is None:
        if _object_size(store, keys["blob_key"]) is not None:
            raise RuntimeError("Canary state is absent while its unique blob remains")
        coordinator_key = partition_artifact_key(keys["digest"])
        if coordinator_storage_factory().exists(coordinator_key):
            raise RuntimeError(
                "Canary state is absent while its coordinator artifact remains"
            )
        return {
            "kind": "corruption",
            "stage": "cleanup",
            "original_generation_restored": True,
            "canary_deleted": True,
            "changed": False,
        }
    state = json.loads(state_payload)
    if state.get("schema") != CORRUPTION_STATE_SCHEMA:
        raise RuntimeError("Corruption cleanup state has an unknown schema")
    current_generation = (
        (_object_bytes(store, "indexes/latest") or b"").decode("utf-8").strip()
    )
    canary_generation = str(state["canary_generation"])
    original_generation = str(state["original_generation"])
    if current_generation == canary_generation:
        store.client.put_object(
            Bucket=store.volume_id,
            Key="indexes/latest",
            Body=original_generation.encode("utf-8"),
        )
    elif current_generation != original_generation:
        raise RuntimeError(
            "Prepared inventory changed during corruption canary; refusing to overwrite it"
        )
    registry_factory().remove_manifest(
        str(state["volume_id"]),
        str(state["manifest_id"]),
        inventory_generation=original_generation,
    )
    if state.get("coordinator_artifact_created"):
        coordinator_storage_factory().delete(str(state["coordinator_artifact_key"]))
    deleted_quarantine = _delete_quarantine_objects(
        store, str(state["quarantine_prefix"])
    )
    for key in (
        str(state["blob_key"]),
        str(state["manifest_key"]),
        str(state["index_key"]),
    ):
        store.client.delete_object(Bucket=store.volume_id, Key=key)
    store.client.delete_object(Bucket=store.volume_id, Key=keys["state_key"])
    restored = (_object_bytes(store, "indexes/latest") or b"").decode("utf-8").strip()
    if restored != original_generation:
        raise RuntimeError("Corruption cleanup did not restore the original generation")
    return {
        "kind": "corruption",
        "stage": "cleanup",
        "original_generation_restored": True,
        "canary_deleted": True,
        "quarantine_objects_deleted": deleted_quarantine,
        "changed": True,
    }


def observe_corruption(
    client: CoordinatorFaultClient,
    job_id: str,
    scenario: str,
    declared_digests: set[str],
    *,
    store_factory: Callable[[dict[str, Any]], RunPodS3PreparedStore] = _prepared_store,
) -> dict[str, Any]:
    """Require quarantine, then restore the tiny valid blob for a job retry."""

    volume, _ = _corruption_target(client, scenario, declared_digests=declared_digests)
    store = store_factory(volume)
    keys = _corruption_keys(scenario)
    state_payload = _object_bytes(store, keys["state_key"])
    if state_payload is None:
        raise RuntimeError("Fresh corruption canary state is absent")
    state = json.loads(state_payload)
    if state.get("schema") != CORRUPTION_STATE_SCHEMA:
        raise RuntimeError("Fresh corruption canary state is invalid")
    valid_payload = _corruption_valid_payload(scenario)
    if _object_bytes(store, str(state["blob_key"])) == valid_payload:
        raise RuntimeError("Fresh corruption canary is no longer corrupt")
    observed = _wait_for_event(
        client,
        job_id,
        {"cache_artifact_quarantined"},
        timeout_seconds=105,
    )
    store.client.put_object(
        Bucket=store.volume_id,
        Key=str(state["blob_key"]),
        Body=valid_payload,
    )
    if _object_bytes(store, str(state["blob_key"])) != valid_payload:
        raise RuntimeError("Could not restore valid retry bytes after quarantine")
    return {
        "kind": "corruption",
        "stage": "observe",
        "quarantine_observed": observed == "cache_artifact_quarantined",
        "valid_retry_object_restored": True,
        "artifact_size": len(valid_payload),
    }


def _windows_process_exists(pid: int, *, kernel32: Any | None = None) -> bool:
    """Query a Windows process without using ``os.kill(pid, 0)``.

    CPython implements ``os.kill`` through Windows termination APIs rather than
    the POSIX existence probe. During process exit, signal 0 can surface as a
    ``SystemError`` and strand the restart canary after it has stopped the old
    coordinator. ``OpenProcess`` plus ``GetExitCodeProcess`` gives the state we
    actually need and never signals the target.
    """

    if pid <= 0:
        return False
    configure = kernel32 is None
    if kernel32 is None:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    if configure:
        kernel32.OpenProcess.argtypes = [ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong]
        kernel32.OpenProcess.restype = ctypes.c_void_p
        kernel32.GetExitCodeProcess.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_ulong),
        ]
        kernel32.GetExitCodeProcess.restype = ctypes.c_int
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_int
    process_query_limited_information = 0x1000
    still_active = 259
    handle = kernel32.OpenProcess(process_query_limited_information, 0, pid)
    if not handle:
        return False
    exit_code = ctypes.c_ulong()
    try:
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            return False
        return exit_code.value == still_active
    finally:
        kernel32.CloseHandle(handle)


def _process_exists(pid: int) -> bool:
    if os.name == "nt":
        return _windows_process_exists(pid)
    try:
        os.kill(pid, 0)
    except (OSError, SystemError):
        return False
    return True


def restart_coordinator(client: CoordinatorFaultClient, job_id: str) -> dict[str, Any]:
    """Restart the exact discovered local coordinator and wait for health."""

    info = read_service_info(require_healthy=True)
    if not info:
        raise ServiceConfigError("No healthy file-discovered coordinator to restart")
    parsed = urlparse(str(info.get("url") or ""))
    if parsed.scheme != "http" or not is_local_host(str(info.get("host") or "")):
        raise RuntimeError("Restart canary supports only a local HTTP coordinator")
    old_pid = int(info.get("pid") or 0)
    if old_pid <= 0 or not _process_exists(old_pid):
        raise RuntimeError("Discovered coordinator PID is not running")
    health_pid = int(client.get("/api/health").get("pid") or 0)
    if health_pid != old_pid:
        raise RuntimeError("Service file PID does not own the healthy coordinator")
    if str(client.job(job_id).get("status") or "") not in {
        "queued",
        "dispatched",
        "running",
    }:
        raise RuntimeError("Restart canary requires a live benchmark job")

    os.kill(old_pid, signal.SIGTERM)
    deadline = time.monotonic() + 15
    while _process_exists(old_pid) and time.monotonic() < deadline:
        time.sleep(0.1)
    if _process_exists(old_pid):
        raise RuntimeError("Coordinator did not stop for restart canary")

    job_tag = job_id[:12]
    stdout_path = CONFIG_DIR / f"benchmark-restart-{job_tag}.out.log"
    stderr_path = CONFIG_DIR / f"benchmark-restart-{job_tag}.err.log"
    command = [
        sys.executable,
        "-m",
        "cloud_offload",
        "serve",
        "--host",
        str(info["host"]),
        "--port",
        str(info["port"]),
    ]
    creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    with stdout_path.open("ab") as stdout, stderr_path.open("ab") as stderr:
        process = subprocess.Popen(
            command,
            cwd=Path.cwd(),
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
            creationflags=creationflags,
        )

    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        current = read_service_info(require_healthy=True)
        if current and int(current.get("pid") or 0) == process.pid:
            replacement = CoordinatorFaultClient(current)
            if int(replacement.get("/api/health").get("pid") or 0) == process.pid:
                replayed = replacement.job(job_id)
                replayed_status = str(replayed.get("status") or "")
                if replayed_status not in {"queued", "dispatched", "running"}:
                    raise RuntimeError(
                        "Replacement coordinator did not replay the live job"
                    )
                cancelled = replacement.post(f"/api/jobs/{job_id}/cancel", {})
                if str(cancelled.get("status") or "") != "failed":
                    raise RuntimeError(
                        "Replacement coordinator did not persist canary cancellation"
                    )
                return {
                    "kind": "restart",
                    "old_process_stopped": True,
                    "replacement_healthy": True,
                    "job_replay_available": True,
                    "replayed_status": replayed_status,
                    "cancellation_recorded": True,
                }
        if process.poll() is not None:
            raise RuntimeError("Replacement coordinator exited during restart canary")
        time.sleep(0.25)
    raise TimeoutError("Replacement coordinator did not become healthy")


def run_fault(kind: str) -> dict[str, Any]:
    normalized = str(kind).strip().lower()
    if normalized not in FAULT_KINDS:
        raise ValueError("Unknown benchmark fault kind")
    job_id, scenario, stage = _benchmark_context(normalized)
    client = CoordinatorFaultClient()
    if normalized == "storage":
        if stage != "observe":
            raise RuntimeError("Storage canary supports only the observe stage")
        return inject_storage_unavailable(client, job_id)
    if normalized == "corruption":
        declared = {
            normalize_digest(item)
            for item in os.environ.get(
                "CLOUD_OFFLOAD_BENCHMARK_ASSET_DIGESTS", ""
            ).split(",")
            if item.strip()
        }
        if not declared:
            raise RuntimeError("Corruption canary received no declared asset digests")
        if stage == "prepare":
            return prepare_corruption(client, scenario, declared)
        if stage == "cleanup":
            return cleanup_corruption(client, scenario, declared)
        return observe_corruption(client, job_id, scenario, declared)
    if stage != "observe":
        raise RuntimeError("Restart canary supports only the observe stage")
    return restart_coordinator(client, job_id)

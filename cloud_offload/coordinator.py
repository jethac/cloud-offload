"""HTTP client used by remote Cloud Offload workers."""

import hashlib
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from cloud_offload.queue import Job, JobStatus


class CoordinatorQueue:
    """JobQueue-like adapter backed by the Cloud Offload coordinator API."""

    def __init__(self, base_url: str, token: str, provider: str, worker_id: str):
        self.base_url = base_url.rstrip("/")
        self.provider = provider
        self.worker_id = worker_id
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {token}"
        # A process-scoped producer id prevents a restarted worker process from
        # reusing sequence numbers under the stable Pod-level worker id.
        self._event_producer_id = f"worker:{worker_id}:{uuid.uuid4()}"
        self._event_sequence = 0
        self._event_sequence_lock = threading.Lock()

    def _post(self, path: str, payload: dict[str, Any] | None = None) -> Any:
        response = self.session.post(
            f"{self.base_url}{path}", json=payload or {}, timeout=120
        )
        response.raise_for_status()
        return response.json()

    def record_worker(
        self,
        worker_id: str,
        provider: str,
        status: str = "active",
        runtime_profile: str | None = None,
        capabilities: list[str] | None = None,
        idle: bool = False,
        detail: str | None = None,
    ) -> dict[str, Any]:
        """Report this worker's own state, claiming nothing.

        Takes the same arguments as ``JobQueue.record_worker`` so a runner can
        call it without knowing which side of the wire it is on. It is the only
        channel a runner has before it claims a job, which makes it the only
        place a failure to start can be attributed to anything.
        """
        return self._post(
            "/api/workers/status",
            {
                "worker_id": worker_id,
                "provider": provider,
                "status": status,
                "runtime_profile": runtime_profile,
                "models": capabilities or [],
                "idle": idle,
                "detail": detail,
            },
        )

    def worker_policy(self) -> dict[str, Any]:
        """Fetch the coordinator-owned worker lifetime policy."""
        response = self.session.get(f"{self.base_url}/api/workers/policy", timeout=30)
        response.raise_for_status()
        return response.json()

    def download_artifact(self, artifact_id: str, destination: str | Path) -> Path:
        """Stream a content-addressed partition artifact from the coordinator."""
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        response = self.session.get(
            f"{self.base_url}/api/workers/artifacts/{artifact_id}",
            timeout=600,
            stream=True,
        )
        response.raise_for_status()
        digest = hashlib.sha256()
        with destination.open("wb") as handle:
            for chunk in response.iter_content(1024 * 1024):
                if chunk:
                    digest.update(chunk)
                    handle.write(chunk)
        if digest.hexdigest() != artifact_id:
            destination.unlink(missing_ok=True)
            raise ValueError(f"Coordinator artifact digest mismatch: {artifact_id}")
        return destination

    def upload_artifact(self, source: str | Path) -> dict[str, Any]:
        """Upload a content-addressed partition artifact to the coordinator."""
        source = Path(source)
        hasher = hashlib.sha256()
        with source.open("rb") as digest_handle:
            for chunk in iter(lambda: digest_handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        digest = hasher.hexdigest()
        with source.open("rb") as handle:
            response = self.session.post(
                f"{self.base_url}/api/workers/artifacts",
                data={"sha256": digest},
                files={
                    "file": (source.name, handle, "application/vnd.comfy.partition+zip")
                },
                timeout=600,
            )
        response.raise_for_status()
        return response.json()

    def claim_jobs(
        self,
        worker_id: str,
        limit: int = 5,
        token: str | None = None,
        provider: str | None = None,
        models: list[str] | None = None,
        runtime_profile: str | None = None,
        gpu_vram_gb: float | None = None,
        gpu_name: str | None = None,
        cache_volume_id: str | None = None,
    ) -> list[Job]:
        data = self._post(
            "/api/workers/claim",
            {
                "worker_id": worker_id,
                "provider": provider or self.provider,
                "limit": limit,
                "models": models or [],
                "runtime_profile": runtime_profile,
                "gpu_vram_gb": gpu_vram_gb,
                "gpu_name": gpu_name,
                "cache_volume_id": cache_volume_id or "",
            },
        )
        return [Job.from_dict(item) for item in data]

    def get(self, job_id: str) -> Job | None:
        """Read the current coordinator state for cancellation checks."""
        response = self.session.get(
            f"{self.base_url}/api/workers/jobs/{job_id}", timeout=30
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return Job.from_dict(response.json())

    def update_status(self, job_id: str, status: JobStatus, **kwargs) -> Job | None:
        if status == JobStatus.RUNNING:
            data = self._post(f"/api/workers/jobs/{job_id}/running")
        elif status == JobStatus.COMPLETED:
            data = self._post(
                f"/api/workers/jobs/{job_id}/complete",
                {"result": kwargs.get("result", {})},
            )
        else:
            raise ValueError(f"Unsupported coordinator status update: {status.value}")
        return Job.from_dict(data)

    def set_progress(self, job_id: str, progress: int) -> Job:
        data = self._post(
            f"/api/workers/jobs/{job_id}/progress", {"progress": progress}
        )
        return Job.from_dict(data)

    def append_event(self, job_id: str, event: dict[str, Any]) -> dict[str, Any]:
        """Publish an incremental remote execution event idempotently."""
        with self._event_sequence_lock:
            self._event_sequence += 1
            producer_sequence = self._event_sequence
        return self._post(
            f"/api/workers/jobs/{job_id}/events",
            {
                "event": event,
                "producer_id": self._event_producer_id,
                "producer_sequence": producer_sequence,
                "occurred_at": datetime.utcnow().isoformat(),
            },
        )

    def sign_prepared_manifest(
        self, proposal: dict[str, Any], *, job_id: str, volume_id: str
    ) -> dict[str, Any]:
        """Ask the coordinator authority to validate policy and sign a proposal."""
        return self._post(
            "/api/workers/cache/manifests/sign",
            {
                "manifest": proposal,
                "job_id": job_id,
                "volume_id": volume_id,
                "worker_id": self.worker_id,
            },
        )

    def verify_prepared_manifest(self, manifest: dict[str, Any]) -> dict[str, Any]:
        """Verify through the coordinator; the signing key stays control-plane-only."""
        return self._post("/api/workers/cache/manifests/verify", {"manifest": manifest})

    def fetch_prepared_manifest(
        self, manifest_id: str, *, job_id: str, volume_id: str
    ) -> dict[str, Any]:
        """Fetch only the exact manifest assigned to this worker's active job."""

        return self._post(
            "/api/workers/cache/manifests/fetch",
            {
                "manifest_id": manifest_id,
                "job_id": job_id,
                "volume_id": volume_id,
                "worker_id": self.worker_id,
            },
        )

    def record_cache_observation(
        self, job_id: str, observation: dict[str, Any]
    ) -> dict[str, Any]:
        return self._post(
            "/api/workers/cache/observations",
            {
                "job_id": job_id,
                "worker_id": self.worker_id,
                "observation": observation,
            },
        )

    def announce_prepared_manifest(
        self,
        manifest: dict[str, Any],
        *,
        job_id: str,
        volume_id: str,
        generation: str,
    ) -> dict[str, Any]:
        """Project a just-published volume manifest into coordinator scheduling state."""
        return self._post(
            "/api/workers/cache/manifests/announce",
            {
                "manifest": manifest,
                "job_id": job_id,
                "volume_id": volume_id,
                "generation": generation,
                "worker_id": self.worker_id,
            },
        )

    def fail_job(self, job_id: str, error: str) -> Job:
        data = self._post(f"/api/workers/jobs/{job_id}/fail", {"error": error})
        return Job.from_dict(data)

"""
Dispatcher - watches queue and spins up cloud workers when needed.

Runs locally, monitors the job queue, and launches cloud instances
when enough jobs are waiting.
"""

import logging
import os
import secrets
import time
from datetime import datetime, timedelta
from pathlib import Path

from cloud_offload.config import CloudConfig
from cloud_offload.providers import create_connector
from cloud_offload.providers.base import CloudConnector, CloudProvider, Instance
from cloud_offload.queue import JobQueue, JobStatus
from cloud_offload.profiles import (
    configured_worker_profiles,
    worker_profile_gpu_type,
    worker_profile_min_gpu_ram,
)

logger = logging.getLogger(__name__)


def _load_or_create_worker_token(config: CloudConfig) -> str:
    """Return a stable coordinator credential for workers across restarts.

    A random token held only in dispatcher memory disconnects every warm worker
    whenever the local dispatcher restarts.  Keep the generated credential next
    to the queue database instead.  An explicitly configured token still wins.
    """
    if config.worker_token:
        return config.worker_token

    token_path = Path(config.queue_db_path).with_name("worker-token")
    token_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        token = token_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        token = secrets.token_urlsafe(32)
        try:
            with token_path.open("x", encoding="utf-8") as handle:
                handle.write(token + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            try:
                token_path.chmod(0o600)
            except OSError:
                logger.warning("Could not restrict worker token file permissions")
        except FileExistsError:
            token = token_path.read_text(encoding="utf-8").strip()

    if len(token) < 32:
        raise RuntimeError(
            f"Persistent worker token is missing or invalid: {token_path}. "
            "Remove it only after terminating all cloud workers."
        )
    return token


class Dispatcher:
    """
    Monitors job queue and manages cloud workers.

    Responsibilities:
    - Watch for queued jobs
    - Spin up workers when queue depth >= threshold
    - Track active workers
    - Clean up idle workers
    """

    def __init__(
        self,
        config: CloudConfig,
        queue: JobQueue | None = None,
        provider: CloudProvider | None = None,
        *,
        connector: CloudConnector | None = None,
    ):
        self.config = config
        self.queue = queue or JobQueue(config.queue_db_path)

        if provider is not None and connector is not None:
            raise ValueError("Pass connector or legacy provider, not both")
        supplied = connector or provider
        if supplied:
            self.connectors = {config.provider: supplied}
        else:
            self.connectors = {
                name: create_connector(name, config)
                for name in config.provider_order
                if config.api_key_for(name)
            }
            if not self.connectors:
                self.connectors = {config.provider: create_connector(config.provider, config)}
        self.connector = self.connectors.get(config.provider) or next(
            iter(self.connectors.values())
        )
        # Compatibility for integrations that accessed ``dispatcher.provider``.
        self.provider = self.connector

        # Track active workers
        self.active_instances: dict[str, Instance] = {}
        self.instance_providers: dict[str, str] = {}
        self.instance_profiles: dict[str, str] = {}
        self.last_activity: dict[str, datetime] = {}
        self.launch_failures: dict[tuple[str, str], int] = {}
        self.next_launch_at: dict[tuple[str, str], float] = {}
        self.worker_token = _load_or_create_worker_token(self.config)
        self.queue.set_worker_token(self.worker_token)

    def run(self, once: bool = False):
        """
        Main dispatcher loop.

        Args:
            once: If True, run one iteration and exit (for testing)
        """
        logger.info(f"Dispatcher starting (min_queue_depth={self.config.min_queue_depth})")

        while True:
            try:
                self._tick()
            except Exception as e:
                logger.error(f"Dispatcher error: {e}")

            if once:
                break

            time.sleep(self.config.poll_interval_seconds)

    def _tick(self):
        """Single dispatcher iteration."""
        # Lifetime policy is user-controlled at runtime through the coordinator.
        # Refresh only these safe fields so toggling keep-warm does not require a
        # dispatcher restart or replace provider credentials/connectors.
        try:
            persisted = CloudConfig.load(resolve_secrets=False)
            self.config.keep_warm = persisted.keep_warm
            self.config.keep_warm_warning_seconds = persisted.keep_warm_warning_seconds
            self.config.idle_shutdown_seconds = persisted.idle_shutdown_seconds
            self.config.runpod_registry_auth_id = persisted.runpod_registry_auth_id
            runpod = self.connectors.get("runpod")
            if runpod is not None and hasattr(runpod, "registry_auth_id"):
                runpod.registry_auth_id = persisted.runpod_registry_auth_id.strip()
        except Exception as exc:
            logger.warning("Could not refresh cloud lifetime policy: %s", exc)

        # Count queued jobs
        queued_count = self.queue.count_by_status(JobStatus.QUEUED)
        logger.debug(f"Queue: {queued_count} queued, {len(self.active_instances)} workers active")

        profiles = configured_worker_profiles(self.config)
        for provider_name in self.connectors:
            queued_jobs = self.queue.list_by_status(
                JobStatus.QUEUED, provider=provider_name
            )
            queued_profiles = {
                str(job.params.get("runtime_profile"))
                for job in queued_jobs
                if job.params.get("runtime_profile")
            }
            for profile_name in queued_profiles:
                launch_key = (provider_name, profile_name)
                profile_queued = sum(
                    job.params.get("runtime_profile") == profile_name
                    for job in queued_jobs
                )
                profile_running = any(
                    self.instance_providers.get(instance_id) == provider_name
                    and self.instance_profiles.get(instance_id) == profile_name
                    for instance_id in self.active_instances
                )
                profile_running = profile_running or any(
                    worker.get("provider") == provider_name
                    and worker.get("runtime_profile") == profile_name
                    for worker in self.queue.list_active_workers()
                )
                if profile_name not in profiles:
                    logger.error(
                        "Queued jobs reference unknown runtime profile %s", profile_name
                    )
                    continue
                if profile_queued < self.config.min_queue_depth or profile_running:
                    continue
                if time.monotonic() < self.next_launch_at.get(launch_key, 0):
                    continue
                logger.info(
                    f"{provider_name}/{profile_name} queue depth {profile_queued} >= "
                    f"{self.config.min_queue_depth}, launching worker"
                )
                self._launch_worker(
                    provider_name,
                    profile_name,
                    [
                        job
                        for job in queued_jobs
                        if job.params.get("runtime_profile") == profile_name
                    ],
                )

        # Update worker tracking
        self._update_workers()

        # Check for idle workers to terminate
        self._check_idle_workers()

    def _launch_worker(
        self,
        provider_name: str | None = None,
        profile_name: str | None = None,
        queued_jobs: list | None = None,
    ) -> Instance | None:
        """Launch a new cloud worker."""
        if not self.config.coordinator_url:
            logger.error("Cloud launch requires CLOUD_OFFLOAD_COORDINATOR_URL")
            return None
        provider_name = provider_name or self.config.provider
        connector = self.connectors[provider_name]
        profiles = configured_worker_profiles(self.config)
        if not profile_name or profile_name not in profiles:
            logger.error("Cloud launch requires a configured runtime profile")
            return None
        profile = profiles[profile_name]
        if provider_name not in profile["providers"]:
            logger.error(
                "Runtime profile %s does not support provider %s",
                profile_name,
                provider_name,
            )
            return None
        minimum_vram = max(
            [worker_profile_min_gpu_ram(profile)]
            + [int(job.params.get("min_gpu_ram_gb") or 0) for job in (queued_jobs or [])]
        )
        requested_gpu_types = [
            str(job.params.get("gpu_type"))
            for job in (queued_jobs or [])
            if str(job.params.get("gpu_type") or "any").lower() != "any"
        ]
        gpu_type = (
            requested_gpu_types[0]
            if requested_gpu_types
            else worker_profile_gpu_type(profile, self.config.gpu_type)
        )
        # Find cheapest available GPU
        offer = connector.find_cheapest(
            gpu_type=gpu_type,
            min_gpu_ram=minimum_vram,
            max_hourly_rate=self.config.max_hourly_rate,
        )

        if not offer:
            logger.warning(
                f"No available GPUs matching criteria "
                f"(type={self.config.gpu_type}, max_rate=${self.config.max_hourly_rate}/hr)"
            )
            self._record_launch_failure(
                provider_name,
                profile_name,
                queued_jobs,
                "No available GPU matched the partition constraints",
            )
            return None

        logger.info(
            f"Launching {offer['gpu_type']} @ ${offer['hourly_rate']:.2f}/hr (offer {offer['id']})"
        )
        self._publish_launch_event(
            queued_jobs,
            {
                "type": "provisioning_started",
                "provider": provider_name,
                "runtime_profile": profile_name,
                "gpu_type": offer["gpu_type"],
                "hourly_rate": offer["hourly_rate"],
                "overall_progress": 1,
            },
        )

        # Build startup script
        startup_script = self._build_startup_script(profile)
        wheelhouse_url = profile["wheelhouse_url"] or self.config.worker_wheelhouse_url
        wheelhouse_sha256 = (
            profile["wheelhouse_sha256"] or self.config.worker_wheelhouse_sha256
        )
        # Environment variables for worker.
        # Older immutable worker images do not know the live keep-warm policy.
        # Give them an effectively indefinite timeout as a compatibility path;
        # current workers additionally refresh policy from the coordinator.
        worker_idle_seconds = (
            10 * 365 * 24 * 60 * 60
            if self.config.keep_warm
            else self.config.idle_shutdown_seconds
        )
        env_vars = {
            "CLOUD_OFFLOAD_WORKER_MODE": "true",
            "CLOUD_OFFLOAD_WORKER_TOKEN": self.worker_token,
            "CLOUD_OFFLOAD_COORDINATOR_URL": self.config.coordinator_url,
            "CLOUD_OFFLOAD_PROVIDER": provider_name,
            "CLOUD_OFFLOAD_IDLE_SHUTDOWN": str(worker_idle_seconds),
            "CLOUD_OFFLOAD_KEEP_WARM": str(self.config.keep_warm).lower(),
            "CLOUD_OFFLOAD_KEEP_WARM_WARNING": str(self.config.keep_warm_warning_seconds),
            "CLOUD_OFFLOAD_WORKER_WHEELHOUSE_URL": wheelhouse_url,
            "CLOUD_OFFLOAD_WORKER_WHEELHOUSE_SHA256": wheelhouse_sha256,
            "CLOUD_OFFLOAD_WORKER_PROFILE": profile_name,
            "CLOUD_OFFLOAD_WORKER_MODELS": ",".join(profile["models"]),
        }

        try:
            instance = connector.launch(
                offer_id=offer["id"],
                docker_image=profile["image"],
                env_vars=env_vars,
                startup_script=startup_script,
            )

            self.active_instances[instance.id] = instance
            self.instance_providers[instance.id] = provider_name
            self.instance_profiles[instance.id] = profile_name
            self.last_activity[instance.id] = datetime.utcnow()

            logger.info(f"Launched worker {instance.id}")
            self.launch_failures.pop((provider_name, profile_name), None)
            self.next_launch_at.pop((provider_name, profile_name), None)
            self._publish_launch_event(
                queued_jobs,
                {
                    "type": "runner_starting",
                    "provider": provider_name,
                    "runtime_profile": profile_name,
                    "worker_instance_id": instance.id,
                    "gpu_type": instance.gpu_type,
                    "hourly_rate": instance.hourly_rate,
                    "overall_progress": 2,
                },
            )
            return instance

        except Exception as e:
            logger.error(f"Failed to launch worker: {e}")
            self._record_launch_failure(
                provider_name, profile_name, queued_jobs, str(e)
            )
            return None

    def _publish_launch_event(self, jobs: list | None, event: dict) -> None:
        for job in jobs or []:
            try:
                self.queue.append_event(job.id, event)
            except (KeyError, ValueError):
                logger.debug("Could not append provisioning event for %s", job.id)

    def _record_launch_failure(
        self,
        provider_name: str,
        profile_name: str,
        jobs: list | None,
        error: str,
    ) -> None:
        key = (provider_name, profile_name)
        failures = self.launch_failures.get(key, 0) + 1
        self.launch_failures[key] = failures
        retry_seconds = min(300, 10 * (2 ** min(failures - 1, 5)))
        self.next_launch_at[key] = time.monotonic() + retry_seconds
        self._publish_launch_event(
            jobs,
            {
                "type": "provisioning_failed",
                "provider": provider_name,
                "runtime_profile": profile_name,
                "error": error,
                "retry_seconds": retry_seconds,
                "attempt": failures,
                "overall_progress": 0,
            },
        )

    def _build_startup_script(self, profile: dict | None = None) -> str | None:
        """Build the startup script that runs on the cloud instance."""
        wheelhouse_url = (profile or {}).get(
            "wheelhouse_url"
        ) or self.config.worker_wheelhouse_url
        wheelhouse_sha256 = (profile or {}).get(
            "wheelhouse_sha256"
        ) or self.config.worker_wheelhouse_sha256
        if profile and not wheelhouse_url and not wheelhouse_sha256:
            # Immutable runtime-profile images define their own worker ENTRYPOINT.
            # RunPod's dockerArgs field replaces CMD but is appended to ENTRYPOINT,
            # so passing a shell script here would produce a command such as
            # ``cloud-offload worker bash -lc ...`` and make the container exit
            # immediately.
            return None
        if not wheelhouse_url or not wheelhouse_sha256:
            raise RuntimeError(
                "Cloud workers require CLOUD_OFFLOAD_WORKER_WHEELHOUSE_URL and "
                "CLOUD_OFFLOAD_WORKER_WHEELHOUSE_SHA256; live registry installs are disabled"
            )
        return """#!/bin/bash
set -e

mkdir -p /opt/cloud-offload-wheelhouse
curl -fsSL "$CLOUD_OFFLOAD_WORKER_WHEELHOUSE_URL" -o /tmp/cloud-offload-wheelhouse.tar.gz
echo "$CLOUD_OFFLOAD_WORKER_WHEELHOUSE_SHA256  /tmp/cloud-offload-wheelhouse.tar.gz" | sha256sum -c -
tar -xzf /tmp/cloud-offload-wheelhouse.tar.gz -C /opt/cloud-offload-wheelhouse

python -m pip install --no-index --find-links /opt/cloud-offload-wheelhouse "cloud-offload[cloud]"

# Run worker
cloud-offload worker --poll 10
"""

    def _update_workers(self):
        """Update status of active workers."""
        for instance_id in list(self.active_instances.keys()):
            provider_name = self.instance_providers[instance_id]
            instance = self.connectors[provider_name].get_instance(instance_id)

            if not instance or instance.status in ("stopped", "terminated"):
                logger.info(f"Worker {instance_id} no longer active")
                del self.active_instances[instance_id]
                self.instance_providers.pop(instance_id, None)
                self.instance_profiles.pop(instance_id, None)
                self.last_activity.pop(instance_id, None)
            else:
                self.active_instances[instance_id] = instance

    def _check_idle_workers(self):
        """Terminate workers that have been idle too long."""
        if self.config.keep_warm:
            return
        # Check for idle workers
        idle_threshold = timedelta(seconds=self.config.idle_shutdown_seconds)
        now = datetime.utcnow()

        for instance_id, last_active in list(self.last_activity.items()):
            provider_name = self.instance_providers[instance_id]
            profile_name = self.instance_profiles[instance_id]
            running_jobs = sum(
                job.params.get("runtime_profile") == profile_name
                for job in self.queue.list_by_status(
                    JobStatus.RUNNING,
                    JobStatus.DISPATCHED,
                    provider=provider_name,
                )
            )
            queued_jobs = sum(
                job.params.get("runtime_profile") == profile_name
                for job in self.queue.list_by_status(
                    JobStatus.QUEUED, provider=provider_name
                )
            )
            if running_jobs > 0 or queued_jobs > 0:
                self.last_activity[instance_id] = now
                continue
            if now - last_active > idle_threshold:
                logger.info(
                    f"Worker {instance_id} idle for "
                    f"{self.config.idle_shutdown_seconds}s, terminating"
                )
                self._terminate_worker(instance_id)

    def _terminate_worker(self, instance_id: str):
        """Terminate a worker instance."""
        provider_name = self.instance_providers[instance_id]
        if self.connectors[provider_name].terminate(instance_id):
            logger.info(f"Terminated worker {instance_id}")
        else:
            logger.warning(f"Failed to terminate worker {instance_id}")

        self.active_instances.pop(instance_id, None)
        self.instance_providers.pop(instance_id, None)
        self.instance_profiles.pop(instance_id, None)
        self.last_activity.pop(instance_id, None)

    def shutdown(self):
        """Terminate all workers and shut down."""
        logger.info("Dispatcher shutting down, terminating workers...")
        for instance_id in list(self.active_instances.keys()):
            self._terminate_worker(instance_id)

    def status(self) -> dict:
        """Get dispatcher status."""
        return {
            "queued_jobs": self.queue.count_by_status(JobStatus.QUEUED),
            "running_jobs": self.queue.count_by_status(JobStatus.RUNNING),
            "dead_letter_jobs": self.queue.count_by_status(JobStatus.DEAD_LETTER),
            "active_workers": len(self.active_instances),
            "workers": [
                {
                    "id": inst.id,
                    "gpu": inst.gpu_type,
                    "hourly_rate": inst.hourly_rate,
                    "status": inst.status,
                    "provider": self.instance_providers.get(inst.id),
                    "runtime_profile": self.instance_profiles.get(inst.id),
                }
                for inst in self.active_instances.values()
            ],
            "config": {
                "min_queue_depth": self.config.min_queue_depth,
                "provider": self.config.provider,
                "max_hourly_rate": self.config.max_hourly_rate,
            },
        }

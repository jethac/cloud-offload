"""
Dispatcher - watches queue and spins up cloud workers when needed.

Runs locally, monitors the job queue, and launches cloud instances
when enough jobs are waiting.
"""

import json
import logging
import os
import secrets
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from cloud_offload.config import CloudConfig, estimate_runpod_storage_monthly
from cloud_offload.cache_registry import CacheRegistry
from cloud_offload.cache_scheduler import (
    PlacementCandidate,
    PlacementDecision,
    choose_placement,
    resolve_prepared_requirements,
    scheduler_runtime,
)
from cloud_offload.credentials import huggingface_token
from cloud_offload.providers import create_connector
from cloud_offload.providers.base import CloudConnector, CloudProvider, Instance
from cloud_offload.providers.base import PlacementConstraints, StorageAttachment
from cloud_offload.queue import JobQueue, JobStatus
from cloud_offload.profiles import (
    configured_worker_profiles,
    profile_providing,
    worker_profile_gpu_type,
    worker_profile_min_gpu_ram,
)

logger = logging.getLogger(__name__)

# How long a specific offer sits out after its host refuses a launch.
OFFER_COOLDOWN_SECONDS = 600

# A provider can report a pod as running before it has created a container
# runtime. If the entrypoint never runs, no worker can report the failure home.
RUNNER_REGISTRATION_TIMEOUT_SECONDS = 3600


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
        self.cache_registry = CacheRegistry(config.queue_db_path)

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
                self.connectors = {
                    config.provider: create_connector(config.provider, config)
                }
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
        self.launched_at: dict[str, datetime] = {}
        self.runner_feedback_at: dict[str, datetime] = {}
        self.event_producer_id = f"dispatcher:{uuid.uuid4()}"
        self.event_producer_sequence = 0
        self.launch_failures: dict[tuple[str, str], int] = {}
        self.next_launch_at: dict[tuple[str, str], float] = {}
        # (provider, offer_id) -> monotonic expiry. A host that refuses a launch
        # keeps being the cheapest offer, so without this the dispatcher retries
        # the same dead machine forever.
        self.offer_cooldowns: dict[tuple[str, str], float] = {}
        self.worker_token = _load_or_create_worker_token(self.config)
        self.queue.set_worker_token(self.worker_token)
        self._tunnel = None  # opened lazily when ingress == "cloudflared"

    def _resolve_coordinator_url(self) -> str | None:
        """The URL a worker uses to reach the coordinator.

        An explicit ``coordinator_url`` always wins. Otherwise, when ingress is
        ``cloudflared``, open (once) an ephemeral tunnel to the local coordinator
        and return its public URL. With ingress ``none`` and no URL, there is no
        way for a worker to call home, so a launch is refused.
        """
        if self.config.coordinator_url:
            return self.config.coordinator_url
        if self.config.ingress != "cloudflared":
            return None

        from cloud_offload.ingress import CloudflaredTunnel, IngressError
        from cloud_offload.service_config import read_service_info

        if self._tunnel is not None and self._tunnel.running:
            return self._tunnel.url

        info = read_service_info()
        if not info or not info.get("port"):
            logger.error("Cannot open ingress: coordinator discovery file missing")
            return None
        try:
            self._tunnel = CloudflaredTunnel()
            return self._tunnel.open(int(info["port"]))
        except IngressError as exc:
            logger.error("Ingress failed: %s", exc)
            self._tunnel = None
            return None

    def run(self, once: bool = False):
        """
        Main dispatcher loop.

        Args:
            once: If True, run one iteration and exit (for testing)
        """
        logger.info(
            f"Dispatcher starting (min_queue_depth={self.config.min_queue_depth})"
        )

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
        # Runtime policy and immutable worker-profile pins are user-controlled
        # through the coordinator. Refresh these non-secret fields each tick so
        # a newly submitted job cannot launch with a stale image or GPU policy.
        # Provider credentials/connectors remain process-owned.
        try:
            config_path = getattr(self.config, "_source_path", None)
            persisted = CloudConfig.load(config_path, resolve_secrets=False)
            self.config.keep_warm = persisted.keep_warm
            self.config.keep_warm_warning_seconds = persisted.keep_warm_warning_seconds
            self.config.idle_shutdown_seconds = persisted.idle_shutdown_seconds
            self.config.min_queue_depth = persisted.min_queue_depth
            self.config.gpu_type = persisted.gpu_type
            self.config.max_hourly_rate = persisted.max_hourly_rate
            self.config.worker_profiles = persisted.worker_profiles
            self.config.runpod_registry_auth_id = persisted.runpod_registry_auth_id
            # Prepared storage can create paid durable resources. Only refresh
            # it for file-backed services; a programmatically constructed
            # config remains authoritative for this opt-in policy.
            if config_path is not None:
                self.config.prepared_storage = persisted.prepared_storage
            runpod = self.connectors.get("runpod")
            if runpod is not None and hasattr(runpod, "registry_auth_id"):
                runpod.registry_auth_id = persisted.runpod_registry_auth_id.strip()
        except Exception as exc:
            logger.warning("Could not refresh cloud runtime policy: %s", exc)

        # Count queued jobs
        queued_count = self.queue.count_by_status(JobStatus.QUEUED)
        logger.debug(
            f"Queue: {queued_count} queued, {len(self.active_instances)} workers active"
        )

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
            for requested_profile in queued_profiles:
                # Jobs carry whatever the client stamped, which is usually a
                # capability like comfyui-partition-v1 rather than an operator's
                # profile name. Resolve it the way routing does, or a correctly
                # configured worker never launches and the job waits forever.
                resolved = profiles.get(requested_profile) or profile_providing(
                    profiles, requested_profile
                )
                if resolved is None:
                    logger.error(
                        "Queued jobs reference unknown runtime profile %s "
                        "(configured profiles: %s)",
                        requested_profile,
                        ", ".join(sorted(profiles)) or "none",
                    )
                    continue
                profile_name = resolved["name"]
                launch_key = (provider_name, profile_name)
                profile_queued = sum(
                    job.params.get("runtime_profile") == requested_profile
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
                        if job.params.get("runtime_profile") == requested_profile
                    ],
                )

        # Update worker tracking
        self._update_workers()

        # A rented pod whose entrypoint never runs cannot report its own failure.
        self._check_unregistered_workers()

        # Check for idle workers to terminate
        self._check_idle_workers()

    def _launch_worker(
        self,
        provider_name: str | None = None,
        profile_name: str | None = None,
        queued_jobs: list | None = None,
    ) -> Instance | None:
        """Launch a new cloud worker."""
        coordinator_url = self._resolve_coordinator_url()
        if not coordinator_url:
            logger.error(
                "Cloud launch needs a reachable coordinator: set coordinator_url "
                "or ingress=cloudflared"
            )
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
            + [
                int(job.params.get("min_gpu_ram_gb") or 0)
                for job in (queued_jobs or [])
            ]
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
        # Find an offer, optionally treating regional prepared state as a
        # schedulable resource. Disabled mode preserves the original call path.
        cooling = self._offers_on_cooldown(provider_name)
        requirements = resolve_prepared_requirements(
            profile_name, profile, queued_jobs or []
        )
        placement_decision = None
        if self.config.prepared_storage.get("enabled"):
            placement_decision = self._choose_cache_placement(
                connector=connector,
                provider_name=provider_name,
                gpu_type=gpu_type,
                minimum_vram=minimum_vram,
                cooling=cooling,
                requirements=requirements,
            )
            self._publish_launch_event(
                queued_jobs,
                {
                    "type": "cache_placement_considered",
                    **placement_decision.explanation(),
                },
            )
            if (
                placement_decision.action != "launch"
                or not placement_decision.candidate
            ):
                detail = placement_decision.reason
                self._record_launch_failure(
                    provider_name, profile_name, queued_jobs, detail
                )
                return None
            offer = placement_decision.candidate.offer
            selected_type = (
                "cache_cold_fallback"
                if placement_decision.fallback
                else "cache_placement_selected"
            )
            self._publish_launch_event(
                queued_jobs,
                {"type": selected_type, **placement_decision.explanation()},
            )
        else:
            offer = connector.find_cheapest(
                gpu_type=gpu_type,
                min_gpu_ram=minimum_vram,
                max_hourly_rate=self.config.max_hourly_rate,
                exclude=cooling,
            )

        if not offer:
            detail = "No available GPU matched the partition constraints"
            if cooling:
                detail += f" ({len(cooling)} offer(s) on launch-failure cooldown)"
            logger.warning(
                f"No available GPUs matching criteria "
                f"(type={self.config.gpu_type}, max_rate=${self.config.max_hourly_rate}/hr"
                + (f", {len(cooling)} on cooldown)" if cooling else ")")
            )
            self._record_launch_failure(
                provider_name, profile_name, queued_jobs, detail
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
            "CLOUD_OFFLOAD_COORDINATOR_URL": coordinator_url,
            "CLOUD_OFFLOAD_PROVIDER": provider_name,
            "CLOUD_OFFLOAD_IDLE_SHUTDOWN": str(worker_idle_seconds),
            "CLOUD_OFFLOAD_KEEP_WARM": str(self.config.keep_warm).lower(),
            "CLOUD_OFFLOAD_KEEP_WARM_WARNING": str(
                self.config.keep_warm_warning_seconds
            ),
            "CLOUD_OFFLOAD_WORKER_WHEELHOUSE_URL": wheelhouse_url,
            "CLOUD_OFFLOAD_WORKER_WHEELHOUSE_SHA256": wheelhouse_sha256,
            "CLOUD_OFFLOAD_WORKER_PROFILE": profile_name,
            "CLOUD_OFFLOAD_WORKER_MODELS": ",".join(profile["models"]),
        }
        if profile.get("weights"):
            # The worker stages these before its first job. Pass the configured
            # Hub token for public weights too: authenticated downloads avoid
            # the stricter anonymous rate limits and are materially more
            # reliable for large model profiles.
            env_vars["CLOUD_OFFLOAD_WEIGHTS"] = json.dumps(
                profile["weights"], separators=(",", ":")
            )
            hub_token = huggingface_token()
            if hub_token:
                env_vars["HF_TOKEN"] = hub_token
            elif any(entry.get("gated") for entry in profile["weights"]):
                logger.warning(
                    "Profile %s has gated weights but no Hugging Face token "
                    "is configured; the download will run anonymously and "
                    "likely fail",
                    profile_name,
                )
        if profile.get("custom_nodes"):
            # Installed by the worker before its first job, alongside weights.
            # These carry no credentials by construction: a registry release and
            # a public clone URL are both fetched anonymously.
            env_vars["CLOUD_OFFLOAD_CUSTOM_NODES"] = json.dumps(
                profile["custom_nodes"], separators=(",", ":")
            )

        placement = placement_decision.placement() if placement_decision else None
        if placement and placement_decision and placement_decision.candidate:
            volume = placement_decision.candidate.volume
            selected_manifest_id = (
                placement_decision.candidate.manifest_ids[0]
                if placement_decision.candidate.manifest_ids
                else ""
            )
            for queued_job in queued_jobs or []:
                queued_job.params["prepared_requirement"] = requirements
                queued_job.params["cache_volume_id"] = volume.id
                queued_job.params["cache_provider_volume_id"] = (
                    volume.provider_volume_id
                )
                queued_job.params["cache_datacenter_id"] = volume.datacenter_id
                if selected_manifest_id:
                    queued_job.params["cache_manifest_id"] = selected_manifest_id
                else:
                    queued_job.params.pop("cache_manifest_id", None)
                self.queue.update(queued_job)
            env_vars.update(
                {
                    "CLOUD_OFFLOAD_CACHE_ROOT": "/workspace/cloud-offload",
                    "CLOUD_OFFLOAD_CACHE_VOLUME_ID": volume.id,
                    "CLOUD_OFFLOAD_CACHE_EXPECTED_PROVIDER_VOLUME_ID": volume.provider_volume_id,
                    "CLOUD_OFFLOAD_CACHE_MANIFEST": (
                        selected_manifest_id
                        if selected_manifest_id
                        else requirements["profile_fingerprint"]
                    ),
                    "CLOUD_OFFLOAD_CACHE_MODE": "restore-and-populate",
                    "CLOUD_OFFLOAD_CACHE_POLICY": json.dumps(
                        self.config.prepared_storage, separators=(",", ":")
                    ),
                    "CLOUD_OFFLOAD_CACHE_REQUIREMENTS": json.dumps(
                        requirements, separators=(",", ":")
                    ),
                }
            )

        disk_gb = self._planned_disk_gb(profile_name, queued_jobs)

        try:
            launch_arguments = dict(
                offer_id=offer["id"],
                docker_image=profile["image"],
                env_vars=env_vars,
                startup_script=startup_script,
                disk_gb=disk_gb,
            )
            if placement is not None:
                launch_arguments["placement"] = placement
            self._publish_launch_event(
                queued_jobs,
                {
                    "schema": "cloud-offload.phase-event.v1",
                    "type": "provider_request_started",
                    "phase": "provider_request",
                    "monotonic_ms": round(time.monotonic() * 1000, 3),
                    "provider": provider_name,
                    "offer_id": offer["id"],
                    "placement": "cached" if placement is not None else "cold",
                },
            )
            instance = connector.launch(**launch_arguments)
            self._publish_launch_event(
                queued_jobs,
                {
                    "schema": "cloud-offload.phase-event.v1",
                    "type": "provider_request_completed",
                    "phase": "provider_request",
                    "monotonic_ms": round(time.monotonic() * 1000, 3),
                    "provider": provider_name,
                    "offer_id": offer["id"],
                    "worker_instance_id": instance.id,
                    "placement": "cached" if placement is not None else "cold",
                },
            )
            return self._remember_launched_instance(
                instance, provider_name, profile_name, queued_jobs
            )

        except Exception as e:
            self._publish_launch_event(
                queued_jobs,
                {
                    "schema": "cloud-offload.phase-event.v1",
                    "type": "provider_request_failed",
                    "phase": "provider_request",
                    "monotonic_ms": round(time.monotonic() * 1000, 3),
                    "provider": provider_name,
                    "offer_id": offer["id"],
                    "failure": str(e),
                    "placement": "cached" if placement is not None else "cold",
                },
            )
            logger.error(f"Failed to launch worker: {e}")
            if (
                placement is not None
                and placement_decision is not None
                and self.config.prepared_storage.get("policy") == "smart"
                and self.config.prepared_storage.get("cold_fallback") == "allow"
            ):
                self._publish_launch_event(
                    queued_jobs,
                    {
                        "type": "cache_cold_fallback",
                        "reason": "cached_placement_launch_failed",
                        "failure": str(e),
                        **placement_decision.explanation(),
                    },
                )
                cold_offer = connector.find_cheapest(
                    gpu_type=gpu_type,
                    min_gpu_ram=minimum_vram,
                    max_hourly_rate=self.config.max_hourly_rate,
                    exclude=cooling,
                )
                if cold_offer:
                    cold_env = {
                        key: value
                        for key, value in env_vars.items()
                        if not key.startswith("CLOUD_OFFLOAD_CACHE_")
                    }
                    for queued_job in queued_jobs or []:
                        for key in (
                            "prepared_requirement",
                            "cache_volume_id",
                            "cache_provider_volume_id",
                            "cache_datacenter_id",
                            "cache_manifest_id",
                        ):
                            queued_job.params.pop(key, None)
                        self.queue.update(queued_job)
                    try:
                        self._publish_launch_event(
                            queued_jobs,
                            {
                                "schema": "cloud-offload.phase-event.v1",
                                "type": "provider_request_started",
                                "phase": "provider_request",
                                "monotonic_ms": round(time.monotonic() * 1000, 3),
                                "provider": provider_name,
                                "offer_id": cold_offer["id"],
                                "placement": "cold_fallback",
                            },
                        )
                        instance = connector.launch(
                            offer_id=cold_offer["id"],
                            docker_image=profile["image"],
                            env_vars=cold_env,
                            startup_script=startup_script,
                            disk_gb=disk_gb,
                        )
                        self._publish_launch_event(
                            queued_jobs,
                            {
                                "schema": "cloud-offload.phase-event.v1",
                                "type": "provider_request_completed",
                                "phase": "provider_request",
                                "monotonic_ms": round(time.monotonic() * 1000, 3),
                                "provider": provider_name,
                                "offer_id": cold_offer["id"],
                                "worker_instance_id": instance.id,
                                "placement": "cold_fallback",
                            },
                        )
                        return self._remember_launched_instance(
                            instance, provider_name, profile_name, queued_jobs
                        )
                    except Exception as cold_exc:
                        e = RuntimeError(
                            f"cached placement failed ({e}); cold fallback failed ({cold_exc})"
                        )
            self.offer_cooldowns[(provider_name, str(offer["id"]))] = (
                time.monotonic() + OFFER_COOLDOWN_SECONDS
            )
            logger.info(
                "Offer %s on cooldown for %ss after launch failure",
                offer["id"],
                OFFER_COOLDOWN_SECONDS,
            )
            self._record_launch_failure(
                provider_name, profile_name, queued_jobs, str(e)
            )
            return None

    def _remember_launched_instance(
        self,
        instance: Instance,
        provider_name: str,
        profile_name: str,
        queued_jobs: list | None,
    ) -> Instance:
        self.active_instances[instance.id] = instance
        self.instance_providers[instance.id] = provider_name
        self.instance_profiles[instance.id] = profile_name
        launched_at = datetime.utcnow()
        self.last_activity[instance.id] = launched_at
        self.launched_at[instance.id] = launched_at
        logger.info("Launched worker %s", instance.id)
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

    def _choose_cache_placement(
        self,
        *,
        connector: CloudConnector,
        provider_name: str,
        gpu_type: str,
        minimum_vram: int,
        cooling: set[str],
        requirements: dict,
    ):
        policy = self.config.prepared_storage
        existing = policy.get("existing_volume_id")

        def storage_failure(reason: str) -> PlacementDecision:
            if (
                policy.get("policy") == "smart"
                and policy.get("cold_fallback") == "allow"
            ):
                cold = [
                    item
                    for item in connector.list_available(
                        gpu_type=gpu_type,
                        min_gpu_ram=minimum_vram,
                        max_hourly_rate=self.config.max_hourly_rate,
                    )
                    if str(item.get("id")) not in cooling
                ]
                if cold:
                    offer = min(
                        cold,
                        key=lambda item: (
                            float(item.get("hourly_rate", float("inf"))),
                            str(item.get("id") or ""),
                        ),
                    )
                    return PlacementDecision(
                        "launch",
                        PlacementCandidate(offer, None),
                        f"{reason}_running_cold",
                        (),
                        fallback=True,
                    )
            return PlacementDecision("unavailable", None, reason, ())

        try:
            # Provider truth is rechecked before every cached placement. A
            # deleted or moved adopted volume is removed from scheduling before
            # Pod creation, not discovered after billing starts.
            for registered in self.cache_registry.list_volumes():
                if registered.provider != provider_name:
                    continue
                actual = connector.get_storage(registered.provider_volume_id)
                if actual is None or actual.datacenter_id != registered.datacenter_id:
                    self.cache_registry.mark_volume(registered.id, "degraded")
                    if registered.provider_volume_id == existing:
                        return storage_failure(
                            "configured_cache_volume_not_found"
                            if actual is None
                            else "configured_cache_volume_wrong_datacenter"
                        )

            if existing and not self.cache_registry.get_provider_volume(
                provider_name, existing
            ):
                provider_volume = connector.get_storage(existing)
                if provider_volume is None:
                    return storage_failure("configured_cache_volume_not_found")
                if policy.get("region") not in {"auto", provider_volume.datacenter_id}:
                    return storage_failure("configured_cache_volume_wrong_datacenter")
                self.cache_registry.upsert_volume(
                    provider=provider_name,
                    provider_volume_id=provider_volume.id,
                    datacenter_id=provider_volume.datacenter_id,
                    ownership="adopted",
                    capacity_bytes=provider_volume.size_gb * 1024**3,
                    policy=policy,
                    s3_compatible=provider_volume.s3_compatible,
                )

            ready = [
                item
                for item in self.cache_registry.list_volumes(status="ready")
                if item.provider == provider_name
            ]
            if not ready and not existing:
                region = str(policy.get("region") or "auto")
                if region == "auto":
                    # RunPod's aggregate GPU-type API cannot prove a specific
                    # datacenter has capacity. Region auto therefore becomes an
                    # actionable one-time decision rather than silently creating
                    # paid, stranded storage or running statelessly.
                    return PlacementDecision(
                        "ask", None, "managed_cache_region_selection_required", ()
                    )
                size_gb = int(policy.get("managed_size_gb") or 250)
                monthly = estimate_runpod_storage_monthly(size_gb)
                budget = policy.get("max_monthly_storage_cost")
                if budget is not None and monthly > float(budget):
                    return PlacementDecision(
                        "unavailable", None, "managed_cache_exceeds_storage_budget", ()
                    )
                provider_volume = connector.create_storage(
                    name=f"cloud-offload-{region.lower()}",
                    size_gb=size_gb,
                    datacenter_id=region,
                )
                self.cache_registry.upsert_volume(
                    provider=provider_name,
                    provider_volume_id=provider_volume.id,
                    datacenter_id=provider_volume.datacenter_id,
                    ownership="managed",
                    capacity_bytes=provider_volume.size_gb * 1024**3,
                    policy=policy,
                    status="ready",
                    s3_compatible=provider_volume.s3_compatible,
                )
        except Exception as exc:
            logger.warning("Prepared storage lifecycle failed: %s", exc)
            return storage_failure(f"prepared_storage_lifecycle_failed: {exc}")

        runtime = scheduler_runtime(requirements)
        coverages = {
            item["volume"].id: item
            for item in self.cache_registry.volume_coverage(
                requirements["required"],
                runtime=runtime,
                tenant=str(policy.get("tenant") or "default"),
                profile_fingerprint=str(requirements["profile_fingerprint"]),
                allow_private=bool(policy.get("cache_private_assets")),
                logical_required=requirements.get("logical_required") or [],
            )
            if item["volume"].provider == provider_name
        }
        cached: list[PlacementCandidate] = []
        for volume in self.cache_registry.list_volumes(status="ready"):
            if volume.provider != provider_name:
                continue
            if policy.get("policy") == "pinned" and volume.datacenter_id != policy.get(
                "region"
            ):
                continue
            constraints = PlacementConstraints(
                datacenter_ids=(volume.datacenter_id,),
                storage_attachments=(
                    StorageAttachment(
                        provider_volume_id=volume.provider_volume_id,
                        mount_path="/workspace",
                        datacenter_id=volume.datacenter_id,
                    ),
                ),
            )
            try:
                offers = connector.list_available(
                    gpu_type=gpu_type,
                    min_gpu_ram=minimum_vram,
                    max_hourly_rate=self.config.max_hourly_rate,
                    placement=constraints,
                )
            except Exception as exc:
                logger.warning(
                    "Prepared placement %s/%s unavailable: %s",
                    volume.datacenter_id,
                    volume.id,
                    exc,
                )
                continue
            coverage = coverages.get(volume.id) or {
                "cached_bytes": 0,
                "required_bytes": sum(requirements["required"].values()),
                "complete": not requirements["required"],
                "manifest_ids": [],
            }
            for offer in offers:
                if str(offer.get("id")) in cooling:
                    continue
                cached.append(
                    PlacementCandidate(
                        offer=offer,
                        volume=volume,
                        cached_bytes=int(coverage["cached_bytes"]),
                        required_bytes=int(coverage["required_bytes"]),
                        complete=bool(coverage["complete"]),
                        manifest_ids=tuple(coverage["manifest_ids"]),
                    )
                )
        cold = [
            offer
            for offer in connector.list_available(
                gpu_type=gpu_type,
                min_gpu_ram=minimum_vram,
                max_hourly_rate=self.config.max_hourly_rate,
            )
            if str(offer.get("id")) not in cooling
        ]
        return choose_placement(
            policy=policy, cached_candidates=cached, cold_offers=cold
        )

    def _planned_disk_gb(self, profile_name: str, queued_jobs: list | None) -> int:
        """The container disk to rent: the configured floor, or a job's plan if larger.

        The coordinator sizes a partition's storage at submission and stamps the
        answer onto the job, so this is the number that keeps a pod from dying
        out of disk after the meter has started. A job queued before storage
        planning existed carries no figure and gets exactly the configured value
        it would have got before.
        """
        configured = int(self.config.runpod_container_disk_gb)
        planned = max(
            [0]
            + [
                int(job.params.get("container_disk_gb") or 0)
                for job in (queued_jobs or [])
            ]
        )
        if planned <= configured:
            return configured
        logger.info(
            "Renting %s GB of container disk for profile %s: the storage plan for "
            "its queued partitions needs more than the configured %s GB",
            planned,
            profile_name,
            configured,
        )
        return planned

    def _offers_on_cooldown(self, provider_name: str) -> set[str]:
        """Offer ids currently sitting out after a failed launch on this provider."""
        now = time.monotonic()
        for key in [k for k, until in self.offer_cooldowns.items() if until <= now]:
            del self.offer_cooldowns[key]
        return {
            offer_id
            for (name, offer_id) in self.offer_cooldowns
            if name == provider_name
        }

    def _publish_launch_event(self, jobs: list | None, event: dict) -> None:
        self.event_producer_sequence += 1
        for job in jobs or []:
            try:
                self.queue.append_event(
                    job.id,
                    event,
                    producer_id=self.event_producer_id,
                    producer_sequence=self.event_producer_sequence,
                )
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
                self.launched_at.pop(instance_id, None)
                self.runner_feedback_at.pop(instance_id, None)
            else:
                self.active_instances[instance_id] = instance

    def _check_unregistered_workers(self):
        """Stop a paid instance whose runner never managed to call home."""
        now = datetime.utcnow()
        workers = self.queue.list_active_workers()
        profiles = configured_worker_profiles(self.config)
        for instance_id, launched_at in list(self.launched_at.items()):
            provider_name = self.instance_providers[instance_id]
            profile_name = self.instance_profiles[instance_id]

            def registered_after_launch(worker: dict) -> bool:
                if (
                    worker.get("provider") != provider_name
                    or worker.get("runtime_profile") != profile_name
                ):
                    return False
                try:
                    seen = datetime.fromisoformat(
                        str(worker.get("last_seen") or "").replace("Z", "+00:00")
                    ).replace(tzinfo=None)
                except ValueError:
                    return False
                return seen >= launched_at

            registered = any(registered_after_launch(worker) for worker in workers)
            if registered:
                self.runner_feedback_at.pop(instance_id, None)
                continue

            queued_jobs = [
                job
                for job in self.queue.list_by_status(
                    JobStatus.QUEUED, provider=provider_name
                )
                if self._job_profile_name(job, profiles) == profile_name
            ]
            last_feedback = self.runner_feedback_at.get(instance_id)
            if queued_jobs and (
                last_feedback is None or now - last_feedback >= timedelta(seconds=10)
            ):
                elapsed = round((now - launched_at).total_seconds(), 1)
                self._publish_launch_event(
                    queued_jobs,
                    {
                        "schema": "cloud-offload.phase-event.v1",
                        "type": "runner_starting_progress",
                        "phase": "runner_starting",
                        "worker_instance_id": instance_id,
                        "elapsed_seconds": elapsed,
                        "overall_progress": 2,
                        "message": (
                            "RunPod is allocating the machine, pulling the pinned "
                            "image, and starting ComfyUI"
                        ),
                    },
                )
                self.runner_feedback_at[instance_id] = now

            if now - launched_at <= timedelta(
                seconds=RUNNER_REGISTRATION_TIMEOUT_SECONDS
            ):
                continue

            detail = (
                f"Runner did not register within {RUNNER_REGISTRATION_TIMEOUT_SECONDS}s"
            )
            logger.error("Worker %s %s; terminating", instance_id, detail.lower())
            self._record_launch_failure(
                provider_name, profile_name, queued_jobs, detail
            )
            self._terminate_worker(instance_id)

    @staticmethod
    def _job_profile_name(job, profiles: dict) -> str:
        """The worker profile a job resolves to, resolved the way routing does.

        Jobs carry the capability a client stamped — ``comfyui-partition-v1`` —
        not an operator's profile name. Comparing the raw strings matched
        nothing, so a pod could be terminated as idle while the queue held the
        very work it had been rented for.
        """
        requested = str(job.params.get("runtime_profile") or "")
        resolved = profiles.get(requested) or profile_providing(profiles, requested)
        return resolved["name"] if resolved else requested

    def _check_idle_workers(self):
        """Terminate workers that have been idle too long."""
        if self.config.keep_warm:
            return
        # Check for idle workers
        idle_threshold = timedelta(seconds=self.config.idle_shutdown_seconds)
        now = datetime.utcnow()

        workers = self.queue.list_active_workers()
        profiles = configured_worker_profiles(self.config)
        for instance_id, last_active in list(self.last_activity.items()):
            provider_name = self.instance_providers[instance_id]
            profile_name = self.instance_profiles[instance_id]
            # A runner that has said it is still starting is not idle, it is
            # busy importing ComfyUI. Terminating it on the idle clock is how a
            # pod gets killed mid-boot and the whole rental paid for nothing.
            starting = any(
                worker.get("provider") == provider_name
                and worker.get("runtime_profile") == profile_name
                and worker.get("status") == "starting"
                for worker in workers
            )
            running_jobs = sum(
                self._job_profile_name(job, profiles) == profile_name
                for job in self.queue.list_by_status(
                    JobStatus.RUNNING,
                    JobStatus.DISPATCHED,
                    provider=provider_name,
                )
            )
            queued_jobs = sum(
                self._job_profile_name(job, profiles) == profile_name
                for job in self.queue.list_by_status(
                    JobStatus.QUEUED, provider=provider_name
                )
            )
            if starting or running_jobs > 0 or queued_jobs > 0:
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
        self.launched_at.pop(instance_id, None)

    def shutdown(self):
        """Terminate all workers and shut down."""
        logger.info("Dispatcher shutting down, terminating workers...")
        for instance_id in list(self.active_instances.keys()):
            self._terminate_worker(instance_id)
        if self._tunnel is not None:
            self._tunnel.close()
            self._tunnel = None

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

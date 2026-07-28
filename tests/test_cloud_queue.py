import json
import sqlite3
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from cloud_offload.config import CloudConfig
from cloud_offload.dispatcher import Dispatcher
from cloud_offload.providers.base import CloudProvider
from cloud_offload.queue import JobQueue, JobStatus
from cloud_offload.router import select_provider
from cloud_offload.worker import Worker


class DummyProvider(CloudProvider):
    @property
    def name(self) -> str:
        return "dummy"

    def list_available(self, *args, **kwargs):
        return []

    def launch(self, *args, **kwargs):
        raise NotImplementedError

    def terminate(self, instance_id: str) -> bool:
        return True

    def get_instance(self, instance_id: str):
        return None

    def list_instances(self):
        return []


def test_config_load_merges_preferences_with_environment(monkeypatch, tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "cloud": {
                    "enabled": True,
                    "provider_order": ["runpod", "vast"],
                    "routing_policy": "cheapest",
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("RUNPOD_API_KEY", "secret")

    config = CloudConfig.load(config_path)

    assert config.enabled is True
    assert config.provider_order == ["runpod", "vast.ai"]
    assert config.routing_policy == "cheapest"
    assert config.runpod_api_key == "secret"


def test_keep_warm_config_round_trips_and_supports_environment(monkeypatch, tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "cloud": {
                    "keep_warm": True,
                    "keep_warm_warning_seconds": 3600,
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("CLOUD_OFFLOAD_KEEP_WARM_WARNING", "7200")

    config = CloudConfig.load(config_path, resolve_secrets=False)

    assert config.keep_warm is True
    assert config.keep_warm_warning_seconds == 7200
    assert config.to_dict()["keep_warm"] is True


def test_config_resolves_provider_credentials_from_the_keychain(monkeypatch, tmp_path):
    """Credentials come from the OS keychain, never from config.json."""
    from cloud_offload import credentials as creds

    config_path = tmp_path / "config.json"
    config_path.write_text('{"cloud":{"enabled":true}}', encoding="utf-8")
    monkeypatch.delenv("VAST_API_KEY", raising=False)
    monkeypatch.delenv("RUNPOD_API_KEY", raising=False)
    monkeypatch.delenv("CLOUD_OFFLOAD_RUNPOD_API_KEY", raising=False)

    vault = {}
    monkeypatch.setattr(
        creds,
        "_keyring",
        lambda: SimpleNamespace(
            get_password=lambda service, user: vault.get(user),
            set_password=lambda service, user, secret: vault.__setitem__(user, secret),
            delete_password=lambda service, user: vault.pop(user, None),
        ),
    )
    monkeypatch.setattr(creds, "legacy_credentials_file", lambda: tmp_path / "none.json")
    vault["runpod"] = "runpod-secret"

    config = CloudConfig.load(config_path)

    assert config.api_key_for("runpod") == "runpod-secret"
    assert config.api_key_for("vast.ai") == ""
    # A credential must never reach the serialized config.
    assert "runpod-secret" not in repr(config.to_dict())


def test_env_var_overrides_the_keychain_for_headless_workers(monkeypatch, tmp_path):
    """A rented worker has no keychain, so the env var has to win."""
    from cloud_offload import credentials as creds

    monkeypatch.delenv("RUNPOD_API_KEY", raising=False)
    monkeypatch.setenv("CLOUD_OFFLOAD_RUNPOD_API_KEY", "from-env")
    monkeypatch.setattr(
        creds,
        "_keyring",
        lambda: SimpleNamespace(
            get_password=lambda service, user: "from-keychain",
            set_password=lambda *a: None,
            delete_password=lambda *a: None,
        ),
    )
    monkeypatch.setattr(creds, "legacy_credentials_file", lambda: tmp_path / "none.json")

    assert CloudConfig().api_key_for("runpod") == "from-env"


def test_cheapest_router_compares_configured_connectors(monkeypatch):
    class Connector:
        def __init__(self, rate):
            self.rate = rate

        def find_cheapest(self, **kwargs):
            return {"id": str(self.rate), "hourly_rate": self.rate}

    connectors = {"vast.ai": Connector(0.45), "runpod": Connector(0.32)}
    monkeypatch.setattr(
        "cloud_offload.router.create_connector", lambda name, config: connectors[name]
    )
    config = CloudConfig(
        provider_order=["vast.ai", "runpod"],
        routing_policy="cheapest",
        vast_api_key="vast",
        runpod_api_key="runpod",
    )

    route = select_provider(config)

    assert route.provider == "runpod"
    assert route.offer["hourly_rate"] == 0.32


def test_router_only_considers_providers_with_compatible_worker_profile(monkeypatch):
    class Connector:
        def find_cheapest(self, **kwargs):
            return {"id": "offer", "hourly_rate": 0.25}

    monkeypatch.setattr(
        "cloud_offload.router.create_connector", lambda name, config: Connector()
    )
    config = CloudConfig(
        provider_order=["vast.ai", "runpod"],
        routing_policy="preferred",
        vast_api_key="vast",
        runpod_api_key="runpod",
        worker_profiles={
            "comfyui": {
                "image": "registry.example/cloud-offload-comfyui@sha256:abc",
                "models": ["comfyui-partition-v1"],
                "providers": ["runpod"],
            }
        },
    )

    route = select_provider(config, model="comfyui-partition-v1")

    assert route.provider == "runpod"
    assert route.profile["name"] == "comfyui"

    with pytest.raises(ValueError, match="vast.ai worker profile"):
        select_provider(config, requested="vast.ai", model="comfyui-partition-v1")


def test_queue_migrates_legacy_schema(tmp_path):
    db_path = tmp_path / "queue.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY,
                model TEXT NOT NULL,
                status TEXT NOT NULL,
                input_path TEXT NOT NULL,
                params TEXT,
                preview_path TEXT,
                result_path TEXT,
                created_at TEXT,
                updated_at TEXT,
                started_at TEXT,
                completed_at TEXT,
                error TEXT,
                worker_id TEXT
            )
            """
        )

    JobQueue(db_path)

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
        version = conn.execute(
            "SELECT value FROM queue_meta WHERE key = 'schema_version'"
        ).fetchone()[0]

    assert {
        "attempts",
        "max_attempts",
        "schema_version",
        "request_json",
        "provider",
        "result_json",
        "progress",
    } <= columns
    assert version == "5"


def test_job_events_are_ordered_and_resumable(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create("comfyui-workflow", "partition://input")

    first = queue.append_event(job.id, {"type": "executing", "node_id": "4"})
    second = queue.append_event(job.id, {"type": "progress", "value": 3, "max": 10})

    assert first["sequence"] < second["sequence"]
    assert [item["event"]["type"] for item in queue.list_events(job.id)] == [
        "executing",
        "progress",
    ]
    assert queue.list_events(job.id, after=first["sequence"]) == [second]


def test_claim_increments_attempts_and_assigns_worker(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create("comfyui-workflow", "input.json")
    queue.update_status(job.id, JobStatus.QUEUED)

    claimed = queue.claim_jobs("worker-a", limit=1)

    assert len(claimed) == 1
    assert claimed[0].status == JobStatus.DISPATCHED
    assert claimed[0].worker_id == "worker-a"
    assert claimed[0].attempts == 1


def test_claim_is_scoped_to_worker_provider(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    vast = queue.create(
        "comfyui-workflow", "inline://request", provider="vast.ai", status=JobStatus.QUEUED
    )
    runpod = queue.create(
        "comfyui-workflow", "inline://request", provider="runpod", status=JobStatus.QUEUED
    )

    claimed = queue.claim_jobs("worker-r", provider="runpod")

    assert [job.id for job in claimed] == [runpod.id]
    assert queue.get(vast.id).status == JobStatus.QUEUED


def test_claim_is_scoped_to_worker_model_capabilities(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    supported = queue.create(
        "comfyui-partition-v1", "inline://request", provider="runpod", status=JobStatus.QUEUED
    )
    unsupported = queue.create(
        "comfyui-workflow",
        "inline://request",
        provider="runpod",
        status=JobStatus.QUEUED,
    )

    claimed = queue.claim_jobs(
        "worker-r", provider="runpod", models=["comfyui-partition-v1"]
    )

    assert [job.id for job in claimed] == [supported.id]
    assert queue.get(unsupported.id).status == JobStatus.QUEUED


def test_worker_heartbeat_reports_profile_and_capabilities(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")

    queue.record_worker(
        "worker-r",
        "runpod",
        runtime_profile="comfyui",
        capabilities=["comfyui-partition-v1"],
    )

    workers = queue.list_active_workers()
    assert workers[0]["runtime_profile"] == "comfyui"
    assert workers[0]["capabilities"] == ["comfyui-partition-v1"]


def test_completed_result_round_trips(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create(
        "comfyui-workflow",
        "inline://request",
        request={"workflow": {}, "provider": "runpod"},
        provider="runpod",
        status=JobStatus.QUEUED,
    )
    job.error = "previous attempt failed"
    queue.update(job)

    completed = queue.complete_job(job.id, {"prompt_id": "xyz", "outputs": {}})

    assert completed.progress == 100
    assert completed.error is None
    assert queue.get(job.id).result["prompt_id"] == "xyz"


def test_worker_heartbeat_tracks_continuous_idle_time(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")

    queue.record_worker("worker-1", "runpod", idle=True)
    first = queue.list_active_workers()[0]
    queue.record_worker("worker-1", "runpod", idle=True)
    second = queue.list_active_workers()[0]

    assert first["idle_since"] is not None
    assert second["idle_since"] == first["idle_since"]

    queue.record_worker("worker-1", "runpod", idle=False)
    active = queue.list_active_workers()[0]
    assert active["idle_since"] is None
    assert active["idle_seconds"] == 0


def test_worker_live_policy_can_keep_an_idle_worker_alive():
    class PolicyQueue:
        def worker_policy(self):
            return {"keep_warm": True, "idle_shutdown_seconds": 1}

    worker = Worker.__new__(Worker)
    worker.config = CloudConfig(idle_shutdown_seconds=1)
    worker.queue = PolicyQueue()
    worker.last_job_time = datetime.utcnow() - timedelta(hours=2)

    assert worker._should_shutdown() is False


def test_dispatcher_does_not_terminate_pinned_workers(tmp_path):
    config = CloudConfig(
        queue_db_path=str(tmp_path / "queue.db"),
        keep_warm=True,
        idle_shutdown_seconds=1,
    )
    dispatcher = Dispatcher(config, provider=DummyProvider())
    dispatcher.active_instances["pod-1"] = SimpleNamespace(id="pod-1")
    dispatcher.instance_providers["pod-1"] = config.provider
    dispatcher.instance_profiles["pod-1"] = "comfyui"
    dispatcher.last_activity["pod-1"] = datetime.utcnow() - timedelta(hours=2)

    dispatcher._check_idle_workers()

    assert "pod-1" in dispatcher.active_instances


def test_dispatcher_reuses_generated_worker_token_across_restarts(tmp_path):
    config = CloudConfig(queue_db_path=str(tmp_path / "queue.db"))

    first = Dispatcher(config, provider=DummyProvider())
    second = Dispatcher(config, provider=DummyProvider())

    assert second.worker_token == first.worker_token
    second.queue.authorize_worker(first.worker_token)
    token_path = tmp_path / "worker-token"
    assert token_path.read_text(encoding="utf-8").strip() == first.worker_token


def test_dispatcher_does_not_launch_over_registered_warm_worker(monkeypatch, tmp_path):
    class LaunchCountingProvider(DummyProvider):
        def __init__(self):
            self.launches = 0

        def find_cheapest(self, **kwargs):
            return {"id": "offer", "gpu_type": "RTX 4090", "hourly_rate": 0.69}

        def launch(self, *args, **kwargs):
            self.launches += 1
            raise AssertionError("an active coordinator worker must be reused")

    provider = LaunchCountingProvider()
    config = CloudConfig(
        provider="runpod",
        provider_order=["runpod"],
        queue_db_path=str(tmp_path / "queue.db"),
        coordinator_url="https://coordinator.invalid",
        min_queue_depth=1,
        worker_profiles={
            "comfyui": {
                "image": "ghcr.io/example/comfyui@sha256:" + "a" * 64,
                "models": ["comfyui-partition-v1"],
                "providers": ["runpod"],
            }
        },
    )
    queue = JobQueue(config.queue_db_path)
    queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"runtime_profile": "comfyui"},
        status=JobStatus.QUEUED,
    )
    queue.record_worker(
        "warm-worker",
        "runpod",
        runtime_profile="comfyui",
        capabilities=["comfyui-partition-v1"],
        idle=True,
    )
    dispatcher = Dispatcher(config, queue=queue, provider=provider)
    monkeypatch.setattr(
        "cloud_offload.dispatcher.CloudConfig.load",
        lambda *args, **kwargs: config,
    )

    dispatcher._tick()

    assert provider.launches == 0


def test_dispatcher_launches_legacy_workers_with_effectively_indefinite_timeout(tmp_path):
    class LaunchProvider(DummyProvider):
        def __init__(self):
            self.env_vars = None

        def find_cheapest(self, **kwargs):
            return {
                "id": "offer-1",
                "gpu_type": "RTX A6000",
                "hourly_rate": 0.49,
            }

        def launch(self, *args, **kwargs):
            self.env_vars = kwargs["env_vars"]
            return SimpleNamespace(
                id="pod-1",
                provider="vast.ai",
                gpu_type="RTX A6000",
                hourly_rate=0.49,
                status="running",
            )

    provider = LaunchProvider()
    config = CloudConfig(
        queue_db_path=str(tmp_path / "queue.db"),
        coordinator_url="https://coordinator.invalid",
        keep_warm=True,
        provider="vast.ai",
        worker_profiles={
            "comfyui": {
                "image": "registry.invalid/comfyui@sha256:abc",
                "models": ["comfyui-partition-v1"],
                "providers": ["vast.ai"],
            }
        },
    )
    dispatcher = Dispatcher(config, provider=provider)

    dispatcher._launch_worker("vast.ai", "comfyui")

    assert provider.env_vars["CLOUD_OFFLOAD_KEEP_WARM"] == "true"
    assert int(provider.env_vars["CLOUD_OFFLOAD_IDLE_SHUTDOWN"]) == 10 * 365 * 24 * 60 * 60


def test_dispatcher_reports_provisioning_and_backs_off_after_launch_failure(tmp_path):
    class FailingProvider(DummyProvider):
        def __init__(self):
            self.launch_count = 0

        def find_cheapest(self, **kwargs):
            return {
                "id": "offer-1",
                "gpu_type": "RTX 4090",
                "hourly_rate": 0.34,
            }

        def launch(self, *args, **kwargs):
            self.launch_count += 1
            raise RuntimeError("private image cannot be pulled")

    provider = FailingProvider()
    config = CloudConfig(
        provider="runpod",
        provider_order=["runpod"],
        queue_db_path=str(tmp_path / "queue.db"),
        coordinator_url="https://coordinator.invalid",
        min_queue_depth=1,
        worker_profiles={
            "comfyui": {
                "image": "ghcr.io/example/private@sha256:" + "a" * 64,
                "models": ["comfyui-partition-v1"],
                "providers": ["runpod"],
                "min_gpu_ram_gb": 16,
            }
        },
    )
    queue = JobQueue(config.queue_db_path)
    job = queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"runtime_profile": "comfyui", "min_gpu_ram_gb": 16},
        status=JobStatus.QUEUED,
    )
    dispatcher = Dispatcher(config, queue=queue, provider=provider)

    dispatcher._tick()
    dispatcher._tick()

    events = [item["event"] for item in queue.list_events(job.id)]
    assert provider.launch_count == 1
    assert [item["type"] for item in events] == [
        "provisioning_started",
        "provisioning_failed",
    ]
    assert events[-1]["retry_seconds"] == 10
    assert events[-1]["error"] == "private image cannot be pulled"


def test_worker_token_required_when_queue_is_configured(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create("comfyui-workflow", "input.json")
    queue.update_status(job.id, JobStatus.QUEUED)
    queue.set_worker_token("secret")

    with pytest.raises(PermissionError):
        queue.claim_jobs("worker-a", limit=1)

    with pytest.raises(PermissionError):
        queue.claim_jobs("worker-a", limit=1, token="wrong")

    claimed = queue.claim_jobs("worker-a", limit=1, token="secret")
    assert claimed[0].id == job.id


def test_claim_jobs_enforces_partition_gpu_constraints(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    small = queue.create(
        "comfyui-partition-v1",
        "small.part",
        provider="runpod",
        params={"gpu_type": "any", "min_gpu_ram_gb": 12},
    )
    large = queue.create(
        "comfyui-partition-v1",
        "large.part",
        provider="runpod",
        params={"gpu_type": "RTX_4090", "min_gpu_ram_gb": 30},
    )
    for job in (small, large):
        queue.update_status(job.id, JobStatus.QUEUED)

    claimed = queue.claim_jobs(
        "worker-a",
        provider="runpod",
        models=["comfyui-partition-v1"],
        gpu_vram_gb=24,
        gpu_name="NVIDIA GeForce RTX 4090",
    )

    assert [job.id for job in claimed] == [small.id]
    assert queue.get(large.id).status == JobStatus.QUEUED


def test_claim_jobs_matches_normalized_gpu_name(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"gpu_type": "RTX_4090", "min_gpu_ram_gb": 16},
    )
    queue.update_status(job.id, JobStatus.QUEUED)

    claimed = queue.claim_jobs(
        "worker-a",
        provider="runpod",
        models=["comfyui-partition-v1"],
        gpu_vram_gb=24,
        gpu_name="NVIDIA GeForce RTX 4090",
    )

    assert [item.id for item in claimed] == [job.id]


def test_fail_job_requeues_then_dead_letters(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create("comfyui-workflow", "input.json")
    job.max_attempts = 2
    queue.update(job)

    queue.update_status(job.id, JobStatus.QUEUED)
    claimed = queue.claim_jobs("worker-a", limit=1)[0]
    failed_once = queue.fail_job(claimed.id, "boom")

    assert failed_once.status == JobStatus.QUEUED
    assert failed_once.error == "boom"
    assert failed_once.completed_at is None

    claimed = queue.claim_jobs("worker-b", limit=1)[0]
    failed_twice = queue.fail_job(claimed.id, "still boom")

    assert failed_twice.status == JobStatus.DEAD_LETTER
    assert failed_twice.error == "still boom"
    assert failed_twice.completed_at is not None


def test_dispatcher_startup_script_uses_wheelhouse_only(tmp_path):
    config = CloudConfig(
        queue_db_path=str(tmp_path / "queue.db"),
        worker_wheelhouse_url="https://example.invalid/cloud-offload-wheelhouse.tar.gz",
        worker_wheelhouse_sha256="0" * 64,
    )
    dispatcher = Dispatcher(config, provider=DummyProvider())

    script = dispatcher._build_startup_script()

    assert "--no-index" in script
    assert "cloud-offload[cloud]" in script
    assert "$CLOUD_OFFLOAD_WORKER_WHEELHOUSE_SHA256" in script


def test_dispatcher_refuses_live_registry_worker_install(tmp_path):
    config = CloudConfig(queue_db_path=str(tmp_path / "queue.db"))
    dispatcher = Dispatcher(config, provider=DummyProvider())

    with pytest.raises(RuntimeError, match="live registry installs are disabled"):
        dispatcher._build_startup_script()


def test_dispatcher_builds_selected_connector_from_registry(tmp_path):
    config = CloudConfig(
        provider="runpod",
        runpod_api_key="runpod-secret",
        queue_db_path=str(tmp_path / "queue.db"),
    )

    dispatcher = Dispatcher(config)

    assert dispatcher.connector.name == "runpod"
    assert dispatcher.provider is dispatcher.connector


def test_profile_startup_assumes_dependencies_are_baked_into_image(tmp_path):
    config = CloudConfig(queue_db_path=str(tmp_path / "queue.db"))
    dispatcher = Dispatcher(config, provider=DummyProvider())
    profile = {
        "name": "comfyui",
        "image": "registry.example/cloud-offload-comfyui@sha256:abc",
        "models": ["comfyui-partition-v1"],
        "providers": ["vast.ai"],
        "wheelhouse_url": "",
        "wheelhouse_sha256": "",
    }

    script = dispatcher._build_startup_script(profile)

    assert script is None


# === Offer cooldown after failed launches ===

class TwoOfferProvider(DummyProvider):
    """Cheapest offer's host refuses to launch; the pricier one works."""

    def __init__(self):
        self.launch_attempts = []

    @property
    def name(self) -> str:
        return "runpod"

    def list_available(self, *args, **kwargs):
        return [
            {"id": "offer-dead", "gpu_type": "RTX 4000 Ada", "hourly_rate": 0.28},
            {"id": "offer-good", "gpu_type": "RTX 2000 Ada", "hourly_rate": 0.30},
        ]

    def launch(self, offer_id, *args, **kwargs):
        self.launch_attempts.append(offer_id)
        if offer_id == "offer-dead":
            raise RuntimeError("This machine does not have the resources")
        return SimpleNamespace(
            id="pod-1",
            provider="runpod",
            gpu_type="RTX 2000 Ada",
            hourly_rate=0.30,
            status="pending",
        )


def _cooldown_dispatcher(tmp_path, provider):
    config = CloudConfig(
        provider="runpod",
        provider_order=["runpod"],
        queue_db_path=str(tmp_path / "queue.db"),
        coordinator_url="https://coordinator.invalid",
        worker_profiles={
            "comfyui": {
                "image": "registry.invalid/comfyui@sha256:" + "a" * 64,
                "models": ["comfyui-partition-v1"],
                "providers": ["runpod"],
            }
        },
    )
    return Dispatcher(config, provider=provider)


def test_failed_offer_cools_down_and_next_launch_routes_around_it(tmp_path):
    provider = TwoOfferProvider()
    dispatcher = _cooldown_dispatcher(tmp_path, provider)

    assert dispatcher._launch_worker("runpod", "comfyui") is None
    assert ("runpod", "offer-dead") in dispatcher.offer_cooldowns

    instance = dispatcher._launch_worker("runpod", "comfyui")
    assert instance is not None and instance.id == "pod-1"
    assert provider.launch_attempts == ["offer-dead", "offer-good"]


def test_offer_cooldown_expires_and_is_pruned(tmp_path):
    import time as time_module

    dispatcher = _cooldown_dispatcher(tmp_path, TwoOfferProvider())
    dispatcher.offer_cooldowns[("runpod", "offer-dead")] = time_module.monotonic() - 1
    assert dispatcher._offers_on_cooldown("runpod") == set()
    assert dispatcher.offer_cooldowns == {}


def test_cooldown_is_scoped_to_its_provider(tmp_path):
    import time as time_module

    dispatcher = _cooldown_dispatcher(tmp_path, TwoOfferProvider())
    dispatcher.offer_cooldowns[("vast.ai", "offer-dead")] = time_module.monotonic() + 60
    assert dispatcher._offers_on_cooldown("runpod") == set()
    assert dispatcher._offers_on_cooldown("vast.ai") == {"offer-dead"}


def test_find_cheapest_exclude_filters_offers(tmp_path):
    provider = TwoOfferProvider()
    assert provider.find_cheapest()["id"] == "offer-dead"
    assert provider.find_cheapest(exclude={"offer-dead"})["id"] == "offer-good"
    assert provider.find_cheapest(exclude={"offer-dead", "offer-good"}) is None


# === Profile resolution: capability or name ===

def _profile_config(tmp_path, profile_names=("comfyui",)):
    profiles = {
        name: {
            "image": "registry.invalid/comfyui@sha256:" + "a" * 64,
            "models": ["comfyui-workflow", "comfyui-partition-v1"],
            "providers": ["runpod"],
        }
        for name in profile_names
    }
    return CloudConfig(
        provider="runpod",
        provider_order=["runpod"],
        runpod_api_key="test-key",
        queue_db_path=str(tmp_path / "queue.db"),
        worker_profiles=profiles,
    )


def test_a_capability_resolves_to_the_profile_providing_it(tmp_path):
    # What a box actually stamps is the capability it needs; only the operator
    # knows what their profiles are called.
    from cloud_offload.router import select_profile_provider

    route = select_profile_provider(_profile_config(tmp_path), "comfyui-partition-v1")

    assert route.profile["name"] == "comfyui"


def test_an_exact_profile_name_still_wins(tmp_path):
    from cloud_offload.router import select_profile_provider

    route = select_profile_provider(_profile_config(tmp_path), "comfyui")

    assert route.profile["name"] == "comfyui"


def test_capability_resolution_is_deterministic_across_profiles(tmp_path):
    from cloud_offload.router import select_profile_provider

    config = _profile_config(tmp_path, profile_names=("zeta", "alpha"))
    route = select_profile_provider(config, "comfyui-partition-v1")

    assert route.profile["name"] == "alpha"


def test_an_unknown_profile_names_what_is_configured(tmp_path):
    from cloud_offload.router import select_profile_provider

    with pytest.raises(ValueError, match="configured profiles: comfyui"):
        select_profile_provider(_profile_config(tmp_path), "nonsense")


def test_dispatcher_launches_for_a_capability_named_job(tmp_path, monkeypatch):
    """A job stamped with a capability must still provision a worker.

    The dispatcher looked its profile up by the raw name, so a job naming
    comfyui-partition-v1 sat queued forever while the log repeated that the
    profile was unknown — with a correctly configured profile providing it.
    """
    provider = TwoOfferProvider()
    config = _profile_config(tmp_path)
    config.coordinator_url = "https://coordinator.invalid"
    config.min_queue_depth = 1
    queue = JobQueue(config.queue_db_path)
    queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"runtime_profile": "comfyui-partition-v1"},
        status=JobStatus.QUEUED,
    )
    dispatcher = Dispatcher(config, queue=queue, provider=provider)
    monkeypatch.setattr(
        "cloud_offload.dispatcher.CloudConfig.load", lambda *a, **k: config
    )

    dispatcher._tick()

    assert provider.launch_attempts, "no worker was launched"


def test_a_card_sold_as_24gb_claims_a_job_needing_24(tmp_path):
    """Advertised size and driver-reported size are not the same number.

    An A5000 is sold as 24 GB and reports 24564 MiB (23.99 GiB). Compared raw,
    a worker refused every job its own GPU had been rented to run, and the job
    waited in the queue with an idle worker beside it.
    """
    queue = JobQueue(str(tmp_path / "queue.db"))
    queue.set_worker_token("t" * 40)
    for vram, expected in ((23.99, 1), (23.69, 1), (22.4, 0), (15.99, 0)):
        job = queue.create(
            "comfyui-partition-v1",
            "input.part",
            provider="runpod",
            params={"runtime_profile": "comfyui", "min_gpu_ram_gb": 24},
            status=JobStatus.QUEUED,
        )
        claimed = queue.claim_jobs(
            "worker-1",
            limit=1,
            token="t" * 40,
            provider="runpod",
            models=["comfyui-partition-v1"],
            gpu_vram_gb=vram,
        )
        assert len(claimed) == expected, f"{vram} GiB against a 24 GiB requirement"
        queue.update_status(job.id, JobStatus.FAILED)


# === A runner that is still coming up is neither absent nor idle ===


class StartingConnector(DummyProvider):
    """A connector that records launches without performing any."""

    def __init__(self):
        self.launches = 0

    @property
    def name(self):
        return "runpod"

    def list_available(self, *args, **kwargs):
        return []

    def find_cheapest(self, **kwargs):
        return {"id": "offer-1", "gpu_type": "RTX A5000", "hourly_rate": 0.27}

    def launch(self, *args, **kwargs):
        self.launches += 1
        return SimpleNamespace(
            id="pod-1",
            provider="runpod",
            gpu_type="RTX A5000",
            hourly_rate=0.27,
            status="running",
        )

    def get_instance(self, instance_id):
        return SimpleNamespace(
            id=instance_id,
            provider="runpod",
            gpu_type="RTX A5000",
            hourly_rate=0.27,
            status="running",
        )

    def terminate(self, instance_id):
        self.terminated = True
        return True

    def list_instances(self):
        return []


def starting_config(tmp_path):
    return CloudConfig(
        enabled=True,
        provider="runpod",
        provider_order=["runpod"],
        runpod_api_key="secret",
        coordinator_url="https://coordinator.invalid",
        min_queue_depth=1,
        idle_shutdown_seconds=1,
        queue_db_path=str(tmp_path / "queue.db"),
        storage_path=str(tmp_path / "storage"),
        worker_profiles={
            "comfyui": {
                "image": "ghcr.io/example/comfyui@sha256:" + "a" * 64,
                "models": ["comfyui-partition-v1"],
                "providers": ["runpod"],
            }
        },
    )


def test_a_starting_runner_stops_a_second_pod_being_rented(tmp_path, monkeypatch):
    from cloud_offload.dispatcher import Dispatcher

    config = starting_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"runtime_profile": "comfyui"},
        status=JobStatus.QUEUED,
    )
    queue.record_worker(
        "worker-boot", "runpod", status="starting", runtime_profile="comfyui"
    )
    monkeypatch.setattr(
        "cloud_offload.dispatcher.CloudConfig.load", lambda *args, **kwargs: config
    )
    provider = StartingConnector()

    Dispatcher(config, queue=queue, provider=provider)._tick()

    assert provider.launches == 0


def long_idle_dispatcher(config, queue):
    from datetime import datetime, timedelta

    from cloud_offload.dispatcher import Dispatcher

    provider = StartingConnector()
    dispatcher = Dispatcher(config, queue=queue, provider=provider)
    dispatcher.active_instances["pod-1"] = provider.get_instance("pod-1")
    dispatcher.instance_providers["pod-1"] = "runpod"
    dispatcher.instance_profiles["pod-1"] = "comfyui"
    dispatcher.last_activity["pod-1"] = datetime.utcnow() - timedelta(hours=1)
    return dispatcher


def test_a_runner_that_is_still_starting_is_not_terminated_as_idle(tmp_path):
    config = starting_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    queue.record_worker(
        "worker-boot", "runpod", status="starting", runtime_profile="comfyui"
    )
    dispatcher = long_idle_dispatcher(config, queue)

    dispatcher._check_idle_workers()

    assert "pod-1" in dispatcher.active_instances


def test_a_pod_is_not_killed_as_idle_while_its_own_work_is_queued(tmp_path):
    """The job carries the capability a client stamped, the pod carries the
    operator's profile name. Comparing them raw matched nothing, so a pod was
    terminated for being idle while the queue held exactly the work it had been
    rented for — and the next tick rented another one."""
    config = starting_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"runtime_profile": "comfyui-partition-v1"},
        status=JobStatus.QUEUED,
    )
    dispatcher = long_idle_dispatcher(config, queue)

    dispatcher._check_idle_workers()

    assert "pod-1" in dispatcher.active_instances

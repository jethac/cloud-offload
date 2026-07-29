"""Cloud Offload configuration."""

import json
import math
import os
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Literal


# Directory that holds the persisted config, queue database, tokens and the
# service-discovery file. Overridable through CLOUD_OFFLOAD_HOME.
CONFIG_DIR = Path(
    os.environ.get("CLOUD_OFFLOAD_HOME", str(Path.home() / ".cloud-offload"))
)
DEFAULT_WORKER_MANIFEST = "/opt/cloud-offload/runtime-profile.json"

# Plaintext credential file written by versions before keychain storage. Kept
# only so existing keys can be migrated out of it; nothing writes it now. The
# credentials module reads this attribute, so tests can redirect it.
CREDENTIALS_FILE = CONFIG_DIR / "credentials.json"
RUNPOD_NETWORK_VOLUME_MAX_GB = 4000


def estimate_runpod_storage_monthly(size_gb: int | float) -> float:
    """Published RunPod network-volume estimate: $0.07/GB then $0.05 over 1TB."""
    size = float(size_gb)
    return round(min(size, 1000) * 0.07 + max(0.0, size - 1000) * 0.05, 2)


# Credential naming and storage live in cloud_offload.credentials, which keeps
# keys in the OS keychain. These names are re-exported so callers keep one
# import site.
from cloud_offload.credentials import (  # noqa: E402
    KeychainUnavailable,
    normalize_provider_name,
    provider_env_var,
)


def save_provider_credential(provider: str, api_key: str) -> None:
    """Store (or clear) one connector credential in the OS keychain."""
    from cloud_offload.credentials import set_credential

    set_credential(provider, api_key)


def load_provider_credentials() -> dict[str, str]:
    """Credentials still sitting in the legacy plaintext file.

    Nothing writes this file any more; it exists so a pre-keychain install can
    be migrated, and so callers can assert that a secret was *not* persisted to
    disk. Live credentials come from ``CloudConfig.api_key_for``.
    """
    from cloud_offload.credentials import _read_legacy_file

    return _read_legacy_file()



def _normalized_on_prem_assets(entries) -> list:
    """Normalize on-prem entries, keeping bare patterns as plain strings.

    A bare pattern keeps the strict meaning it has always had, so an existing
    config never silently loosens; ``scope`` is opt-in for the licence case.
    """
    normalized: list = []
    for entry in entries or []:
        if isinstance(entry, dict):
            pattern = str(entry.get("pattern") or "").strip()
            if not pattern:
                continue
            scope = str(entry.get("scope") or "derived").strip()
            if scope not in {"weights", "derived"}:
                raise ValueError(
                    f"on_prem_assets: scope must be 'weights' or 'derived', got {scope!r}"
                )
            normalized.append({"pattern": pattern, "scope": scope})
            continue
        pattern = str(entry).strip()
        if pattern:
            normalized.append(pattern)
    return normalized


def normalized_prepared_storage(value: Any) -> dict[str, Any]:
    """Validate the durable prepared-state policy without accepting secrets."""
    defaults: dict[str, Any] = {
        "enabled": False,
        "provider": "runpod",
        "policy": "smart",
        "region": "auto",
        "cold_fallback": "allow",
        "managed_size_gb": 250,
        "existing_volume_id": None,
        "max_monthly_storage_cost": None,
        "confirmed": False,
        "tenant": "default",
        "cache_private_assets": False,
        "shadow_admission": True,
    }
    if value is None:
        return defaults
    if not isinstance(value, dict):
        raise ValueError("prepared_storage must be an object")
    forbidden = {
        key for key in value
        if any(fragment in key.lower() for fragment in ("secret", "token", "api_key", "access_key"))
    }
    if forbidden:
        raise ValueError(
            "prepared_storage cannot contain credentials: " + ", ".join(sorted(forbidden))
        )
    unknown = set(value) - set(defaults)
    if unknown:
        raise ValueError("Unknown prepared_storage fields: " + ", ".join(sorted(unknown)))
    result = {**defaults, **value}
    result["enabled"] = bool(result["enabled"])
    result["confirmed"] = bool(result["confirmed"])
    result["cache_private_assets"] = bool(result["cache_private_assets"])
    result["shadow_admission"] = bool(result["shadow_admission"])
    result["provider"] = str(result["provider"]).strip().lower()
    result["policy"] = str(result["policy"]).strip().lower()
    result["region"] = str(result["region"]).strip()
    result["cold_fallback"] = str(result["cold_fallback"]).strip().lower()
    result["tenant"] = str(result["tenant"]).strip()
    if result["provider"] != "runpod":
        raise ValueError("prepared_storage.provider must currently be runpod")
    if result["policy"] not in {"off", "smart", "strict", "pinned"}:
        raise ValueError("prepared_storage.policy must be off, smart, strict, or pinned")
    if result["cold_fallback"] not in {"allow", "ask", "deny"}:
        raise ValueError("prepared_storage.cold_fallback must be allow, ask, or deny")
    if result["policy"] == "pinned" and result["region"].lower() == "auto":
        raise ValueError("prepared_storage.region is required for pinned policy")
    if not result["tenant"]:
        raise ValueError("prepared_storage.tenant cannot be empty")
    result["managed_size_gb"] = int(result["managed_size_gb"])
    if result["managed_size_gb"] < 1:
        raise ValueError("prepared_storage.managed_size_gb must be at least 1")
    if result["managed_size_gb"] > RUNPOD_NETWORK_VOLUME_MAX_GB:
        raise ValueError(
            f"prepared_storage.managed_size_gb cannot exceed {RUNPOD_NETWORK_VOLUME_MAX_GB}"
        )
    if result["existing_volume_id"] is not None:
        result["existing_volume_id"] = str(result["existing_volume_id"]).strip() or None
    budget = result["max_monthly_storage_cost"]
    if budget is not None and float(budget) < 0:
        raise ValueError("prepared_storage.max_monthly_storage_cost cannot be negative")
    result["max_monthly_storage_cost"] = None if budget is None else float(budget)
    # `off` is an explicit stateless policy even if an older UI left enabled true.
    if result["policy"] == "off":
        result["enabled"] = False
    if result["enabled"] and not result["confirmed"]:
        raise ValueError("prepared_storage must be confirmed before it is enabled")
    return result


@dataclass
class CloudConfig:
    """Configuration for Cloud Offload operations."""

    # Queue settings
    enabled: bool = False
    min_queue_depth: int = 3  # Don't spin up until N jobs waiting

    # Provider settings. RunPod is the default provider.
    # ``provider`` is resolved through the connector registry, so plugins may add
    # names without changing this dataclass.
    provider: str = "runpod"
    provider_order: list[str] = field(default_factory=lambda: ["runpod", "vast.ai"])
    routing_policy: Literal["preferred", "cheapest"] = "preferred"
    gpu_type: str = "RTX_4090"
    max_hourly_rate: float = 0.50  # USD, skip instances above this
    max_total_job_cost: float | None = None
    max_job_runtime_seconds: int = 7200
    recommendation_policy: Literal["balanced", "cheapest", "fastest", "manual"] = (
        "balanced"
    )
    rental_confirmation: Literal["always", "material_changes", "never"] = "always"
    confirmation_countdown_seconds: int = 10
    allowed_regions: list[str] = field(default_factory=list)
    material_price_change_percent: float = 5.0
    material_cost_change_percent: float = 10.0

    # Worker settings
    idle_shutdown_seconds: int = 300  # Shut down worker after idle
    keep_warm: bool = False  # Explicitly keep cloud workers alive while idle
    keep_warm_warning_seconds: int = 3600  # Warn for each idle interval while pinned
    poll_interval_seconds: int = 10
    lease_ttl_seconds: int = 300
    worker_token: str = field(
        default_factory=lambda: os.environ.get("CLOUD_OFFLOAD_WORKER_TOKEN", "")
    )
    worker_wheelhouse_url: str = field(
        default_factory=lambda: os.environ.get("CLOUD_OFFLOAD_WORKER_WHEELHOUSE_URL", "")
    )
    worker_wheelhouse_sha256: str = field(
        default_factory=lambda: os.environ.get("CLOUD_OFFLOAD_WORKER_WHEELHOUSE_SHA256", "")
    )
    worker_profiles: dict[str, dict[str, Any]] = field(default_factory=dict)
    worker_profile: str = field(
        default_factory=lambda: os.environ.get("CLOUD_OFFLOAD_WORKER_PROFILE", "")
    )
    worker_models: list[str] = field(
        default_factory=lambda: [
            item.strip()
            for item in os.environ.get("CLOUD_OFFLOAD_WORKER_MODELS", "").split(",")
            if item.strip()
        ]
    )
    worker_manifest_path: str = field(
        default_factory=lambda: os.environ.get(
            "CLOUD_OFFLOAD_WORKER_MANIFEST", DEFAULT_WORKER_MANIFEST
        )
    )
    coordinator_url: str = field(
        default_factory=lambda: os.environ.get("CLOUD_OFFLOAD_COORDINATOR_URL", "")
    )
    # Public ingress a rented worker uses to reach this coordinator.
    #   "none"        -- require an explicit coordinator_url (safe default; a
    #                    launch is refused if none is set, so nothing is exposed)
    #   "cloudflared" -- the dispatcher opens an ephemeral Cloudflare quick
    #                    tunnel and hands workers its URL. Exposes the
    #                    coordinator publicly, gated by the bearer token.
    ingress: Literal["none", "cloudflared"] = field(
        default_factory=lambda: os.environ.get("CLOUD_OFFLOAD_INGRESS", "none")
    )
    # Asset residency policy: case-insensitive glob patterns (fnmatch-style,
    # ``*`` and ``?``) naming assets that must never leave operator-controlled
    # hardware. The node pack's queue-time compiler reads this list through
    # GET /api/config and blocks cloud submission for any partition that
    # references, or depends on, a matching asset.
    #
    # An entry is a bare pattern, or ``{"pattern": ..., "scope": ...}``. Scope
    # ``derived`` (the default, and what a bare pattern means) also restricts
    # everything computed from the asset. Scope ``weights`` restricts only the
    # file itself, which is what most licences say — the weights may not be
    # redistributed, but the images they produce are yours, so a downstream
    # upscale can still be offloaded.
    on_prem_assets: list = field(
        default_factory=lambda: [
            item.strip()
            for item in os.environ.get("CLOUD_OFFLOAD_ON_PREM_ASSETS", "").split(",")
            if item.strip()
        ]
    )
    # Where a declared model file can be obtained, keyed by its sha256. A
    # partition that declares an asset this registry (or the artifact store, or
    # the target profile's pinned weights) cannot account for is refused before
    # anything is provisioned. Values are ``{repo_id, revision, filename}`` or
    # ``{url}``.
    asset_sources: dict[str, dict[str, Any]] = field(default_factory=dict)

    # Durable regional prepared state is opt-in. This object is intentionally
    # limited to policy and provider identities; credentials resolve separately.
    prepared_storage: dict[str, Any] = field(default_factory=dict)

    # Storage settings
    storage_type: Literal["local", "gcs", "s3"] = "local"
    storage_path: str = ""  # Local path or bucket name

    # Paths
    queue_db_path: str = ""

    # API keys (loaded from env)
    vast_api_key: str = field(default_factory=lambda: os.environ.get("VAST_API_KEY", ""))
    runpod_api_key: str = field(default_factory=lambda: os.environ.get("RUNPOD_API_KEY", ""))
    vast_api_url: str = field(
        default_factory=lambda: os.environ.get("VAST_API_URL", "https://console.vast.ai/api/v0")
    )
    runpod_graphql_url: str = field(
        default_factory=lambda: os.environ.get(
            "RUNPOD_GRAPHQL_URL", "https://api.runpod.io/graphql"
        )
    )
    runpod_rest_url: str = field(
        default_factory=lambda: os.environ.get("RUNPOD_REST_URL", "https://rest.runpod.io/v1")
    )
    runpod_cloud_type: Literal["SECURE", "COMMUNITY"] = "SECURE"
    runpod_container_disk_gb: int = 20
    runpod_volume_gb: int = 0
    # Ceiling on the container disk a storage plan may ask for. A partition that
    # plans past this is refused before provisioning, like every other
    # pre-flight refusal: an unnoticed 900 GB request is a bill, not a warning.
    max_container_disk_gb: int = 500
    # Opaque RunPod credential record ID. The registry password/token remains
    # stored by RunPod and is never persisted in configuration.
    runpod_registry_auth_id: str = ""
    # Non-secret per-provider settings, keyed by connector name. Lets a plugin
    # carry its own knobs without adding fields to this dataclass.
    connector_options: dict[str, Any] = field(default_factory=dict)
    # In-process credential overrides, mainly for tests. Real credentials come
    # from the environment or the OS keychain via ``api_key_for``; this is never
    # serialized by ``to_dict`` and never persisted.
    provider_credentials: dict[str, str] = field(default_factory=dict)
    gcs_credentials: str = field(
        default_factory=lambda: os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    )

    def __post_init__(self):
        self.provider = self.provider.strip().lower()
        if self.provider == "vast":
            self.provider = "vast.ai"
        self.provider_order = [
            "vast.ai" if item.strip().lower() == "vast" else item.strip().lower()
            for item in self.provider_order
            if item.strip()
        ]
        self.provider_order = list(dict.fromkeys(self.provider_order))
        if self.provider not in self.provider_order:
            self.provider_order.insert(0, self.provider)
        if self.routing_policy not in {"preferred", "cheapest"}:
            raise ValueError("routing_policy must be preferred or cheapest")
        self.max_hourly_rate = float(self.max_hourly_rate)
        if not math.isfinite(self.max_hourly_rate) or self.max_hourly_rate <= 0:
            raise ValueError("max_hourly_rate must be greater than zero")
        if self.max_total_job_cost is not None:
            self.max_total_job_cost = float(self.max_total_job_cost)
            if (
                not math.isfinite(self.max_total_job_cost)
                or self.max_total_job_cost <= 0
            ):
                raise ValueError("max_total_job_cost must be greater than zero")
        self.max_job_runtime_seconds = int(self.max_job_runtime_seconds)
        if self.max_job_runtime_seconds < 60:
            raise ValueError("max_job_runtime_seconds must be at least 60")
        self.recommendation_policy = str(self.recommendation_policy).strip().lower()
        if self.recommendation_policy not in {
            "balanced",
            "cheapest",
            "fastest",
            "manual",
        }:
            raise ValueError(
                "recommendation_policy must be balanced, cheapest, fastest, or manual"
            )
        self.rental_confirmation = str(self.rental_confirmation).strip().lower()
        if self.rental_confirmation not in {
            "always",
            "material_changes",
            "never",
        }:
            raise ValueError(
                "rental_confirmation must be always, material_changes, or never"
            )
        self.confirmation_countdown_seconds = int(
            self.confirmation_countdown_seconds
        )
        if not 0 <= self.confirmation_countdown_seconds <= 60:
            raise ValueError("confirmation_countdown_seconds must be from 0 to 60")
        self.allowed_regions = list(
            dict.fromkeys(
                str(item).strip()
                for item in self.allowed_regions or []
                if str(item).strip()
            )
        )
        for field_name in (
            "material_price_change_percent",
            "material_cost_change_percent",
        ):
            value = float(getattr(self, field_name))
            if not math.isfinite(value) or not 0 <= value <= 100:
                raise ValueError(f"{field_name} must be from 0 to 100")
            setattr(self, field_name, value)
        if self.ingress not in {"none", "cloudflared"}:
            raise ValueError("ingress must be none or cloudflared")
        self.on_prem_assets = _normalized_on_prem_assets(self.on_prem_assets)
        self.runpod_cloud_type = self.runpod_cloud_type.upper()
        if self.runpod_cloud_type not in {"SECURE", "COMMUNITY"}:
            raise ValueError("runpod_cloud_type must be SECURE or COMMUNITY")
        if self.runpod_container_disk_gb < 1:
            raise ValueError("runpod_container_disk_gb must be at least 1")
        if self.runpod_volume_gb < 0:
            raise ValueError("runpod_volume_gb cannot be negative")
        if self.max_container_disk_gb < 1:
            raise ValueError("max_container_disk_gb must be at least 1")
        if self.idle_shutdown_seconds < 1:
            raise ValueError("idle_shutdown_seconds must be at least 1")
        if self.keep_warm_warning_seconds < 60:
            raise ValueError("keep_warm_warning_seconds must be at least 60")
        self.lease_ttl_seconds = int(self.lease_ttl_seconds)
        if self.lease_ttl_seconds < 30:
            raise ValueError("lease_ttl_seconds must be at least 30")
        if not self.queue_db_path:
            self.queue_db_path = str(CONFIG_DIR / "jobs.db")
        if not self.storage_path:
            self.storage_path = str(CONFIG_DIR / "job_files")
        # Malformed pinned weights and node packs fail here, at load, not at
        # dispatch time when a worker is already being paid for. Asset sources
        # fail here for the same reason: a half-read registry would refuse jobs
        # it could serve.
        from cloud_offload.assets import normalized_asset_sources
        from cloud_offload.profiles import (
            normalized_profile_custom_nodes,
            normalized_profile_digest,
            normalized_profile_disk_gb,
            normalized_profile_weights,
        )

        for profile_name, profile in (self.worker_profiles or {}).items():
            if isinstance(profile, dict):
                normalized_profile_weights(str(profile_name), profile.get("weights"))
                normalized_profile_custom_nodes(
                    str(profile_name), profile.get("custom_nodes")
                )
                for storage_field in ("extra_disk_gb", "image_size_gb"):
                    normalized_profile_disk_gb(
                        str(profile_name), storage_field, profile.get(storage_field)
                    )
                for digest_field in (
                    "object_info_digest",
                    "dependency_lock_digest",
                ):
                    normalized_profile_digest(
                        str(profile_name), digest_field, profile.get(digest_field)
                    )
        self.asset_sources = normalized_asset_sources(self.asset_sources)
        self.prepared_storage = normalized_prepared_storage(self.prepared_storage)

    @classmethod
    def from_file(cls, path: str | Path) -> "CloudConfig":
        """Load config from JSON file (top-level fields, or nested under 'cloud')."""
        with open(path) as f:
            data = json.load(f)
        cloud_data = data.get("cloud", data)
        allowed = {item.name for item in fields(cls)}
        config = cls(**{key: value for key, value in cloud_data.items() if key in allowed})
        # Runtime services may refresh mutable policy while they run. Keep the
        # exact source private so an explicitly supplied config never starts
        # reading unrelated preferences from the default user config.
        config._source_path = Path(path)
        return config

    @classmethod
    def from_env(cls) -> "CloudConfig":
        """Load config from environment variables."""
        return cls(
            enabled=os.environ.get("CLOUD_OFFLOAD_ENABLED", "").lower() == "true",
            min_queue_depth=int(os.environ.get("CLOUD_OFFLOAD_MIN_QUEUE_DEPTH", "3")),
            provider=os.environ.get("CLOUD_OFFLOAD_PROVIDER", "runpod"),
            provider_order=[
                item.strip()
                for item in os.environ.get(
                    "CLOUD_OFFLOAD_PROVIDERS", "runpod,vast.ai"
                ).split(",")
                if item.strip()
            ],
            routing_policy=os.environ.get(
                "CLOUD_OFFLOAD_ROUTING_POLICY", "preferred"
            ).lower(),
            gpu_type=os.environ.get("CLOUD_OFFLOAD_GPU_TYPE", "RTX_4090"),
            max_hourly_rate=float(os.environ.get("CLOUD_OFFLOAD_MAX_HOURLY_RATE", "0.50")),
            max_total_job_cost=(
                float(os.environ["CLOUD_OFFLOAD_MAX_TOTAL_JOB_COST"])
                if os.environ.get("CLOUD_OFFLOAD_MAX_TOTAL_JOB_COST")
                else None
            ),
            max_job_runtime_seconds=int(
                os.environ.get("CLOUD_OFFLOAD_MAX_JOB_RUNTIME_SECONDS", "7200")
            ),
            recommendation_policy=os.environ.get(
                "CLOUD_OFFLOAD_RECOMMENDATION_POLICY", "balanced"
            ),
            rental_confirmation=os.environ.get(
                "CLOUD_OFFLOAD_RENTAL_CONFIRMATION", "always"
            ),
            confirmation_countdown_seconds=int(
                os.environ.get("CLOUD_OFFLOAD_CONFIRMATION_COUNTDOWN", "10")
            ),
            allowed_regions=[
                item.strip()
                for item in os.environ.get("CLOUD_OFFLOAD_ALLOWED_REGIONS", "").split(",")
                if item.strip()
            ],
            material_price_change_percent=float(
                os.environ.get("CLOUD_OFFLOAD_MATERIAL_PRICE_CHANGE_PERCENT", "5")
            ),
            material_cost_change_percent=float(
                os.environ.get("CLOUD_OFFLOAD_MATERIAL_COST_CHANGE_PERCENT", "10")
            ),
            idle_shutdown_seconds=int(os.environ.get("CLOUD_OFFLOAD_IDLE_SHUTDOWN", "300")),
            keep_warm=os.environ.get("CLOUD_OFFLOAD_KEEP_WARM", "").lower() == "true",
            keep_warm_warning_seconds=int(
                os.environ.get("CLOUD_OFFLOAD_KEEP_WARM_WARNING", "3600")
            ),
            poll_interval_seconds=int(os.environ.get("CLOUD_OFFLOAD_POLL_INTERVAL", "10")),
            lease_ttl_seconds=int(os.environ.get("CLOUD_OFFLOAD_LEASE_TTL", "300")),
            worker_token=os.environ.get("CLOUD_OFFLOAD_WORKER_TOKEN", ""),
            worker_wheelhouse_url=os.environ.get("CLOUD_OFFLOAD_WORKER_WHEELHOUSE_URL", ""),
            worker_wheelhouse_sha256=os.environ.get("CLOUD_OFFLOAD_WORKER_WHEELHOUSE_SHA256", ""),
            worker_profiles=json.loads(os.environ.get("CLOUD_OFFLOAD_WORKER_PROFILES_JSON", "{}")),
            worker_profile=os.environ.get("CLOUD_OFFLOAD_WORKER_PROFILE", ""),
            worker_models=[
                item.strip()
                for item in os.environ.get("CLOUD_OFFLOAD_WORKER_MODELS", "").split(",")
                if item.strip()
            ],
            worker_manifest_path=os.environ.get(
                "CLOUD_OFFLOAD_WORKER_MANIFEST", DEFAULT_WORKER_MANIFEST
            ),
            coordinator_url=os.environ.get("CLOUD_OFFLOAD_COORDINATOR_URL", ""),
            ingress=os.environ.get("CLOUD_OFFLOAD_INGRESS", "none"),
            on_prem_assets=[
                item.strip()
                for item in os.environ.get("CLOUD_OFFLOAD_ON_PREM_ASSETS", "").split(",")
                if item.strip()
            ],
            asset_sources=json.loads(os.environ.get("CLOUD_OFFLOAD_ASSET_SOURCES_JSON", "{}")),
            prepared_storage=json.loads(
                os.environ.get("CLOUD_OFFLOAD_PREPARED_STORAGE_JSON", "{}")
            ),
            storage_type=os.environ.get("CLOUD_OFFLOAD_STORAGE_TYPE", "local"),
            storage_path=os.environ.get("CLOUD_OFFLOAD_STORAGE_PATH", ""),
            vast_api_key=os.environ.get("VAST_API_KEY", ""),
            runpod_api_key=os.environ.get("RUNPOD_API_KEY", ""),
            vast_api_url=os.environ.get("VAST_API_URL", "https://console.vast.ai/api/v0"),
            runpod_graphql_url=os.environ.get(
                "RUNPOD_GRAPHQL_URL", "https://api.runpod.io/graphql"
            ),
            runpod_rest_url=os.environ.get("RUNPOD_REST_URL", "https://rest.runpod.io/v1"),
            runpod_cloud_type=os.environ.get("RUNPOD_CLOUD_TYPE", "SECURE").upper(),
            runpod_container_disk_gb=int(os.environ.get("RUNPOD_CONTAINER_DISK_GB", "20")),
            runpod_volume_gb=int(os.environ.get("RUNPOD_VOLUME_GB", "0")),
            max_container_disk_gb=int(
                os.environ.get("CLOUD_OFFLOAD_MAX_CONTAINER_DISK_GB", "500")
            ),
            runpod_registry_auth_id=os.environ.get("RUNPOD_REGISTRY_AUTH_ID", ""),
        )

    @classmethod
    def load(
        cls,
        path: str | Path | None = None,
        *,
        resolve_secrets: bool = True,
    ) -> "CloudConfig":
        """Load persisted preferences and overlay environment-owned secrets/settings."""
        config_path = Path(path) if path else CONFIG_DIR / "config.json"
        config = cls.from_file(config_path) if config_path.exists() else cls()
        config._source_path = config_path
        env_map: dict[str, tuple[str, Any]] = {
            "CLOUD_OFFLOAD_ENABLED": ("enabled", lambda value: value.lower() == "true"),
            "CLOUD_OFFLOAD_MIN_QUEUE_DEPTH": ("min_queue_depth", int),
            "CLOUD_OFFLOAD_PROVIDER": ("provider", str),
            "CLOUD_OFFLOAD_PROVIDERS": (
                "provider_order",
                lambda value: [item.strip() for item in value.split(",") if item.strip()],
            ),
            "CLOUD_OFFLOAD_ROUTING_POLICY": ("routing_policy", str),
            "CLOUD_OFFLOAD_GPU_TYPE": ("gpu_type", str),
            "CLOUD_OFFLOAD_MAX_HOURLY_RATE": ("max_hourly_rate", float),
            "CLOUD_OFFLOAD_MAX_TOTAL_JOB_COST": ("max_total_job_cost", float),
            "CLOUD_OFFLOAD_MAX_JOB_RUNTIME_SECONDS": (
                "max_job_runtime_seconds",
                int,
            ),
            "CLOUD_OFFLOAD_RECOMMENDATION_POLICY": ("recommendation_policy", str),
            "CLOUD_OFFLOAD_RENTAL_CONFIRMATION": ("rental_confirmation", str),
            "CLOUD_OFFLOAD_CONFIRMATION_COUNTDOWN": (
                "confirmation_countdown_seconds",
                int,
            ),
            "CLOUD_OFFLOAD_ALLOWED_REGIONS": (
                "allowed_regions",
                lambda value: [
                    item.strip() for item in value.split(",") if item.strip()
                ],
            ),
            "CLOUD_OFFLOAD_MATERIAL_PRICE_CHANGE_PERCENT": (
                "material_price_change_percent",
                float,
            ),
            "CLOUD_OFFLOAD_MATERIAL_COST_CHANGE_PERCENT": (
                "material_cost_change_percent",
                float,
            ),
            "CLOUD_OFFLOAD_IDLE_SHUTDOWN": ("idle_shutdown_seconds", int),
            "CLOUD_OFFLOAD_KEEP_WARM": (
                "keep_warm",
                lambda value: value.lower() == "true",
            ),
            "CLOUD_OFFLOAD_KEEP_WARM_WARNING": ("keep_warm_warning_seconds", int),
            "CLOUD_OFFLOAD_POLL_INTERVAL": ("poll_interval_seconds", int),
            "CLOUD_OFFLOAD_LEASE_TTL": ("lease_ttl_seconds", int),
            "CLOUD_OFFLOAD_WORKER_TOKEN": ("worker_token", str),
            "CLOUD_OFFLOAD_WORKER_WHEELHOUSE_URL": ("worker_wheelhouse_url", str),
            "CLOUD_OFFLOAD_WORKER_WHEELHOUSE_SHA256": ("worker_wheelhouse_sha256", str),
            "CLOUD_OFFLOAD_WORKER_PROFILES_JSON": ("worker_profiles", json.loads),
            "CLOUD_OFFLOAD_WORKER_PROFILE": ("worker_profile", str),
            "CLOUD_OFFLOAD_WORKER_MODELS": (
                "worker_models",
                lambda value: [item.strip() for item in value.split(",") if item.strip()],
            ),
            "CLOUD_OFFLOAD_WORKER_MANIFEST": ("worker_manifest_path", str),
            "CLOUD_OFFLOAD_COORDINATOR_URL": ("coordinator_url", str),
            "CLOUD_OFFLOAD_INGRESS": ("ingress", str),
            "CLOUD_OFFLOAD_ON_PREM_ASSETS": (
                "on_prem_assets",
                lambda value: [item.strip() for item in value.split(",") if item.strip()],
            ),
            "CLOUD_OFFLOAD_ASSET_SOURCES_JSON": ("asset_sources", json.loads),
            "CLOUD_OFFLOAD_PREPARED_STORAGE_JSON": ("prepared_storage", json.loads),
            "CLOUD_OFFLOAD_STORAGE_TYPE": ("storage_type", str),
            "CLOUD_OFFLOAD_STORAGE_PATH": ("storage_path", str),
            "CLOUD_OFFLOAD_QUEUE_DB": ("queue_db_path", str),
            "VAST_API_KEY": ("vast_api_key", str),
            "RUNPOD_API_KEY": ("runpod_api_key", str),
            "VAST_API_URL": ("vast_api_url", str),
            "RUNPOD_GRAPHQL_URL": ("runpod_graphql_url", str),
            "RUNPOD_REST_URL": ("runpod_rest_url", str),
            "RUNPOD_CLOUD_TYPE": ("runpod_cloud_type", str),
            "RUNPOD_CONTAINER_DISK_GB": ("runpod_container_disk_gb", int),
            "RUNPOD_VOLUME_GB": ("runpod_volume_gb", int),
            "CLOUD_OFFLOAD_MAX_CONTAINER_DISK_GB": ("max_container_disk_gb", int),
            "RUNPOD_REGISTRY_AUTH_ID": ("runpod_registry_auth_id", str),
        }
        for env_name, (field_name, converter) in env_map.items():
            if env_name in os.environ:
                setattr(config, field_name, converter(os.environ[env_name]))
        config.__post_init__()
        return config

    def to_dict(self) -> dict:
        """Convert to dictionary (excludes secrets)."""
        return {
            "enabled": self.enabled,
            "min_queue_depth": self.min_queue_depth,
            "provider": self.provider,
            "provider_order": self.provider_order,
            "routing_policy": self.routing_policy,
            "provider_auth_configured": self.provider_auth_configured,
            "huggingface_configured": self.huggingface_configured,
            "gpu_type": self.gpu_type,
            "max_hourly_rate": self.max_hourly_rate,
            "max_total_job_cost": self.max_total_job_cost,
            "max_job_runtime_seconds": self.max_job_runtime_seconds,
            "recommendation_policy": self.recommendation_policy,
            "rental_confirmation": self.rental_confirmation,
            "confirmation_countdown_seconds": self.confirmation_countdown_seconds,
            "allowed_regions": self.allowed_regions,
            "material_price_change_percent": self.material_price_change_percent,
            "material_cost_change_percent": self.material_cost_change_percent,
            "idle_shutdown_seconds": self.idle_shutdown_seconds,
            "keep_warm": self.keep_warm,
            "keep_warm_warning_seconds": self.keep_warm_warning_seconds,
            "poll_interval_seconds": self.poll_interval_seconds,
            "lease_ttl_seconds": self.lease_ttl_seconds,
            "worker_auth_configured": bool(self.worker_token),
            "worker_wheelhouse_configured": bool(self.worker_wheelhouse_url),
            "worker_profiles": self.worker_profiles,
            "worker_profile": self.worker_profile,
            "worker_models": self.worker_models,
            "worker_manifest_path": self.worker_manifest_path,
            "coordinator_configured": bool(self.coordinator_url),
            "ingress": self.ingress,
            "on_prem_assets": self.on_prem_assets,
            "asset_sources": self.asset_sources,
            "prepared_storage": self.prepared_storage,
            "storage_type": self.storage_type,
            "storage_path": self.storage_path,
            "queue_db_path": self.queue_db_path,
            "runpod_cloud_type": self.runpod_cloud_type,
            "runpod_container_disk_gb": self.runpod_container_disk_gb,
            "runpod_volume_gb": self.runpod_volume_gb,
            "max_container_disk_gb": self.max_container_disk_gb,
            "runpod_registry_auth_id": self.runpod_registry_auth_id,
            "vast_api_url": self.vast_api_url,
            "runpod_graphql_url": self.runpod_graphql_url,
            "runpod_rest_url": self.runpod_rest_url,
            "connector_options": self.connector_options,
        }

    @property
    def provider_auth_configured(self) -> bool:
        """Whether the selected connector has credentials."""
        return bool(self.api_key_for(self.provider))

    @property
    def huggingface_configured(self) -> bool:
        """Whether a Hugging Face token resolves, without exposing it."""
        from cloud_offload.credentials import huggingface_token

        return bool(huggingface_token())

    def api_key_for(self, provider: str) -> str:
        """Return a connector credential without serializing it.

        Delegates to :mod:`cloud_offload.credentials`, which resolves the
        generic ``CLOUD_OFFLOAD_<PROVIDER>_API_KEY`` environment variable first
        (the only option on a headless worker), then the OS keychain, then the
        legacy plaintext file. The two legacy typed fields are still honoured so
        existing ``VAST_API_KEY`` / ``RUNPOD_API_KEY`` setups keep working.
        """
        from cloud_offload.credentials import get_credential

        provider = normalize_provider_name(provider)
        if provider == "vast.ai" and self.vast_api_key:
            return self.vast_api_key
        if provider == "runpod" and self.runpod_api_key:
            return self.runpod_api_key
        # An explicit in-process override beats ambient sources.
        override = str(self.provider_credentials.get(provider, "")).strip()
        return override or get_credential(provider)

    def settings_for(self, provider: str) -> dict:
        """Return non-secret per-provider settings for a connector."""
        return dict(self.connector_options.get(normalize_provider_name(provider), {}))

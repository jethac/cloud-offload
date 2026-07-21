"""Cloud Offload configuration."""

import json
import os
import subprocess
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Literal


_BWS_CREDENTIAL_CACHE: dict[str, str] | None = None

# Directory that holds the persisted config, queue database, tokens and the
# service-discovery file. Overridable through CLOUD_OFFLOAD_HOME.
CONFIG_DIR = Path(
    os.environ.get("CLOUD_OFFLOAD_HOME", str(Path.home() / ".cloud-offload"))
)
DEFAULT_WORKER_MANIFEST = "/opt/cloud-offload/runtime-profile.json"

# Credentials for connectors that have no typed field on ``CloudConfig`` live
# here rather than in config.json, so secrets never round-trip through the
# config API.
CREDENTIALS_FILE = CONFIG_DIR / "credentials.json"


def normalize_provider_name(provider: str) -> str:
    """Canonicalize a provider name (``vast`` is an alias of ``vast.ai``)."""
    normalized = str(provider or "").strip().lower()
    return "vast.ai" if normalized == "vast" else normalized


def provider_env_var(provider: str) -> str:
    """Environment variable holding a generic connector credential."""
    slug = "".join(
        character if character.isalnum() else "_"
        for character in normalize_provider_name(provider)
    ).strip("_")
    return f"CLOUD_OFFLOAD_{slug.upper()}_API_KEY"


def load_provider_credentials() -> dict[str, str]:
    """Read the credential file, tolerating absence or corruption."""
    try:
        payload = json.loads(CREDENTIALS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        normalize_provider_name(name): str(value)
        for name, value in payload.items()
        if isinstance(name, str) and isinstance(value, str) and value.strip()
    }


def save_provider_credential(provider: str, api_key: str) -> None:
    """Persist (or clear) one connector credential with owner-only permissions."""
    provider = normalize_provider_name(provider)
    if not provider:
        raise ValueError("Provider name is required")
    credentials = load_provider_credentials()
    if api_key.strip():
        credentials[provider] = api_key.strip()
    else:
        credentials.pop(provider, None)
    CREDENTIALS_FILE.parent.mkdir(parents=True, exist_ok=True)
    CREDENTIALS_FILE.write_text(
        json.dumps(credentials, indent=2, sort_keys=True), encoding="utf-8"
    )
    try:
        CREDENTIALS_FILE.chmod(0o600)
    except OSError:  # pragma: no cover - best effort on platforms without POSIX modes
        pass


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

    # Worker settings
    idle_shutdown_seconds: int = 300  # Shut down worker after idle
    keep_warm: bool = False  # Explicitly keep cloud workers alive while idle
    keep_warm_warning_seconds: int = 3600  # Warn for each idle interval while pinned
    poll_interval_seconds: int = 10
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
    # Opaque RunPod credential record ID. The registry password/token remains
    # stored by RunPod and is never persisted in configuration.
    runpod_registry_auth_id: str = ""
    # Non-secret per-provider settings, keyed by connector name. Lets a plugin
    # carry its own knobs without adding fields to this dataclass.
    connector_options: dict[str, Any] = field(default_factory=dict)
    # Credentials for connectors without a typed field, loaded from the
    # credential file. Never serialized by ``to_dict``.
    provider_credentials: dict[str, str] = field(default_factory=load_provider_credentials)
    bws_secrets: bool = False
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
        self.runpod_cloud_type = self.runpod_cloud_type.upper()
        if self.runpod_cloud_type not in {"SECURE", "COMMUNITY"}:
            raise ValueError("runpod_cloud_type must be SECURE or COMMUNITY")
        if self.runpod_container_disk_gb < 1:
            raise ValueError("runpod_container_disk_gb must be at least 1")
        if self.runpod_volume_gb < 0:
            raise ValueError("runpod_volume_gb cannot be negative")
        if self.idle_shutdown_seconds < 1:
            raise ValueError("idle_shutdown_seconds must be at least 1")
        if self.keep_warm_warning_seconds < 60:
            raise ValueError("keep_warm_warning_seconds must be at least 60")
        if not self.queue_db_path:
            self.queue_db_path = str(CONFIG_DIR / "jobs.db")
        if not self.storage_path:
            self.storage_path = str(CONFIG_DIR / "job_files")

    @classmethod
    def from_file(cls, path: str | Path) -> "CloudConfig":
        """Load config from JSON file (top-level fields, or nested under 'cloud')."""
        with open(path) as f:
            data = json.load(f)
        cloud_data = data.get("cloud", data)
        allowed = {item.name for item in fields(cls)}
        return cls(**{key: value for key, value in cloud_data.items() if key in allowed})

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
            idle_shutdown_seconds=int(os.environ.get("CLOUD_OFFLOAD_IDLE_SHUTDOWN", "300")),
            keep_warm=os.environ.get("CLOUD_OFFLOAD_KEEP_WARM", "").lower() == "true",
            keep_warm_warning_seconds=int(
                os.environ.get("CLOUD_OFFLOAD_KEEP_WARM_WARNING", "3600")
            ),
            poll_interval_seconds=int(os.environ.get("CLOUD_OFFLOAD_POLL_INTERVAL", "10")),
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
            runpod_registry_auth_id=os.environ.get("RUNPOD_REGISTRY_AUTH_ID", ""),
            bws_secrets=os.environ.get("CLOUD_OFFLOAD_BWS_SECRETS", "").lower() == "true",
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
            "CLOUD_OFFLOAD_IDLE_SHUTDOWN": ("idle_shutdown_seconds", int),
            "CLOUD_OFFLOAD_KEEP_WARM": (
                "keep_warm",
                lambda value: value.lower() == "true",
            ),
            "CLOUD_OFFLOAD_KEEP_WARM_WARNING": ("keep_warm_warning_seconds", int),
            "CLOUD_OFFLOAD_POLL_INTERVAL": ("poll_interval_seconds", int),
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
            "RUNPOD_REGISTRY_AUTH_ID": ("runpod_registry_auth_id", str),
            "CLOUD_OFFLOAD_BWS_SECRETS": (
                "bws_secrets",
                lambda value: value.lower() == "true",
            ),
        }
        for env_name, (field_name, converter) in env_map.items():
            if env_name in os.environ:
                setattr(config, field_name, converter(os.environ[env_name]))
        config.__post_init__()
        config._bws_resolution_deferred = bool(config.bws_secrets and not resolve_secrets)
        if resolve_secrets:
            config._load_bws_credentials()
        return config

    def _load_bws_credentials(self) -> None:
        """Resolve provider keys from Bitwarden Secrets Manager when enabled."""
        global _BWS_CREDENTIAL_CACHE
        if not self.bws_secrets or (self.vast_api_key and self.runpod_api_key):
            return
        if _BWS_CREDENTIAL_CACHE is None:
            try:
                completed = subprocess.run(
                    ["bws", "secret", "list", "--output", "json"],
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=15,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                _BWS_CREDENTIAL_CACHE = {
                    item.get("key"): item.get("value", "")
                    for item in json.loads(completed.stdout)
                    if isinstance(item, dict)
                    and item.get("key") in {"VAST_API_KEY", "RUNPOD_API_KEY"}
                }
            except (OSError, ValueError, subprocess.SubprocessError):
                return
        secrets = _BWS_CREDENTIAL_CACHE
        if not self.vast_api_key:
            self.vast_api_key = secrets.get("VAST_API_KEY", "")
        if not self.runpod_api_key:
            self.runpod_api_key = secrets.get("RUNPOD_API_KEY", "")

    def to_dict(self) -> dict:
        """Convert to dictionary (excludes secrets)."""
        return {
            "enabled": self.enabled,
            "min_queue_depth": self.min_queue_depth,
            "provider": self.provider,
            "provider_order": self.provider_order,
            "routing_policy": self.routing_policy,
            "provider_auth_configured": self.provider_auth_configured,
            "bws_secrets": self.bws_secrets,
            "gpu_type": self.gpu_type,
            "max_hourly_rate": self.max_hourly_rate,
            "idle_shutdown_seconds": self.idle_shutdown_seconds,
            "keep_warm": self.keep_warm,
            "keep_warm_warning_seconds": self.keep_warm_warning_seconds,
            "poll_interval_seconds": self.poll_interval_seconds,
            "worker_auth_configured": bool(self.worker_token),
            "worker_wheelhouse_configured": bool(self.worker_wheelhouse_url),
            "worker_profiles": self.worker_profiles,
            "worker_profile": self.worker_profile,
            "worker_models": self.worker_models,
            "worker_manifest_path": self.worker_manifest_path,
            "coordinator_configured": bool(self.coordinator_url),
            "storage_type": self.storage_type,
            "storage_path": self.storage_path,
            "queue_db_path": self.queue_db_path,
            "runpod_cloud_type": self.runpod_cloud_type,
            "runpod_container_disk_gb": self.runpod_container_disk_gb,
            "runpod_volume_gb": self.runpod_volume_gb,
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

    def api_key_for(self, provider: str) -> str:
        """Return a connector credential without serializing it.

        Resolution order, so that third-party connectors work without editing
        this class: the legacy typed fields for the two built-ins, then the
        generic ``CLOUD_OFFLOAD_<PROVIDER>_API_KEY`` environment variable, then
        the credential file written by the settings API.
        """
        provider = normalize_provider_name(provider)
        if provider == "vast.ai" and self.vast_api_key:
            return self.vast_api_key
        if provider == "runpod" and self.runpod_api_key:
            return self.runpod_api_key
        env_key = os.environ.get(provider_env_var(provider), "").strip()
        if env_key:
            return env_key
        return str(self.provider_credentials.get(provider, "")).strip()

    def settings_for(self, provider: str) -> dict:
        """Return non-secret per-provider settings for a connector."""
        return dict(self.connector_options.get(normalize_provider_name(provider), {}))

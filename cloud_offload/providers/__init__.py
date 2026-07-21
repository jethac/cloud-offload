"""Registry and factory for pluggable cloud compute connectors.

Third-party connectors can register a factory at runtime. A factory accepts a
``CloudConfig`` and returns a ``CloudConnector`` instance. RunPod is registered
as the default provider; Vast.ai is the worked "add a provider" example.
"""

from collections.abc import Callable
from typing import TYPE_CHECKING

from cloud_offload.providers.base import CloudConnector, CloudProvider, Instance

if TYPE_CHECKING:
    from cloud_offload.config import CloudConfig

ConnectorFactory = Callable[["CloudConfig"], CloudConnector]

_CONNECTORS: dict[str, ConnectorFactory] = {}
_CANONICAL_NAMES: dict[str, str] = {}
_METADATA: dict[str, dict] = {}


def _normalize(name: str) -> str:
    return name.strip().lower()


def register_connector(
    name: str,
    factory: ConnectorFactory,
    *,
    aliases: tuple[str, ...] = (),
    replace: bool = False,
    display_name: str | None = None,
    kind: str = "builtin",
    settings_schema: list[dict] | None = None,
) -> None:
    """Register a connector factory under a canonical name and aliases.

    ``display_name``, ``kind`` (``builtin`` | ``plugin`` | ``declarative``) and
    ``settings_schema`` are presentation metadata: they let a settings UI render
    fields for a connector it has never heard of.
    """
    canonical = _normalize(name)
    if not canonical:
        raise ValueError("Connector name cannot be empty")

    names = (canonical, *(_normalize(alias) for alias in aliases))
    for candidate in names:
        if not candidate:
            raise ValueError("Connector alias cannot be empty")
        if candidate in _CONNECTORS and not replace:
            raise ValueError(f"Connector already registered: {candidate}")

    for candidate in names:
        _CONNECTORS[candidate] = factory
        _CANONICAL_NAMES[candidate] = canonical
    _METADATA[canonical] = {
        "display_name": display_name or canonical,
        "kind": kind,
        "settings_schema": list(settings_schema or []),
    }


def connector_metadata(name: str) -> dict:
    """Return presentation metadata for a connector name."""
    canonical = _CANONICAL_NAMES.get(_normalize(name))
    if canonical is None:
        return {"display_name": name, "kind": "unknown", "registered": False,
                "settings_schema": []}
    return {**_METADATA.get(canonical, {}), "registered": True}


def create_connector(name: str, config: "CloudConfig") -> CloudConnector:
    """Construct a registered connector from Cloud Offload configuration."""
    normalized = _normalize(name)
    try:
        factory = _CONNECTORS[normalized]
    except KeyError as exc:
        supported = ", ".join(connector_names()) or "none"
        raise ValueError(
            f"Unsupported cloud connector: {name}. Registered connectors: {supported}"
        ) from exc
    connector = factory(config)
    if not isinstance(connector, CloudConnector):
        raise TypeError(f"Connector factory {normalized!r} did not return CloudConnector")
    return connector


def connector_names() -> tuple[str, ...]:
    """Return canonical registered connector names."""
    return tuple(sorted(set(_CANONICAL_NAMES.values())))


def _create_vast(config: "CloudConfig") -> CloudConnector:
    from cloud_offload.providers.vast import VastConnector

    return VastConnector(api_key=config.vast_api_key, base_url=config.vast_api_url)


def _create_runpod(config: "CloudConfig") -> CloudConnector:
    from cloud_offload.providers.runpod import RunPodConnector

    return RunPodConnector(
        api_key=config.runpod_api_key,
        graphql_url=config.runpod_graphql_url,
        rest_url=config.runpod_rest_url,
        cloud_type=config.runpod_cloud_type,
        container_disk_gb=config.runpod_container_disk_gb,
        volume_gb=config.runpod_volume_gb,
        registry_auth_id=config.runpod_registry_auth_id,
    )


register_connector(
    "runpod",
    _create_runpod,
    display_name="RunPod",
    settings_schema=[
        {"key": "cloud_type", "label": "Cloud type", "type": "enum",
         "options": ["SECURE", "COMMUNITY"], "default": "SECURE"},
        {"key": "container_disk_gb", "label": "Container disk (GB)", "type": "int",
         "default": 20},
        {"key": "registry_auth_id", "label": "Registry credential ID", "type": "string",
         "help": "Required only for private container images"},
    ],
)
register_connector(
    "vast.ai",
    _create_vast,
    aliases=("vast",),
    display_name="Vast.ai",
    settings_schema=[
        {"key": "api_url", "label": "API base URL", "type": "string",
         "default": "https://console.vast.ai/api/v0"},
    ],
)

__all__ = [
    "CloudConnector",
    "CloudProvider",
    "ConnectorFactory",
    "Instance",
    "connector_metadata",
    "connector_names",
    "create_connector",
    "register_connector",
]

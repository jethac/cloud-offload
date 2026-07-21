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


def _normalize(name: str) -> str:
    return name.strip().lower()


def register_connector(
    name: str,
    factory: ConnectorFactory,
    *,
    aliases: tuple[str, ...] = (),
    replace: bool = False,
) -> None:
    """Register a connector factory under a canonical name and aliases."""
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


register_connector("runpod", _create_runpod)
register_connector("vast.ai", _create_vast, aliases=("vast",))

__all__ = [
    "CloudConnector",
    "CloudProvider",
    "ConnectorFactory",
    "Instance",
    "connector_names",
    "create_connector",
    "register_connector",
]

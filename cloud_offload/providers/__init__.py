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


def _create_runpod(config: "CloudConfig") -> CloudConnector:
    from cloud_offload.providers.runpod import RunPodConnector

    return RunPodConnector(
        # Through api_key_for, not the raw field: the credential may live in the
        # OS keychain or a generic env var rather than on the dataclass.
        api_key=config.api_key_for("runpod"),
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

def _register_builtin_specs() -> None:
    """Register the declarative specs we ship, at import time.

    Vast.ai is served by ``specs/vast.json`` rather than a coded connector: its
    API is plain REST, so the declarative engine covers it, and running a shipped
    provider through that engine keeps it honest instead of letting it rot as a
    demo. RunPod stays coded because its offers, pod creation and balance are
    GraphQL, which no REST spec can express.

    Only built-in specs load here. User specs in ``CONFIG_DIR/providers`` are
    third-party input and load through ``plugins.load_connector_plugins()``
    alongside connector plugins, so importing this package never reads user
    files.
    """
    try:
        from cloud_offload.providers.declarative import register_declarative_providers

        register_declarative_providers(include_builtin=True, include_user=False)
    except Exception as exc:  # noqa: BLE001 - a bad spec must not break imports
        import logging

        logging.getLogger("cloud-offload").warning(
            f"Built-in provider specs unavailable: {exc}"
        )


_register_builtin_specs()

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

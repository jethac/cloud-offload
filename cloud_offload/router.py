"""Provider selection for cloud jobs."""

from dataclasses import dataclass
from typing import Any

from cloud_offload.config import CloudConfig
from cloud_offload.providers import connector_metadata, create_connector
from cloud_offload.profiles import (
    cloud_profiles_for_model,
    configured_worker_profiles,
    worker_profile_gpu_type,
    worker_profile_min_gpu_ram,
)


@dataclass(frozen=True)
class Route:
    provider: str
    offer: dict[str, Any] | None = None
    profile: dict[str, Any] | None = None


def _configured(config: CloudConfig, provider: str) -> bool:
    return bool(config.api_key_for(provider))


def select_provider(
    config: CloudConfig,
    requested: str | None = None,
    model: str | None = None,
) -> Route:
    """Resolve an explicit provider or apply the configured routing policy."""
    if requested and requested.lower() not in {"auto", "cloud"}:
        name = requested.strip().lower()
        if name == "vast":
            name = "vast.ai"
        if not _configured(config, name):
            raise ValueError(f"Cloud provider is not configured: {name}")
        create_connector(name, config)
        profiles = cloud_profiles_for_model(config, model, name) if model else []
        if model and not profiles:
            raise ValueError(
                f"No configured {name} worker profile supports model: {model}"
            )
        return Route(name, profile=profiles[0] if profiles else None)

    candidates = [name for name in config.provider_order if _configured(config, name)]
    if model:
        candidates = [
            name
            for name in candidates
            if cloud_profiles_for_model(config, model, name)
        ]
    if not candidates:
        if model:
            raise ValueError(
                f"No configured cloud worker profile supports model: {model}"
            )
        raise ValueError("No cloud provider credentials are configured")
    if config.routing_policy == "preferred":
        profile = cloud_profiles_for_model(config, model, candidates[0])[0] if model else None
        if profile:
            preferred = [name for name in profile["providers"] if name in candidates]
            if preferred:
                return Route(preferred[0], profile=profile)
        return Route(candidates[0], profile=profile)

    offers: list[tuple[float, str, dict[str, Any]]] = []
    errors: list[str] = []
    for name in candidates:
        try:
            profile = cloud_profiles_for_model(config, model, name)[0] if model else None
            minimum_vram = (
                worker_profile_min_gpu_ram(profile) if profile else 24
            )
            gpu_type = (
                worker_profile_gpu_type(profile, config.gpu_type) if profile else None
            )
            offer = create_connector(name, config).find_cheapest(
                gpu_type=gpu_type,
                min_gpu_ram=minimum_vram,
                max_hourly_rate=config.max_hourly_rate,
            )
            if offer:
                offers.append((float(offer["hourly_rate"]), name, offer))
        except Exception as exc:
            errors.append(f"{name}: {exc}")
    if not offers:
        detail = f" ({'; '.join(errors)})" if errors else ""
        raise ValueError(f"No matching cloud offers are available{detail}")
    _, name, offer = min(offers, key=lambda item: item[0])
    profile = cloud_profiles_for_model(config, model, name)[0] if model else None
    return Route(name, offer, profile)


def _profile_providing(profiles: dict, capability: str) -> dict | None:
    """Resolve a capability such as ``comfyui-partition-v1`` to a profile.

    A client knows which capability its job needs; it cannot know what the
    operator called their profiles. Accepting either keeps the wire contract
    honest without making every box hardcode a local name.
    """
    matches = [
        profile
        for _, profile in sorted(profiles.items())
        if capability in profile.get("models", [])
    ]
    return matches[0] if matches else None


def select_profile_provider(
    config: CloudConfig,
    profile_name: str,
    requested: str | None = None,
    *,
    residency: str = "cloud",
) -> Route:
    """Select a provider for a worker profile such as the ComfyUI runner.

    ``residency`` is the job's requirement, not a preference: ``"on-prem"``
    restricts selection to connectors registered with that residency class, so
    a partition tainted by on-prem-only assets can never route to rented
    hardware even if a client asks for it by name.
    """
    profiles = configured_worker_profiles(config)
    profile = profiles.get(profile_name) or _profile_providing(profiles, profile_name)
    if not profile:
        known = ", ".join(sorted(profiles)) or "none"
        raise ValueError(
            f"No worker profile named or providing {profile_name!r} is configured "
            f"(configured profiles: {known})"
        )
    supported = [
        name
        for name in profile["providers"]
        if name in config.provider_order and _configured(config, name)
    ]
    if residency == "on-prem":
        supported = [
            name
            for name in supported
            if connector_metadata(name).get("residency_class") == "on-prem"
        ]
        if not supported:
            raise ValueError(
                "Partition requires on-prem execution (on-prem-only assets) "
                "but no on-prem backend is registered"
            )
    if requested and requested.lower() not in {"auto", "cloud"}:
        name = "vast.ai" if requested.lower() == "vast" else requested.lower()
        if name not in supported:
            raise ValueError(
                f"Cloud provider {name} is not configured for profile {profile_name}"
            )
        return Route(name, profile=profile)
    if not supported:
        raise ValueError(f"No configured provider supports profile: {profile_name}")
    if config.routing_policy == "preferred":
        return Route(supported[0], profile=profile)

    offers: list[tuple[float, str, dict[str, Any]]] = []
    for name in supported:
        connector = create_connector(name, config)
        offer = connector.find_cheapest(
            gpu_type=worker_profile_gpu_type(profile, config.gpu_type),
            min_gpu_ram=worker_profile_min_gpu_ram(profile),
            max_hourly_rate=config.max_hourly_rate,
        )
        if offer:
            offers.append((float(offer["hourly_rate"]), name, offer))
    if not offers:
        raise ValueError(f"No matching cloud offers are available for {profile_name}")
    _, name, offer = min(offers, key=lambda item: item[0])
    return Route(name, offer, profile)

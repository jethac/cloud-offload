"""Provider credential storage backed by the operating system keychain.

Credentials never belong in ``config.json`` (it is served by the config API) and
they no longer belong in a plaintext file either. They live in the OS keychain:
Windows Credential Manager (DPAPI), macOS Keychain, or Secret Service on Linux.

Resolution order used by :func:`get_credential`:

1. ``CLOUD_OFFLOAD_<PROVIDER>_API_KEY`` in the environment. This stays first and
   is not going away: a headless runner, a container, or CI has no keychain to
   unlock, so an env var is the only workable escape hatch there.
2. The OS keychain.
3. The legacy plaintext ``~/.cloud-offload/credentials.json``, which earlier
   versions wrote. Reading one warns; :func:`migrate_legacy_file` moves them into
   the keychain and deletes the file.

A machine with no usable keychain backend (common in containers) degrades to
"env var only" with a clear warning rather than silently failing to persist.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

#: Keychain service name. Entries appear under this in Credential Manager.
KEYCHAIN_SERVICE = "cloud-offload"

#: Named credential (not a connector) holding the Hugging Face Hub token that
#: workers use to download gated profile weights.
HUGGINGFACE_CREDENTIAL = "huggingface"

# RunPod's S3-compatible network-volume API uses a credential pair that is
# deliberately separate from the ordinary RunPod control-plane token.  Keep
# both halves as named keychain entries; neither is a connector and neither may
# enter config.json.
RUNPOD_S3_ACCESS_CREDENTIAL = "runpod-s3-access-key"
RUNPOD_S3_SECRET_CREDENTIAL = "runpod-s3-secret-key"

__all__ = [
    "HUGGINGFACE_CREDENTIAL",
    "RUNPOD_S3_ACCESS_CREDENTIAL",
    "RUNPOD_S3_SECRET_CREDENTIAL",
    "KEYCHAIN_SERVICE",
    "KeychainUnavailable",
    "delete_credential",
    "get_credential",
    "huggingface_token",
    "keychain_status",
    "legacy_credentials_file",
    "list_credentialed_providers",
    "migrate_legacy_file",
    "normalize_provider_name",
    "provider_env_var",
    "set_credential",
]


class KeychainUnavailable(RuntimeError):
    """No usable OS keychain backend is available on this machine."""


def normalize_provider_name(provider: str) -> str:
    """Canonicalize a provider name (``vast`` is an alias of ``vast.ai``)."""
    normalized = str(provider or "").strip().lower()
    return "vast.ai" if normalized == "vast" else normalized


def provider_env_var(provider: str) -> str:
    """Environment variable holding a connector credential."""
    slug = "".join(
        character if character.isalnum() else "_"
        for character in normalize_provider_name(provider)
    ).strip("_")
    return f"CLOUD_OFFLOAD_{slug.upper()}_API_KEY"


def legacy_credentials_file() -> Path:
    """The plaintext file earlier versions wrote, if it still exists.

    Read through the config module rather than recomputed, so redirecting
    ``config.CREDENTIALS_FILE`` in a test redirects this too.
    """
    from cloud_offload import config

    return Path(config.CREDENTIALS_FILE)


def _keyring():
    """Return the keyring module, or raise if it cannot store anything.

    ``keyring`` installs a null backend when no OS service is reachable, which
    accepts writes and returns nothing. Treat that as unavailable rather than
    letting a credential silently evaporate.
    """
    try:
        import keyring
        from keyring.backends.fail import Keyring as FailKeyring
    except ImportError as exc:  # pragma: no cover - dependency is declared
        raise KeychainUnavailable(
            "the 'keyring' package is not installed; set "
            "CLOUD_OFFLOAD_<PROVIDER>_API_KEY instead"
        ) from exc

    backend = keyring.get_keyring()
    if isinstance(backend, FailKeyring):
        raise KeychainUnavailable(
            "no OS keychain backend is available on this machine; set "
            "CLOUD_OFFLOAD_<PROVIDER>_API_KEY instead"
        )
    return keyring


def keychain_status() -> dict:
    """Describe the keychain backend, for diagnostics and the settings UI."""
    try:
        backend = _keyring().get_keyring()
    except KeychainUnavailable as exc:
        return {"available": False, "backend": None, "reason": str(exc)}
    return {
        "available": True,
        "backend": f"{type(backend).__module__}.{type(backend).__name__}",
        "reason": None,
    }


def _read_legacy_file() -> dict[str, str]:
    try:
        payload = json.loads(legacy_credentials_file().read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        normalize_provider_name(name): str(value)
        for name, value in payload.items()
        if isinstance(name, str) and isinstance(value, str) and value.strip()
    }


def get_credential(provider: str) -> str:
    """Resolve one provider credential. Returns "" when there is none."""
    provider = normalize_provider_name(provider)
    if not provider:
        return ""

    from_env = os.environ.get(provider_env_var(provider), "").strip()
    if from_env:
        return from_env

    try:
        stored = _keyring().get_password(KEYCHAIN_SERVICE, provider)
    except KeychainUnavailable:
        stored = None
    except Exception as exc:  # noqa: BLE001 - a locked keychain must not crash routing
        logger.warning(f"Could not read {provider} credential from the keychain: {exc}")
        stored = None
    if stored and stored.strip():
        return stored.strip()

    legacy = _read_legacy_file().get(provider, "").strip()
    if legacy:
        logger.warning(
            f"{provider} credential is still in the plaintext file "
            f"{legacy_credentials_file()}; run migrate_legacy_file() to move it "
            "into the OS keychain"
        )
    return legacy


def huggingface_token() -> str:
    """Resolve the Hugging Face Hub token. Returns "" when there is none.

    ``HF_TOKEN`` stays first: it is the variable ``huggingface_hub`` itself
    reads, the same way the legacy ``RUNPOD_API_KEY`` stays authoritative for
    its connector. Then the generic ``CLOUD_OFFLOAD_HUGGINGFACE_API_KEY``, then
    the keychain entry named ``huggingface``.
    """
    from_env = os.environ.get("HF_TOKEN", "").strip()
    if from_env:
        return from_env
    return get_credential(HUGGINGFACE_CREDENTIAL)


def set_credential(provider: str, api_key: str) -> None:
    """Store (or clear) one provider credential in the OS keychain."""
    provider = normalize_provider_name(provider)
    if not provider:
        raise ValueError("Provider name is required")
    keyring = _keyring()
    if api_key.strip():
        keyring.set_password(KEYCHAIN_SERVICE, provider, api_key.strip())
    else:
        delete_credential(provider)


def delete_credential(provider: str) -> bool:
    """Remove a credential from the keychain and the legacy file."""
    provider = normalize_provider_name(provider)
    removed = False
    try:
        keyring = _keyring()
        if keyring.get_password(KEYCHAIN_SERVICE, provider) is not None:
            keyring.delete_password(KEYCHAIN_SERVICE, provider)
            removed = True
    except KeychainUnavailable:
        pass
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"Could not delete {provider} credential: {exc}")

    legacy = _read_legacy_file()
    if provider in legacy:
        legacy.pop(provider)
        _write_legacy_file(legacy)
        removed = True
    return removed


def _write_legacy_file(credentials: dict[str, str]) -> None:
    path = legacy_credentials_file()
    if not credentials:
        path.unlink(missing_ok=True)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(credentials, indent=2, sort_keys=True), encoding="utf-8")


def list_credentialed_providers(known: tuple[str, ...] = ()) -> list[str]:
    """Providers that currently resolve a credential from any source.

    Keychains cannot be enumerated portably, so callers pass the provider names
    they care about (the registry's) and this reports which of them resolve.
    """
    candidates = {normalize_provider_name(name) for name in known}
    candidates.update(_read_legacy_file())
    return sorted(name for name in candidates if name and get_credential(name))


def migrate_legacy_file(*, delete: bool = True) -> dict:
    """Move credentials out of the plaintext file into the OS keychain.

    Returns ``{"migrated": [...], "failed": [...], "removed_file": bool}``. The
    file is only deleted when every credential in it was stored successfully, so
    a partial failure never loses a key.
    """
    legacy = _read_legacy_file()
    if not legacy:
        return {"migrated": [], "failed": [], "removed_file": False}

    migrated: list[str] = []
    failed: list[dict] = []
    for provider, api_key in sorted(legacy.items()):
        try:
            set_credential(provider, api_key)
            migrated.append(provider)
        except Exception as exc:  # noqa: BLE001 - report, never lose the key
            failed.append({"provider": provider, "error": str(exc)})

    removed = False
    if delete and not failed:
        legacy_credentials_file().unlink(missing_ok=True)
        removed = True
    if migrated:
        logger.info(
            f"Migrated {len(migrated)} credential(s) into the OS keychain"
            + ("; removed the plaintext file" if removed else "")
        )
    return {"migrated": migrated, "failed": failed, "removed_file": removed}

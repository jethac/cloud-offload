"""Declared partition assets: proving a runner can be given the right bytes.

A compiled partition may declare the model files it references, each pinned by
sha256 (the node pack builds that manifest inside ComfyUI, where the files
actually live). This module answers the question that has to be settled before
any money is spent: for every declared file, can this coordinator put those
exact bytes on a runner?

Three answers count as yes — a registered source keyed by digest, a copy already
in the coordinator's artifact store, or a name match against the target
profile's pinned ``weights``. The last one is the legacy path and is reported as
such: it matches on ``(category, filename)``, which is exactly the identity that
lets two different sets of weights share a name.

A no is a refusal, not a warning. Renting a GPU and discovering the model is
missing costs real money; refusing at submission costs nothing.
"""

from __future__ import annotations

from typing import Any

from cloud_offload.profiles import require_models_relative
from cloud_offload.storage import partition_artifact_key

# Serialization families the node pack reports, mirrored here so a malformed
# manifest is rejected at the door rather than confusing a runner.
ASSET_FORMATS = frozenset({"safetensors", "pickle", "other"})

NAME_MATCHED_WARNING = (
    "matched by name against the worker profile's pinned weights, not by digest; "
    "the runner may hold different bytes under this filename"
)

UNRESOLVED_REMEDY = (
    "Register a source for it in asset_sources, or upload the file to the "
    "coordinator's artifact store."
)


def _normalized_digest(value: Any, label: str) -> str:
    digest = str(value or "").strip().lower()
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValueError(f"{label}: sha256 must be a 64-character hex digest")
    return digest


def normalized_asset_sources(entries: Any) -> dict[str, dict[str, Any]]:
    """Validate the ``asset_sources`` registry: sha256 -> where to fetch it.

    An entry is either a pinned Hugging Face file (``repo_id``, ``revision``,
    ``filename``) or a direct ``url``. Malformed entries raise, naming the
    digest: a source registry that silently drops half its entries would send a
    partition to provisioning on a promise it cannot keep.
    """
    if entries is None:
        return {}
    if not isinstance(entries, dict):
        raise ValueError("asset_sources must be an object keyed by sha256 digest")
    normalized: dict[str, dict[str, Any]] = {}
    for key, entry in entries.items():
        label = f"asset_sources[{str(key)!r}]"
        digest = _normalized_digest(key, label)
        if not isinstance(entry, dict):
            raise ValueError(f"{label} must be an object")
        url = str(entry.get("url") or "").strip()
        repo_id = str(entry.get("repo_id") or "").strip()
        revision = str(entry.get("revision") or "").strip()
        filename = str(entry.get("filename") or "").strip()
        if url and (repo_id or revision or filename):
            raise ValueError(
                f"{label}: give either url or repo_id/revision/filename, not both"
            )
        if url:
            if not url.startswith(("http://", "https://")):
                raise ValueError(f"{label}: url must be http or https")
            normalized[digest] = {"url": url}
            continue
        if not repo_id:
            raise ValueError(f"{label}: repo_id is required (or a url)")
        if not revision:
            raise ValueError(
                f"{label}: revision is required; pin a commit hash, not a branch"
            )
        if not filename:
            raise ValueError(f"{label}: filename is required")
        require_models_relative(label, "filename", filename)
        normalized[digest] = {
            "repo_id": repo_id,
            "revision": revision,
            "filename": filename,
        }
    return normalized


def normalized_partition_assets(entries: Any) -> list[dict[str, Any]]:
    """Validate the ``assets`` list a compiled partition declares."""
    if entries is None:
        return []
    if not isinstance(entries, list):
        raise ValueError("Partition assets must be a list")
    normalized: list[dict[str, Any]] = []
    for index, entry in enumerate(entries):
        label = f"Partition assets[{index}]"
        if not isinstance(entry, dict):
            raise ValueError(f"{label} must be an object")
        category = str(entry.get("category") or "").strip()
        filename = str(entry.get("filename") or "").strip()
        if not category:
            raise ValueError(f"{label}: category is required")
        if not filename:
            raise ValueError(f"{label}: filename is required")
        # Both halves land in a path on the runner, so both are checked here
        # rather than trusted from a client that may not be the node pack.
        require_models_relative(label, "category", category)
        require_models_relative(label, "filename", filename)
        try:
            size = int(entry.get("size"))
        except (TypeError, ValueError):
            raise ValueError(f"{label}: size must be an integer byte count")
        if size < 0:
            raise ValueError(f"{label}: size cannot be negative")
        asset_format = str(entry.get("format") or "other").strip().lower()
        if asset_format not in ASSET_FORMATS:
            raise ValueError(
                f"{label}: format must be one of {', '.join(sorted(ASSET_FORMATS))}"
            )
        normalized.append(
            {
                "category": category,
                "filename": filename,
                "sha256": _normalized_digest(entry.get("sha256"), label),
                "size": size,
                "format": asset_format,
            }
        )
    return normalized


def _profile_weight_match(
    profile: dict[str, Any] | None, category: str, filename: str
) -> dict[str, Any] | None:
    """Find a pinned weights entry that would land this file on the runner."""
    base = filename.replace("\\", "/").rsplit("/", 1)[-1]
    for entry in (profile or {}).get("weights") or []:
        if entry.get("dest", "").replace("\\", "/").strip("/") != category:
            continue
        files = entry.get("files")
        if files is None:
            # A whole-snapshot entry stages the repo into this category, so the
            # file may well arrive; name-matched at best, like the rest.
            return entry
        for item in files:
            candidate = str(item).replace("\\", "/")
            if candidate == filename.replace("\\", "/") or candidate.rsplit("/", 1)[-1] == base:
                return entry
    return None


def resolve_partition_assets(
    config: Any,
    assets: list[dict[str, Any]],
    profile: dict[str, Any] | None,
    storage: Any = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Resolve each declared asset to a fetchable origin.

    Returns ``(resolved, unresolved)``. Resolution order is strongest identity
    first: a digest-keyed source, then the coordinator's own artifact store,
    then a name match against the profile's pinned weights.
    """
    sources = normalized_asset_sources(getattr(config, "asset_sources", None))
    resolved: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    for asset in assets:
        digest = asset["sha256"]
        source = sources.get(digest)
        if source:
            resolved.append({**asset, "origin": "source", "source": dict(source)})
            continue
        if storage is not None and _store_holds(storage, digest):
            resolved.append(
                {**asset, "origin": "store", "source": {"artifact_id": digest}}
            )
            continue
        weights = _profile_weight_match(profile, asset["category"], asset["filename"])
        if weights:
            resolved.append(
                {
                    **asset,
                    "origin": "profile",
                    "source": {
                        "repo_id": weights["repo_id"],
                        "revision": weights["revision"],
                        "dest": weights["dest"],
                    },
                    "warning": NAME_MATCHED_WARNING,
                }
            )
            continue
        unresolved.append(dict(asset))
    return resolved, unresolved


def _store_holds(storage: Any, digest: str) -> bool:
    """Whether the artifact store already holds these exact bytes."""
    try:
        return bool(storage.exists(partition_artifact_key(digest)))
    except Exception:
        # A storage backend that cannot answer is not a promise that it can.
        return False


def unresolved_assets_message(unresolved: list[dict[str, Any]]) -> str:
    """One line naming every file the coordinator cannot obtain, and the remedy."""
    described = "; ".join(
        "{filename} ({category}, sha256 {digest}, {size:.1f} MiB)".format(
            filename=asset["filename"],
            category=asset["category"],
            digest=asset["sha256"][:12],
            size=asset["size"] / (1024 * 1024),
        )
        for asset in unresolved
    )
    noun = "model file" if len(unresolved) == 1 else "model files"
    return (
        f"Cloud Offload cannot obtain {len(unresolved)} {noun} declared by this "
        f"partition: {described}. {UNRESOLVED_REMEDY}"
    )

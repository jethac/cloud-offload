"""Resolving how big a profile's pinned weights are, once, and remembering it.

A weights entry pins a repository, a revision and a file list, so its size is a
fixed fact: the bytes behind ``org/repo@abc123/model.safetensors`` will never be
different bytes. That makes it worth one lookup and a permanent cache entry, and
it makes the cache safe — there is no staleness to reason about.

Two rules hold everywhere here. Nothing raises: a Hub that is down, rate
limiting, or a repository that has been made private all degrade to "unknown"
for that entry, which the planner then charges a conservative default and
reports. And nothing guesses a number silently — an unresolved entry is simply
absent from the result, so the caller can tell "8 GiB" from "we do not know".

The network call is injectable so tests never touch it, and the submission path
never makes one at all: it reads :func:`cached_weight_sizes`, because a
submission must not wait on, or fail because of, a third-party API.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

from cloud_offload.storage_plan import profile_weight_targets, weight_targets

logger = logging.getLogger(__name__)

# The revision endpoint answers for a whole pinned revision in one request, and
# ``blobs=true`` makes it report each file's size, so a profile with eight
# pinned files from one repository costs one call rather than eight.
HF_REVISION_URL = "https://huggingface.co/api/models/{repo_id}/revision/{revision}"

CACHE_FILENAME = "weight-sizes.json"

_USER_AGENT = "cloud-offload-coordinator/0.1"


def weight_size_cache_path(config: Any) -> Path:
    """Where resolved sizes live: beside the queue database, like the worker token."""
    return Path(config.queue_db_path).with_name(CACHE_FILENAME)


def load_size_cache(path: str | Path) -> dict[str, int]:
    """Read the on-disk cache, treating any damage as an empty cache."""
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    sizes: dict[str, int] = {}
    for key, value in data.items():
        try:
            size = int(value)
        except (TypeError, ValueError):
            continue
        if size >= 0:
            sizes[str(key)] = size
    return sizes


def save_size_cache(path: str | Path, sizes: dict[str, int]) -> None:
    """Persist resolved sizes. A cache that cannot be written is not an error."""
    target = Path(path)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(sizes, indent=2, sort_keys=True), encoding="utf-8"
        )
    except OSError as exc:
        logger.warning("Could not write the weight size cache %s: %s", target, exc)


def cached_weight_sizes(config: Any, profile: dict[str, Any] | None) -> dict[str, int]:
    """Sizes already resolved for this profile's weights. Never touches the network."""
    cache = load_size_cache(weight_size_cache_path(config))
    return {
        key: cache[key]
        for key, _filename, _entry in profile_weight_targets(profile)
        if key in cache
    }


def huggingface_file_sizes(
    entries: Any,
    *,
    fetch: Callable[[str, dict[str, str]], Any] | None = None,
    cache: dict[str, int] | None = None,
) -> dict[str, int]:
    """Resolve the size of every file a weights list pins, as far as it can.

    Returns ``weight_key -> bytes`` for the entries that resolved, and omits the
    ones that did not. ``cache`` is a mutable mapping of previously resolved
    keys: entries found in it are answered without a request, and newly resolved
    ones are added to it.
    """
    fetch = fetch or _default_fetch
    known = cache if cache is not None else {}
    resolved: dict[str, int] = {}

    grouped: dict[tuple[str, str], list[tuple[str, str | None]]] = {}
    for key, filename, entry in weight_targets(entries):
        repo_id = str(entry.get("repo_id") or "")
        revision = str(entry.get("revision") or "")
        if not repo_id or not revision:
            continue
        grouped.setdefault((repo_id, revision), []).append((key, filename))

    for (repo_id, revision), wanted in grouped.items():
        outstanding: list[tuple[str, str | None]] = []
        for key, filename in wanted:
            if key in known:
                resolved[key] = int(known[key])
            else:
                outstanding.append((key, filename))
        if not outstanding:
            continue
        listing = _revision_file_sizes(repo_id, revision, fetch)
        if listing is None:
            continue
        sizes, complete = listing
        for key, filename in outstanding:
            size = _target_size(sizes, complete, filename)
            if size is None:
                continue
            resolved[key] = size
            known[key] = size
    return resolved


def refresh_weight_sizes(
    config: Any,
    profile: dict[str, Any] | None,
    *,
    fetch: Callable[[str, dict[str, str]], Any] | None = None,
) -> dict[str, int]:
    """Resolve this profile's unknown weight sizes and persist what was learned."""
    path = weight_size_cache_path(config)
    cache = load_size_cache(path)
    before = len(cache)
    sizes = huggingface_file_sizes(
        (profile or {}).get("weights"), fetch=fetch, cache=cache
    )
    if len(cache) != before:
        save_size_cache(path, cache)
    return sizes


def _target_size(
    sizes: dict[str, int], complete: bool, filename: str | None
) -> int | None:
    """The size of one target, or None when the listing cannot answer for it."""
    if filename is not None:
        return sizes.get(filename.replace("\\", "/"))
    # A whole-snapshot entry stages the entire revision, so a listing that is
    # missing any file's size cannot size it -- and a partial sum would be an
    # under-estimate presented as a fact.
    if not complete or not sizes:
        return None
    return sum(sizes.values())


def _revision_file_sizes(
    repo_id: str,
    revision: str,
    fetch: Callable[[str, dict[str, str]], Any],
) -> tuple[dict[str, int], bool] | None:
    """Every sized file at a pinned revision, plus whether the listing was whole."""
    url = HF_REVISION_URL.format(
        repo_id=quote(repo_id.strip("/"), safe="/"),
        revision=quote(revision, safe=""),
    ) + "?blobs=true"
    try:
        response = fetch(url, _headers())
        status = int(getattr(response, "status_code", 200) or 200)
        if status >= 400:
            logger.info(
                "Weight sizes for %s@%s unavailable: HTTP %s",
                repo_id,
                revision,
                status,
            )
            return None
        payload = response.json()
    except Exception as exc:  # noqa: BLE001 - discovery is best-effort by design
        logger.info("Could not resolve weight sizes for %s@%s: %s", repo_id, revision, exc)
        return None
    if not isinstance(payload, dict):
        return None
    siblings = payload.get("siblings")
    if not isinstance(siblings, list):
        return None
    sizes: dict[str, int] = {}
    for sibling in siblings:
        if not isinstance(sibling, dict):
            continue
        name = str(sibling.get("rfilename") or "").strip()
        # ``x-linked-size`` is what a HEAD on the resolve URL would report for
        # an LFS file; the listing calls the same number ``size``.
        raw = sibling.get("size", sibling.get("x-linked-size"))
        if not name or raw is None:
            continue
        try:
            sizes[name] = int(raw)
        except (TypeError, ValueError):
            continue
    return sizes, bool(siblings) and len(sizes) == len(siblings)


def _headers() -> dict[str, str]:
    headers = {"User-Agent": _USER_AGENT, "Accept": "application/json"}
    try:
        from cloud_offload.credentials import huggingface_token

        token = huggingface_token()
    except Exception:  # noqa: BLE001 - an unavailable keychain is not fatal here
        token = ""
    if token:
        # A gated repository answers 401 anonymously, which would report every
        # one of its files as unknown and inflate the plan for no reason.
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _default_fetch(url: str, headers: dict[str, str]) -> Any:
    import requests

    return requests.request("GET", url, headers=headers, timeout=30)

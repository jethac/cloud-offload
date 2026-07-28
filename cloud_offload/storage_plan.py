"""Sizing a rented pod's disk before the meter starts.

A worker was rented with a fixed container disk, and the pod died out of space
after the meter had started: the runner image was 14.6 GB and the partition
staged a 19.6 GB model onto the same partition. That is the failure the
pre-provision checks exist to prevent — the coordinator refuses at submission,
where a refusal is free, rather than at runtime, where it is not.

Almost everything needed to size that disk is known before anything is rented.
Declared assets carry exact byte counts. Profile weights are pinned to a repo,
a revision and a file list, so their sizes can be looked up once and cached
forever. Node packs are small but unbounded, so they get an allowance. The image
size is knowable and can simply be declared.

What cannot be known is a custom node that downloads its own weights when it
first runs: a node calling diffusers ``from_pretrained`` pulls tens of
gigabytes that no manifest mentions and no static analysis can see. That is what
the profile's ``extra_disk_gb`` is for — the operator declaring what the
coordinator cannot observe.

Everything here is a pure function. Size *discovery* needs the network and lives
in :mod:`cloud_offload.weight_sizes`; this module only does arithmetic, so the
number a submission is refused on can be reproduced exactly from the manifest.
"""

from __future__ import annotations

import math
from typing import Any

GIB = 1024 ** 3

# Per declared custom node pack. A pack installs itself with an unpinned
# ``pip install -r requirements.txt``: one pack pulls nothing, the next pulls a
# CUDA-linked wheel set. Nothing in the manifest bounds that, so this is an
# allowance, not a measurement — a figure chosen to cover the ordinary case
# without pretending the coordinator knows what the wheels weigh.
PACK_ALLOWANCE_BYTES = 2 * GIB

# Substitutes for components whose real size could not be determined. Each errs
# high deliberately: a plan that guesses low rents a pod that dies mid-job,
# which is the exact failure being prevented, while a plan that guesses high
# costs a few cents of disk. Every substitution is also named in the plan's
# ``unknown`` list, so an operator can always tell measurements from assumptions.
UNKNOWN_IMAGE_BYTES = 30 * GIB  # runner images run 10-20 GB today
UNKNOWN_WEIGHT_FILE_BYTES = 8 * GIB  # one large checkpoint file
UNKNOWN_SNAPSHOT_BYTES = 32 * GIB  # a whole repository snapshot

# Working space: outputs, temp files, pip caches, and the half-written copy of
# whatever is downloading. A flat floor is what a small job actually needs; the
# percentage is what a large one needs, because a staging directory holding a
# 100 GB model wants far more than 10 GB of slack.
HEADROOM_FLOOR_BYTES = 10 * GIB
HEADROOM_FRACTION = 0.20

# Nothing below this is worth renting: the runner image alone is roughly 15 GB,
# so a smaller disk cannot hold the image and a single temporary file.
MINIMUM_DISK_GB = 20


def _gib(value: int | float) -> str:
    return f"{value / GIB:.1f} GiB"


def _plural(count: int, noun: str, plural: str | None = None) -> str:
    return f"{count} {noun}" if count == 1 else f"{count} {plural or noun + 's'}"


def weight_key(repo_id: str, revision: str, filename: str | None = None) -> str:
    """The stable key one pinned weights file is sized and cached under.

    A pinned revision never changes, so this key is a permanent identity: once
    a size is resolved for it, it never needs resolving again.
    """
    base = f"{repo_id}@{revision}"
    return f"{base}/{filename}" if filename else base


def weight_targets(entries: Any) -> list[tuple[str, str | None, dict[str, Any]]]:
    """``(key, filename, entry)`` for every file a weights list would stage.

    A ``files: null`` entry stages a whole snapshot, which is one target with no
    filename: its size is the size of the repository at that revision.
    """
    targets: list[tuple[str, str | None, dict[str, Any]]] = []
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        repo_id = str(entry.get("repo_id") or "")
        revision = str(entry.get("revision") or "")
        files = entry.get("files")
        if files is None:
            targets.append((weight_key(repo_id, revision), None, entry))
            continue
        for filename in files:
            name = str(filename)
            targets.append((weight_key(repo_id, revision, name), name, entry))
    return targets


def profile_weight_targets(
    profile: dict[str, Any] | None,
) -> list[tuple[str, str | None, dict[str, Any]]]:
    """Every weights target a worker profile would stage onto its runner."""
    return weight_targets((profile or {}).get("weights"))


def plan_storage(
    assets: list[dict[str, Any]] | None,
    profile: dict[str, Any] | None,
    *,
    image_bytes: int | None = None,
    weight_bytes: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Size the container disk a partition needs on this worker profile.

    ``assets`` are the partition's declared files, each carrying an exact
    ``size``. ``image_bytes`` is the runner image if it is known, and
    ``weight_bytes`` maps :func:`weight_key` to a resolved size for whichever
    pinned weights could be looked up — both may be absent or partial.

    Every figure in the returned plan is in bytes. A component whose size could
    not be determined is named in ``unknown`` *and* charged a documented
    conservative default, never zero: silently omitting it would produce exactly
    the confident under-estimate this module exists to stop.
    """
    profile = profile or {}
    known: dict[str, int] = {
        str(key): int(value)
        for key, value in (weight_bytes or {}).items()
        if value is not None
    }
    unknown: list[str] = []
    components: list[dict[str, Any]] = []

    image = int(image_bytes or 0)
    if image > 0:
        image_detail = f"runner image, declared as {_gib(image)}"
    else:
        image = UNKNOWN_IMAGE_BYTES
        image_detail = f"runner image size is not declared; assuming {_gib(image)}"
        unknown.append(
            "the runner image (declare its size as image_size_gb on the worker profile)"
        )
    components.append({"name": "image", "bytes": image, "detail": image_detail})

    declared = list(assets or [])
    asset_total = sum(int(asset.get("size") or 0) for asset in declared)
    components.append(
        {
            "name": "assets",
            "bytes": asset_total,
            "detail": (
                f"{_plural(len(declared), 'declared model file')}, sized exactly by "
                "the partition manifest"
            ),
        }
    )

    targets = profile_weight_targets(profile)
    weights = 0
    assumed = 0
    for key, filename, _entry in targets:
        size = known.get(key)
        if size is not None:
            weights += int(size)
            continue
        assumed += 1
        if filename is None:
            weights += UNKNOWN_SNAPSHOT_BYTES
            unknown.append(
                f"the whole-snapshot weights entry {key}, charged "
                f"{_gib(UNKNOWN_SNAPSHOT_BYTES)}"
            )
        else:
            weights += UNKNOWN_WEIGHT_FILE_BYTES
            unknown.append(
                f"the pinned weights file {key}, charged "
                f"{_gib(UNKNOWN_WEIGHT_FILE_BYTES)}"
            )
    weights_detail = _plural(
        len(targets), "pinned profile weights entry", "pinned profile weights entries"
    )
    if assumed:
        weights_detail += f", {assumed} of unknown size"
    components.append({"name": "weights", "bytes": weights, "detail": weights_detail})

    pack_count = len(profile.get("custom_nodes") or [])
    packs = pack_count * PACK_ALLOWANCE_BYTES
    components.append(
        {
            "name": "packs",
            "bytes": packs,
            "detail": (
                f"{_plural(pack_count, 'custom node pack')} at a "
                f"{_gib(PACK_ALLOWANCE_BYTES)} allowance each; a pack's pip "
                "install is unbounded, so this is an allowance, not a measurement"
            ),
        }
    )

    # Counted in GiB like every other figure in this plan, so the reserve an
    # operator declares is the reserve the plan adds.
    reserve = int(float(profile.get("extra_disk_gb") or 0) * GIB)
    components.append(
        {
            "name": "reserve",
            "bytes": reserve,
            "detail": (
                "operator-declared extra_disk_gb: storage the coordinator cannot see"
                if reserve
                else (
                    "no extra_disk_gb declared; a node that downloads its own "
                    "weights at runtime is invisible to this plan"
                )
            ),
        }
    )

    subtotal = image + asset_total + weights + packs + reserve
    headroom = max(HEADROOM_FLOOR_BYTES, int(subtotal * HEADROOM_FRACTION))
    components.append(
        {
            "name": "headroom",
            "bytes": headroom,
            "detail": (
                f"working space for outputs, temp files and pip caches: the larger "
                f"of {_gib(HEADROOM_FLOOR_BYTES)} and "
                f"{int(HEADROOM_FRACTION * 100)}% of {_gib(subtotal)}"
            ),
        }
    )

    return {
        "image": image,
        "assets": asset_total,
        "weights": weights,
        "packs": packs,
        "reserve": reserve,
        "headroom": headroom,
        "total": subtotal + headroom,
        "unknown": unknown,
        "components": components,
    }


def plan_disk_gb(plan: dict[str, Any]) -> int:
    """The container disk to request for a plan, in whole GB.

    Floored at :data:`MINIMUM_DISK_GB` because a pod too small to hold its own
    runner image is not worth renting at any price.
    """
    total = int(plan.get("total") or 0)
    return max(MINIMUM_DISK_GB, math.ceil(total / GIB))


def plan_summary(plan: dict[str, Any]) -> dict[str, Any]:
    """The plan as a submitter sees it: what was requested, and what was guessed."""
    return {
        "total_gb": plan_disk_gb(plan),
        "total_bytes": int(plan.get("total") or 0),
        "components": list(plan.get("components") or []),
        "unknown": list(plan.get("unknown") or []),
    }


def exceeds_ceiling_message(plan: dict[str, Any], disk_gb: int, ceiling_gb: int) -> str:
    """One line naming the planned total, its largest parts, and the remedy."""
    largest = sorted(
        (component for component in plan.get("components") or []),
        key=lambda component: int(component.get("bytes") or 0),
        reverse=True,
    )[:3]
    described = ", ".join(
        f"{component['name']} {_gib(int(component.get('bytes') or 0))}"
        for component in largest
    )
    return (
        f"Cloud Offload will not rent a worker for this partition: it plans "
        f"{disk_gb} GB of container disk, above the configured ceiling of "
        f"{ceiling_gb} GB. Largest components: {described}. Raise "
        f"max_container_disk_gb, or reduce what this partition and its worker "
        f"profile stage."
    )

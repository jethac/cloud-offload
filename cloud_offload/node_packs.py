"""Required custom node packs: proving a runner will have the right code.

A compiled partition may declare the node packs its subgraph needs, each with a
content digest (the node pack builds that list inside ComfyUI, where the packs
actually live). This module answers the question that has to be settled before
any money is spent: for every required pack, has the target worker profile been
told to install it?

The check is a name check, deliberately. A pack is "provided" when the profile's
``custom_nodes`` declares an entry that answers to the required id — its
``registry_id``, or the last path segment of its clone URL — compared
case-insensitively, because a registry id, a repository name and a checkout
directory routinely disagree only in case.

A missing pack is a refusal, not a warning. Renting a GPU and discovering the
node type does not exist there costs real money; refusing at submission costs
nothing. A *divergent* pack is the opposite: see ``node_pack_version_warnings``.
"""

from __future__ import annotations

from typing import Any

from cloud_offload.profiles import profile_pack_identifier, require_models_relative

MISSING_REMEDY_SINGULAR = (
    "Add it to the worker profile's custom_nodes, or remove those nodes from the box."
)
MISSING_REMEDY_PLURAL = (
    "Add them to the worker profile's custom_nodes, or remove those nodes from the box."
)

VERSION_DIVERGENCE_WARNING = (
    "the worker profile pins a different version than the client declared; the "
    "coordinator cannot know the runner's actual content digest until the runner "
    "reports it, and a matching version would not have proven a code match either"
)


def _normalized_digest(value: Any, label: str) -> str:
    digest = str(value or "").strip().lower()
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValueError(f"{label}: digest must be a 64-character sha256 hex digest")
    return digest


def normalized_partition_node_packs(entries: Any) -> list[dict[str, Any]]:
    """Validate the ``node_packs`` list a compiled partition declares."""
    if entries is None:
        return []
    if not isinstance(entries, list):
        raise ValueError("Partition node_packs must be a list")
    normalized: list[dict[str, Any]] = []
    for index, entry in enumerate(entries):
        label = f"Partition node_packs[{index}]"
        if not isinstance(entry, dict):
            raise ValueError(f"{label} must be an object")
        identifier = str(entry.get("id") or "").strip()
        directory = str(entry.get("directory") or "").strip()
        if not identifier:
            raise ValueError(f"{label}: id is required")
        if not directory:
            raise ValueError(f"{label}: directory is required")
        # The directory names a folder the runner will install into, so it is
        # checked here rather than trusted from a client that may not be the
        # node pack.
        require_models_relative(label, "directory", directory)
        normalized.append(
            {
                "id": identifier,
                "directory": directory,
                # Absent for a pack whose pyproject declares no version, which
                # is ordinary; only the digest is ever required.
                "version": str(entry.get("version") or "").strip(),
                "digest": _normalized_digest(entry.get("digest"), label),
            }
        )
    return normalized


def _declared_entries(profile: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """The profile's declared packs, keyed by the name each answers to."""
    declared: dict[str, dict[str, Any]] = {}
    for entry in (profile or {}).get("custom_nodes") or []:
        identifier = profile_pack_identifier(entry).lower()
        if identifier:
            declared.setdefault(identifier, entry)
    return declared


def missing_node_packs(
    required: list[dict[str, Any]], profile: dict[str, Any] | None
) -> list[dict[str, Any]]:
    """Every required pack the target profile has not been told to install."""
    declared = _declared_entries(profile)
    return [pack for pack in required if pack["id"].lower() not in declared]


def node_pack_version_warnings(
    required: list[dict[str, Any]], profile: dict[str, Any] | None
) -> list[dict[str, Any]]:
    """Surface packs the profile pins at a version the client did not declare.

    A divergence is reported, not refused, because refusing would be dishonest
    about what the coordinator actually knows. It cannot see the runner's content
    until the runner reports its own digest, and a version *match* would not have
    proven a code match either: a pack can carry a security fix and still declare
    the version number of the unpatched release published under it. So version
    equality is evidence of nothing in either direction, and the useful thing to
    do with a disagreement is show it to the submitter.
    """
    declared = _declared_entries(profile)
    warnings: list[dict[str, Any]] = []
    for pack in required:
        entry = declared.get(pack["id"].lower())
        if not entry:
            continue
        pinned = str(entry.get("version") or "").strip()
        if not pinned or not pack["version"] or pinned == pack["version"]:
            continue
        warnings.append(
            {
                "id": pack["id"],
                "declared_version": pack["version"],
                "profile_version": pinned,
                "digest": pack["digest"],
                "warning": VERSION_DIVERGENCE_WARNING,
            }
        )
    return warnings


def missing_node_packs_message(missing: list[dict[str, Any]]) -> str:
    """One line naming every pack the coordinator cannot supply, and the remedy."""
    described = ", ".join(pack["id"] for pack in missing)
    noun = "custom node pack" if len(missing) == 1 else "custom node packs"
    remedy = MISSING_REMEDY_SINGULAR if len(missing) == 1 else MISSING_REMEDY_PLURAL
    return (
        f"Cloud Offload cannot provide {len(missing)} {noun} required by this "
        f"partition: {described}. {remedy}"
    )

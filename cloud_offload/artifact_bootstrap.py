"""Fail-closed bootstrap for content-addressed partition input artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from cloud_offload.storage import partition_artifact_key


class ArtifactBootstrapError(RuntimeError):
    """The declared artifact could not be verified or imported safely."""


@dataclass(frozen=True)
class DeclaredArtifact:
    """One unique input artifact and the workflow boundaries that reference it."""

    digest: str
    expected_size: int | None = None
    roles: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        digest = _normalize_digest(self.digest)
        object.__setattr__(self, "digest", digest)
        if self.expected_size is not None and int(self.expected_size) < 0:
            raise ValueError("expected_size must not be negative")
        object.__setattr__(self, "roles", tuple(sorted({str(role) for role in self.roles})))


@dataclass(frozen=True)
class ImportedArtifact:
    digest: str
    size: int
    roles: tuple[str, ...]
    already_present: bool


def _normalize_digest(value: str) -> str:
    digest = str(value).strip().lower()
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValueError(f"Invalid artifact digest: {value!r}")
    return digest


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
                size += len(chunk)
    except OSError as exc:
        raise ArtifactBootstrapError(f"unable to read artifact source: {path.name}") from exc
    return digest.hexdigest(), size


def _source_path(source_root: Path, digest: str) -> Path:
    return source_root / Path(partition_artifact_key(digest))


def declared_input_artifacts(
    benchmark_plans: Iterable[str | Path | dict[str, Any]],
) -> list[DeclaredArtifact]:
    """Extract unique input IDs and workflow roles from private benchmark plans."""

    collected: dict[str, set[str]] = {}
    for plan_source in benchmark_plans:
        if isinstance(plan_source, (str, Path)):
            plan = json.loads(Path(plan_source).read_text(encoding="utf-8"))
        else:
            plan = plan_source
        for scenario in plan.get("scenarios") or []:
            request = scenario.get("request") or {}
            input_artifacts = request.get("input_artifacts") or {}
            partition = request.get("partition") or {}
            roles = {
                str(item.get("key")): str(
                    item.get("target_input") or item.get("name") or item.get("key")
                )
                for item in partition.get("inputs") or []
                if isinstance(item, dict) and item.get("key")
            }
            for boundary, raw_digest in input_artifacts.items():
                digest = _normalize_digest(raw_digest)
                role = f"{boundary}:{roles.get(str(boundary), str(boundary))}"
                collected.setdefault(digest, set()).add(role)
    return [
        DeclaredArtifact(digest=digest, roles=tuple(sorted(roles)))
        for digest, roles in sorted(collected.items())
    ]


def import_declared_artifacts(
    source_root: str | Path,
    destination_root: str | Path,
    declarations: Iterable[DeclaredArtifact],
) -> list[ImportedArtifact]:
    """Verify and atomically copy declared artifacts into a local artifact store.

    Existing exact bytes are accepted as an idempotent import. Any missing source,
    source digest/size mismatch, interrupted copy, or conflicting destination is a
    hard failure and leaves no partial destination object.
    """

    source_root = Path(source_root)
    destination_root = Path(destination_root)
    imported: list[ImportedArtifact] = []
    for declaration in declarations:
        digest = _normalize_digest(declaration.digest)
        expected_size = declaration.expected_size
        source = _source_path(source_root, digest)
        if not source.is_file():
            raise ArtifactBootstrapError(f"source artifact is missing: {digest}")
        source_digest, source_size = _hash_file(source)
        if source_digest != digest:
            raise ArtifactBootstrapError(f"source digest mismatch: {digest}")
        if expected_size is not None and source_size != int(expected_size):
            raise ArtifactBootstrapError(f"source size mismatch: {digest}")

        target = _source_path(destination_root, digest)
        if target.exists():
            if not target.is_file():
                raise ArtifactBootstrapError(f"destination artifact mismatch: {digest}")
            target_digest, target_size = _hash_file(target)
            if target_digest != digest or target_size != source_size:
                raise ArtifactBootstrapError(f"destination artifact mismatch: {digest}")
            imported.append(
                ImportedArtifact(digest, source_size, declaration.roles, True)
            )
            continue

        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
        try:
            shutil.copyfile(source, temporary)
            copied_digest, copied_size = _hash_file(temporary)
            if copied_digest != digest or copied_size != source_size:
                raise ArtifactBootstrapError(f"copied artifact verification failed: {digest}")
            os.replace(temporary, target)
            final_digest, final_size = _hash_file(target)
            if final_digest != digest or final_size != source_size:
                raise ArtifactBootstrapError(f"destination artifact verification failed: {digest}")
        except ArtifactBootstrapError:
            temporary.unlink(missing_ok=True)
            raise
        except OSError as exc:
            temporary.unlink(missing_ok=True)
            raise ArtifactBootstrapError(f"copy failed: {digest}") from exc
        imported.append(ImportedArtifact(digest, source_size, declaration.roles, False))
    return imported

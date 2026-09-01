"""Fail-closed bootstrap and startup receipt for M7 partition artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from cloud_offload.storage import LocalStorage, partition_artifact_key


MAX_PARTITION_ARTIFACT_BYTES = int(
    os.environ.get("CLOUD_OFFLOAD_MAX_PARTITION_ARTIFACT_BYTES", str(2 * 1024 * 1024 * 1024))
)
RECEIPT_SCHEMA = "cloud-offload.m7-artifact-bootstrap-receipt.v1"
RECEIPT_NAME = ".m7-artifact-bootstrap-receipt.json"


class ArtifactBootstrapError(RuntimeError):
    """The declared artifact set could not be verified or committed safely."""


@dataclass(frozen=True)
class DeclaredArtifact:
    """One unique input artifact and the workflow boundaries that reference it."""

    digest: str
    expected_size: int | None = None
    roles: tuple[str, ...] = ()
    plan_digests: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "digest", _normalize_digest(self.digest))
        if self.expected_size is not None and int(self.expected_size) < 0:
            raise ValueError("expected_size must not be negative")
        object.__setattr__(self, "roles", tuple(sorted({str(role) for role in self.roles})))
        object.__setattr__(
            self,
            "plan_digests",
            tuple(sorted({_normalize_plan_digest(item) for item in self.plan_digests})),
        )


@dataclass(frozen=True)
class ImportedArtifact:
    digest: str
    size: int
    roles: tuple[str, ...]
    already_present: bool
    plan_digests: tuple[str, ...] = ()


def _normalize_digest(value: str) -> str:
    digest = str(value).strip().lower()
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValueError(f"Invalid artifact digest: {value!r}")
    return digest


def _normalize_plan_digest(value: str) -> str:
    digest = str(value).strip().lower()
    if digest.startswith("sha256:"):
        digest = digest[7:]
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValueError(f"Invalid benchmark plan digest: {value!r}")
    return digest


def file_digest(path: str | Path) -> str:
    digest, _ = _hash_file(Path(path))
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


def _source_path(root: Path, digest: str) -> Path:
    return root / Path(partition_artifact_key(digest))


def bootstrap_receipt_path(destination_root: str | Path) -> Path:
    return Path(destination_root).resolve() / RECEIPT_NAME


def config_artifact_store(config: Any, isolated_home: str | Path) -> Path:
    """Resolve the exact local store for an explicitly isolated config/home."""
    home = Path(isolated_home).resolve()
    if str(getattr(config, "storage_type", "local")).strip().lower() != "local":
        raise ArtifactBootstrapError("M7 bootstrap requires local artifact storage")
    configured = str(getattr(config, "storage_path", "") or "").strip()
    if not configured:
        return (home / "job_files").resolve()
    configured_path = Path(configured).expanduser()
    if not configured_path.is_absolute():
        configured_path = home / configured_path
    destination = configured_path.resolve()
    try:
        destination.relative_to(home)
    except ValueError as exc:
        raise ArtifactBootstrapError(
            "configured artifact store is outside the isolated home"
        ) from exc
    return destination


def _value(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def declared_input_artifacts(
    benchmark_plans: Iterable[Any],
) -> list[DeclaredArtifact]:
    """Extract declarations from already-loaded plans without reparsing files."""
    collected: dict[str, dict[str, set[str]]] = {}
    for entry in benchmark_plans:
        plan_digest = None
        plan = entry
        if isinstance(entry, tuple) and len(entry) == 2:
            plan_digest, plan = entry
        if isinstance(plan, (str, Path)):
            plan = json.loads(Path(plan).read_text(encoding="utf-8"))
        for scenario in _value(plan, "scenarios", []) or []:
            request = _value(scenario, "request", {}) or {}
            input_artifacts = _value(request, "input_artifacts", {}) or {}
            partition = _value(request, "partition", {}) or {}
            roles = {
                str(_value(item, "key")): str(
                    _value(item, "target_input")
                    or _value(item, "name")
                    or _value(item, "key")
                )
                for item in (_value(partition, "inputs", []) or [])
                if _value(item, "key")
            }
            for boundary, raw_digest in input_artifacts.items():
                digest = _normalize_digest(raw_digest)
                role = f"{boundary}:{roles.get(str(boundary), str(boundary))}"
                record = collected.setdefault(digest, {"roles": set(), "plans": set()})
                record["roles"].add(role)
                if plan_digest:
                    record["plans"].add(_normalize_plan_digest(plan_digest))
    return [
        DeclaredArtifact(
            digest=digest,
            roles=tuple(sorted(record["roles"])),
            plan_digests=tuple(sorted(record["plans"])),
        )
        for digest, record in sorted(collected.items())
    ]


def _receipt_payload(
    destination_root: Path,
    declarations: list[DeclaredArtifact],
    *,
    release_plan_digest: str,
    config_digest: str,
    stored_sizes: dict[str, int],
) -> dict[str, Any]:
    return {
        "schema": RECEIPT_SCHEMA,
        "release_plan_digest": _normalize_plan_digest(release_plan_digest),
        "config_digest": _normalize_plan_digest(config_digest),
        "destination": str(destination_root.resolve()),
        "artifacts": [
            {
                "digest": item.digest,
                "declared_size": item.expected_size,
                "stored_size": int(stored_sizes[item.digest]),
                "roles": list(item.roles),
                "plan_digests": list(item.plan_digests),
            }
            for item in declarations
        ],
    }


def config_digest(config: Any, destination_root: str | Path) -> str:
    """Digest the redacted effective config plus the exact destination."""
    payload = {
        "config": config.to_dict() if hasattr(config, "to_dict") else {},
        "destination": str(Path(destination_root).resolve()),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def _write_receipt(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _verify_destination(destination_root: Path, declaration: DeclaredArtifact, size: int) -> None:
    target = _source_path(destination_root, declaration.digest)
    if not target.is_file():
        raise ArtifactBootstrapError(f"destination artifact is missing: {declaration.digest}")
    target_digest, target_size = _hash_file(target)
    if target_digest != declaration.digest or target_size != size:
        raise ArtifactBootstrapError(f"destination artifact mismatch: {declaration.digest}")


def import_declared_artifacts(
    source_root: str | Path,
    destination_root: str | Path,
    declarations: Iterable[DeclaredArtifact],
    *,
    release_plan_digest: str | None = None,
    config_digest: str | None = None,
) -> list[ImportedArtifact]:
    """Validate all sources, stage all bytes, then atomically publish one receipt."""
    source_root = Path(source_root).resolve()
    destination_root = Path(destination_root).resolve()
    declarations = [DeclaredArtifact(**item.__dict__) if not isinstance(item, DeclaredArtifact) else item for item in declarations]
    if not declarations:
        raise ArtifactBootstrapError("no declared artifacts")
    if release_plan_digest is None:
        release_plan_digest = "0" * 64
    if config_digest is None:
        config_digest = "0" * 64
    receipt_path = bootstrap_receipt_path(destination_root)

    source_records: list[tuple[DeclaredArtifact, Path, int]] = []
    for declaration in declarations:
        source = _source_path(source_root, declaration.digest)
        if not source.is_file():
            raise ArtifactBootstrapError(f"source artifact is missing: {declaration.digest}")
        source_digest, source_size = _hash_file(source)
        if source_size > MAX_PARTITION_ARTIFACT_BYTES:
            raise ArtifactBootstrapError(f"artifact exceeds upload size limit: {declaration.digest}")
        if source_digest != declaration.digest:
            raise ArtifactBootstrapError(f"source digest mismatch: {declaration.digest}")
        if declaration.expected_size is not None and source_size != int(declaration.expected_size):
            raise ArtifactBootstrapError(f"source size mismatch: {declaration.digest}")
        source_records.append((declaration, source, source_size))

    stored_sizes = {declaration.digest: source_size for declaration, _, source_size in source_records}
    receipt_payload = _receipt_payload(
        destination_root,
        declarations,
        release_plan_digest=release_plan_digest,
        config_digest=config_digest,
        stored_sizes=stored_sizes,
    )
    if receipt_path.exists():
        try:
            existing = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ArtifactBootstrapError("bootstrap receipt is unreadable") from exc
        if existing != receipt_payload:
            raise ArtifactBootstrapError("bootstrap receipt mismatch")

    destination_root.mkdir(parents=True, exist_ok=True)
    staging_root = destination_root.parent / f".{destination_root.name}.m7-stage-{uuid.uuid4().hex}"
    staging_root.mkdir(parents=True, exist_ok=False)
    new_targets: list[Path] = []
    records: list[ImportedArtifact] = []
    try:
        for declaration, source, source_size in source_records:
            target = _source_path(destination_root, declaration.digest)
            if target.exists():
                _verify_destination(destination_root, declaration, source_size)
                records.append(
                    ImportedArtifact(
                        declaration.digest,
                        source_size,
                        declaration.roles,
                        True,
                        declaration.plan_digests,
                    )
                )
                continue
            staged = _source_path(staging_root, declaration.digest)
            staged.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copyfile(source, staged)
            except OSError as exc:
                raise ArtifactBootstrapError(
                    f"copy failed: {declaration.digest}"
                ) from exc
            copied_digest, copied_size = _hash_file(staged)
            if copied_digest != declaration.digest or copied_size != source_size:
                raise ArtifactBootstrapError(f"staged artifact verification failed: {declaration.digest}")

        storage = LocalStorage(destination_root)
        for declaration, _, source_size in source_records:
            target = _source_path(destination_root, declaration.digest)
            if target.exists():
                continue
            staged = _source_path(staging_root, declaration.digest)
            new_targets.append(target)
            storage.upload(staged, partition_artifact_key(declaration.digest))
            _verify_destination(destination_root, declaration, source_size)
            records.append(
                ImportedArtifact(
                    declaration.digest,
                    source_size,
                    declaration.roles,
                    False,
                    declaration.plan_digests,
                )
            )
        _write_receipt(receipt_path, receipt_payload)
        return records
    except ArtifactBootstrapError:
        for target in new_targets:
            target.unlink(missing_ok=True)
        raise
    except OSError as exc:
        for target in new_targets:
            target.unlink(missing_ok=True)
        raise ArtifactBootstrapError("receipt publication failed") from exc
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)


def verify_bootstrap_receipt(
    destination_root: str | Path,
    declarations: Iterable[DeclaredArtifact],
    *,
    release_plan_digest: str,
    config_digest: str,
) -> dict[str, Any]:
    destination_root = Path(destination_root).resolve()
    path = bootstrap_receipt_path(destination_root)
    if not path.is_file():
        raise ArtifactBootstrapError("bootstrap receipt is missing")
    try:
        actual = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ArtifactBootstrapError("bootstrap receipt is unreadable") from exc
    declarations = [
        item if isinstance(item, DeclaredArtifact) else DeclaredArtifact(**item.__dict__)
        for item in declarations
    ]
    if not declarations:
        raise ArtifactBootstrapError("no declared artifacts")
    actual_artifacts = actual.get("artifacts") if isinstance(actual, dict) else None
    if not isinstance(actual_artifacts, list) or len(actual_artifacts) != len(declarations):
        raise ArtifactBootstrapError("bootstrap receipt mismatch")
    measured_sizes: dict[str, int] = {}
    for declaration, item in zip(declarations, actual_artifacts):
        if not isinstance(item, dict) or item.get("digest") != declaration.digest:
            raise ArtifactBootstrapError("bootstrap receipt mismatch")
        if item.get("declared_size") != declaration.expected_size:
            raise ArtifactBootstrapError("bootstrap receipt mismatch")
        declaration = DeclaredArtifact(
            digest=item["digest"],
            expected_size=item.get("declared_size"),
            roles=tuple(item["roles"]),
            plan_digests=tuple(item["plan_digests"]),
        )
        stored_size = item.get("stored_size")
        if not isinstance(stored_size, int) or stored_size < 0:
            raise ArtifactBootstrapError("bootstrap receipt mismatch")
        _verify_destination(destination_root, declaration, stored_size)
        measured_sizes[declaration.digest] = stored_size
    expected = _receipt_payload(
        destination_root,
        declarations,
        release_plan_digest=release_plan_digest,
        config_digest=config_digest,
        stored_sizes=measured_sizes,
    )
    if actual != expected:
        raise ArtifactBootstrapError("bootstrap receipt mismatch")
    return actual

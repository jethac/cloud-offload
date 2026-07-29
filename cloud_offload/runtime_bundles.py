"""Reproducible archives for prepared custom-node and Python state."""

from __future__ import annotations

import hashlib
import os
import tarfile
from pathlib import Path
from typing import Any


EXCLUDED_PARTS = frozenset({".git", "__pycache__", ".pytest_cache", ".mypy_cache"})
EXCLUDED_SUFFIXES = frozenset({".pyc", ".pyo"})


def _included(path: Path, root: Path) -> bool:
    relative = path.relative_to(root)
    return not any(part in EXCLUDED_PARTS for part in relative.parts) and not (
        path.is_file() and path.suffix.lower() in EXCLUDED_SUFFIXES
    )


def build_reproducible_bundle(
    source_root: str | Path,
    destination: str | Path,
) -> dict[str, Any]:
    """Write a byte-stable safe tar archive of one directory tree.

    The archive has ordered members, fixed ownership and time, normalized
    permissions, no links, and no transient Python or source-control state.
    It stays uncompressed because compression library versions are not a stable
    part of the closure. Prepared storage still gives it the canonical bundle
    key, and ``tarfile`` detects its real format during restore.
    """

    root = Path(source_root).resolve()
    target = Path(destination).resolve()
    if not root.is_dir():
        raise ValueError(f"Runtime bundle source is not a directory: {root}")
    try:
        target.relative_to(root)
    except ValueError:
        pass
    else:
        raise ValueError("Runtime bundle destination must be outside its source")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".partial")
    temporary.unlink(missing_ok=True)
    members = sorted(
        (path for path in root.rglob("*") if _included(path, root)),
        key=lambda path: path.relative_to(root).as_posix(),
    )
    file_count = 0
    try:
        with tarfile.open(temporary, mode="w", format=tarfile.PAX_FORMAT) as archive:
            for path in members:
                if path.is_symlink() or not (path.is_dir() or path.is_file()):
                    raise ValueError(f"Runtime bundle contains an unsafe member: {path}")
                relative = path.relative_to(root).as_posix()
                info = tarfile.TarInfo(relative)
                info.uid = 0
                info.gid = 0
                info.uname = ""
                info.gname = ""
                info.mtime = 0
                if path.is_dir():
                    info.type = tarfile.DIRTYPE
                    info.mode = 0o755
                    archive.addfile(info)
                    continue
                info.type = tarfile.REGTYPE
                info.mode = 0o755 if path.stat().st_mode & 0o111 else 0o644
                info.size = path.stat().st_size
                with path.open("rb") as handle:
                    archive.addfile(info, handle)
                file_count += 1
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    digest = hashlib.sha256()
    with target.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return {
        "path": target,
        "sha256": digest.hexdigest(),
        "size": target.stat().st_size,
        "file_count": file_count,
    }

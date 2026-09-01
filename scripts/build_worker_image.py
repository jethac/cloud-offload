"""Build a worker image from exact Git tree/blob bytes."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
import tempfile


@dataclass(frozen=True)
class TreeEntry:
    path: str
    mode: int
    object_type: str
    oid: str


def _git(repo_root: Path, *args: str, text: bool = True) -> str | bytes:
    output = subprocess.check_output(["git", "-C", str(repo_root), *args], text=text)
    return output.strip() if text else output


def _commit_revision(repo_root: Path, revision: str) -> str:
    try:
        commit = str(_git(repo_root, "rev-parse", "--verify", f"{revision}^{{commit}}"))
    except subprocess.CalledProcessError as exc:
        raise ValueError(f"invalid source revision: {revision}") from exc
    if len(commit) != 40:
        raise ValueError(f"source revision is not a full commit: {commit}")
    return commit


def _validate_git_path(path: str) -> None:
    pure = PurePosixPath(path)
    if not path or "\x00" in path or pure.is_absolute() or ".." in pure.parts or "\\" in path:
        raise ValueError(f"unsafe tracked path: {path!r}")


def tree_entries(repo_root: Path, revision: str) -> tuple[TreeEntry, ...]:
    """Return every file entry and its exact Git object identity."""

    repo_root = Path(repo_root).resolve()
    commit = _commit_revision(repo_root, revision)
    raw = _git(repo_root, "ls-tree", "-r", "-z", commit, text=False)
    entries: list[TreeEntry] = []
    for record in bytes(raw).split(b"\0"):
        if not record:
            continue
        header, raw_path = record.split(b"\t", 1)
        mode_raw, object_type_raw, oid_raw = header.split()
        path = raw_path.decode("utf-8", errors="surrogateescape")
        _validate_git_path(path)
        mode = int(mode_raw, 8)
        object_type = object_type_raw.decode("ascii")
        oid = oid_raw.decode("ascii")
        if object_type != "blob" or mode not in {0o100644, 0o100755, 0o120000}:
            raise ValueError(f"unsupported Git tree entry: {path!r} ({mode_raw!r})")
        entries.append(TreeEntry(path, mode, object_type, oid))
    return tuple(sorted(entries, key=lambda entry: entry.path))


def tracked_manifest(repo_root: Path, revision: str) -> tuple[str, ...]:
    """Return the exact tracked file manifest for a commit."""

    return tuple(entry.path for entry in tree_entries(repo_root, revision))


def _inside(root: Path, candidate: Path) -> bool:
    try:
        candidate.relative_to(root)
    except ValueError:
        return False
    return True


def _safe_target(destination: Path, name: str) -> Path:
    _validate_git_path(name)
    target = (destination / Path(*PurePosixPath(name).parts)).resolve(strict=False)
    if not _inside(destination, target):
        raise ValueError(f"unsafe context path: {name!r}")
    return target


def _blob_bytes(repo_root: Path, oid: str) -> bytes:
    return bytes(_git(repo_root, "cat-file", "blob", oid, text=False))


def _link_target(destination: Path, entry: TreeEntry, raw: bytes) -> str:
    link = raw.decode("utf-8", errors="surrogateescape")
    if not link or "\x00" in link or "\\" in link:
        raise ValueError(f"unsafe symlink target: {entry.path!r}")
    target = _safe_target(destination, str(PurePosixPath(entry.path).parent / link))
    if not _inside(destination, target):
        raise ValueError(f"unsafe symlink target: {entry.path!r}")
    return os.path.relpath(target, (destination / Path(*PurePosixPath(entry.path).parts)).parent)


def _materialize_entry(repo_root: Path, destination: Path, entry: TreeEntry) -> None:
    target = _safe_target(destination, entry.path)
    target.parent.mkdir(parents=True, exist_ok=True)
    raw = _blob_bytes(repo_root, entry.oid)
    if entry.mode == 0o120000:
        try:
            os.symlink(_link_target(destination, entry, raw), target)
        except FileExistsError as exc:
            raise ValueError(f"duplicate context path: {entry.path!r}") from exc
        except OSError as exc:
            raise RuntimeError(
                f"cannot materialize symlink {entry.path!r}; enable symlink support"
            ) from exc
        return
    try:
        target.write_bytes(raw)
        target.chmod(stat.S_IMODE(entry.mode))
    except OSError as exc:
        raise RuntimeError(f"cannot materialize Git blob {entry.path!r}") from exc


def prepare_context(repo_root: Path, revision: str, destination: Path) -> tuple[str, ...]:
    """Materialize a tracked-only context from Git blobs, with safe paths/modes."""

    repo_root = Path(repo_root).resolve()
    destination = Path(destination).resolve()
    if _inside(repo_root, destination) or _inside(destination, repo_root):
        raise ValueError("build context must be disjoint from source repository")
    if destination.exists():
        raise ValueError(f"build context already exists: {destination}")

    entries = tree_entries(repo_root, revision)
    destination.mkdir(parents=True)
    try:
        for entry in entries:
            _materialize_entry(repo_root, destination, entry)
        actual = tuple(
            sorted(
                path.relative_to(destination).as_posix()
                for path in destination.rglob("*")
                if path.is_file() or path.is_symlink()
            )
        )
        expected = tuple(entry.path for entry in entries)
        if actual != expected:
            raise ValueError("Git tree manifest changed while staging build context")
        return actual
    except Exception:
        shutil.rmtree(destination, ignore_errors=True)
        raise


def build_image(repo_root: Path, revision: str, tag: str) -> tuple[str, ...]:
    """Build a tag from a temporary exact-blob context and remove the context."""

    repo_root = Path(repo_root).resolve()
    commit = _commit_revision(repo_root, revision)
    with tempfile.TemporaryDirectory(prefix="cloud-offload-image-") as temp_dir:
        context = Path(temp_dir) / "context"
        manifest = prepare_context(repo_root, commit, context)
        subprocess.run(
            [
                "docker", "buildx", "build", "--progress", "plain",
                "--file", str(context / "deploy/runtime-profiles/comfyui/Dockerfile.overlay"),
                "--build-arg", f"CLOUD_OFFLOAD_SOURCE_REVISION={commit}",
                "--tag", tag, "--load", str(context),
            ],
            check=True,
        )
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", required=True)
    parser.add_argument("--revision", default="HEAD")
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    manifest = build_image(repo_root, args.revision, args.tag)
    print(f"staged {len(manifest)} tracked files")
    print(f"built {args.tag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

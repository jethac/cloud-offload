"""Build a worker image from a verified, tracked-only git archive."""

from __future__ import annotations

import argparse
import io
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tarfile
import tempfile


def _git(repo_root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repo_root), *args], text=True
    ).strip()


def _commit_revision(repo_root: Path, revision: str) -> str:
    try:
        commit = _git(repo_root, "rev-parse", "--verify", f"{revision}^{{commit}}")
    except subprocess.CalledProcessError as exc:
        raise ValueError(f"invalid source revision: {revision}") from exc
    if len(commit) != 40:
        raise ValueError(f"source revision is not a full commit: {commit}")
    return commit


def tracked_manifest(repo_root: Path, revision: str) -> tuple[str, ...]:
    """Return the exact tracked file manifest for a commit."""

    repo_root = Path(repo_root).resolve()
    commit = _commit_revision(repo_root, revision)
    paths = tuple(
        sorted(
            path
            for path in _git(repo_root, "ls-tree", "-r", "--name-only", commit).splitlines()
            if path
        )
    )
    for path in paths:
        pure = PurePosixPath(path)
        if pure.is_absolute() or ".." in pure.parts or "\\" in path:
            raise ValueError(f"unsafe tracked path: {path}")
    return paths


def _archive_bytes(repo_root: Path, revision: str) -> bytes:
    commit = _commit_revision(Path(repo_root).resolve(), revision)
    return subprocess.check_output(
        ["git", "-C", str(Path(repo_root).resolve()), "archive", "--format=tar", commit]
    )


def _inside(root: Path, candidate: Path) -> bool:
    try:
        candidate.relative_to(root)
    except ValueError:
        return False
    return True


def _safe_member_path(destination: Path, name: str) -> Path:
    if not name or "\x00" in name:
        raise ValueError(f"unsafe archive path: {name!r}")
    pure = PurePosixPath(name)
    if pure.is_absolute() or ".." in pure.parts or "\\" in name:
        raise ValueError(f"unsafe archive path: {name!r}")
    target = (destination / Path(*pure.parts)).resolve(strict=False)
    if not _inside(destination, target):
        raise ValueError(f"unsafe archive path: {name!r}")
    return target


def _extract_archive(archive: bytes, destination: Path) -> None:
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as tar:
        members = tar.getmembers()
        for member in members:
            target = _safe_member_path(destination, member.name)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            if member.issym() or member.islnk():
                link_target = _safe_member_path(
                    destination, str(PurePosixPath(member.name).parent / member.linkname)
                )
                if not _inside(destination, link_target):
                    raise ValueError(f"unsafe archive link: {member.name!r}")
                os.symlink(os.path.relpath(link_target, target.parent), target)
                continue
            if not member.isreg():
                raise ValueError(f"unsupported archive member: {member.name!r}")
            source = tar.extractfile(member)
            if source is None:
                raise ValueError(f"archive member has no contents: {member.name!r}")
            target.write_bytes(source.read())
            target.chmod(member.mode & 0o777)


def prepare_context(repo_root: Path, revision: str, destination: Path) -> tuple[str, ...]:
    """Materialize a tracked-only build context and verify its manifest."""

    repo_root = Path(repo_root).resolve()
    destination = Path(destination).resolve()
    if destination.exists():
        raise ValueError(f"build context already exists: {destination}")
    if _inside(repo_root, destination):
        raise ValueError("build context must not be inside the source repository")

    expected = tracked_manifest(repo_root, revision)
    destination.mkdir(parents=True)
    try:
        _extract_archive(_archive_bytes(repo_root, revision), destination)
        actual = tuple(
            sorted(
                path.relative_to(destination).as_posix()
                for path in destination.rglob("*")
                if path.is_file() or path.is_symlink()
            )
        )
        if actual != expected:
            raise ValueError("git archive manifest changed while staging build context")
        return actual
    except Exception:
        shutil.rmtree(destination, ignore_errors=True)
        raise


def build_image(repo_root: Path, revision: str, tag: str) -> tuple[str, ...]:
    """Build a tag from a temporary tracked-only context and remove the context."""

    repo_root = Path(repo_root).resolve()
    commit = _commit_revision(repo_root, revision)
    with tempfile.TemporaryDirectory(prefix="cloud-offload-image-") as temp_dir:
        context = Path(temp_dir) / "context"
        manifest = prepare_context(repo_root, commit, context)
        subprocess.run(
            [
                "docker",
                "buildx",
                "build",
                "--file",
                str(context / "deploy/runtime-profiles/comfyui/Dockerfile.overlay"),
                "--build-arg",
                f"CLOUD_OFFLOAD_SOURCE_REVISION={commit}",
                "--tag",
                tag,
                "--load",
                str(context),
                "--progress",
                "plain",
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

"""Public ingress for the coordinator, so a rented worker can call home.

A worker runs in a datacenter and must reach the coordinator to claim jobs, but
the coordinator lives on a desktop behind NAT. Rather than make the operator
stand up a tunnel and paste its URL into config, the dispatcher can bring up an
ephemeral Cloudflare quick tunnel itself and hand the worker the resulting
``*.trycloudflare.com`` URL. The operator flips one switch; no URL is ever typed.

This exposes the coordinator to the public internet, which is safe only because
the bearer token is required on every route by default — the tunnel carries the
token requirement with it. See ``server._resolve_auth_required``.

The ``cloudflared`` binary is pinned to a specific release and verified by
SHA-256 before it is ever executed, the same discipline used for runner image
digests. We deliberately do not use ``pycloudflared``: it fetches ``latest``
unpinned and unverified into ``site-packages``.
"""

from __future__ import annotations

import atexit
from collections import deque
import hashlib
import logging
import os
import platform
import re
import subprocess
import tarfile
import tempfile
import threading
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

# Pinned cloudflared release. Bump deliberately; Cloudflare publishes a SHA-256
# for each asset in the release body (gh api repos/cloudflare/cloudflared/
# releases/tags/<ver> --jq .body).
CLOUDFLARED_VERSION = "2026.7.2"
_RELEASE_BASE = (
    "https://github.com/cloudflare/cloudflared/releases/download/"
    f"{CLOUDFLARED_VERSION}"
)

# asset key -> (asset filename, sha256, is_tgz)
_ASSETS: dict[str, tuple[str, str, bool]] = {
    "windows-amd64": (
        "cloudflared-windows-amd64.exe",
        "cdb5d4432f6ae1595654a692a51308b69d2bf7af961f5578d9391837cf072df9",
        False,
    ),
    "linux-amd64": (
        "cloudflared-linux-amd64",
        "ec905ea7b7e327ff8abdde8cb64697a2152de74dbcdbf6aec9db8364eb3886cd",
        False,
    ),
    "linux-arm64": (
        "cloudflared-linux-arm64",
        "405df476437e027fc6d18729a5a77155c0a33a6082aeee60a799a688f3052e66",
        False,
    ),
    "darwin-arm64": (
        "cloudflared-darwin-arm64.tgz",
        "0588df58494a6cadd38b9deb6078908a5054063c80784d92fdb8d4a5f3de1c67",
        True,
    ),
    "darwin-amd64": (
        "cloudflared-darwin-amd64.tgz",
        "a5afb0ba3da859da47bebc9a918d5b196bf7e4aec23589419b46356731bcc75f",
        True,
    ),
}

_QUICK_TUNNEL_URL = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")
_URL_TIMEOUT_SECONDS = 40


class IngressError(RuntimeError):
    """The coordinator could not establish public ingress."""


def _platform_key() -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()
    arch = "arm64" if machine in {"arm64", "aarch64"} else "amd64"
    if system == "windows":
        return "windows-amd64"
    if system == "darwin":
        return f"darwin-{arch}"
    if system == "linux":
        return f"linux-{arch}"
    raise IngressError(f"Unsupported platform for cloudflared: {system}/{machine}")


def _binary_dir() -> Path:
    from cloud_offload.config import CONFIG_DIR

    path = CONFIG_DIR / "bin"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_cloudflared() -> Path:
    """Return a verified cloudflared binary, downloading it once if needed."""
    key = _platform_key()
    asset_name, expected_sha, is_tgz = _ASSETS[key]
    suffix = ".exe" if key.startswith("windows") else ""
    binary = _binary_dir() / f"cloudflared-{CLOUDFLARED_VERSION}{suffix}"

    if binary.is_file() and _sha256(binary) == expected_sha:
        return binary

    url = f"{_RELEASE_BASE}/{asset_name}"
    logger.info("Downloading cloudflared %s from %s", CLOUDFLARED_VERSION, url)
    with tempfile.TemporaryDirectory(prefix="cloud-offload-cf-") as tmp:
        download = Path(tmp) / asset_name
        try:
            urllib.request.urlretrieve(url, download)  # noqa: S310 - pinned https URL
        except OSError as exc:
            raise IngressError(f"Could not download cloudflared: {exc}") from exc

        if is_tgz:
            # macOS ships only a tarball; verify the tarball, then extract.
            actual = _sha256(download)
            if actual != expected_sha:
                raise IngressError(
                    f"cloudflared checksum mismatch (expected {expected_sha}, got {actual})"
                )
            with tarfile.open(download, "r:gz") as archive:
                member = next(
                    (m for m in archive.getmembers() if m.name.endswith("cloudflared")),
                    None,
                )
                if member is None:
                    raise IngressError("cloudflared tarball had no binary")
                extracted = archive.extractfile(member)
                if extracted is None:
                    raise IngressError("cloudflared tarball member unreadable")
                binary.write_bytes(extracted.read())
        else:
            actual = _sha256(download)
            if actual != expected_sha:
                raise IngressError(
                    f"cloudflared checksum mismatch (expected {expected_sha}, got {actual})"
                )
            binary.write_bytes(download.read_bytes())

    if not key.startswith("windows"):
        binary.chmod(0o755)
    logger.info("cloudflared %s verified at %s", CLOUDFLARED_VERSION, binary)
    return binary


class CloudflaredTunnel:
    """An ephemeral Cloudflare quick tunnel to a local port.

    Start with :meth:`open`, read :attr:`url`, and always :meth:`close`. Also
    usable as a context manager. The tunnel URL rotates every run and is not
    persistable — that is fine, since the dispatcher injects it into each worker
    rather than remembering it.
    """

    def __init__(self, binary: Path | None = None):
        self._binary = binary
        self._process: subprocess.Popen | None = None
        self._url: str | None = None
        self._reader: threading.Thread | None = None
        self._stderr_tail: deque[str] = deque(maxlen=40)

    @property
    def url(self) -> str | None:
        return self._url

    @property
    def running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def open(self, port: int) -> str:
        if self.running and self._url:
            return self._url
        binary = self._binary or ensure_cloudflared()

        # Point cloudflared at an isolated, empty config dir: a stray
        # ~/.cloudflared/config.yaml disables quick tunnels.
        env = dict(os.environ)
        env["TUNNEL_ORIGIN_CERT"] = ""
        self._process = subprocess.Popen(
            [
                str(binary),
                "tunnel",
                "--no-autoupdate",
                "--url",
                f"http://127.0.0.1:{port}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        atexit.register(self.close)

        found: dict[str, str] = {}
        ready = threading.Event()

        def scan() -> None:
            assert self._process is not None and self._process.stderr is not None
            for line in self._process.stderr:
                self._stderr_tail.append(line.rstrip())
                match = _QUICK_TUNNEL_URL.search(line)
                if match and "url" not in found:
                    found["url"] = match.group(0)
                    ready.set()
                # Keep draining after the URL appears. cloudflared writes its
                # lifetime diagnostics to stderr; returning here eventually
                # fills the pipe and can strand a paid worker behind a dead
                # quick-tunnel URL.
            ready.set()  # stderr closed without a URL (process died)
            process = self._process
            if process is not None:
                logger.warning(
                    "cloudflared stderr closed (exit=%s); tail: %s",
                    process.poll(),
                    " | ".join(self._stderr_tail),
                )

        self._reader = threading.Thread(target=scan, daemon=True)
        self._reader.start()

        if not ready.wait(timeout=_URL_TIMEOUT_SECONDS) or "url" not in found:
            self.close()
            raise IngressError(
                "cloudflared did not report a tunnel URL within "
                f"{_URL_TIMEOUT_SECONDS}s"
            )
        self._url = found["url"]
        logger.info("Cloud Offload ingress ready at %s", self._url)
        return self._url

    def close(self) -> None:
        process, self._process, self._url = self._process, None, None
        if process is None or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()

    def __enter__(self) -> "CloudflaredTunnel":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

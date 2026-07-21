"""Cloud Offload local service configuration and discovery."""

from __future__ import annotations

import json
import os
import secrets
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen
from urllib.parse import urlparse

from cloud_offload.config import CONFIG_DIR

OLLAMA_PORT = 11434
DEFAULT_SERVICE_PORT = 11435
MAX_AUTO_PORT = 11550
SERVICE_NAME = "cloud-offload"
VERSION = "0.1.0"
API_VERSION = "v1"
SERVICE_FILE_ENV = "CLOUD_OFFLOAD_SERVICE_FILE"
URL_ENV = "CLOUD_OFFLOAD_URL"
PORT_ENV = "CLOUD_OFFLOAD_PORT"
TOKEN_ENV = "CLOUD_OFFLOAD_TOKEN"
TOKEN_FILE_ENV = "CLOUD_OFFLOAD_TOKEN_FILE"
HEALTH_TIMEOUT_SECONDS = 0.1
CONNECT_TIMEOUT_SECONDS = 0.005


class ServiceConfigError(ValueError):
    """Raised when service configuration would conflict with another service."""


def default_service_file() -> Path:
    configured = os.environ.get(SERVICE_FILE_ENV)
    if configured:
        return Path(configured).expanduser()
    return CONFIG_DIR / "service.json"


def default_token_file() -> Path:
    configured = os.environ.get(TOKEN_FILE_ENV)
    if configured:
        return Path(configured).expanduser()
    return CONFIG_DIR / "token"


def is_local_host(host: str) -> bool:
    host = (host or "").lower()
    return host in {"localhost", "127.0.0.1", "::1"} or host.startswith("127.")


def validate_bind_host(host: str, allow_lan: bool = False) -> None:
    if is_local_host(host):
        return
    if allow_lan:
        return
    raise ServiceConfigError(
        f"Refusing to bind Cloud Offload to {host!r} without --allow-lan; "
        "use 127.0.0.1 for local use"
    )


def _url_port(url: str) -> int | None:
    parsed = urlparse(url)
    if parsed.port is not None:
        return parsed.port
    if parsed.scheme == "http":
        return 80
    if parsed.scheme == "https":
        return 443
    return None


def reject_ollama_port(port: int | None, source: str) -> None:
    if port == OLLAMA_PORT:
        raise ServiceConfigError(
            f"{source} resolves to port {OLLAMA_PORT}, which is reserved for Ollama"
        )


def normalize_service_url(url: str, source: str = URL_ENV) -> str:
    normalized = url.rstrip("/")
    reject_ollama_port(_url_port(normalized), source)
    return normalized


def get_or_create_service_token(path: Path | None = None) -> tuple[str, Path]:
    configured = os.environ.get(TOKEN_ENV)
    token_path = path or default_token_file()
    if configured:
        return configured.strip(), token_path

    try:
        token = token_path.read_text(encoding="utf-8").strip()
        if token:
            return token, token_path
    except FileNotFoundError:
        pass

    token_path.parent.mkdir(parents=True, exist_ok=True)
    token = secrets.token_urlsafe(32)
    token_path.write_text(f"{token}\n", encoding="utf-8")
    try:
        token_path.chmod(0o600)
    except OSError:
        pass
    return token, token_path


def read_service_token(path: str | Path | None) -> str | None:
    if not path:
        configured = os.environ.get(TOKEN_ENV)
        return configured.strip() if configured else None
    try:
        token = Path(path).expanduser().read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return token or None


def is_port_available(host: str, port: int) -> bool:
    bind_host = host
    if host in {"localhost"}:
        bind_host = "127.0.0.1"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((bind_host, port))
        except OSError:
            return False
    return True


def is_port_connectable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=CONNECT_TIMEOUT_SECONDS):
            return True
    except OSError:
        return False


def choose_service_port(host: str, requested_port: int | None = None) -> int:
    env_port = os.environ.get(PORT_ENV)
    requested_source = "--port"
    if requested_port is None and env_port:
        try:
            requested_port = int(env_port)
            requested_source = PORT_ENV
        except ValueError as exc:
            raise ServiceConfigError(f"{PORT_ENV} must be an integer") from exc

    if requested_port not in {None, 0}:
        reject_ollama_port(requested_port, requested_source)
        if not is_port_available(host, requested_port):
            raise ServiceConfigError(f"Port {requested_port} is already in use")
        return requested_port

    for port in range(DEFAULT_SERVICE_PORT, MAX_AUTO_PORT + 1):
        if port == OLLAMA_PORT:
            continue
        if is_port_available(host, port):
            return port

    raise ServiceConfigError(
        f"No available Cloud Offload service port in {DEFAULT_SERVICE_PORT}-{MAX_AUTO_PORT}"
    )


def local_service_url(host: str, port: int, scheme: str = "http") -> str:
    display_host = host
    if host in {"0.0.0.0", "::", ""}:
        display_host = "127.0.0.1"
    return f"{scheme}://{display_host}:{port}"


def is_healthy_service_url(url: str, token: str | None = None) -> bool:
    try:
        request = Request(f"{normalize_service_url(url)}/api/health")
        if token:
            request.add_header("Authorization", f"Bearer {token}")
        with urlopen(request, timeout=HEALTH_TIMEOUT_SECONDS) as response:
            if response.status != 200:
                return False
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, URLError, json.JSONDecodeError, ServiceConfigError):
        return False
    return payload.get("name") == SERVICE_NAME and payload.get("status") == "ok"


def write_service_info(
    host: str,
    port: int,
    path: Path | None = None,
    *,
    auth_required: bool = False,
    token_path: Path | None = None,
    scheme: str = "http",
) -> Path:
    reject_ollama_port(port, "Cloud Offload service")
    service_path = path or default_service_file()
    service_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        # Carries the scheme so a client discovering a TLS coordinator does not
        # try to reach it over plaintext http.
        "url": local_service_url(host, port, scheme),
        "host": host,
        "port": port,
        "pid": os.getpid(),
        "auth_required": auth_required,
        "token_path": str(token_path) if auth_required and token_path else None,
        "version": VERSION,
        "api_version": API_VERSION,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    tmp_path = service_path.with_suffix(f"{service_path.suffix}.tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp_path, service_path)
    return service_path


def read_service_info(path: Path | None = None, *, require_healthy: bool = False) -> dict[str, Any] | None:
    service_path = path or default_service_file()
    try:
        payload = json.loads(service_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None

    url = payload.get("url")
    port = payload.get("port")
    if isinstance(port, str) and port.isdigit():
        port = int(port)
    if isinstance(port, int):
        reject_ollama_port(port, str(service_path))
    if isinstance(url, str):
        payload["url"] = normalize_service_url(url, str(service_path))
    if payload.get("auth_required") and payload.get("token_path"):
        payload["token"] = read_service_token(payload["token_path"])
    if require_healthy:
        if not isinstance(payload.get("url"), str):
            return None
        if not is_healthy_service_url(payload["url"], payload.get("token")):
            return None
    return payload


def scan_service_info() -> dict[str, Any] | None:
    for port in range(DEFAULT_SERVICE_PORT, MAX_AUTO_PORT + 1):
        if port == OLLAMA_PORT:
            continue
        url = local_service_url("127.0.0.1", port)
        if is_port_connectable("127.0.0.1", port) and is_healthy_service_url(url):
            return {
                "url": url,
                "host": "127.0.0.1",
                "port": port,
                "auth_required": False,
                "version": VERSION,
                "api_version": API_VERSION,
            }
    return None


def discover_service_info(*, require_healthy: bool = False) -> dict[str, Any]:
    configured = os.environ.get(URL_ENV)
    if configured:
        url = normalize_service_url(configured, URL_ENV)
        token = read_service_token(None)
        if require_healthy and not is_healthy_service_url(url, token):
            raise ServiceConfigError(f"Cloud Offload service is not healthy at {url}")
        return {"url": url, "token": token, "auth_required": bool(token)}

    service_info = read_service_info(require_healthy=require_healthy)
    if service_info and isinstance(service_info.get("url"), str):
        return service_info

    if require_healthy:
        scanned = scan_service_info()
        if scanned:
            return scanned
        raise ServiceConfigError("No healthy Cloud Offload service found")

    return {
        "url": local_service_url("127.0.0.1", DEFAULT_SERVICE_PORT),
        "auth_required": False,
    }


def discover_service_url() -> str:
    return discover_service_info()["url"]


if __name__ == "__main__":
    print(discover_service_url())

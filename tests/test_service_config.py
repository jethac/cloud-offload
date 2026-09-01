import json
import os

import pytest
from fastapi.testclient import TestClient

from cloud_offload import server
from cloud_offload.config import CloudConfig
from cloud_offload.queue import JobQueue
from cloud_offload.service_config import (
    OLLAMA_PORT,
    SERVICE_NAME,
    ServiceConfigError,
    choose_service_port,
    normalize_service_url,
    read_service_info,
    reject_ollama_port,
    validate_bind_host,
    write_service_info,
)


def test_choose_service_port_refuses_ollama_port():
    with pytest.raises(ServiceConfigError, match="Ollama"):
        choose_service_port("127.0.0.1", OLLAMA_PORT)


def test_normalize_service_url_refuses_ollama_port():
    with pytest.raises(ServiceConfigError, match="Ollama"):
        normalize_service_url("http://127.0.0.1:11434")


def test_reject_ollama_port_allows_default_port():
    reject_ollama_port(11435, "test")  # does not raise


def test_bind_host_requires_allow_lan_for_non_localhost():
    validate_bind_host("127.0.0.1")  # local host is always fine
    with pytest.raises(ServiceConfigError, match="allow-lan"):
        validate_bind_host("0.0.0.0")
    validate_bind_host("0.0.0.0", allow_lan=True)  # explicit opt-in


def test_service_info_round_trip(tmp_path):
    service_file = tmp_path / "service.json"
    write_service_info("127.0.0.1", 11435, service_file)

    info = read_service_info(service_file)
    assert info["url"] == "http://127.0.0.1:11435"
    assert info["port"] == 11435
    assert info["version"]


def test_strict_service_info_rejects_a_string_port(tmp_path):
    service_file = tmp_path / "service.json"
    service_file.write_text(
        json.dumps(
            {
                "url": "http://127.0.0.1:11435",
                "host": "127.0.0.1",
                "port": "11435",
                "pid": 111,
                "auth_required": False,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ServiceConfigError, match="integer"):
        read_service_info(service_file, strict=True)


def test_health_endpoint_reports_service_name():
    response = TestClient(server.app).get("/api/health")
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == SERVICE_NAME
    assert body["status"] == "ok"
    assert body["pid"] == os.getpid()


def test_root_endpoint_reports_service_name():
    response = TestClient(server.app).get("/")
    assert response.json()["name"] == SERVICE_NAME


def test_lan_bearer_middleware_challenges_and_exempts_worker_channel(
    monkeypatch, tmp_path
):
    config = CloudConfig(queue_db_path=str(tmp_path / "queue.db"))
    queue = JobQueue(config.queue_db_path)
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "auth_required", True)
    monkeypatch.setattr(server, "auth_token", "lan-token")
    client = TestClient(server.app)

    # A public route without the LAN bearer token is challenged by the middleware.
    challenged = client.get("/api/health")
    assert challenged.status_code == 401
    assert challenged.json()["error"]["code"] == "cloud_offload.auth_required"

    # The same route with the token passes.
    assert (
        client.get(
            "/api/health", headers={"Authorization": "Bearer lan-token"}
        ).status_code
        == 200
    )

    # The worker channel is exempt from the global bearer, so it reaches the
    # handler (which then enforces its own worker-token auth) instead of being
    # challenged by the middleware.
    worker = client.post(
        "/api/workers/claim",
        json={"worker_id": "w1", "provider": "runpod"},
    )
    assert worker.status_code == 401
    assert worker.json()["error"]["code"] != "cloud_offload.auth_required"


def test_port_probe_ignores_time_wait_sockets_from_a_stopped_server():
    """A coordinator restart must not be blocked by its own TIME_WAIT sockets.

    When the coordinator is stopped while it has live keep-alive connections,
    the server side closes first and the port lingers in TIME_WAIT. The real
    bind (uvicorn) uses SO_REUSEADDR, so the availability probe must too, or
    the restart canary fails with "Port ... is already in use" for up to 60s.
    """

    import socket
    import threading

    from cloud_offload.service_config import is_port_available

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    def connect_and_wait():
        with socket.create_connection(("127.0.0.1", port)) as client_sock:
            client_sock.recv(1)

    client = threading.Thread(target=connect_and_wait)
    client.start()
    accepted, _ = listener.accept()
    # Server closes first, leaving the server side of the pair in TIME_WAIT.
    accepted.close()
    client.join()
    listener.close()

    assert is_port_available("127.0.0.1", port)

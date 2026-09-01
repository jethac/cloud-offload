import json

from fastapi.testclient import TestClient
import pytest

from cloud_offload import config as config_module
from cloud_offload import preflight, server
from cloud_offload.config import CloudConfig


def _write_config(path, policy="smart"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"cloud": {"prepared_storage": {"policy": policy}}}),
        encoding="utf-8",
    )


def test_isolated_config_update_changes_live_preflight_policy_and_exact_source(
    monkeypatch, tmp_path
):
    isolated_home = tmp_path / "isolated"
    source = isolated_home / "config.json"
    unrelated_home = tmp_path / "process-home"
    unrelated_source = unrelated_home / "config.json"
    _write_config(source)
    _write_config(unrelated_source, policy="strict")
    runtime = CloudConfig.from_file(source, home=isolated_home)
    observed = []

    def fake_preflight(**kwargs):
        observed.append(kwargs["config"].prepared_storage["policy"])
        return {
            "schema": preflight.PREFLIGHT_SCHEMA,
            "preflight_id": "preflight-runtime-policy",
            "manifest_digest": "sha256:" + "a" * 64,
            "status": "blocked",
            "created_at": "2026-09-01T00:00:00Z",
            "expires_at": "2026-09-01T00:01:00Z",
            "blockers": [],
        }

    monkeypatch.setattr(config_module, "CONFIG_DIR", unrelated_home)
    monkeypatch.setattr(server, "_runtime_config", runtime)
    monkeypatch.setattr(server, "auth_required", False)
    monkeypatch.setattr(preflight, "build_partition_preflight", fake_preflight)
    client = TestClient(server.app)

    for requested in ("off", "strict", "smart"):
        updated = client.post(
            "/api/config", json={"prepared_storage": {"policy": requested}}
        )
        checked = client.post(
            "/api/preflight",
            json={
                "partition": {
                    "schema": "comfy.partition.job.v1",
                    "partition_id": "policy-check",
                    "workflow": {},
                }
            },
        )

        assert updated.status_code == 200
        assert updated.json()["config"]["prepared_storage"]["policy"] == requested
        assert updated.json()["applied_fields"] == ["prepared_storage"]
        assert updated.json()["pending_restart_fields"] == []
        assert updated.json()["restart_required"] is False
        assert checked.status_code == 200

    assert observed == ["off", "strict", "smart"]
    assert json.loads(source.read_text(encoding="utf-8"))["cloud"][
        "prepared_storage"
    ]["policy"] == "smart"
    assert json.loads(unrelated_source.read_text(encoding="utf-8"))["cloud"][
        "prepared_storage"
    ]["policy"] == "strict"
    restarted = CloudConfig.from_file(source, home=isolated_home)
    assert restarted.prepared_storage["policy"] == "smart"


def test_runtime_config_update_is_atomic_when_persistence_fails(monkeypatch, tmp_path):
    isolated_home = tmp_path / "isolated"
    source = isolated_home / "config.json"
    _write_config(source)
    runtime = CloudConfig.from_file(source, home=isolated_home)

    def fail_write(path, data):
        raise OSError("test write failure")

    monkeypatch.setattr(server, "_runtime_config", runtime)
    monkeypatch.setattr(server, "auth_required", False)
    monkeypatch.setattr(server, "_atomic_write_persisted_config", fail_write)
    response = TestClient(server.app, raise_server_exceptions=False).post(
        "/api/config", json={"prepared_storage": {"policy": "off"}}
    )

    assert response.status_code == 500
    assert server._runtime_config.prepared_storage["policy"] == "smart"
    assert json.loads(source.read_text(encoding="utf-8"))["cloud"][
        "prepared_storage"
    ]["policy"] == "smart"


def test_dispatcher_reloads_policy_written_by_isolated_config_route(
    monkeypatch, tmp_path
):
    from cloud_offload.dispatcher import Dispatcher

    isolated_home = tmp_path / "isolated"
    source = isolated_home / "config.json"
    unrelated_home = tmp_path / "process-home"
    _write_config(source)
    runtime = CloudConfig.from_file(source, home=isolated_home)
    dispatcher = Dispatcher(runtime, connector=object())

    class StopAfterReload(Exception):
        pass

    def stop_after_reload():
        raise StopAfterReload

    monkeypatch.setattr(config_module, "CONFIG_DIR", unrelated_home)
    monkeypatch.setattr(server, "_runtime_config", runtime)
    monkeypatch.setattr(server, "auth_required", False)
    monkeypatch.setattr(dispatcher, "_reconcile_leases", stop_after_reload)
    updated = TestClient(server.app).post(
        "/api/config", json={"prepared_storage": {"policy": "strict"}}
    )

    assert updated.status_code == 200
    assert dispatcher.config.prepared_storage["policy"] == "smart"
    with pytest.raises(StopAfterReload):
        dispatcher._tick()
    assert dispatcher.config.prepared_storage["policy"] == "strict"


def test_runtime_config_update_requires_service_auth_before_state_change(
    monkeypatch, tmp_path
):
    isolated_home = tmp_path / "isolated"
    source = isolated_home / "config.json"
    _write_config(source)
    runtime = CloudConfig.from_file(source, home=isolated_home)
    monkeypatch.setattr(server, "_runtime_config", runtime)
    monkeypatch.setattr(server, "auth_required", True)
    monkeypatch.setattr(server, "auth_token", "service-token")
    client = TestClient(server.app)

    rejected = client.post(
        "/api/config", json={"prepared_storage": {"policy": "off"}}
    )
    accepted = client.post(
        "/api/config",
        headers={"Authorization": "Bearer service-token"},
        json={"prepared_storage": {"policy": "strict"}},
    )

    assert rejected.status_code == 401
    assert accepted.status_code == 200
    assert accepted.json()["config"]["prepared_storage"]["policy"] == "strict"
    assert server._runtime_config.prepared_storage["policy"] == "strict"


def test_isolated_volume_binding_updates_live_source_only_and_survives_restart(
    monkeypatch, tmp_path
):
    isolated_home = tmp_path / "isolated"
    source = isolated_home / "config.json"
    unrelated_home = tmp_path / "process-home"
    unrelated_source = unrelated_home / "config.json"
    _write_config(source, policy="strict")
    _write_config(unrelated_source, policy="off")
    unrelated_before = unrelated_source.read_bytes()
    runtime = CloudConfig.from_file(source, home=isolated_home)
    monkeypatch.setattr(config_module, "CONFIG_DIR", unrelated_home)
    monkeypatch.setattr(server, "_runtime_config", runtime)

    changed = server._persist_prepared_volume_binding(runtime, "isolated-volume")

    assert changed is True
    assert server._runtime_config.prepared_storage["policy"] == "strict"
    assert (
        server._runtime_config.prepared_storage["existing_volume_id"]
        == "isolated-volume"
    )
    persisted = json.loads(source.read_text(encoding="utf-8"))
    assert persisted["cloud"]["prepared_storage"]["existing_volume_id"] == (
        "isolated-volume"
    )
    assert unrelated_source.read_bytes() == unrelated_before
    restarted = CloudConfig.from_file(source, home=isolated_home)
    assert restarted.prepared_storage["existing_volume_id"] == "isolated-volume"


def test_isolated_volume_binding_write_failure_is_atomic(monkeypatch, tmp_path):
    isolated_home = tmp_path / "isolated"
    source = isolated_home / "config.json"
    _write_config(source, policy="strict")
    source_before = source.read_bytes()
    runtime = CloudConfig.from_file(source, home=isolated_home)

    def fail_write(path, data):
        raise OSError("test binding write failure")

    monkeypatch.setattr(server, "_runtime_config", runtime)
    monkeypatch.setattr(server, "_atomic_write_persisted_config", fail_write)

    with pytest.raises(OSError, match="binding write failure"):
        server._persist_prepared_volume_binding(runtime, "never-applied")

    assert server._runtime_config.prepared_storage["existing_volume_id"] is None
    assert source.read_bytes() == source_before


def test_structural_config_update_stays_pending_until_restart(monkeypatch, tmp_path):
    from cloud_offload.dispatcher import Dispatcher

    isolated_home = tmp_path / "isolated"
    source = isolated_home / "config.json"
    _write_config(source)
    runtime = CloudConfig.from_file(source, home=isolated_home)
    old_queue = runtime.queue_db_path
    connector = object()
    dispatcher = Dispatcher(runtime, connector=connector)
    new_queue = str(isolated_home / "replacement.db")

    class StopAfterReload(Exception):
        pass

    def stop_after_reload():
        raise StopAfterReload

    monkeypatch.setattr(server, "_runtime_config", runtime)
    monkeypatch.setattr(server, "auth_required", False)
    monkeypatch.setattr(dispatcher, "_reconcile_leases", stop_after_reload)
    response = TestClient(server.app).post(
        "/api/config",
        json={
            "queue_db_path": new_queue,
            "provider": "vast.ai",
            "provider_order": ["vast.ai"],
            "connector_options": {"vast.ai": {"endpoint": "test"}},
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["restart_required"] is True
    assert body["applied_fields"] == []
    assert body["pending_restart_fields"] == [
        "connector_options",
        "provider",
        "provider_order",
        "queue_db_path",
    ]
    assert body["config"]["queue_db_path"] == old_queue
    assert body["config"]["provider"] == "runpod"
    assert body["config"]["connector_options"] == {}
    assert server._runtime_config.queue_db_path == old_queue
    assert server._runtime_config.provider == "runpod"
    assert dispatcher.config.queue_db_path == old_queue
    assert dispatcher.config.provider == "runpod"
    assert dispatcher.connectors == {"runpod": connector}
    with pytest.raises(StopAfterReload):
        dispatcher._tick()
    assert dispatcher.config.queue_db_path == old_queue
    assert dispatcher.config.provider == "runpod"
    assert dispatcher.connectors == {"runpod": connector}

    restarted = CloudConfig.from_file(source, home=isolated_home)
    assert restarted.queue_db_path == new_queue
    assert restarted.provider == "vast.ai"
    assert restarted.connector_options == {"vast.ai": {"endpoint": "test"}}


def test_structural_config_write_failure_changes_neither_live_nor_pending_state(
    monkeypatch, tmp_path
):
    isolated_home = tmp_path / "isolated"
    source = isolated_home / "config.json"
    _write_config(source)
    source_before = source.read_bytes()
    runtime = CloudConfig.from_file(source, home=isolated_home)
    old_queue = runtime.queue_db_path

    def fail_write(path, data):
        raise OSError("test structural write failure")

    monkeypatch.setattr(server, "_runtime_config", runtime)
    monkeypatch.setattr(server, "auth_required", False)
    monkeypatch.setattr(server, "_atomic_write_persisted_config", fail_write)
    response = TestClient(server.app, raise_server_exceptions=False).post(
        "/api/config",
        json={"queue_db_path": str(isolated_home / "replacement.db")},
    )

    assert response.status_code == 500
    assert server._runtime_config.queue_db_path == old_queue
    assert source.read_bytes() == source_before


def test_serve_without_explicit_config_pins_one_runtime_snapshot(
    monkeypatch, tmp_path
):
    source = tmp_path / "config.json"
    _write_config(source)
    runtime = CloudConfig.from_file(source)
    monkeypatch.setattr(
        config_module.CloudConfig,
        "load",
        classmethod(lambda cls: runtime),
    )
    monkeypatch.setattr(server, "validate_bind_host", lambda *args, **kwargs: None)
    monkeypatch.setattr(server, "_resolve_tls", lambda *args: (None, None))
    monkeypatch.setattr(server, "choose_service_port", lambda *args: 11435)
    monkeypatch.setattr(server, "_resolve_auth_required", lambda *args: False)
    monkeypatch.setattr(server, "write_service_info", lambda *args, **kwargs: source)
    monkeypatch.setattr(server.uvicorn, "run", lambda *args, **kwargs: None)

    server.serve(config=None)

    assert server._runtime_config is runtime


def test_unknown_config_field_is_rejected_without_state_or_file_change(
    monkeypatch, tmp_path
):
    isolated_home = tmp_path / "isolated"
    source = isolated_home / "config.json"
    _write_config(source)
    source_before = source.read_bytes()
    runtime = CloudConfig.from_file(source, home=isolated_home)
    old_queue = runtime.queue_db_path
    monkeypatch.setattr(server, "_runtime_config", runtime)
    monkeypatch.setattr(server, "auth_required", False)

    response = TestClient(server.app).post(
        "/api/config",
        json={"queue_db_pth": str(isolated_home / "typo.db")},
    )

    assert response.status_code == 400
    assert "Unknown config fields: queue_db_pth" in response.json()["error"]["message"]
    assert server._runtime_config.queue_db_path == old_queue
    assert source.read_bytes() == source_before


def test_round_tripped_public_config_ignores_read_only_and_unchanged_fields(
    monkeypatch, tmp_path
):
    isolated_home = tmp_path / "isolated"
    source = isolated_home / "config.json"
    _write_config(source)
    source_before = source.read_bytes()
    runtime = CloudConfig.from_file(source, home=isolated_home)
    monkeypatch.setattr(server, "_runtime_config", runtime)
    monkeypatch.setattr(server, "auth_required", False)
    client = TestClient(server.app)

    public = client.get("/api/config").json()
    response = client.post("/api/config", json=public)

    assert response.status_code == 200
    assert response.json()["applied_fields"] == []
    assert response.json()["pending_restart_fields"] == []
    assert response.json()["restart_required"] is False
    assert source.read_bytes() == source_before
    persisted = json.loads(source.read_text(encoding="utf-8"))["cloud"]
    for field_name in (
        "provider_auth_configured",
        "huggingface_configured",
        "worker_auth_configured",
        "worker_wheelhouse_configured",
        "coordinator_configured",
    ):
        assert field_name not in persisted

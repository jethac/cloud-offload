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

"""Shared test setup.

The important one is ``isolate_credentials``: it is autouse, so no test can
reach the developer's real OS keychain. Without it a test that expects "no
credential configured" would quietly pick up a real provider key from Credential
Manager, pass or fail for the wrong reason, and — worse — a test that exercised
a connector could spend real money.
"""

import pytest


class _InMemoryKeyring:
    """Stands in for the OS keychain for the duration of a test."""

    def __init__(self):
        self.store: dict[str, str] = {}

    def get_password(self, service, username):
        return self.store.get(username)

    def set_password(self, service, username, password):
        self.store[username] = password

    def delete_password(self, service, username):
        self.store.pop(username, None)

    def get_keyring(self):
        return self


@pytest.fixture(autouse=True)
def isolate_server_module_state():
    """Keep one in-process coordinator test from changing the next test."""
    from cloud_offload import server

    snapshot = {
        "auth_required": server.auth_required,
        "auth_token": server.auth_token,
        "last_error": server.last_error,
        "runtime_config": server._runtime_config,
        "hf_source_digests": dict(server._HF_SOURCE_DIGESTS),
        "dependency_overrides": dict(server.app.dependency_overrides),
    }
    server.auth_required = False
    server.auth_token = None
    server.last_error = None
    server._runtime_config = None
    server._HF_SOURCE_DIGESTS.clear()
    server.app.dependency_overrides.clear()
    try:
        yield
    finally:
        server.auth_required = snapshot["auth_required"]
        server.auth_token = snapshot["auth_token"]
        server.last_error = snapshot["last_error"]
        server._runtime_config = snapshot["runtime_config"]
        server._HF_SOURCE_DIGESTS.clear()
        server._HF_SOURCE_DIGESTS.update(snapshot["hf_source_digests"])
        server.app.dependency_overrides.clear()
        server.app.dependency_overrides.update(snapshot["dependency_overrides"])


@pytest.fixture(autouse=True)
def isolate_credentials(monkeypatch, tmp_path):
    """Point credential storage at throwaway state for every test."""
    from cloud_offload import config, credentials

    vault = _InMemoryKeyring()
    monkeypatch.setattr(credentials, "_keyring", lambda: vault)
    monkeypatch.setattr(config, "CREDENTIALS_FILE", tmp_path / "credentials.json")

    # Ambient provider credentials in the developer's environment must not leak
    # into tests either.
    for variable in (
        "VAST_API_KEY",
        "RUNPOD_API_KEY",
        "CLOUD_OFFLOAD_RUNPOD_API_KEY",
        "CLOUD_OFFLOAD_VAST_AI_API_KEY",
        "HF_TOKEN",
        "CLOUD_OFFLOAD_HUGGINGFACE_API_KEY",
    ):
        monkeypatch.delenv(variable, raising=False)
    return vault

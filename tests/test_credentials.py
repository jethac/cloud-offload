"""Credential storage: OS keychain, env override, legacy-file migration."""

import json

import pytest

from cloud_offload import credentials as creds


class FakeKeyring:
    """In-memory stand-in for the OS keychain."""

    def __init__(self, *, fail=False):
        self.store: dict[tuple[str, str], str] = {}
        self.fail = fail

    def get_password(self, service, username):
        if self.fail:
            raise RuntimeError("keychain is locked")
        return self.store.get((service, username))

    def set_password(self, service, username, password):
        if self.fail:
            raise RuntimeError("keychain is locked")
        self.store[(service, username)] = password

    def delete_password(self, service, username):
        if self.fail:
            raise RuntimeError("keychain is locked")
        self.store.pop((service, username), None)

    def get_keyring(self):
        return self


@pytest.fixture
def keychain(monkeypatch, tmp_path):
    """Isolate both the fake keychain and the legacy file location."""
    fake = FakeKeyring()
    monkeypatch.setattr(creds, "_keyring", lambda: fake)
    monkeypatch.setattr(creds, "legacy_credentials_file", lambda: tmp_path / "credentials.json")
    for provider in ("acme", "runpod", "vast.ai"):
        monkeypatch.delenv(creds.provider_env_var(provider), raising=False)
    return fake


def write_legacy(tmp_path, payload):
    (tmp_path / "credentials.json").write_text(json.dumps(payload), encoding="utf-8")


# === Naming ===

def test_provider_env_var_slugs_names():
    assert creds.provider_env_var("acme") == "CLOUD_OFFLOAD_ACME_API_KEY"
    assert creds.provider_env_var("vast.ai") == "CLOUD_OFFLOAD_VAST_AI_API_KEY"
    assert creds.provider_env_var("vast") == "CLOUD_OFFLOAD_VAST_AI_API_KEY"  # alias


# === Resolution order ===

def test_keychain_round_trip(keychain):
    assert creds.get_credential("acme") == ""

    creds.set_credential("acme", "secret-key")
    assert creds.get_credential("acme") == "secret-key"

    assert creds.delete_credential("acme") is True
    assert creds.get_credential("acme") == ""


def test_env_var_wins_over_keychain(keychain, monkeypatch):
    creds.set_credential("acme", "from-keychain")
    monkeypatch.setenv("CLOUD_OFFLOAD_ACME_API_KEY", "from-env")

    # Headless boxes have no keychain, so the env var must stay authoritative.
    assert creds.get_credential("acme") == "from-env"


def test_legacy_file_is_the_last_resort(keychain, tmp_path):
    write_legacy(tmp_path, {"acme": "from-file"})
    assert creds.get_credential("acme") == "from-file"

    # The keychain outranks it once the credential is stored there.
    creds.set_credential("acme", "from-keychain")
    assert creds.get_credential("acme") == "from-keychain"


def test_locked_keychain_does_not_crash_resolution(monkeypatch, tmp_path):
    """A locked or broken keychain must not take routing down."""
    fake = FakeKeyring(fail=True)
    monkeypatch.setattr(creds, "_keyring", lambda: fake)
    monkeypatch.setattr(creds, "legacy_credentials_file", lambda: tmp_path / "credentials.json")
    monkeypatch.delenv("CLOUD_OFFLOAD_ACME_API_KEY", raising=False)

    assert creds.get_credential("acme") == ""


def test_missing_backend_degrades_to_env_only(monkeypatch, tmp_path):
    def unavailable():
        raise creds.KeychainUnavailable("no backend")

    monkeypatch.setattr(creds, "_keyring", unavailable)
    monkeypatch.setattr(creds, "legacy_credentials_file", lambda: tmp_path / "credentials.json")

    monkeypatch.delenv("CLOUD_OFFLOAD_ACME_API_KEY", raising=False)
    assert creds.get_credential("acme") == ""

    monkeypatch.setenv("CLOUD_OFFLOAD_ACME_API_KEY", "env-key")
    assert creds.get_credential("acme") == "env-key"

    with pytest.raises(creds.KeychainUnavailable):
        creds.set_credential("acme", "x")

    status = creds.keychain_status()
    assert status["available"] is False and status["reason"]


# === Hugging Face token (named credential, not a connector) ===

def test_huggingface_token_resolution_order(keychain, monkeypatch):
    creds.set_credential("huggingface", "from-keychain")
    monkeypatch.setenv("CLOUD_OFFLOAD_HUGGINGFACE_API_KEY", "from-generic-env")
    monkeypatch.setenv("HF_TOKEN", "from-hf-token")

    # HF_TOKEN is what huggingface_hub itself reads, so it must stay canonical.
    assert creds.huggingface_token() == "from-hf-token"

    monkeypatch.delenv("HF_TOKEN")
    assert creds.huggingface_token() == "from-generic-env"

    monkeypatch.delenv("CLOUD_OFFLOAD_HUGGINGFACE_API_KEY")
    assert creds.huggingface_token() == "from-keychain"


def test_huggingface_token_is_empty_when_unconfigured(keychain, monkeypatch):
    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.delenv("CLOUD_OFFLOAD_HUGGINGFACE_API_KEY", raising=False)

    assert creds.huggingface_token() == ""


# === Migration ===

def test_migrate_moves_credentials_and_removes_the_file(keychain, tmp_path):
    write_legacy(tmp_path, {"runpod": "runpod-key", "vast.ai": "vast-key"})

    result = creds.migrate_legacy_file()

    assert result["migrated"] == ["runpod", "vast.ai"]
    assert result["failed"] == []
    assert result["removed_file"] is True
    assert not (tmp_path / "credentials.json").exists()
    assert creds.get_credential("runpod") == "runpod-key"
    assert creds.get_credential("vast.ai") == "vast-key"


def test_migration_keeps_the_file_when_a_key_fails(monkeypatch, tmp_path):
    fake = FakeKeyring()
    monkeypatch.setattr(creds, "_keyring", lambda: fake)
    monkeypatch.setattr(creds, "legacy_credentials_file", lambda: tmp_path / "credentials.json")
    write_legacy(tmp_path, {"good": "ok", "bad": "nope"})

    original = fake.set_password

    def flaky(service, username, password):
        if username == "bad":
            raise RuntimeError("denied")
        original(service, username, password)

    fake.set_password = flaky
    result = creds.migrate_legacy_file()

    # A partial migration must never delete the only copy of a key.
    assert result["migrated"] == ["good"]
    assert [entry["provider"] for entry in result["failed"]] == ["bad"]
    assert result["removed_file"] is False
    assert (tmp_path / "credentials.json").exists()


def test_migrate_is_a_no_op_without_a_legacy_file(keychain):
    assert creds.migrate_legacy_file() == {
        "migrated": [],
        "failed": [],
        "removed_file": False,
    }


def test_list_credentialed_providers_reports_resolvable_names(keychain, tmp_path):
    creds.set_credential("runpod", "key")
    write_legacy(tmp_path, {"legacy-only": "key"})

    found = creds.list_credentialed_providers(known=("runpod", "vast.ai"))

    assert "runpod" in found
    assert "legacy-only" in found  # discovered from the file
    assert "vast.ai" not in found  # registered but has no credential

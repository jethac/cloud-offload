"""Ingress: binary pinning, tunnel URL parsing, and dispatcher wiring.

No real cloudflared download or tunnel here — the binary is stubbed and the
subprocess is faked. The real download + tunnel is exercised by hand against a
live coordinator, not in CI.
"""

import io

import pytest

from cloud_offload import ingress


class FakeProc:
    """Minimal Popen stand-in that streams canned stderr lines."""

    def __init__(self, lines, *, exits=False):
        self.stderr = io.StringIO("".join(lines))
        self._exits = exits
        self._terminated = False

    def poll(self):
        return 0 if self._terminated else None

    def terminate(self):
        self._terminated = True

    def wait(self, timeout=None):
        self._terminated = True
        return 0

    def kill(self):
        self._terminated = True


BANNER = [
    "2026-07-21 INF Requesting new quick Tunnel on trycloudflare.com...\n",
    "2026-07-21 INF +----------------------------------------+\n",
    "2026-07-21 INF | https://calm-owl-river.trycloudflare.com |\n",
    "2026-07-21 INF +----------------------------------------+\n",
]


def test_platform_key_is_recognized():
    # Whatever the test host is, it must be a key we ship a checksum for.
    assert ingress._platform_key() in ingress._ASSETS


def test_every_pinned_asset_has_a_sha256():
    for key, (name, sha, _is_tgz) in ingress._ASSETS.items():
        assert name and len(sha) == 64 and all(c in "0123456789abcdef" for c in sha), key


def test_tunnel_parses_the_url_from_stderr(monkeypatch):
    monkeypatch.setattr(ingress, "ensure_cloudflared", lambda: "cloudflared")
    monkeypatch.setattr(ingress.subprocess, "Popen", lambda *a, **k: FakeProc(BANNER))

    tunnel = ingress.CloudflaredTunnel()
    url = tunnel.open(11436)

    assert url == "https://calm-owl-river.trycloudflare.com"
    assert tunnel.url == url
    tunnel.close()


def test_tunnel_raises_when_no_url_appears(monkeypatch):
    monkeypatch.setattr(ingress, "ensure_cloudflared", lambda: "cloudflared")
    monkeypatch.setattr(ingress, "_URL_TIMEOUT_SECONDS", 1)
    monkeypatch.setattr(
        ingress.subprocess,
        "Popen",
        lambda *a, **k: FakeProc(["2026 INF starting...\n", "2026 INF connected\n"]),
    )

    tunnel = ingress.CloudflaredTunnel()
    with pytest.raises(ingress.IngressError, match="did not report a tunnel URL"):
        tunnel.open(11436)


# === Dispatcher integration ===

def _dispatcher(tmp_path, **overrides):
    from types import SimpleNamespace
    from cloud_offload.dispatcher import Dispatcher
    from cloud_offload.config import CloudConfig

    config = CloudConfig(
        enabled=True,
        provider="runpod",
        provider_order=["runpod"],
        queue_db_path=str(tmp_path / "queue.db"),
        storage_path=str(tmp_path / "storage"),
        **overrides,
    )
    dispatcher = Dispatcher(config, connector=SimpleNamespace(name="runpod"))
    return dispatcher


def test_explicit_coordinator_url_needs_no_tunnel(tmp_path):
    dispatcher = _dispatcher(tmp_path, coordinator_url="https://coord.example")
    assert dispatcher._resolve_coordinator_url() == "https://coord.example"
    assert dispatcher._tunnel is None


def test_ingress_none_refuses_without_a_url(tmp_path):
    dispatcher = _dispatcher(tmp_path, ingress="none")
    assert dispatcher._resolve_coordinator_url() is None


def test_ingress_cloudflared_opens_a_tunnel_once(tmp_path, monkeypatch):
    from cloud_offload import ingress as ingress_module

    opens = []

    class FakeTunnel:
        def __init__(self):
            self._url = None

        @property
        def running(self):
            return self._url is not None

        @property
        def url(self):
            return self._url

        def open(self, port):
            opens.append(port)
            self._url = "https://fake.trycloudflare.com"
            return self._url

        def close(self):
            self._url = None

    monkeypatch.setattr(ingress_module, "CloudflaredTunnel", FakeTunnel)
    monkeypatch.setattr(
        "cloud_offload.service_config.read_service_info",
        lambda *a, **k: {"port": 11436, "url": "http://127.0.0.1:11436"},
    )

    dispatcher = _dispatcher(tmp_path, ingress="cloudflared")
    first = dispatcher._resolve_coordinator_url()
    second = dispatcher._resolve_coordinator_url()

    assert first == second == "https://fake.trycloudflare.com"
    assert opens == [11436]  # opened once, then reused

    dispatcher.shutdown()
    assert dispatcher._tunnel is None

"""Tests for the declarative provider spec CRUD, validation and dry-run routes.

These routes are the last mile of "add a provider without editing our source":
the engine could already validate and dry-run a spec, but only a human editing
``~/.cloud-offload/providers/*.json`` could put one there.

Every test runs against a temporary ``CLOUD_OFFLOAD_HOME`` and restores the
global connector registry afterwards, because registering a spec is a process
wide side effect that would otherwise leak into unrelated tests.
"""

import copy
import json

import pytest
from fastapi.testclient import TestClient

from cloud_offload import config as config_module
from cloud_offload import providers as providers_module
from cloud_offload import server
from cloud_offload.config import CloudConfig
from cloud_offload.providers import create_connector
from cloud_offload.providers.declarative import spec_directory, spec_file_path


# ---------------------------------------------------------------------------
# Fixtures and fakes
# ---------------------------------------------------------------------------


ACME_SPEC = {
    "spec_version": 1,
    "name": "acme",
    "display_name": "Acme GPU",
    "base_url": "https://api.acme.dev/v1",
    "auth": {"type": "bearer"},
    "endpoints": {
        "offers": {
            "method": "GET",
            "path": "offers",
            "items": "$.data",
            "map": {
                "id": {"path": "$.id", "type": "str", "required": True},
                "gpu_type": {"path": "$.gpu.name", "default": "unknown"},
                "gpu_ram_gb": {"path": "$.gpu.vram_mb", "unit": "MB->GB", "default": 0},
                "hourly_rate": {"path": "$.price_per_hour", "default": 0},
            },
        }
    },
}


class Response:
    def __init__(self, payload=None, *, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.text = "" if payload is None else json.dumps(payload)
        self.content = self.text.encode("utf-8")

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self.payload


class HTTP:
    """A fake HTTP client. No test in this file ever reaches a real provider."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        response = self.responses.pop(0) if self.responses else Response({"data": []})
        if isinstance(response, Exception):
            raise response
        return response


@pytest.fixture(autouse=True)
def sandbox(tmp_path, monkeypatch):
    """Temporary CONFIG_DIR plus a restored connector registry."""
    saved = {
        attribute: copy.deepcopy(getattr(providers_module, attribute))
        for attribute in ("_CONNECTORS", "_CANONICAL_NAMES", "_METADATA")
    }

    home = tmp_path / "cloud-offload"
    home.mkdir()
    monkeypatch.setenv("CLOUD_OFFLOAD_HOME", str(home))
    monkeypatch.setattr(config_module, "CONFIG_DIR", home)
    monkeypatch.setattr(config_module, "CREDENTIALS_FILE", home / "credentials.json")

    config = CloudConfig(queue_db_path=str(home / "queue.db"))
    config.provider_credentials = {"acme": "acme-secret"}
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)

    yield home

    for attribute, snapshot in saved.items():
        live = getattr(providers_module, attribute)
        live.clear()
        live.update(snapshot)


@pytest.fixture
def client():
    return TestClient(server.app)


@pytest.fixture
def dry_run(monkeypatch):
    """Point the dry-run route at a fake HTTP client and hand it back.

    Only the wiring is redirected: the real ``dry_run_spec`` still builds a real
    connector and maps the response. Nothing in this file can reach a provider.
    """
    from cloud_offload.providers.declarative import dry_run_spec

    def use(*responses):
        http = HTTP(*responses)
        monkeypatch.setattr(
            server,
            "_dry_run",
            lambda spec, api_key: dry_run_spec(spec, api_key=api_key, http=http),
        )
        return http

    return use


def write_spec(home, name, spec):
    directory = home / "providers"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.json"
    path.write_text(json.dumps(spec), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# CRUD round-trip
# ---------------------------------------------------------------------------


def test_spec_crud_round_trip(client, sandbox):
    """Create, list, get, update and delete one user spec over HTTP."""
    assert client.get("/api/providers/specs").json()["specs"] == []

    created = client.put("/api/providers/specs/acme", json=ACME_SPEC)
    assert created.status_code == 200
    body = created.json()
    assert body["name"] == "acme"
    assert body["created"] is True
    assert body["registered"] is True
    assert body["errors"] == []
    # Written where the loader looks, and to the name we asked for.
    assert body["source"] == str(spec_directory() / "acme.json")
    assert json.loads((sandbox / "providers" / "acme.json").read_text())["name"] == "acme"

    listing = client.get("/api/providers/specs").json()
    assert listing["directory"] == str(spec_directory())
    assert listing["specs"] == [
        {
            "name": "acme",
            "display_name": "Acme GPU",
            "source": str(spec_directory() / "acme.json"),
            "valid": True,
            "problems": [],
            "registered": True,
        }
    ]

    fetched = client.get("/api/providers/specs/acme").json()
    assert fetched["valid"] is True
    assert fetched["builtin"] is False
    assert fetched["spec"]["base_url"] == "https://api.acme.dev/v1"

    updated_spec = {**copy.deepcopy(ACME_SPEC), "display_name": "Acme Renamed"}
    updated = client.put("/api/providers/specs/acme", json=updated_spec)
    assert updated.status_code == 200
    assert updated.json()["created"] is False
    assert updated.json()["display_name"] == "Acme Renamed"
    assert client.get("/api/providers/specs/acme").json()["spec"]["display_name"] == (
        "Acme Renamed"
    )

    deleted = client.delete("/api/providers/specs/acme")
    assert deleted.status_code == 200
    # Honest about the registry having no unregister: it serves until restart.
    assert deleted.json()["deleted"] is True
    assert deleted.json()["restart_required"] is True
    assert not (sandbox / "providers" / "acme.json").exists()
    assert client.get("/api/providers/specs").json()["specs"] == []
    assert client.get("/api/providers/specs/acme").status_code == 404
    assert client.delete("/api/providers/specs/acme").status_code == 404


def test_created_spec_is_routable_through_create_connector(client, sandbox):
    """The point of re-registering on write: usable without a restart."""
    assert client.put("/api/providers/specs/acme", json=ACME_SPEC).status_code == 200

    config = CloudConfig(queue_db_path=str(sandbox / "queue.db"))
    config.provider_credentials = {"acme": "acme-secret"}
    connector = create_connector("acme", config)

    assert connector.name == "acme"
    assert connector.display_name == "Acme GPU"
    assert connector.base_url == "https://api.acme.dev/v1"

    # And it appears in the discovery route the UI reads.
    providers = client.get("/api/providers").json()["providers"]
    acme = next(entry for entry in providers if entry["provider"] == "acme")
    assert acme["kind"] == "declarative"
    assert acme["display_name"] == "Acme GPU"


def test_listing_reports_an_invalid_spec_with_its_problems(client, sandbox):
    """A hand-edited broken file is described, not hidden and not fatal."""
    write_spec(sandbox, "broken", {"name": "broken", "base_url": "ftp://nope"})
    write_spec(sandbox, "acme", ACME_SPEC)

    specs = client.get("/api/providers/specs").json()["specs"]
    by_name = {entry["name"]: entry for entry in specs}

    assert by_name["acme"]["valid"] is True
    assert by_name["broken"]["valid"] is False
    assert any("base_url" in problem for problem in by_name["broken"]["problems"])


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


def test_invalid_spec_is_rejected_before_anything_is_written(client, sandbox):
    broken = copy.deepcopy(ACME_SPEC)
    del broken["base_url"]
    broken["endpoints"]["offers"]["map"]["id"] = {"path": "$.[]"}

    response = client.put("/api/providers/specs/acme", json=broken)

    assert response.status_code == 400
    error = response.json()["error"]
    assert error["code"] == "cloud_offload.invalid_provider_spec"
    problems = error["details"]["problems"]
    assert any("base_url" in problem for problem in problems)
    assert len(problems) >= 2
    # Nothing reached disk, so an invalid spec cannot be loaded at next startup.
    assert not (sandbox / "providers" / "acme.json").exists()


def test_spec_may_not_shadow_a_coded_connector(client, sandbox):
    """The same rule register_declarative_providers enforces, applied earlier."""
    runpod = {**copy.deepcopy(ACME_SPEC), "name": "runpod"}

    response = client.put("/api/providers/specs/runpod", json=runpod)

    assert response.status_code == 409
    assert "already served by a builtin connector" in response.json()["error"]["message"]
    assert not (sandbox / "providers" / "runpod.json").exists()

    # An alias that collides is refused just as firmly as the name.
    aliased = {**copy.deepcopy(ACME_SPEC), "aliases": ["runpod"]}
    assert client.put("/api/providers/specs/acme", json=aliased).status_code == 409

    # Vast.ai is declarative, so a user spec may deliberately override it.
    override = {**copy.deepcopy(ACME_SPEC), "name": "vast.ai"}
    assert client.put("/api/providers/specs/vast.ai", json=override).status_code == 200


@pytest.mark.parametrize(
    "name",
    ["..", "../../etc/passwd", "a/b", "a\\b", "C:evil", ".hidden", "-lead", "a b", "x" * 80],
)
def test_spec_file_path_refuses_names_that_are_not_bare_stems(name, sandbox):
    """The sanitizer itself, exercised on inputs a route may never survive.

    Starlette decodes ``%2F`` before routing, so a name containing a separator
    404s at the router rather than reaching the handler. That is one layer, not
    the layer: this is the one that has to hold.
    """
    with pytest.raises(ValueError):
        spec_file_path(name)


def test_spec_file_path_accepts_a_bare_stem(sandbox):
    assert spec_file_path("acme") == spec_directory() / "acme.json"


@pytest.mark.parametrize("name", [".hidden", "-lead", "a b", "x" * 80])
def test_odd_names_are_refused_by_the_write_route(client, sandbox, name):
    response = client.put(f"/api/providers/specs/{name}", json=ACME_SPEC)

    assert response.status_code == 400
    assert "spec name" in response.json()["error"]["message"].lower()
    assert not (sandbox / "providers").exists()


def test_spec_name_must_match_the_url(client):
    response = client.put(
        "/api/providers/specs/acme", json={**copy.deepcopy(ACME_SPEC), "name": "other"}
    )
    assert response.status_code == 400
    assert "'other'" in response.json()["error"]["message"]


def test_spec_carrying_a_credential_is_refused(client, sandbox):
    """Specs are shareable precisely because they hold no secrets.

    The rule lives in ``validate_spec``, so /validate, the write route and the
    startup loader all give the same answer — the UI cannot report "Valid" for
    a spec that Save would then reject.
    """
    leaky = copy.deepcopy(ACME_SPEC)
    leaky["headers"] = {"Authorization": "Bearer sk-live-123"}

    checked = client.post("/api/providers/specs/validate", json=leaky).json()
    assert checked["valid"] is False
    assert "spec.headers.Authorization looks like a credential" in checked["problems"][0]

    response = client.put("/api/providers/specs/acme", json=leaky)
    assert response.status_code == 400
    error = response.json()["error"]
    assert error["code"] == "cloud_offload.invalid_provider_spec"
    assert any(
        "spec.headers.Authorization" in problem
        for problem in error["details"]["problems"]
    )
    assert not (sandbox / "providers" / "acme.json").exists()

    # A spec hand-dropped into the directory is reported the same way, because
    # the rule is in the loader's validation rather than in the write route.
    write_spec(sandbox, "leaky", {**leaky, "name": "leaky"})
    listed = client.get("/api/providers/specs").json()["specs"]
    entry = next(item for item in listed if item["name"] == "leaky")
    assert entry["valid"] is False
    assert any("looks like a credential" in problem for problem in entry["problems"])


def test_spec_names_are_canonicalized_like_every_other_provider_route(client, sandbox):
    """``vast`` is an alias of ``vast.ai`` everywhere, including here."""
    assert client.get("/api/providers/specs/vast").json()["name"] == "vast.ai"

    saved = client.put(
        "/api/providers/specs/vast", json={**copy.deepcopy(ACME_SPEC), "name": "vast.ai"}
    )
    assert saved.status_code == 200
    assert saved.json()["name"] == "vast.ai"
    assert (sandbox / "providers" / "vast.ai.json").exists()
    assert not (sandbox / "providers" / "vast.json").exists()


def test_builtin_specs_are_readable_but_not_deletable(client):
    fetched = client.get("/api/providers/specs/vast.ai")
    assert fetched.status_code == 200
    assert fetched.json()["builtin"] is True
    assert fetched.json()["editable"] is False
    assert fetched.json()["spec"]["endpoints"]["offers"]["path"] == "bundles"

    refused = client.delete("/api/providers/specs/vast.ai")
    assert refused.status_code == 409
    assert "built-in spec" in refused.json()["error"]["message"]


# ---------------------------------------------------------------------------
# Validate and dry run
# ---------------------------------------------------------------------------


def test_validate_route_writes_nothing(client, sandbox):
    ok = client.post("/api/providers/specs/validate", json=ACME_SPEC)
    assert ok.status_code == 200
    assert ok.json() == {"valid": True, "problems": []}

    broken = copy.deepcopy(ACME_SPEC)
    broken["auth"] = {"type": "magic"}
    del broken["endpoints"]["offers"]["map"]

    bad = client.post("/api/providers/specs/validate", json={"spec": broken})
    assert bad.status_code == 200
    assert bad.json()["valid"] is False
    problems = "\n".join(bad.json()["problems"])
    assert "unknown auth type" in problems
    assert "a 'map' is required" in problems

    assert not (sandbox / "providers").exists()


def test_dry_run_reports_offer_count_and_a_mapped_sample(client, dry_run, sandbox):
    http = dry_run(
        Response(
            {
                "data": [
                    {"id": 1, "gpu": {"name": "RTX 4090", "vram_mb": 24564},
                     "price_per_hour": 0.44},
                    {"id": 2, "gpu": {"name": "A100", "vram_mb": 81920},
                     "price_per_hour": 1.4},
                ]
            }
        )
    )

    response = client.post(
        "/api/providers/specs/dry-run",
        json={"spec": ACME_SPEC, "api_key": "probe-only-key"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["offer_count"] == 2
    assert payload["provider"] == "acme"
    assert payload["sample"]["gpu_type"] == "RTX 4090"
    assert payload["sample"]["gpu_ram_gb"] == pytest.approx(23.99, abs=0.01)
    assert payload["sample"]["provider"] == "acme"

    # The probe key authenticated the request but is not stored or echoed.
    assert http.requests[0][2]["headers"]["Authorization"] == "Bearer probe-only-key"
    assert "probe-only-key" not in response.text
    assert not (sandbox / "credentials.json").exists()
    assert not (sandbox / "providers").exists()


def test_dry_run_reports_failure_without_raising(client, dry_run):
    dry_run(Response({"detail": "unauthorized"}, status_code=401))

    response = client.post(
        "/api/providers/specs/dry-run", json={"spec": ACME_SPEC, "api_key": "wrong-key"}
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is False
    assert payload["offer_count"] == 0
    assert "401" in payload["error"]

    # An invalid spec fails as problems, never as a request.
    broken = dry_run()
    invalid = client.post(
        "/api/providers/specs/dry-run", json={"spec": {"name": "acme"}}
    ).json()
    assert invalid["ok"] is False
    assert invalid["problems"]
    assert broken.requests == []


def test_dry_run_falls_back_to_the_stored_credential(client, dry_run):
    """No api_key in the body means "use whatever this provider resolves to"."""
    http = dry_run(Response({"data": []}))

    payload = client.post(
        "/api/providers/specs/dry-run", json={"spec": ACME_SPEC}
    ).json()

    assert payload["ok"] is True
    assert payload["offer_count"] == 0
    assert http.requests[0][2]["headers"]["Authorization"] == "Bearer acme-secret"


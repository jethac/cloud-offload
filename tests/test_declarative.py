"""Tests for the declarative, spec-driven REST connector.

The headline test drives the shipped Vast.ai spec against fake payloads shaped
like real Vast.ai responses and asserts the declarative connector reproduces the
coded ``VastConnector`` byte for byte.  That differential is what justifies
retiring the coded connector.
"""

import base64
import copy
import json
from pathlib import Path

import pytest

from cloud_offload import providers as provider_registry
from cloud_offload.config import CloudConfig
from cloud_offload.providers import connector_metadata, create_connector
from cloud_offload.providers.base import Instance
from cloud_offload.providers.declarative import (
    BUILTIN_SPEC_DIR,
    DeclarativeRequestError,
    DeclarativeRestConnector,
    DeclarativeSpecError,
    builtin_provider_specs,
    dry_run_spec,
    load_provider_specs,
    parse_path,
    register_declarative_providers,
    resolve_path,
    validate_spec,
)
from cloud_offload.providers.vast import VastConnector

REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class Response:
    def __init__(self, payload=None, *, status_code=200, text=None):
        self.payload = payload
        self.status_code = status_code
        self.text = ("" if payload is None else json.dumps(payload)) if text is None else text
        self.content = self.text.encode("utf-8")

    def raise_for_status(self):
        if self.status_code >= 400:
            error = RuntimeError(f"HTTP {self.status_code}")
            error.response = self
            raise error

    def json(self):
        if self.payload is None and self.text:
            raise ValueError("not JSON")
        return self.payload


class HTTP:
    """Replays canned responses and records what was asked for."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        if not self.responses:
            raise AssertionError(f"unexpected request: {method} {url}")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    @property
    def last(self):
        return self.requests[-1]


class Clock:
    """Fake monotonic clock that only advances when something sleeps."""

    def __init__(self):
        self.now = 0.0
        self.slept = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


@pytest.fixture
def registry_sandbox():
    """Snapshot the connector registry so tests cannot leak registrations."""
    saved = (
        dict(provider_registry._CONNECTORS),
        dict(provider_registry._CANONICAL_NAMES),
        copy.deepcopy(provider_registry._METADATA),
    )
    yield provider_registry
    provider_registry._CONNECTORS.clear()
    provider_registry._CONNECTORS.update(saved[0])
    provider_registry._CANONICAL_NAMES.clear()
    provider_registry._CANONICAL_NAMES.update(saved[1])
    provider_registry._METADATA.clear()
    provider_registry._METADATA.update(saved[2])


# ---------------------------------------------------------------------------
# A generic (non-Vast) spec, exercising the engine on its own terms
# ---------------------------------------------------------------------------


ACME_SPEC = {
    "spec_version": 1,
    "name": "acme",
    "display_name": "Acme GPU",
    "base_url": "https://api.acme.dev/v1",
    "auth": {"type": "bearer"},
    "status_map": {"provisioning": "pending", "active": "running", "halted": "stopped"},
    "endpoints": {
        "offers": {
            "method": "GET",
            "path": "offers",
            "query": {"region": "any"},
            "items": "$.data.items",
            "map": {
                "id": {"path": "$.id", "type": "str"},
                "gpu_type": "$.gpu.name",
                "gpu_count": {"path": "$.gpu.count", "default": 1},
                "gpu_ram_gb": {"path": "$.gpu.vram_mb", "unit": "MB->GB", "default": 0},
                "hourly_rate": {"path": "$.price.cents_per_hour", "unit": "cents->USD"},
                "location": {"path": "$.region", "default": "unknown"},
            },
        },
        "launch": {
            "method": "POST",
            "path": "instances",
            "body": {
                "offer": "{{offer_id}}",
                "image": "{{docker_image}}",
                "env": {"$value": "env_vars", "omit_if": "empty"},
            },
            "map": {
                "id": {"path": "$.instance.id", "type": "str", "required": True},
                "status": "$.instance.state",
                "gpu_type": "$.instance.gpu",
                "hourly_rate": "$.instance.price",
            },
        },
        "get": {
            "method": "GET",
            "path": "instances/{{instance_id}}",
            "map": {
                "id": {"path": "$.id", "type": "str", "required": True},
                "status": "$.state",
                "gpu_type": "$.gpu",
                "ip_address": "$.net.ip",
                "ssh_port": "$.net.ssh_port",
                "metadata": {"zone": "$.zone"},
            },
        },
        "list": {
            "method": "GET",
            "path": "instances",
            "items": "$.data",
            "select": {"where": "$.state", "in": ["active", "provisioning"]},
            "map": {
                "id": {"path": "$.id", "type": "str", "required": True},
                "status": "$.state",
                "gpu_type": "$.gpu",
            },
        },
        "terminate": {"method": "DELETE", "path": "instances/{{instance_id}}"},
        "balance": {
            "method": "GET",
            "path": "account",
            "currency": "EUR",
            "map": {"balance": {"path": "$.credit_cents", "unit": "cents->USD"}},
        },
    },
}


OFFERS_PAYLOAD = {
    "data": {
        "items": [
            {
                "id": 101,
                "gpu": {"name": "RTX 4090", "count": 1, "vram_mb": 24576},
                "price": {"cents_per_hour": 42},
                "region": "eu-west",
            },
            {
                "id": 102,
                "gpu": {"name": "A100", "count": 2, "vram_mb": 81920},
                "price": {"cents_per_hour": 210},
                "region": "us-east",
            },
        ]
    }
}


def acme(*responses, spec=None, **kwargs):
    http = HTTP(*responses)
    connector = DeclarativeRestConnector(
        spec or ACME_SPEC, api_key="acme-secret", http=http, **kwargs
    )
    return connector, http


# ---------------------------------------------------------------------------
# Path accessor
# ---------------------------------------------------------------------------


def test_path_accessor_handles_dots_indexes_and_absences():
    data = {"a": {"b": [{"c": 7}]}}

    assert parse_path("$.a.b[0].c") == ["a", "b", 0, "c"]
    assert parse_path("a.b") == ["a", "b"]
    assert resolve_path(data, "$.a.b[0].c") == 7
    assert resolve_path(data, "$.a.b[9].c", "fallback") == "fallback"
    assert resolve_path(data, "$.missing.deep", None) is None
    assert resolve_path(data, "$") == data

    for bad in ("$.a..b", "$.a[", "$.a[x]", "$.a]b", ""):
        with pytest.raises(DeclarativeSpecError):
            parse_path(bad)


# ---------------------------------------------------------------------------
# Offers
# ---------------------------------------------------------------------------


def test_offers_map_scale_units_and_normalize():
    connector, http = acme(Response(OFFERS_PAYLOAD))

    offers = connector.list_available()

    assert [offer["id"] for offer in offers] == ["101", "102"]
    # MB -> GB and cents -> USD are declared in the spec, not coded here.
    assert offers[0]["gpu_ram_gb"] == 24.0
    assert offers[1]["gpu_ram_gb"] == 80.0
    assert offers[0]["hourly_rate"] == pytest.approx(0.42)
    assert offers[0]["provider"] == "acme"
    assert offers[0]["gpu_type"] == "RTX 4090"
    assert offers[0]["location"] == "eu-west"
    assert offers[0]["raw"] is OFFERS_PAYLOAD["data"]["items"][0]

    method, url, kwargs = http.last
    assert (method, url) == ("GET", "https://api.acme.dev/v1/offers")
    assert kwargs["params"] == {"region": "any"}
    assert kwargs["headers"]["Authorization"] == "Bearer acme-secret"


def test_offers_client_filter_enforces_caps_without_server_filters():
    connector, _ = acme(Response(OFFERS_PAYLOAD))
    assert [o["id"] for o in connector.list_available(max_hourly_rate=1.0)] == ["101"]

    connector, _ = acme(Response(OFFERS_PAYLOAD))
    assert [o["id"] for o in connector.list_available(min_gpu_ram=40)] == ["102"]

    connector, _ = acme(Response(OFFERS_PAYLOAD))
    assert [o["id"] for o in connector.list_available(gpu_type="rtx_4090")] == ["101"]

    # find_cheapest is inherited from CloudConnector and must see filtered offers.
    connector, _ = acme(Response(OFFERS_PAYLOAD))
    assert connector.find_cheapest(min_gpu_ram=1, max_hourly_rate=5)["id"] == "101"


def test_offers_client_filter_can_be_disabled_when_the_server_filters():
    spec = copy.deepcopy(ACME_SPEC)
    spec["client_filter"] = False
    connector, _ = acme(Response(OFFERS_PAYLOAD), spec=spec)

    assert len(connector.list_available(max_hourly_rate=0.01)) == 2


def test_json_encoded_query_parameter_and_conditional_filters():
    spec = copy.deepcopy(ACME_SPEC)
    spec["endpoints"]["offers"]["query"] = {
        "q": {
            "$json": {
                "verified": {"eq": True},
                "gpu_name": {"$when": "gpu_type", "eq": "{{gpu_type}}"},
                "gpu_ram": {
                    "$when": "min_gpu_ram",
                    "gte": {"$value": "min_gpu_ram", "scale": 1024},
                },
            }
        }
    }

    connector, http = acme(Response(OFFERS_PAYLOAD), spec=spec)
    connector.list_available()
    assert json.loads(http.last[2]["params"]["q"]) == {"verified": {"eq": True}}

    connector, http = acme(Response(OFFERS_PAYLOAD), spec=spec)
    connector.list_available(gpu_type="A100", min_gpu_ram=40)
    assert json.loads(http.last[2]["params"]["q"]) == {
        "verified": {"eq": True},
        "gpu_name": {"eq": "A100"},
        "gpu_ram": {"gte": 40960},
    }


# ---------------------------------------------------------------------------
# Launch / get / list / terminate / balance
# ---------------------------------------------------------------------------


def test_launch_maps_instance_and_status_map():
    payload = {
        "instance": {"id": 55, "state": "provisioning", "gpu": "A100", "price": 1.25}
    }
    connector, http = acme(Response(payload))

    instance = connector.launch("101", "ghcr.io/example/runner:1", env_vars={"A": "1"})

    assert isinstance(instance, Instance)
    assert instance.id == "55"
    assert instance.status == "pending"  # provisioning -> pending via status_map
    assert instance.gpu_type == "A100"
    assert instance.hourly_rate == 1.25
    assert instance.provider == "acme"

    method, url, kwargs = http.last
    assert (method, url) == ("POST", "https://api.acme.dev/v1/instances")
    assert kwargs["json"] == {
        "offer": "101",
        "image": "ghcr.io/example/runner:1",
        "env": {"A": "1"},
    }


def test_launch_omits_empty_optional_body_fields():
    payload = {"instance": {"id": 55, "state": "active", "gpu": "A100", "price": 1.0}}
    connector, http = acme(Response(payload))

    connector.launch("101", "img")

    assert "env" not in http.last[2]["json"]


def test_launch_waits_for_running_with_a_fake_clock():
    spec = copy.deepcopy(ACME_SPEC)
    spec["endpoints"]["launch"]["wait_for"] = {
        "status": "running",
        "timeout_seconds": 60,
        "interval_seconds": 5,
    }
    clock = Clock()
    connector, http = acme(
        Response({"instance": {"id": 55, "state": "provisioning"}}),
        Response({"id": 55, "state": "provisioning", "gpu": "A100"}),
        Response({"id": 55, "state": "active", "gpu": "A100", "net": {"ip": "1.2.3.4"}}),
        spec=spec,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )

    instance = connector.launch("101", "img")

    assert instance.status == "running"
    assert instance.ip_address == "1.2.3.4"
    assert clock.slept == [5]  # polled twice, slept once, never really slept
    assert [request[0] for request in http.requests] == ["POST", "GET", "GET"]


def test_launch_wait_for_times_out_without_sleeping_for_real():
    spec = copy.deepcopy(ACME_SPEC)
    spec["endpoints"]["launch"]["wait_for"] = {
        "status": "running",
        "timeout_seconds": 10,
        "interval_seconds": 5,
    }
    clock = Clock()
    connector, _ = acme(
        Response({"instance": {"id": 55, "state": "provisioning"}}),
        *[Response({"id": 55, "state": "provisioning"}) for _ in range(3)],
        spec=spec,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )

    with pytest.raises(TimeoutError, match="did not reach 'running'"):
        connector.launch("101", "img")
    assert clock.now == 10


def test_get_instance_maps_metadata_and_missing_returns_none():
    payload = {
        "id": 55,
        "state": "active",
        "gpu": "A100",
        "net": {"ip": "10.0.0.5", "ssh_port": 2222},
        "zone": "eu-west-1a",
    }
    connector, http = acme(Response(payload))

    instance = connector.get_instance("55")

    assert instance.id == "55"
    assert instance.status == "running"
    assert instance.ip_address == "10.0.0.5"
    assert instance.ssh_port == 2222
    assert instance.metadata == {"zone": "eu-west-1a"}
    assert http.last[1] == "https://api.acme.dev/v1/instances/55"

    connector, _ = acme(Response(None, status_code=404))
    assert connector.get_instance("nope") is None


def test_get_instance_selects_from_a_collection_when_there_is_no_by_id_route():
    spec = copy.deepcopy(ACME_SPEC)
    spec["endpoints"]["get"] = {
        "method": "GET",
        "path": "instances",
        "items": "$.data",
        "select": {"where": "$.id", "equals": "{{instance_id}}", "type": "str"},
        "map": {"id": {"path": "$.id", "type": "str"}, "status": "$.state", "gpu_type": "$.gpu"},
    }
    payload = {
        "data": [
            {"id": 1, "state": "halted", "gpu": "T4"},
            {"id": 55, "state": "active", "gpu": "A100"},
        ]
    }

    connector, _ = acme(Response(payload), spec=spec)
    found = connector.get_instance("55")
    assert (found.id, found.status, found.gpu_type) == ("55", "running", "A100")

    connector, _ = acme(Response(payload), spec=spec)
    assert connector.get_instance("999") is None


def test_list_instances_applies_the_select_filter():
    payload = {
        "data": [
            {"id": 1, "state": "active", "gpu": "A100"},
            {"id": 2, "state": "halted", "gpu": "T4"},
            {"id": 3, "state": "provisioning", "gpu": "L40"},
        ]
    }
    connector, _ = acme(Response(payload))

    instances = connector.list_instances()

    assert [(i.id, i.status) for i in instances] == [("1", "running"), ("3", "pending")]


def test_terminate_and_balance():
    connector, http = acme(Response(None, status_code=204))
    assert connector.terminate("55") is True
    assert http.last[0] == "DELETE"

    connector, _ = acme(Response({"error": "nope"}, status_code=409))
    assert connector.terminate("55") is False

    connector, _ = acme(Response({"credit_cents": 12345}))
    assert connector.account_balance() == {
        "available": True,
        "currency": "EUR",
        "balance": pytest.approx(123.45),
    }

    spec = copy.deepcopy(ACME_SPEC)
    del spec["endpoints"]["balance"]
    connector, _ = acme(spec=spec)
    assert connector.account_balance() == {"available": False, "currency": "USD"}


# ---------------------------------------------------------------------------
# Auth styles
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "auth, expected_headers, expected_params",
    [
        ({"type": "bearer"}, {"Authorization": "Bearer acme-secret"}, {}),
        (
            {"type": "header", "name": "X-Api-Key"},
            {"X-Api-Key": "acme-secret"},
            {},
        ),
        (
            {"type": "header", "name": "X-Api-Key", "prefix": "Token "},
            {"X-Api-Key": "Token acme-secret"},
            {},
        ),
        ({"type": "query", "name": "api_key"}, {}, {"api_key": "acme-secret"}),
        (
            {"type": "basic"},
            {
                "Authorization": "Basic "
                + base64.b64encode(b"acme-secret:").decode("ascii")
            },
            {},
        ),
        (
            {"type": "basic", "in": "password", "username": "apikey"},
            {
                "Authorization": "Basic "
                + base64.b64encode(b"apikey:acme-secret").decode("ascii")
            },
            {},
        ),
    ],
)
def test_auth_styles(auth, expected_headers, expected_params):
    spec = copy.deepcopy(ACME_SPEC)
    spec["auth"] = auth
    connector, http = acme(Response(OFFERS_PAYLOAD), spec=spec)

    connector.list_available()

    _, _, kwargs = http.last
    for key, value in expected_headers.items():
        assert kwargs["headers"][key] == value
    for key, value in expected_params.items():
        assert kwargs["params"][key] == value
    if not expected_params:
        assert "api_key" not in kwargs.get("params", {})


def test_auth_none_needs_no_credential_but_others_do():
    spec = copy.deepcopy(ACME_SPEC)
    spec["auth"] = {"type": "none"}
    connector = DeclarativeRestConnector(spec, http=HTTP(Response(OFFERS_PAYLOAD)))
    assert connector.list_available()

    with pytest.raises(ValueError, match="API key required"):
        DeclarativeRestConnector(ACME_SPEC, api_key="", http=HTTP())


# ---------------------------------------------------------------------------
# Defensive behaviour
# ---------------------------------------------------------------------------


def test_errors_are_loud_rather_than_silently_wrong():
    connector, _ = acme(Response({"boom": True}, status_code=500))
    with pytest.raises(DeclarativeRequestError, match="HTTP 500"):
        connector.list_available()

    connector, _ = acme(Response(None, text="<html>gateway error</html>"))
    with pytest.raises(DeclarativeRequestError, match="non-JSON body"):
        connector.list_available()

    connector, _ = acme(Response({"data": {}}))
    with pytest.raises(DeclarativeRequestError, match="no '\\$\\.data\\.items' collection"):
        connector.list_available()

    connector, _ = acme(Response({"data": {"items": {"not": "a list"}}}))
    with pytest.raises(DeclarativeRequestError, match="expected a list"):
        connector.list_available()

    connector, _ = acme(Response({"instance": {"state": "active"}}))
    with pytest.raises(DeclarativeRequestError, match="required field"):
        connector.launch("1", "img")

    spec = copy.deepcopy(ACME_SPEC)
    del spec["endpoints"]["terminate"]
    connector, _ = acme(spec=spec)
    with pytest.raises(DeclarativeSpecError, match="no 'get' endpoint|no 'terminate'"):
        connector._endpoint("terminate")

    connector, _ = acme(Response({"data": {"items": [{"id": 1, "gpu": {"vram_mb": "lots"}}]}}))
    with pytest.raises(DeclarativeRequestError, match="cannot scale non-numeric"):
        connector.list_available()


# ---------------------------------------------------------------------------
# validate_spec
# ---------------------------------------------------------------------------


def test_validate_spec_accepts_the_shipped_specs():
    assert validate_spec(ACME_SPEC) == []
    for path in sorted(BUILTIN_SPEC_DIR.glob("*.json")):
        assert validate_spec(json.loads(path.read_text(encoding="utf-8"))) == []
    for path in sorted((REPO_ROOT / "examples" / "providers").glob("*.json")):
        assert validate_spec(json.loads(path.read_text(encoding="utf-8"))) == []


def test_validate_spec_reports_a_missing_section():
    problems = validate_spec({"name": "acme", "base_url": "https://api.acme.dev"})

    assert any("offers" in problem for problem in problems)
    assert validate_spec({"endpoints": {"offers": {}}})  # missing name and base_url
    assert "spec must be a JSON object" in validate_spec(["not", "a", "spec"])[0]


def test_validate_spec_reports_an_unknown_auth_type():
    spec = copy.deepcopy(ACME_SPEC)
    spec["auth"] = {"type": "oauth2"}

    problems = validate_spec(spec)

    assert any("unknown auth type 'oauth2'" in problem for problem in problems)
    assert any("supported: bearer, header, query, basic, none" in p for p in problems)


def test_validate_spec_reports_malformed_mappings_and_templates():
    spec = copy.deepcopy(ACME_SPEC)
    spec["endpoints"]["offers"]["map"]["id"] = "$.a[[0]"
    spec["endpoints"]["offers"]["map"]["gpu_type"] = {"scale": "fast"}
    spec["endpoints"]["offers"]["map"]["location"] = {"path": "$.region", "type": "decimal"}
    spec["endpoints"]["offers"]["items"] = "$.data..items"
    spec["endpoints"]["get"]["path"] = "instances/{{instanceId}}"
    spec["status_map"] = {"active": "ONLINE"}

    problems = "\n".join(validate_spec(spec))

    assert "invalid list index" in problems
    assert "map entry needs a 'path' or 'const'" in problems
    assert "scale must be a number" in problems
    assert "type must be one of str, int, float, bool" in problems
    assert "empty path segment" in problems
    assert "unknown template variable {{instanceId}}" in problems
    assert "'ONLINE' is not one of" in problems


def test_validate_spec_guards_wait_for_and_select():
    spec = copy.deepcopy(ACME_SPEC)
    spec["endpoints"]["launch"]["wait_for"] = {
        "status": "airborne",
        "timeout_seconds": 0,
    }
    spec["endpoints"]["list"]["select"] = {"equals": "x"}

    problems = "\n".join(validate_spec(spec))

    assert "wait_for.status must be one of" in problems
    assert "wait_for.timeout_seconds must be positive" in problems
    assert "select needs a 'where' path" in problems


def test_constructing_a_connector_from_a_bad_spec_fails_immediately():
    with pytest.raises(DeclarativeSpecError, match="Invalid provider spec"):
        DeclarativeRestConnector({"name": "broken"}, api_key="k", http=HTTP())


# ---------------------------------------------------------------------------
# dry_run_spec
# ---------------------------------------------------------------------------


def test_dry_run_spec_success_only_touches_the_offers_call():
    http = HTTP(Response(OFFERS_PAYLOAD))

    result = dry_run_spec(ACME_SPEC, api_key="acme-secret", http=http)

    assert result["ok"] is True
    assert result["offer_count"] == 2
    assert result["sample"]["id"] == "101"
    assert result["sample"]["gpu_ram_gb"] == 24.0
    assert result["error"] is None
    assert len(http.requests) == 1
    assert http.requests[0][0] == "GET"


def test_dry_run_spec_failure_paths_explain_the_problem():
    invalid = dry_run_spec({"name": "acme"}, api_key="k", http=HTTP())
    assert invalid["ok"] is False
    assert invalid["problems"]
    assert "Invalid provider spec" in invalid["error"]

    http_failure = dry_run_spec(
        ACME_SPEC, api_key="k", http=HTTP(Response({"detail": "nope"}, status_code=401))
    )
    assert http_failure["ok"] is False
    assert "HTTP 401" in http_failure["error"]
    assert http_failure["offer_count"] == 0

    missing_key = dry_run_spec(ACME_SPEC, api_key=None, http=HTTP())
    assert missing_key["ok"] is False
    assert "API key required" in missing_key["error"]


# ---------------------------------------------------------------------------
# Spec files and registration
# ---------------------------------------------------------------------------


def write_spec(directory, filename, spec):
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / filename
    path.write_text(
        spec if isinstance(spec, str) else json.dumps(spec, indent=2), encoding="utf-8"
    )
    return path


def test_load_provider_specs_skips_invalid_files(tmp_path):
    write_spec(tmp_path, "good.json", ACME_SPEC)
    write_spec(tmp_path, "corrupt.json", '{"name": "oops",')
    write_spec(tmp_path, "invalid.json", {"name": "bad", "base_url": "https://x.dev"})

    specs = load_provider_specs(tmp_path)

    assert [spec["name"] for spec in specs] == ["acme"]
    assert specs[0]["_source"].endswith("good.json")
    assert load_provider_specs(tmp_path / "does-not-exist") == []


def test_register_declarative_providers_loads_good_and_reports_bad(
    tmp_path, registry_sandbox
):
    write_spec(tmp_path, "acme.json", ACME_SPEC)
    write_spec(tmp_path, "corrupt.json", "{not json at all")

    result = register_declarative_providers(tmp_path, include_builtin=False)

    assert result["loaded"] == ["acme"]
    assert [failure["name"] for failure in result["failed"]] == ["corrupt"]
    assert "could not be read as JSON" in result["failed"][0]["errors"][0]

    metadata = connector_metadata("acme")
    assert metadata["kind"] == "declarative"
    assert metadata["display_name"] == "Acme GPU"
    assert metadata["settings_schema"]

    config = CloudConfig(connector_options={"acme": {"base_url": "https://mirror.acme.dev"}})
    config.provider_credentials["acme"] = "from-credential-file"
    connector = create_connector("acme", config)
    assert isinstance(connector, DeclarativeRestConnector)
    assert connector.api_key == "from-credential-file"
    assert connector.base_url == "https://mirror.acme.dev"

    # Idempotent: running again neither raises nor duplicates.
    again = register_declarative_providers(tmp_path, include_builtin=False)
    assert again["loaded"] == ["acme"]


def test_register_declarative_providers_refuses_to_shadow_a_coded_connector(
    tmp_path, registry_sandbox
):
    spec = copy.deepcopy(ACME_SPEC)
    spec["name"] = "runpod"
    write_spec(tmp_path, "runpod.json", spec)

    result = register_declarative_providers(tmp_path, include_builtin=False)

    assert result["loaded"] == []
    assert "already served by a builtin connector" in result["failed"][0]["errors"][0]
    assert connector_metadata("runpod")["kind"] == "builtin"


def test_builtin_specs_are_shipped_and_registered_by_default(tmp_path, registry_sandbox):
    names = [spec["name"] for spec in builtin_provider_specs()]
    assert "vast.ai" in names

    # While the coded Vast connector is still registered the built-in spec stands
    # down rather than silently taking over: it is reported as skipped.
    result = register_declarative_providers(tmp_path)
    assert [entry["name"] for entry in result["skipped"]] == ["vast.ai"]

    # Simulate the cutover: drop the coded registration, re-run, and Vast.ai is
    # served declaratively under the same canonical name and alias.
    for key in ("vast.ai", "vast"):
        registry_sandbox._CONNECTORS.pop(key, None)
        registry_sandbox._CANONICAL_NAMES.pop(key, None)
    registry_sandbox._METADATA.pop("vast.ai", None)

    result = register_declarative_providers(tmp_path)
    assert result["loaded"] == ["vast.ai"]
    assert result["skipped"] == []
    assert connector_metadata("vast")["kind"] == "declarative"

    config = CloudConfig(vast_api_key="legacy-vast-key", vast_api_url="https://mirror.vast/api/v0")
    connector = create_connector("vast", config)
    assert isinstance(connector, DeclarativeRestConnector)
    assert connector.name == "vast.ai"
    assert connector.api_key == "legacy-vast-key"
    assert connector.base_url == "https://mirror.vast/api/v0"


def test_example_spec_matches_the_shipped_vast_spec():
    """The example must not drift from the spec that actually serves Vast.ai."""
    builtin = json.loads((BUILTIN_SPEC_DIR / "vast.json").read_text(encoding="utf-8"))
    example = json.loads(
        (REPO_ROOT / "examples" / "providers" / "vast-declarative.json").read_text(
            encoding="utf-8"
        )
    )

    for key in ("name", "display_name", "aliases", "base_url_config_field"):
        builtin.pop(key, None)
        example.pop(key, None)
    assert builtin == example
    assert example != {}


# ---------------------------------------------------------------------------
# Headline test: differential parity against the coded VastConnector
# ---------------------------------------------------------------------------


VAST_OFFERS = {
    "offers": [
        {
            "id": 1234567,
            "gpu_name": "RTX 4090",
            "num_gpus": 1,
            "gpu_ram": 24564,
            "dph_total": 0.412,
            "cpu_cores": 16,
            "cpu_ram": 64332,
            "disk_space": 512.5,
            "reliability": 0.9932,
            "geolocation": "Warsaw, PL",
            "verified": True,
        },
        {
            "id": 7654321,
            "gpu_name": "A100 SXM4",
            "num_gpus": 4,
            "gpu_ram": 81920,
            "dph_total": 3.204,
            "cpu_cores": 64,
            "cpu_ram": 258048,
            "disk_space": 2048,
            "reliability": 0.9781,
            # geolocation deliberately absent: both connectors must say "unknown"
        },
    ]
}

VAST_INSTANCES = {
    "instances": [
        {
            "id": 9988776,
            "gpu_name": "RTX 4090",
            "num_gpus": 1,
            "dph_total": 0.412,
            "actual_status": "running",
            "public_ipaddr": "203.0.113.7",
            "ssh_port": 41022,
            "ssh_host": "ssh5.vast.ai",
            "machine_id": 4242,
            "start_date": 1721500000.0,
        },
        {
            "id": 5544332,
            "gpu_name": "A100 SXM4",
            "num_gpus": 4,
            "dph_total": 3.204,
            "actual_status": "loading",
            "ssh_host": "ssh9.vast.ai",
            "machine_id": 1717,
            "start_date": 1721500900.0,
        },
        {
            "id": 1111111,
            "gpu_name": "T4",
            "num_gpus": 1,
            "dph_total": 0.09,
            "actual_status": "exited",
            "machine_id": 33,
        },
    ]
}

VAST_USER = {"balance": 12.5, "credit": 3.25, "email": "someone@example.invalid"}


def vast_pair(*responses):
    """Build a coded and a declarative Vast connector over identical payloads."""
    spec = json.loads((BUILTIN_SPEC_DIR / "vast.json").read_text(encoding="utf-8"))
    coded_http = HTTP(*[copy.deepcopy(r) for r in responses])
    declarative_http = HTTP(*[copy.deepcopy(r) for r in responses])

    coded = VastConnector(api_key="vast-secret")
    coded.requests = coded_http
    clock = Clock()
    declarative = DeclarativeRestConnector(
        spec,
        api_key="vast-secret",
        http=declarative_http,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )
    return coded, coded_http, declarative, declarative_http


def test_vast_spec_reproduces_the_coded_connector_offers():
    coded, coded_http, declarative, declarative_http = vast_pair(Response(VAST_OFFERS))

    assert declarative.name == coded.name == "vast.ai"
    assert declarative.list_available() == coded.list_available()

    # Same URL and byte-identical Vast filter DSL, not merely a similar one.
    assert declarative_http.last[1] == coded_http.last[1]
    assert declarative_http.last[2]["params"]["q"] == coded_http.last[2]["params"]["q"]
    assert declarative_http.last[2]["headers"]["Authorization"] == (
        coded_http.last[2]["headers"]["Authorization"]
    )


def test_vast_spec_reproduces_the_coded_connector_offer_filters():
    coded, coded_http, declarative, declarative_http = vast_pair(Response(VAST_OFFERS))

    arguments = {"gpu_type": "RTX 4090", "min_gpu_ram": 24, "max_hourly_rate": 0.5}
    assert declarative.list_available(**arguments) == coded.list_available(**arguments)

    query = json.loads(declarative_http.last[2]["params"]["q"])
    assert query == json.loads(coded_http.last[2]["params"]["q"])
    assert query["gpu_ram"] == {"gte": 24576}  # GB -> MB done declaratively
    assert query["gpu_name"] == {"eq": "RTX 4090"}
    assert query["dph_total"] == {"lte": 0.5}


def test_vast_spec_reproduces_the_coded_connector_instances():
    coded, _, declarative, _ = vast_pair(Response(VAST_INSTANCES))
    assert declarative.list_instances() == coded.list_instances()

    coded, _, declarative, _ = vast_pair(Response(VAST_INSTANCES))
    coded_instances = coded.list_instances()
    assert [i.status for i in coded_instances] == ["running", "pending"]

    for instance_id in ("9988776", "5544332", "1111111", "404404"):
        coded, _, declarative, _ = vast_pair(Response(VAST_INSTANCES))
        assert declarative.get_instance(instance_id) == coded.get_instance(instance_id)


def test_vast_spec_reproduces_the_coded_connector_launch():
    launched = Response({"success": True, "new_contract": 9988776})
    coded, coded_http, declarative, declarative_http = vast_pair(
        launched, Response(VAST_INSTANCES)
    )

    arguments = ("1234567", "ghcr.io/example/runner:1")
    keywords = {"env_vars": {"TOKEN": "abc"}, "startup_script": "echo hi"}
    coded_instance = coded.launch(*arguments, **keywords)
    declarative_instance = declarative.launch(*arguments, **keywords)

    assert declarative_instance == coded_instance
    assert declarative_instance.status == "running"
    assert declarative_instance.metadata == {
        "ssh_host": "ssh5.vast.ai",
        "machine_id": 4242,
        "start_date": 1721500000.0,
    }

    coded_put = coded_http.requests[0]
    declarative_put = declarative_http.requests[0]
    assert declarative_put[0] == coded_put[0] == "PUT"
    assert declarative_put[1] == coded_put[1]
    assert declarative_put[2]["json"] == coded_put[2]["json"] == {
        "client_id": "me",
        "image": "ghcr.io/example/runner:1",
        "disk": 20,
        "runtype": "ssh",
        "env": {"TOKEN": "abc"},
        "onstart": "echo hi",
    }


def test_vast_spec_reproduces_the_coded_connector_launch_without_optional_fields():
    launched = Response({"new_contract": 9988776})
    coded, coded_http, declarative, declarative_http = vast_pair(
        launched, Response(VAST_INSTANCES)
    )

    coded.launch("1234567", "img")
    declarative.launch("1234567", "img")

    assert declarative_http.requests[0][2]["json"] == coded_http.requests[0][2]["json"]
    assert "env" not in declarative_http.requests[0][2]["json"]
    assert "onstart" not in declarative_http.requests[0][2]["json"]


def test_vast_spec_reproduces_the_coded_connector_terminate_and_balance():
    coded, coded_http, declarative, declarative_http = vast_pair(Response(None))
    assert declarative.terminate("9988776") == coded.terminate("9988776") is True
    assert declarative_http.last[0] == coded_http.last[0] == "DELETE"
    assert declarative_http.last[1] == coded_http.last[1]

    coded, _, declarative, _ = vast_pair(Response({"detail": "gone"}, status_code=404))
    assert declarative.terminate("9988776") == coded.terminate("9988776") is False

    coded, _, declarative, _ = vast_pair(Response(VAST_USER))
    assert declarative.account_balance() == coded.account_balance()
    assert declarative.account_balance.__self__.name == "vast.ai"


def test_vast_spec_reproduces_the_coded_connector_error_degradation():
    """vast.py swallows read errors; the spec declares that with on_error."""
    failure = Response({"detail": "upstream is down"}, status_code=503)

    coded, _, declarative, _ = vast_pair(failure)
    assert declarative.list_available() == coded.list_available() == []

    coded, _, declarative, _ = vast_pair(failure)
    assert declarative.list_instances() == coded.list_instances() == []

    coded, _, declarative, _ = vast_pair(failure)
    assert declarative.get_instance("9988776") == coded.get_instance("9988776") is None

    # account_balance has no such policy in either connector: it raises.
    coded, _, declarative, _ = vast_pair(failure)
    with pytest.raises(DeclarativeRequestError):
        declarative.account_balance()
    with pytest.raises(Exception):
        coded.account_balance()


def test_vast_spec_reproduces_the_coded_connector_on_empty_payloads():
    """A 200 with no collection at all must not blow up where vast.py returns []."""
    for payload in ({}, {"success": True}, {"offers": []}):
        coded, _, declarative, _ = vast_pair(Response(payload))
        assert declarative.list_available() == coded.list_available() == []

    for payload in ({}, {"instances": []}):
        coded, _, declarative, _ = vast_pair(Response(payload))
        assert declarative.list_instances() == coded.list_instances() == []
        coded, _, declarative, _ = vast_pair(Response(payload))
        assert declarative.get_instance("1") == coded.get_instance("1") is None


def test_missing_collections_are_loud_unless_the_spec_declares_a_default():
    connector, _ = acme(Response({"data": {}}))
    with pytest.raises(DeclarativeRequestError, match="collection"):
        connector.list_available()

    spec = copy.deepcopy(ACME_SPEC)
    spec["endpoints"]["offers"]["items"] = {"path": "$.data.items", "default": []}
    connector, _ = acme(Response({"data": {}}), spec=spec)
    assert connector.list_available() == []

    spec["endpoints"]["offers"]["items"] = {"default": "not a list"}
    problems = "\n".join(validate_spec(spec))
    assert "items: needs a 'path'" in problems
    assert "items: 'default' must be a list" in problems


def test_declarative_engine_is_deliberately_stricter_than_vast_py_about_nulls():
    """The two known, intentional divergences from the coded connector.

    ``vast.py`` uses ``dict.get(key, default)``, which only defaults on a
    *missing* key -- an explicit JSON ``null`` flows through as ``None`` and, for
    ``gpu_ram``, actually raises TypeError. The engine treats null as absent and
    matches statuses case-insensitively. Both are improvements; they are pinned
    here so the difference is a decision rather than a surprise.
    """
    payload = {"offers": [{"id": 4, "gpu_name": None, "geolocation": None}]}
    coded, _, declarative, _ = vast_pair(Response(payload))

    coded_offer = coded.list_available()[0]
    declarative_offer = declarative.list_available()[0]
    assert (coded_offer["gpu_type"], coded_offer["location"]) == (None, None)
    assert (declarative_offer["gpu_type"], declarative_offer["location"]) == (
        "unknown",
        "unknown",
    )

    # And a null numeric that crashes the coded connector maps cleanly here.
    payload = {"offers": [{"id": 5, "gpu_ram": None}]}
    coded, _, declarative, _ = vast_pair(Response(payload))
    with pytest.raises(TypeError):
        coded.list_available()
    assert declarative.list_available()[0]["gpu_ram_gb"] == 0

    payload = {"instances": [{"id": 9, "actual_status": "RUNNING"}]}
    coded, _, declarative, _ = vast_pair(Response(payload))
    assert coded.get_instance("9").status == "unknown"
    assert declarative.get_instance("9").status == "running"


def test_on_error_policy_is_opt_in_and_validated():
    # Default is loud.
    connector, _ = acme(Response({}, status_code=503))
    with pytest.raises(DeclarativeRequestError):
        connector.list_available()

    spec = copy.deepcopy(ACME_SPEC)
    spec["endpoints"]["offers"]["on_error"] = "empty"
    connector, _ = acme(Response({}, status_code=503), spec=spec)
    assert connector.list_available() == []

    spec["endpoints"]["offers"]["on_error"] = "null"
    spec["endpoints"]["terminate"]["on_error"] = "empty"
    problems = "\n".join(validate_spec(spec))
    assert "offers.on_error: must be one of raise, empty" in problems
    assert "terminate.on_error: only get, list, offers may set" in problems


def test_vast_spec_dry_run_reports_a_usable_sample():
    spec = json.loads((BUILTIN_SPEC_DIR / "vast.json").read_text(encoding="utf-8"))

    result = dry_run_spec(spec, api_key="vast-secret", http=HTTP(Response(VAST_OFFERS)))

    assert result["ok"] is True
    assert result["offer_count"] == 2
    assert result["sample"]["gpu_ram_gb"] == pytest.approx(23.98, abs=0.01)
    assert result["sample"]["provider"] == "vast.ai"

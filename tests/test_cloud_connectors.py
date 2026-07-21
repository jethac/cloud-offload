import base64

import pytest

from cloud_offload.config import CloudConfig
from cloud_offload.providers import (
    connector_metadata,
    connector_names,
    create_connector,
    register_connector,
)
from cloud_offload.providers.base import CloudConnector
from cloud_offload.providers.runpod import RunPodConnector


class FakeResponse:
    def __init__(self, payload=None, *, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.content = b"" if payload is None else b"json"
        self.text = "" if payload is None else "json"

    def raise_for_status(self):
        if self.status_code >= 400:
            error = RuntimeError(f"HTTP {self.status_code}")
            error.response = self
            raise error

    def json(self):
        return self.payload


class FakeHttp:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        if not self.responses:
            raise AssertionError(f"Unexpected request: {method} {url}")
        return self.responses.pop(0)


def test_builtin_connector_registry_and_vast_compatibility():
    from cloud_offload.providers.declarative import DeclarativeRestConnector

    assert connector_names() == ("runpod", "vast.ai")

    config = CloudConfig(vast_api_key="vast-secret")
    connector = create_connector("vast", config)

    # Vast.ai is served by the bundled declarative spec now, but the canonical
    # name, the "vast" alias and the legacy credential field are unchanged, so
    # existing provider_order and VAST_API_KEY configuration keep working.
    assert isinstance(connector, DeclarativeRestConnector)
    assert connector.name == "vast.ai"
    assert connector_metadata("vast.ai")["kind"] == "declarative"


def test_runpod_is_the_default_provider():
    config = CloudConfig()

    assert config.provider == "runpod"
    assert config.provider_order[0] == "runpod"
    connector = create_connector("runpod", CloudConfig(runpod_api_key="secret"))
    assert connector.name == "runpod"


def test_custom_connector_can_be_registered(tmp_path):
    class ExampleConnector(CloudConnector):
        name = "example"

        def list_available(self, **kwargs):
            return []

        def launch(self, *args, **kwargs):
            raise NotImplementedError

        def get_instance(self, instance_id):
            return None

        def terminate(self, instance_id):
            return True

        def list_instances(self):
            return []

    name = f"example-{tmp_path.name}"
    register_connector(name, lambda config: ExampleConnector())

    assert isinstance(create_connector(name, CloudConfig()), ExampleConnector)


def test_runpod_gpu_discovery_normalizes_and_filters_offers():
    http = FakeHttp(
        FakeResponse(
            {
                "data": {
                    "gpuTypes": [
                        {
                            "id": "NVIDIA GeForce RTX 4090",
                            "displayName": "RTX 4090",
                            "memoryInGb": 24,
                            "secureCloud": True,
                            "communityCloud": True,
                            "lowestPrice": {
                                "minimumBidPrice": 0.30,
                                "uninterruptablePrice": 0.44,
                            },
                        },
                        {
                            "id": "NVIDIA A100 80GB PCIe",
                            "displayName": "A100 PCIe",
                            "memoryInGb": 80,
                            "secureCloud": True,
                            "communityCloud": True,
                            "lowestPrice": {
                                "minimumBidPrice": 1.19,
                                "uninterruptablePrice": 1.19,
                            },
                        },
                    ]
                }
            }
        )
    )
    connector = RunPodConnector(api_key="secret", http_client=http)

    offers = connector.list_available(gpu_type="RTX_4090", min_gpu_ram=24, max_hourly_rate=0.50)

    assert offers == [
        {
            "id": "NVIDIA GeForce RTX 4090",
            "provider": "runpod",
            "gpu_type": "RTX 4090",
            "gpu_count": 1,
            "gpu_ram_gb": 24,
            "hourly_rate": 0.44,
            "cloud_type": "SECURE",
            "raw": {
                "id": "NVIDIA GeForce RTX 4090",
                "displayName": "RTX 4090",
                "memoryInGb": 24,
                "secureCloud": True,
                "communityCloud": True,
                "lowestPrice": {
                    "minimumBidPrice": 0.30,
                    "uninterruptablePrice": 0.44,
                },
            },
        }
    ]
    assert http.requests[0][2]["json"]["variables"] == {"secureCloud": True}


def test_runpod_community_gpu_discovery_requests_community_price():
    http = FakeHttp(FakeResponse({"data": {"gpuTypes": []}}))
    connector = RunPodConnector(
        api_key="secret", cloud_type="COMMUNITY", http_client=http
    )

    assert connector.list_available() == []
    assert http.requests[0][2]["json"]["variables"] == {"secureCloud": False}


def test_runpod_launch_passes_worker_environment_and_startup_script():
    http = FakeHttp(
        FakeResponse(
            {
                "data": {
                    "podFindAndDeployOnDemand": {
                        "id": "pod-1",
                        "desiredStatus": "CREATED",
                    }
                }
            }
        ),
        FakeResponse(
            {
                "id": "pod-1",
                "name": "cloud-offload-worker-test",
                "desiredStatus": "RUNNING",
                "gpuTypeId": "NVIDIA GeForce RTX 4090",
                "gpuCount": 1,
                "costPerHr": 0.44,
                "runtime": {
                    "ports": [
                        {
                            "ip": "203.0.113.10",
                            "isIpPublic": True,
                            "privatePort": 22,
                            "publicPort": 22022,
                        }
                    ]
                },
            }
        ),
    )
    connector = RunPodConnector(
        api_key="secret",
        registry_auth_id="registry-auth-1",
        http_client=http,
        launch_timeout=1,
        poll_interval=0,
    )

    instance = connector.launch(
        "NVIDIA GeForce RTX 4090",
        "pytorch/image:latest",
        env_vars={"CLOUD_OFFLOAD_WORKER_MODE": "true"},
        startup_script="cloud-offload worker --poll 10\n",
    )

    create_body = http.requests[0][2]["json"]
    pod_input = create_body["variables"]["input"]
    encoded_script = pod_input["dockerArgs"].split("echo ", 1)[1].split(" ", 1)[0]
    assert base64.b64decode(encoded_script).decode() == "cloud-offload worker --poll 10\n"
    assert pod_input["env"] == [{"key": "CLOUD_OFFLOAD_WORKER_MODE", "value": "true"}]
    assert pod_input["gpuTypeId"] == "NVIDIA GeForce RTX 4090"
    assert pod_input["containerRegistryAuthId"] == "registry-auth-1"
    assert instance.status == "running"
    assert instance.ip_address == "203.0.113.10"
    assert instance.ssh_port == 22022


def test_runpod_refuses_private_ghcr_image_before_renting_pod():
    http = FakeHttp(FakeResponse(status_code=401))
    connector = RunPodConnector(api_key="secret", http_client=http)

    with pytest.raises(RuntimeError, match="RUNPOD_REGISTRY_AUTH_ID"):
        connector.launch(
            "NVIDIA GeForce RTX 4090",
            "ghcr.io/example/private@sha256:" + "a" * 64,
        )

    assert len(http.requests) == 1
    assert http.requests[0][0] == "GET"
    assert http.requests[0][1].startswith("https://ghcr.io/token?")


def test_runpod_lifecycle_uses_rest_api():
    http = FakeHttp(
        FakeResponse(
            [
                {
                    "id": "pod-running",
                    "desiredStatus": "RUNNING",
                    "gpuTypeId": "NVIDIA A40",
                    "gpuCount": 1,
                },
                {"id": "pod-exited", "desiredStatus": "EXITED"},
            ]
        ),
        FakeResponse(None, status_code=204),
    )
    connector = RunPodConnector(api_key="secret", http_client=http)

    instances = connector.list_instances()
    terminated = connector.terminate("pod-running")

    assert [instance.id for instance in instances] == ["pod-running"]
    assert terminated is True
    assert http.requests[0][0:2] == (
        "GET",
        "https://rest.runpod.io/v1/pods",
    )
    assert http.requests[1][0:2] == (
        "DELETE",
        "https://rest.runpod.io/v1/pods/pod-running",
    )


def test_connectors_normalize_account_balances():
    vast_http = FakeHttp(FakeResponse({"balance": 2.5, "credit": 8.0}))
    runpod_http = FakeHttp(
        FakeResponse(
            {"data": {"myself": {"clientBalance": 12.75, "currentSpendPerHr": 0.4}}}
        )
    )

    config = CloudConfig(vast_api_key="secret")
    vast_connector = create_connector("vast.ai", config)
    vast_connector.http = vast_http
    vast = vast_connector.account_balance()
    runpod = RunPodConnector(api_key="secret", http_client=runpod_http).account_balance()

    assert vast["balance"] == 2.5
    assert vast["credit"] == 8.0
    assert runpod["balance"] == 12.75
    assert runpod["current_spend_per_hour"] == 0.4


def test_cloud_config_loads_runpod_without_exposing_secret(monkeypatch):
    monkeypatch.setenv("CLOUD_OFFLOAD_PROVIDER", "runpod")
    monkeypatch.setenv("RUNPOD_API_KEY", "runpod-secret")
    monkeypatch.setenv("RUNPOD_CLOUD_TYPE", "community")
    monkeypatch.setenv("RUNPOD_REGISTRY_AUTH_ID", "registry-auth-1")

    config = CloudConfig.from_env()
    public = config.to_dict()

    assert config.provider == "runpod"
    assert config.runpod_api_key == "runpod-secret"
    assert config.runpod_cloud_type == "COMMUNITY"
    assert config.runpod_registry_auth_id == "registry-auth-1"
    assert public["provider_auth_configured"] is True
    assert "runpod-secret" not in repr(public)


def test_cloud_config_file_ignores_derived_public_status_fields(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(
        '{"cloud":{"provider":"runpod","provider_auth_configured":true,'
        '"worker_auth_configured":false,"runpod_container_disk_gb":40}}'
    )

    config = CloudConfig.from_file(path)

    assert config.provider == "runpod"
    assert config.runpod_container_disk_gb == 40

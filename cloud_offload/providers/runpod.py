"""RunPod cloud connector using the official REST and GraphQL APIs.

RunPod is the default Cloud Offload provider.
"""

from __future__ import annotations

import base64
import os
import time
import uuid
from typing import Any
from urllib.parse import quote
from urllib.parse import urlencode

from cloud_offload.providers.base import CloudConnector, Instance

GPU_TYPES_QUERY = """
query CloudOffloadGpuTypes($secureCloud: Boolean) {
  gpuTypes {
    id
    displayName
    memoryInGb
    secureCloud
    communityCloud
    lowestPrice(input: { gpuCount: 1, secureCloud: $secureCloud }) {
      minimumBidPrice
      uninterruptablePrice
    }
  }
}
"""


CREATE_POD_MUTATION = """
mutation CloudOffloadCreatePod($input: PodFindAndDeployOnDemandInput!) {
  podFindAndDeployOnDemand(input: $input) {
    id
    name
    imageName
    desiredStatus
    costPerHr
    gpuCount
    memoryInGb
    machine {
      gpuDisplayName
      location
    }
  }
}
"""

ACCOUNT_QUERY = """
query CloudOffloadAccountBalance {
  myself {
    clientBalance
    currentSpendPerHr
  }
}
"""

_USER_AGENT = "cloud-offload-connector/0.1"


class RunPodConnector(CloudConnector):
    """Provision Cloud Offload workers as RunPod GPU pods."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        graphql_url: str = "https://api.runpod.io/graphql",
        rest_url: str = "https://rest.runpod.io/v1",
        cloud_type: str = "SECURE",
        container_disk_gb: int = 20,
        volume_gb: int = 0,
        registry_auth_id: str = "",
        launch_timeout: int = 300,
        poll_interval: float = 5.0,
        http_client: Any | None = None,
    ):
        self.api_key = api_key or os.environ.get("RUNPOD_API_KEY")
        if not self.api_key:
            raise ValueError("RUNPOD_API_KEY required")

        if http_client is None:
            try:
                import requests
            except ImportError as exc:
                raise ImportError("requests required: pip install requests") from exc
            http_client = requests

        cloud_type = cloud_type.upper()
        if cloud_type not in {"SECURE", "COMMUNITY"}:
            raise ValueError("RunPod cloud_type must be SECURE or COMMUNITY")

        self.http = http_client
        self.graphql_url = graphql_url.rstrip("/")
        self.rest_url = rest_url.rstrip("/")
        self.cloud_type = cloud_type
        self.container_disk_gb = container_disk_gb
        self.volume_gb = volume_gb
        self.registry_auth_id = registry_auth_id.strip()
        self.launch_timeout = launch_timeout
        self.poll_interval = poll_interval

    @property
    def name(self) -> str:
        return "runpod"

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "User-Agent": _USER_AGENT,
        }

    def _graphql(self, query: str, variables: dict | None = None) -> dict:
        response = self.http.request(
            "POST",
            self.graphql_url,
            headers=self._headers,
            json={"query": query, "variables": variables or {}},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        errors = payload.get("errors") or []
        if errors:
            message = errors[0].get("message", "unknown GraphQL error")
            raise RuntimeError(f"RunPod API error: {message}")
        return payload.get("data", {})

    def _rest(self, method: str, endpoint: str, **kwargs) -> Any:
        response = self.http.request(
            method,
            f"{self.rest_url}/{endpoint.lstrip('/')}",
            headers=self._headers,
            timeout=30,
            **kwargs,
        )
        response.raise_for_status()
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    def list_available(
        self,
        gpu_type: str | None = None,
        min_gpu_ram: int | None = None,
        max_hourly_rate: float | None = None,
    ) -> list[dict]:
        """List normalized GPU types with current on-demand prices."""
        # RunPod reports different on-demand prices for Secure and Community
        # Cloud.  Without this filter ``lowestPrice`` can quote the cheaper
        # tier even though launch() explicitly requests the other one.
        gpu_types = self._graphql(
            GPU_TYPES_QUERY,
            {"secureCloud": self.cloud_type == "SECURE"},
        ).get("gpuTypes", [])
        offers: list[dict] = []
        for gpu in gpu_types:
            if gpu_type and not self._matches_gpu(gpu_type, gpu):
                continue
            memory_gb = int(gpu.get("memoryInGb") or 0)
            if min_gpu_ram is not None and memory_gb < min_gpu_ram:
                continue
            if self.cloud_type == "SECURE" and not gpu.get("secureCloud"):
                continue
            if self.cloud_type == "COMMUNITY" and not gpu.get("communityCloud"):
                continue

            prices = gpu.get("lowestPrice") or {}
            hourly_rate = prices.get("uninterruptablePrice")
            if hourly_rate is None:
                continue
            hourly_rate = float(hourly_rate)
            if max_hourly_rate is not None and hourly_rate > max_hourly_rate:
                continue

            offers.append(
                {
                    "id": str(gpu["id"]),
                    "provider": self.name,
                    "gpu_type": gpu.get("displayName") or gpu["id"],
                    "gpu_count": 1,
                    "gpu_ram_gb": memory_gb,
                    "hourly_rate": hourly_rate,
                    "cloud_type": self.cloud_type,
                    "raw": gpu,
                }
            )
        return offers

    @staticmethod
    def _matches_gpu(requested: str, gpu: dict) -> bool:
        def normalize(value: str) -> str:
            return " ".join(value.replace("_", " ").replace("-", " ").lower().split())

        needle = normalize(requested)
        candidates = (
            normalize(str(gpu.get("id", ""))),
            normalize(str(gpu.get("displayName", ""))),
        )
        return any(needle == candidate or needle in candidate for candidate in candidates)

    def launch(
        self,
        offer_id: str,
        docker_image: str,
        env_vars: dict | None = None,
        startup_script: str | None = None,
    ) -> Instance:
        """Launch a RunPod pod and wait until it reaches running state."""
        self._ensure_image_pullable(docker_image)
        pod_input: dict[str, Any] = {
            "cloudType": self.cloud_type,
            "containerDiskInGb": self.container_disk_gb,
            "env": [
                {"key": str(key), "value": str(value)} for key, value in (env_vars or {}).items()
            ],
            "gpuCount": 1,
            "gpuTypeId": offer_id,
            "imageName": docker_image,
            "name": f"cloud-offload-worker-{uuid.uuid4().hex[:8]}",
            "startSsh": True,
            "volumeInGb": self.volume_gb,
            "volumeMountPath": "/workspace",
        }
        if self.registry_auth_id:
            pod_input["containerRegistryAuthId"] = self.registry_auth_id
        if startup_script:
            encoded = base64.b64encode(startup_script.encode("utf-8")).decode("ascii")
            pod_input["dockerArgs"] = "bash -lc 'echo " + encoded + " | base64 -d | bash'"

        data = self._graphql(CREATE_POD_MUTATION, {"input": pod_input})
        pod = data.get("podFindAndDeployOnDemand")
        if not pod or not pod.get("id"):
            raise RuntimeError("RunPod pod creation returned no pod ID")
        return self._wait_for_ready(str(pod["id"]))

    def _ensure_image_pullable(self, docker_image: str) -> None:
        """Fail before renting a pod when a private GHCR image has no auth ID."""
        if self.registry_auth_id or not docker_image.lower().startswith("ghcr.io/"):
            return
        repository = docker_image.split("@", 1)[0]
        tail = repository.rsplit("/", 1)[-1]
        if ":" in tail:
            repository = repository.rsplit(":", 1)[0]
        repository = repository[len("ghcr.io/") :]
        query = urlencode(
            {"service": "ghcr.io", "scope": f"repository:{repository}:pull"}
        )
        response = self.http.request(
            "GET",
            f"https://ghcr.io/token?{query}",
            headers={"User-Agent": _USER_AGENT},
            timeout=30,
        )
        if response.status_code in {401, 403}:
            raise RuntimeError(
                "GHCR image is private but RUNPOD_REGISTRY_AUTH_ID is not configured; "
                "refused to rent a pod that cannot pull its runner image"
            )
        response.raise_for_status()

    def _wait_for_ready(self, instance_id: str) -> Instance:
        deadline = time.monotonic() + self.launch_timeout
        while time.monotonic() < deadline:
            instance = self.get_instance(instance_id)
            if instance and instance.status == "running":
                return instance
            time.sleep(self.poll_interval)
        raise TimeoutError(
            f"RunPod instance {instance_id} did not start within {self.launch_timeout}s"
        )

    def get_instance(self, instance_id: str) -> Instance | None:
        """Get a RunPod pod by ID."""
        try:
            data = self._rest(
                "GET", f"pods/{quote(instance_id, safe='')}", params={"includeMachine": "true"}
            )
        except Exception as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code == 404:
                return None
            raise
        return self._parse_instance(data)

    def terminate(self, instance_id: str) -> bool:
        """Permanently terminate a RunPod pod."""
        try:
            self._rest("DELETE", f"pods/{quote(instance_id, safe='')}")
            return True
        except Exception:
            return False

    def list_instances(self) -> list[Instance]:
        """List active RunPod GPU pods."""
        pods = self._rest("GET", "pods", params={"computeType": "GPU"}) or []
        return [
            instance
            for pod in pods
            if (instance := self._parse_instance(pod)).status in {"pending", "running"}
        ]

    def account_balance(self) -> dict:
        """Return RunPod account balance and current hourly spend."""
        account = self._graphql(ACCOUNT_QUERY).get("myself") or {}
        return {
            "available": True,
            "currency": "USD",
            "balance": float(account.get("clientBalance") or 0),
            "current_spend_per_hour": float(account.get("currentSpendPerHr") or 0),
        }

    def _parse_instance(self, data: dict) -> Instance:
        desired_status = str(data.get("desiredStatus", "")).upper()
        status = {
            "CREATED": "pending",
            "PENDING": "pending",
            "RUNNING": "running",
            "RESTARTING": "pending",
            "EXITED": "stopped",
            "STOPPED": "stopped",
            "TERMINATED": "terminated",
        }.get(desired_status, "unknown")

        machine = data.get("machine") or {}
        runtime = data.get("runtime") or {}
        ip_address = None
        ssh_port = None
        for port in runtime.get("ports") or []:
            if port.get("isIpPublic") and int(port.get("privatePort") or 0) == 22:
                ip_address = port.get("ip")
                ssh_port = port.get("publicPort")
                break

        return Instance(
            id=str(data["id"]),
            provider=self.name,
            gpu_type=(machine.get("gpuDisplayName") or data.get("gpuTypeId") or "unknown"),
            gpu_count=int(data.get("gpuCount") or 1),
            hourly_rate=float(data.get("costPerHr") or 0),
            status=status,
            ip_address=ip_address,
            ssh_port=int(ssh_port) if ssh_port is not None else None,
            metadata={
                "name": data.get("name"),
                "image": data.get("imageName"),
                "location": machine.get("location"),
            },
        )


# Symmetric compatibility name for callers that still use provider terminology.
RunPodProvider = RunPodConnector

"""Small preflight stubs for tests of older partition-submit concerns."""

from cloud_offload.providers import connector_metadata
from cloud_offload.router import resolve_worker_profile


def accept_test_preflight(monkeypatch, server, config):
    """Patch binding only when a test is about another submit-route contract."""

    def accepted(*, request, config=config, storage=None):
        runner = request.partition.get("runner") or {}
        profile_name = str(runner.get("profile") or "comfyui")
        profile = resolve_worker_profile(config, profile_name) or {}
        residency = request.partition.get("residency", "cloud")
        providers = [
            name
            for name in profile.get("providers") or config.provider_order
            if config.api_key_for(name)
            and (
                residency != "on-prem"
                or connector_metadata(name).get("residency_class") == "on-prem"
            )
        ]
        requested = str(request.provider or "auto").lower()
        provider = (
            requested
            if requested not in {"auto", "cloud"}
            else (providers[0] if providers else config.provider)
        )
        gpu_type = str(runner.get("gpu_type") or "any")
        candidate = {
            "candidate_id": "sha256:" + "c" * 64,
            "offer_id": "test-offer",
            "provider": provider,
            "gpu_type": gpu_type,
            "gpu_count": 1,
            "gpu_ram_gb": float(runner.get("min_gpu_ram_gb") or 16),
            "hourly_rate": min(0.5, float(config.max_hourly_rate)),
            "region": None,
            "prepared_volume_id": None,
            "estimate": {
                "paid_lifetime_seconds": [1.0, 2.0],
                "total_job_cost_usd": [0.0, 0.001],
            },
        }
        report = {
            "preflight_id": "test-preflight",
            "manifest_digest": "sha256:" + "d" * 64,
            "expires_at": "2099-01-01T00:00:00Z",
            "request_policy": {
                "provider": requested,
                "recommendation_policy": "balanced",
                "max_hourly_rate": float(config.max_hourly_rate),
                "max_total_job_cost": None,
                "allowed_regions": [],
            },
        }
        return {"accepted": True, "report": report, "candidate": candidate}

    monkeypatch.setattr(server, "_revalidate_partition_preflight", accepted)

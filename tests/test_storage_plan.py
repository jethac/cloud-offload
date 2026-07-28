"""Storage planning: sizing a pod's disk before the meter starts.

The failure being prevented is specific and was real: a worker was rented with a
fixed 20 GB container disk, the runner image took 14.6 GB of it, and the
partition then staged a 19.6 GB model onto what was left. The pod died out of
space *after* it started charging — the exact class of failure every other
pre-provision check in this coordinator exists to move to submission time, where
a refusal costs nothing.

So the tests below care about direction as much as arithmetic. A component whose
size cannot be determined must push the total *up* and say so; it must never be
quietly treated as zero, because a confident under-estimate is what buys a dead
pod.
"""


from cloud_offload.storage_plan import (
    GIB,
    HEADROOM_FLOOR_BYTES,
    MINIMUM_DISK_GB,
    PACK_ALLOWANCE_BYTES,
    UNKNOWN_IMAGE_BYTES,
    UNKNOWN_SNAPSHOT_BYTES,
    UNKNOWN_WEIGHT_FILE_BYTES,
    plan_disk_gb,
    plan_storage,
    weight_key,
)

IMAGE_BYTES = 14_600_000_000
MODEL_BYTES = 19_600_000_000

SDXL_WEIGHTS = [
    {
        "repo_id": "stabilityai/sdxl-base",
        "revision": "462165984030d82259a11f4367a4eed129e94a7b",
        "files": ["sd_xl_base_1.0.safetensors"],
        "dest": "checkpoints",
        "gated": False,
    }
]
SDXL_KEY = weight_key(
    "stabilityai/sdxl-base",
    "462165984030d82259a11f4367a4eed129e94a7b",
    "sd_xl_base_1.0.safetensors",
)
SNAPSHOT_WEIGHTS = [
    {
        "repo_id": "org/vae",
        "revision": "def456",
        "files": None,
        "dest": "vae",
        "gated": False,
    }
]
SNAPSHOT_KEY = weight_key("org/vae", "def456")


def asset(size, filename="model.safetensors"):
    return {
        "category": "checkpoints",
        "filename": filename,
        "sha256": "a" * 64,
        "size": size,
        "format": "safetensors",
    }


def profile(**overrides):
    """A profile whose image size is known, so tests isolate one unknown at a time."""
    return {
        "name": "comfyui",
        "image": "ghcr.io/example/comfyui@sha256:" + "a" * 64,
        "models": ["comfyui-partition-v1"],
        "providers": ["runpod"],
        "weights": [],
        "custom_nodes": [],
        "extra_disk_gb": 0,
        "image_size_gb": IMAGE_BYTES / GIB,
        **overrides,
    }


def component(plan, name):
    return next(item for item in plan["components"] if item["name"] == name)


# ---------------------------------------------------------------------------
# Planner arithmetic
# ---------------------------------------------------------------------------


def test_declared_assets_are_counted_to_the_byte():
    plan = plan_storage([asset(MODEL_BYTES), asset(1024, "vae.pt")], profile(), image_bytes=IMAGE_BYTES)

    assert plan["assets"] == MODEL_BYTES + 1024
    assert plan["weights"] == 0
    assert plan["packs"] == 0
    assert plan["reserve"] == 0
    assert plan["unknown"] == []


def test_a_plan_totals_every_component():
    plan = plan_storage(
        [asset(MODEL_BYTES)],
        profile(weights=SDXL_WEIGHTS, custom_nodes=[{"registry_id": "pack", "version": "1"}]),
        image_bytes=IMAGE_BYTES,
        weight_bytes={SDXL_KEY: 6_938_040_714},
    )

    assert plan["image"] == IMAGE_BYTES
    assert plan["assets"] == MODEL_BYTES
    assert plan["weights"] == 6_938_040_714
    assert plan["packs"] == PACK_ALLOWANCE_BYTES
    assert plan["total"] == (
        plan["image"]
        + plan["assets"]
        + plan["weights"]
        + plan["packs"]
        + plan["reserve"]
        + plan["headroom"]
    )
    assert plan["unknown"] == []


def test_the_rent_that_failed_is_now_planned_for():
    """The original incident: a 14.6 GB image plus a 19.6 GB model on a 20 GB disk."""
    plan = plan_storage([asset(MODEL_BYTES)], profile(), image_bytes=IMAGE_BYTES)

    assert plan_disk_gb(plan) > 20


def test_each_declared_pack_gets_a_flat_allowance():
    packs = [{"registry_id": "one", "version": "1"}, {"registry_id": "two", "version": "1"}]

    plan = plan_storage([], profile(custom_nodes=packs), image_bytes=IMAGE_BYTES)

    assert plan["packs"] == 2 * PACK_ALLOWANCE_BYTES
    assert "allowance, not a measurement" in component(plan, "packs")["detail"]


def test_extra_disk_gb_becomes_the_reserve():
    plan = plan_storage([], profile(extra_disk_gb=60), image_bytes=IMAGE_BYTES)

    assert plan["reserve"] == 60 * GIB
    assert "extra_disk_gb" in component(plan, "reserve")["detail"]


def test_a_profile_declaring_no_reserve_says_what_it_cannot_see():
    # The real case behind the field: a node whose from_pretrained call pulls
    # 53.8 GB that no manifest mentions.
    plan = plan_storage([], profile(), image_bytes=IMAGE_BYTES)

    assert plan["reserve"] == 0
    assert "downloads its own weights at runtime" in component(plan, "reserve")["detail"]


def test_headroom_falls_back_to_its_floor_for_a_small_plan():
    plan = plan_storage([asset(1024)], profile(), image_bytes=1024)

    assert plan["headroom"] == HEADROOM_FLOOR_BYTES


def test_headroom_scales_with_a_large_plan():
    plan = plan_storage([asset(400 * GIB)], profile(), image_bytes=IMAGE_BYTES)

    subtotal = plan["image"] + plan["assets"]
    assert plan["headroom"] == int(subtotal * 0.20)
    assert plan["headroom"] > HEADROOM_FLOOR_BYTES


# ---------------------------------------------------------------------------
# Unknowns: named, and charged upward
# ---------------------------------------------------------------------------


def test_an_undeclared_image_size_is_named_and_charged():
    known = plan_storage([], profile(), image_bytes=IMAGE_BYTES)
    unknown = plan_storage([], profile(), image_bytes=None)

    assert unknown["image"] == UNKNOWN_IMAGE_BYTES
    assert unknown["total"] > known["total"]
    assert any("runner image" in item for item in unknown["unknown"])
    assert "image_size_gb" in unknown["unknown"][0]


def test_an_unresolved_weight_file_is_named_and_charged():
    resolved = plan_storage(
        [], profile(weights=SDXL_WEIGHTS), image_bytes=IMAGE_BYTES,
        weight_bytes={SDXL_KEY: 1024},
    )
    unresolved = plan_storage(
        [], profile(weights=SDXL_WEIGHTS), image_bytes=IMAGE_BYTES, weight_bytes={}
    )

    assert unresolved["weights"] == UNKNOWN_WEIGHT_FILE_BYTES
    assert unresolved["total"] > resolved["total"]
    assert unresolved["unknown"] == [
        f"the pinned weights file {SDXL_KEY}, charged 8.0 GiB"
    ]


def test_an_unresolved_snapshot_is_charged_more_than_a_single_file():
    plan = plan_storage([], profile(weights=SNAPSHOT_WEIGHTS), image_bytes=IMAGE_BYTES)

    assert plan["weights"] == UNKNOWN_SNAPSHOT_BYTES
    assert UNKNOWN_SNAPSHOT_BYTES > UNKNOWN_WEIGHT_FILE_BYTES
    assert SNAPSHOT_KEY in plan["unknown"][0]


def test_a_partial_weight_mapping_charges_only_what_is_missing():
    weights = SDXL_WEIGHTS + SNAPSHOT_WEIGHTS

    plan = plan_storage(
        [], profile(weights=weights), image_bytes=IMAGE_BYTES,
        weight_bytes={SDXL_KEY: 1024},
    )

    assert plan["weights"] == 1024 + UNKNOWN_SNAPSHOT_BYTES
    assert len(plan["unknown"]) == 1
    assert "2 pinned profile weights entries, 1 of unknown size" == component(
        plan, "weights"
    )["detail"]


def test_nothing_known_still_plans_a_usable_pod():
    plan = plan_storage(None, None)

    assert plan["image"] == UNKNOWN_IMAGE_BYTES
    assert plan["total"] > UNKNOWN_IMAGE_BYTES
    assert plan_disk_gb(plan) >= MINIMUM_DISK_GB


# ---------------------------------------------------------------------------
# plan_disk_gb
# ---------------------------------------------------------------------------


def test_the_requested_disk_rounds_up_to_a_whole_gibibyte():
    assert plan_disk_gb({"total": 200 * GIB + 1}) == 201
    assert plan_disk_gb({"total": 200 * GIB}) == 200


def test_the_requested_disk_never_falls_below_the_minimum():
    assert plan_disk_gb({"total": 1}) == MINIMUM_DISK_GB
    assert plan_disk_gb({"total": 0}) == MINIMUM_DISK_GB
    assert plan_disk_gb({}) == MINIMUM_DISK_GB

# Node Pack Prefix Installation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep the pinned PyTorch stack from the worker image while custom node requirements install into the prepared runtime environment.

**Architecture:** Install node-pack requirements with pip `--prefix` so pip can see and reuse the base image packages. Add only the prefix Python ABI site-packages directory to `PYTHONPATH`, while the complete prefix root remains the cached runtime bundle.

**Tech Stack:** Python 3.11, pip, Bash, pytest, ComfyUI worker image.

**Spec:** `docs/cloud-offload-product-goal.md`

## Global Constraints

- The worker image pinned PyTorch, torchvision, and torchaudio packages stay authoritative.
- Custom node requirements must not install a second unpinned torch stack over the base image.
- Prepared runtime bundles keep the complete environment prefix and remain Python-ABI-specific.
- No paid retry occurs until local image validation imports torchvision and ComfyUI after both node-pack requirements install.

---

### Task 1: Use a visible pip prefix for node-pack requirements

**Files:**
- Modify: `cloud_offload/worker.py`
- Modify: `deploy/runtime-profiles/comfyui/entrypoint.sh`
- Test: `tests/test_node_packs.py`
- Test: `tests/test_runner_boot.py`

**Interfaces:**
- Consumes: `CLOUD_OFFLOAD_ENV_ROOT`, Python 3.11 worker image layout.
- Produces: a pip prefix at the environment root and an import path at `<root>/lib/python3.11/site-packages`.

- [ ] **Step 1: Write failing tests**

Change the node-pack command assertion to require `--prefix`. Add an entrypoint assertion that requires the Python 3.11 prefix site-packages directory before any prior `PYTHONPATH` value.

- [ ] **Step 2: Verify the tests fail**

Run: `python -m pytest tests/test_node_packs.py::test_requirements_are_installed_and_their_output_captured tests/test_runner_boot.py::test_the_entrypoint_uses_the_environment_prefix_site_packages -q`

Expected: both tests fail because production still uses `--target` and the prefix root.

- [ ] **Step 3: Write the minimal implementation**

Replace pip `--target` with `--prefix`. Set `PYTHONPATH` to `${CLOUD_OFFLOAD_ENV_ROOT:-/opt/cloud-offload/environment}/lib/python3.11/site-packages`, followed by the prior value when it exists.

- [ ] **Step 4: Verify focused and full tests**

Run the two focused tests, the node-pack and runner boot files, and then the full pytest suite.

- [ ] **Step 5: Verify the real worker image locally**

Build the worker image. Install the two pinned node-pack requirement files with the new prefix behavior. Prove that torch, torchvision, torchaudio, ComfyUI, and both node packs import without replacing the pinned image stack.

- [ ] **Step 6: Commit and publish for review**

Commit only the plan, tests, worker, entrypoint, and required profile metadata. Push the feature branch, file one PR, merge it after required checks pass, and update the runtime profile digest before a bounded paid retry.

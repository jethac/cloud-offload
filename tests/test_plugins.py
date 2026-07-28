"""Discovery of third-party connector plugins.

Every test runs against a temporary ``CLOUD_OFFLOAD_HOME`` and restores the
global connector registry afterwards, so the real ``~/.cloud-offload`` is never
touched and other test modules still see only the built-in connectors.
"""

import copy
import importlib.metadata
import sys

import pytest

from cloud_offload import config as config_module
from cloud_offload import plugins as plugins_module
from cloud_offload import providers as providers_module
from cloud_offload.config import CloudConfig
from cloud_offload.plugins import (
    describe_plugins,
    load_connector_plugins,
    plugin_directory,
)
from cloud_offload.providers import (
    connector_metadata,
    connector_names,
    create_connector,
)
from cloud_offload.providers.base import CloudConnector, Instance

# A complete, working connector plugin: the shape a user would actually write.
PLUGIN_SOURCE = '''
from cloud_offload.providers import register_connector
from cloud_offload.providers.base import CloudConnector, Instance


class __CLASS__(CloudConnector):
    @property
    def name(self):
        return "__NAME__"

    def list_available(self, gpu_type=None, min_gpu_ram=None, max_hourly_rate=None):
        return [{"id": "offer-1", "provider": "__NAME__", "hourly_rate": 0.25}]

    def launch(
        self, offer_id, docker_image, env_vars=None, startup_script=None, disk_gb=None
    ):
        return Instance(
            id="i-1", provider="__NAME__", gpu_type="RTX_4090",
            gpu_count=1, hourly_rate=0.25, status="pending",
        )

    def get_instance(self, instance_id):
        return None

    def terminate(self, instance_id):
        return True

    def list_instances(self):
        return []


register_connector(
    "__NAME__",
    lambda config: __CLASS__(),
    display_name="__DISPLAY__",
    kind="plugin",
    settings_schema=[{"key": "region", "type": "string"}],
)
'''


class DemoConnector(CloudConnector):
    """Minimal in-process connector used by the entry point tests."""

    display_name = "Demo Cloud"
    settings_schema = [{"key": "zone", "type": "string"}]

    def __init__(self, config=None):
        self.config = config

    @property
    def name(self) -> str:
        return "demo"

    def list_available(self, gpu_type=None, min_gpu_ram=None, max_hourly_rate=None):
        return []

    def launch(
        self, offer_id, docker_image, env_vars=None, startup_script=None, disk_gb=None
    ):
        return Instance(
            id="i-1", provider="demo", gpu_type="RTX_4090",
            gpu_count=1, hourly_rate=0.1, status="pending",
        )

    def get_instance(self, instance_id):
        return None

    def terminate(self, instance_id):
        return True

    def list_instances(self):
        return []


class FakeEntryPoint:
    """Stand-in for ``importlib.metadata.EntryPoint``."""

    def __init__(self, name, target):
        self.name = name
        self.group = plugins_module.ENTRY_POINT_GROUP
        self._target = target

    def load(self):
        if isinstance(self._target, BaseException):
            raise self._target
        return self._target


@pytest.fixture(autouse=True)
def sandbox(tmp_path, monkeypatch):
    """Redirect CONFIG_DIR to tmp_path and restore global registry state."""
    saved_connectors = dict(providers_module._CONNECTORS)
    saved_canonical = dict(providers_module._CANONICAL_NAMES)
    saved_metadata = copy.deepcopy(providers_module._METADATA)

    home = tmp_path / "cloud-offload"
    home.mkdir()
    monkeypatch.setenv("CLOUD_OFFLOAD_HOME", str(home))
    monkeypatch.setattr(config_module, "CONFIG_DIR", home)
    # Hermetic by default: tests that care about entry points opt in.
    monkeypatch.setattr(importlib.metadata, "entry_points", lambda **kwargs: [])
    plugins_module.reset_plugins()

    yield home

    plugins_module.reset_plugins()
    providers_module._CONNECTORS.clear()
    providers_module._CONNECTORS.update(saved_connectors)
    providers_module._CANONICAL_NAMES.clear()
    providers_module._CANONICAL_NAMES.update(saved_canonical)
    providers_module._METADATA.clear()
    providers_module._METADATA.update(saved_metadata)
    for module_name in [
        key for key in sys.modules if key.startswith(plugins_module._MODULE_PREFIX)
    ]:
        del sys.modules[module_name]


def write_plugin(name, *, filename=None, display=None, class_name="PluginConnector"):
    """Write a working connector plugin into the connectors directory."""
    source = (
        PLUGIN_SOURCE.replace("__CLASS__", class_name)
        .replace("__NAME__", name)
        .replace("__DISPLAY__", display or name.title())
    )
    path = plugin_directory() / (filename or f"{name}.py")
    path.write_text(source, encoding="utf-8")
    return path


def use_entry_points(monkeypatch, *entry_points):
    """Serve the given entry points from our discovery group only."""

    def fake_entry_points(**kwargs):
        if kwargs.get("group") not in (None, plugins_module.ENTRY_POINT_GROUP):
            return []
        return list(entry_points)

    monkeypatch.setattr(importlib.metadata, "entry_points", fake_entry_points)


# --------------------------------------------------------------------------
# Directory discovery
# --------------------------------------------------------------------------


def test_plugin_directory_is_created_under_config_dir(sandbox):
    directory = plugin_directory()

    assert directory == sandbox / "connectors"
    assert directory.is_dir()


def test_directory_plugin_is_registered_and_constructible():
    write_plugin("nimbus", display="Nimbus GPU")

    summary = load_connector_plugins(include_declarative=False)

    assert summary["failed"] == []
    assert "nimbus" in summary["loaded"]
    assert "nimbus" in connector_names()

    connector = create_connector("nimbus", CloudConfig())
    assert isinstance(connector, CloudConnector)
    assert connector.name == "nimbus"
    assert connector.list_available()[0]["hourly_rate"] == 0.25

    metadata = connector_metadata("nimbus")
    assert metadata["display_name"] == "Nimbus GPU"
    assert metadata["kind"] == "plugin"
    assert metadata["settings_schema"] == [{"key": "region", "type": "string"}]


def test_builtin_connectors_survive_plugin_discovery():
    write_plugin("nimbus")

    load_connector_plugins(include_declarative=False)

    assert {"runpod", "vast.ai"} <= set(connector_names())


def test_private_and_dunder_modules_are_skipped():
    (plugin_directory() / "_helper.py").write_text(
        "raise RuntimeError('helpers must not execute')", encoding="utf-8"
    )
    write_plugin("nimbus")

    summary = load_connector_plugins(include_declarative=False)

    assert summary["failed"] == []
    assert summary["loaded"] == ["nimbus"]


# --------------------------------------------------------------------------
# Containment: a broken plugin must never take the coordinator down
# --------------------------------------------------------------------------


def test_broken_plugin_is_recorded_and_others_still_load():
    # Sorts first, so it would block the good plugin if failures propagated.
    (plugin_directory() / "aaa_broken.py").write_text(
        "raise RuntimeError('boom')", encoding="utf-8"
    )
    write_plugin("zzz", filename="zzz_good.py")

    summary = load_connector_plugins(include_declarative=False)

    assert summary["loaded"] == ["zzz"]
    assert "zzz" in connector_names()
    assert len(summary["failed"]) == 1
    failure = summary["failed"][0]
    assert "aaa_broken.py" in failure["source"]
    assert "RuntimeError" in failure["error"]
    assert "boom" in failure["error"]


def test_plugin_with_a_bad_import_is_recorded_not_raised():
    (plugin_directory() / "bad_import.py").write_text(
        "import definitely_not_a_real_module_xyz", encoding="utf-8"
    )

    summary = load_connector_plugins(include_declarative=False)

    assert summary["loaded"] == []
    assert len(summary["failed"]) == 1
    assert "ModuleNotFoundError" in summary["failed"][0]["error"]


def test_plugin_calling_sys_exit_does_not_stop_the_coordinator():
    (plugin_directory() / "exiting.py").write_text(
        "import sys\nsys.exit(3)\n", encoding="utf-8"
    )
    write_plugin("nimbus")

    summary = load_connector_plugins(include_declarative=False)

    assert "nimbus" in summary["loaded"]
    assert len(summary["failed"]) == 1
    assert "SystemExit" in summary["failed"][0]["error"]


def test_broken_plugin_module_is_not_left_in_sys_modules():
    (plugin_directory() / "broken.py").write_text(
        "raise RuntimeError('boom')", encoding="utf-8"
    )

    load_connector_plugins(include_declarative=False)

    assert f"{plugins_module._MODULE_PREFIX}.broken" not in sys.modules


def test_duplicate_registration_does_not_crash_discovery():
    # A plugin that claims a name a built-in already owns.
    (plugin_directory() / "collide.py").write_text(
        "from cloud_offload.providers import register_connector\n"
        "register_connector('runpod', lambda config: None)\n",
        encoding="utf-8",
    )
    write_plugin("nimbus")

    summary = load_connector_plugins(include_declarative=False)

    assert "nimbus" in summary["loaded"]
    assert len(summary["failed"]) == 1
    assert "already registered" in summary["failed"][0]["error"]
    # The built-in survived the attempted clash.
    assert connector_metadata("runpod")["kind"] == "builtin"


def test_two_plugins_claiming_one_name_leaves_the_first_intact():
    write_plugin("shared", filename="a_first.py", display="First")
    write_plugin("shared", filename="b_second.py", display="Second", class_name="Second")

    summary = load_connector_plugins(include_declarative=False)

    assert summary["loaded"] == ["shared"]
    assert len(summary["failed"]) == 1
    assert connector_metadata("shared")["display_name"] == "First"


# --------------------------------------------------------------------------
# Idempotence
# --------------------------------------------------------------------------


def test_loading_twice_is_safe_and_stable():
    write_plugin("nimbus")

    first = load_connector_plugins(include_declarative=False)
    second = load_connector_plugins(include_declarative=False)

    assert first == second
    assert second["loaded"] == ["nimbus"]
    assert second["failed"] == []
    assert create_connector("nimbus", CloudConfig()).name == "nimbus"


def test_a_plugin_module_is_executed_only_once():
    marker = plugin_directory() / "executions.txt"
    (plugin_directory() / "counter.py").write_text(
        "from pathlib import Path\n"
        f"path = Path(r'{marker}')\n"
        "path.write_text(path.read_text() + 'x' if path.exists() else 'x')\n",
        encoding="utf-8",
    )

    load_connector_plugins(include_declarative=False)
    load_connector_plugins(include_declarative=False)
    load_connector_plugins(include_declarative=False)

    assert marker.read_text() == "x"


def test_second_load_picks_up_a_newly_added_plugin():
    write_plugin("nimbus")
    load_connector_plugins(include_declarative=False)

    write_plugin("beta", class_name="BetaConnector")
    summary = load_connector_plugins(include_declarative=False)

    assert summary["loaded"] == ["beta", "nimbus"]


# --------------------------------------------------------------------------
# Entry point discovery
# --------------------------------------------------------------------------


def test_entry_point_connector_class_is_registered(monkeypatch):
    use_entry_points(monkeypatch, FakeEntryPoint("demo", DemoConnector))

    summary = load_connector_plugins(include_declarative=False)

    assert summary["loaded"] == ["demo"]
    assert summary["sources"] == {"entry-point:demo": ["demo"]}
    config = CloudConfig()
    connector = create_connector("demo", config)
    assert isinstance(connector, DemoConnector)
    # A class taking a config is handed the config.
    assert connector.config is config
    metadata = connector_metadata("demo")
    assert metadata["display_name"] == "Demo Cloud"
    assert metadata["kind"] == "plugin"
    assert metadata["settings_schema"] == [{"key": "zone", "type": "string"}]


def test_entry_point_class_taking_no_arguments_is_registered(monkeypatch):
    class ZeroArgConnector(DemoConnector):
        def __init__(self):
            super().__init__(None)

    use_entry_points(monkeypatch, FakeEntryPoint("zero", ZeroArgConnector))

    load_connector_plugins(include_declarative=False)

    assert isinstance(create_connector("zero", CloudConfig()), ZeroArgConnector)


def test_entry_point_factory_is_registered(monkeypatch):
    def make_connector(config):
        return DemoConnector(config)

    use_entry_points(monkeypatch, FakeEntryPoint("factory-cloud", make_connector))

    load_connector_plugins(include_declarative=False)

    assert "factory-cloud" in connector_names()
    assert isinstance(create_connector("factory-cloud", CloudConfig()), DemoConnector)


def test_entry_point_hook_self_registers(monkeypatch):
    registered = []

    def register_everything():
        providers_module.register_connector(
            "hooked", lambda config: DemoConnector(config), kind="plugin"
        )
        registered.append(True)

    use_entry_points(monkeypatch, FakeEntryPoint("hook", register_everything))

    summary = load_connector_plugins(include_declarative=False)

    assert registered == [True]
    assert summary["loaded"] == ["hooked"]
    assert isinstance(create_connector("hooked", CloudConfig()), DemoConnector)


def test_entry_point_that_fails_to_load_is_recorded(monkeypatch):
    use_entry_points(
        monkeypatch,
        FakeEntryPoint("rotten", ImportError("no such package")),
        FakeEntryPoint("demo", DemoConnector),
    )

    summary = load_connector_plugins(include_declarative=False)

    assert summary["loaded"] == ["demo"]
    assert summary["failed"] == [
        {"source": "entry-point:rotten", "error": "ImportError: no such package"}
    ]


def test_entry_point_of_the_wrong_type_is_recorded(monkeypatch):
    use_entry_points(monkeypatch, FakeEntryPoint("nonsense", 42))

    summary = load_connector_plugins(include_declarative=False)

    assert summary["loaded"] == []
    assert "TypeError" in summary["failed"][0]["error"]


def test_entry_points_and_directory_plugins_both_load(monkeypatch):
    use_entry_points(monkeypatch, FakeEntryPoint("demo", DemoConnector))
    write_plugin("nimbus")

    summary = load_connector_plugins(include_declarative=False)

    assert summary["loaded"] == ["demo", "nimbus"]
    assert summary["failed"] == []


def test_unreadable_entry_point_metadata_is_not_fatal(monkeypatch):
    def explode(**kwargs):
        raise RuntimeError("broken distribution metadata")

    monkeypatch.setattr(importlib.metadata, "entry_points", explode)
    write_plugin("nimbus")

    summary = load_connector_plugins(include_declarative=False)

    assert summary["loaded"] == ["nimbus"]


# --------------------------------------------------------------------------
# Declarative providers and diagnostics
# --------------------------------------------------------------------------


def test_absent_declarative_module_is_not_a_failure(monkeypatch):
    # None in sys.modules makes the import fail the way a missing module does.
    monkeypatch.setitem(sys.modules, "cloud_offload.providers.declarative", None)

    summary = load_connector_plugins(include_declarative=True)

    assert summary["failed"] == []
    assert "declarative" not in summary["sources"]


def test_declarative_providers_are_registered_when_available(monkeypatch):
    import types

    module = types.ModuleType("cloud_offload.providers.declarative")

    def register_declarative_providers():
        providers_module.register_connector(
            "spec-cloud", lambda config: DemoConnector(config), kind="declarative"
        )

    module.register_declarative_providers = register_declarative_providers
    monkeypatch.setitem(sys.modules, "cloud_offload.providers.declarative", module)

    summary = load_connector_plugins(include_declarative=True)

    assert summary["loaded"] == ["spec-cloud"]
    assert summary["sources"]["declarative"] == ["spec-cloud"]


def test_declarative_can_be_skipped(monkeypatch):
    import types

    module = types.ModuleType("cloud_offload.providers.declarative")
    module.register_declarative_providers = lambda: pytest.fail("should not run")
    monkeypatch.setitem(sys.modules, "cloud_offload.providers.declarative", module)

    summary = load_connector_plugins(include_declarative=False)

    assert "declarative" not in summary["sources"]


def test_describe_plugins_reports_the_last_load(sandbox):
    write_plugin("nimbus")

    summary = load_connector_plugins(include_declarative=False)
    described = describe_plugins()

    assert described == summary
    assert described["directory"] == str(sandbox / "connectors")
    assert described["entry_point_group"] == "cloud_offload.connectors"


def test_describe_plugins_before_any_load_is_empty():
    described = describe_plugins()

    assert described["loaded"] == []
    assert described["failed"] == []


def test_summary_is_a_copy_callers_cannot_corrupt():
    write_plugin("nimbus")

    summary = load_connector_plugins(include_declarative=False)
    summary["loaded"].append("injected")

    assert describe_plugins()["loaded"] == ["nimbus"]


def test_discovery_without_a_connectors_directory_is_quiet(sandbox):
    summary = load_connector_plugins(include_declarative=False)

    assert summary == {
        "loaded": [],
        "failed": [],
        "sources": {},
        "directory": str(sandbox / "connectors"),
        "entry_point_group": "cloud_offload.connectors",
    }


# --------------------------------------------------------------------------
# CLI wiring
# --------------------------------------------------------------------------


def test_cli_helper_logs_a_summary_and_never_raises(monkeypatch, caplog):
    from cloud_offload.__main__ import load_plugins

    write_plugin("nimbus")

    with caplog.at_level("INFO"):
        summary = load_plugins()

    assert summary["loaded"] == ["nimbus"]
    assert "connector plugins: loaded=1 failed=0" in caplog.text


def test_cli_helper_survives_a_broken_plugin_system(monkeypatch):
    from cloud_offload import __main__ as main_module

    def explode(**kwargs):
        raise RuntimeError("discovery itself is broken")

    monkeypatch.setattr(plugins_module, "load_connector_plugins", explode)

    summary = main_module.load_plugins()

    assert summary["loaded"] == []
    assert summary["failed"][0]["source"] == "discovery"

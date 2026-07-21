"""Discovery of third-party connector plugins.

Cloud Offload ships connectors for RunPod and Vast.ai, but the registry in
:mod:`cloud_offload.providers` is open: anything that calls
``register_connector()`` becomes a routable provider. This module finds that
third-party code at startup so a user can add a cloud without editing our
source.

Two discovery mechanisms, applied in order:

1. **Entry points** in the ``cloud_offload.connectors`` group — for connectors
   distributed as installable packages. Each entry point may load to a
   zero-argument callable that self-registers, a :class:`CloudConnector`
   subclass, or a factory taking a ``CloudConfig``.
2. **Loose ``*.py`` files** in ``~/.cloud-offload/connectors/`` — for a
   single-file connector a user drops in by hand. Each file is executed by
   path, so its module-level ``register_connector()`` calls run.

Plugins are *code the user chose to install*: the same trust model as ComfyUI
custom nodes. What this module does guarantee is containment — one broken
plugin is logged and skipped, never fatal, because the coordinator staying up
matters more than any single provider.
"""

import copy
import importlib.util
import inspect
import logging
import sys
from collections.abc import Callable, Iterable, Iterator
from importlib import metadata
from pathlib import Path

from cloud_offload import config as _config
from cloud_offload.providers import CloudConnector, connector_names, register_connector

logger = logging.getLogger(__name__)

#: Setuptools entry point group scanned for packaged connectors.
ENTRY_POINT_GROUP = "cloud_offload.connectors"

#: Subdirectory of ``CONFIG_DIR`` scanned for loose connector modules.
PLUGIN_DIR_NAME = "connectors"

#: Prefix for the synthetic module names given to file-based plugins.
_MODULE_PREFIX = "cloud_offload_connector_plugins"

# Discovery state, keyed by source id ("entry-point:foo", "file:/path/x.py").
# Kept at module scope so repeated calls are idempotent rather than duplicative.
_LOADED: dict[str, list[str]] = {}
_FAILURES: dict[str, str] = {}
_LAST_SUMMARY: dict | None = None


def _plugin_dir() -> Path:
    """Plugin directory path, without touching the filesystem."""
    # Read CONFIG_DIR through the module so tests (and CLOUD_OFFLOAD_HOME
    # reloads) can redirect it without this module caching a stale path.
    return Path(_config.CONFIG_DIR) / PLUGIN_DIR_NAME


def plugin_directory() -> Path:
    """Return the connector plugin directory, creating it on demand."""
    directory = _plugin_dir()
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except OSError as exc:  # pragma: no cover - unwritable home is not fatal
        logger.warning("Could not create connector plugin directory %s: %s", directory, exc)
    return directory


def describe_plugins() -> dict:
    """Return the most recent load summary, for diagnostics endpoints."""
    if _LAST_SUMMARY is None:
        return _build_summary()
    return copy.deepcopy(_LAST_SUMMARY)


def reset_plugins() -> None:
    """Forget discovery state so the next load rescans everything.

    Intended for tests. This does not unregister connectors, so a rescan after
    a reset only succeeds if the registry was restored too.
    """
    global _LAST_SUMMARY
    _LOADED.clear()
    _FAILURES.clear()
    _LAST_SUMMARY = None


def load_connector_plugins(*, include_declarative: bool = True) -> dict:
    """Discover and register third-party connectors.

    Returns ``{"loaded": [names], "failed": [{"source", "error"}], ...}``.
    Safe to call repeatedly: a source that already loaded is skipped, so a
    second call never re-executes plugin code or trips the registry's
    duplicate-name guard. Newly added plugins are still picked up.
    """
    global _LAST_SUMMARY

    if include_declarative:
        # The declarative connector is optional and may not be present yet.
        _run_source("declarative", _register_declarative, optional=True)

    for entry_point in _iter_entry_points():
        name = getattr(entry_point, "name", "?")
        _run_source(
            f"entry-point:{name}",
            lambda entry_point=entry_point: _load_entry_point(entry_point),
        )

    for path in _iter_plugin_files():
        _run_source(f"file:{path}", lambda path=path: _load_plugin_file(path))

    _LAST_SUMMARY = _build_summary()
    logger.debug(
        "connector plugins: loaded=%d failed=%d",
        len(_LAST_SUMMARY["loaded"]),
        len(_LAST_SUMMARY["failed"]),
    )
    return copy.deepcopy(_LAST_SUMMARY)


# --------------------------------------------------------------------------
# Source execution
# --------------------------------------------------------------------------


def _run_source(
    source: str,
    action: Callable[[], None],
    *,
    optional: bool = False,
) -> None:
    """Run one discovery step, recording what it registered or how it broke."""
    if source in _LOADED:
        return

    before = set(connector_names())
    try:
        action()
    except ImportError as exc:
        if optional:
            logger.debug("Optional connector source unavailable (%s): %s", source, exc)
            return
        _record_failure(source, exc)
        return
    except (Exception, SystemExit) as exc:  # noqa: BLE001 - containment is the point
        # SystemExit is included deliberately: a stray sys.exit() in plugin code
        # must not take the coordinator down with it.
        _record_failure(source, exc)
        return

    # Attribute whatever appeared in the registry to this source, so a hook
    # that registers several connectors is reported accurately.
    _LOADED[source] = sorted(set(connector_names()) - before)
    _FAILURES.pop(source, None)


def _record_failure(source: str, exc: BaseException) -> None:
    logger.warning(
        "Skipping connector plugin %s: %s: %s", source, type(exc).__name__, exc
    )
    logger.debug("Connector plugin traceback for %s", source, exc_info=True)
    _FAILURES[source] = f"{type(exc).__name__}: {exc}"
    _LOADED.pop(source, None)


def _build_summary() -> dict:
    loaded = sorted({name for names in _LOADED.values() for name in names})
    return {
        "loaded": loaded,
        "failed": [
            {"source": source, "error": error}
            for source, error in sorted(_FAILURES.items())
        ],
        "sources": {source: list(names) for source, names in sorted(_LOADED.items())},
        "directory": str(_plugin_dir()),
        "entry_point_group": ENTRY_POINT_GROUP,
    }


def _register_declarative() -> None:
    """Register providers defined by JSON specs, when that module exists."""
    from cloud_offload.providers.declarative import register_declarative_providers

    register_declarative_providers()


# --------------------------------------------------------------------------
# Entry point discovery
# --------------------------------------------------------------------------


def _iter_entry_points() -> Iterable:
    """Return entry points in our group, tolerating packaging weirdness."""
    try:
        selected = metadata.entry_points(group=ENTRY_POINT_GROUP)
    except TypeError:
        # Pre-selectable API: entry_points() returns a group -> list mapping.
        try:
            selected = metadata.entry_points().get(ENTRY_POINT_GROUP, [])
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not read connector entry points: %s", exc)
            return []
    except Exception as exc:  # noqa: BLE001 - broken distribution metadata
        logger.warning("Could not read connector entry points: %s", exc)
        return []
    try:
        return list(selected)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not read connector entry points: %s", exc)
        return []


def _load_entry_point(entry_point) -> None:
    """Load one entry point and register whatever it resolves to."""
    name = str(getattr(entry_point, "name", "") or "").strip()
    if not name:
        raise ValueError("Connector entry point has no name")
    _consume_plugin_object(name, entry_point.load())


def _consume_plugin_object(name: str, obj) -> None:
    """Interpret an entry point target: connector class, factory, or hook."""
    if isinstance(obj, type):
        if not issubclass(obj, CloudConnector):
            raise TypeError(
                f"Connector entry point {name!r} resolved to {obj.__name__}, "
                "which is not a CloudConnector subclass"
            )
        _register(name, _factory_from_class(obj), source=obj)
        return

    if callable(obj):
        if _requires_argument(obj):
            # Takes a CloudConfig: it is a connector factory.
            _register(name, obj, source=obj)
        else:
            # Takes nothing: it is a hook that registers connectors itself.
            obj()
        return

    raise TypeError(
        f"Connector entry point {name!r} resolved to {type(obj).__name__}, "
        "expected a CloudConnector subclass, a factory, or a callable hook"
    )


def _factory_from_class(cls: type) -> Callable:
    """Adapt a connector class to the ``(config) -> connector`` factory shape."""
    # Hand the config over whenever the class can accept one, even if the
    # parameter is optional. Connectors taking no arguments still work.
    wants_config = _accepts_argument(cls)

    def factory(config):
        return cls(config) if wants_config else cls()

    factory.__name__ = f"create_{cls.__name__}"
    factory.__doc__ = f"Construct {cls.__name__} for a Cloud Offload config."
    return factory


_POSITIONAL_KINDS = (
    inspect.Parameter.POSITIONAL_ONLY,
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
)


def _parameters(target) -> list | None:
    """Parameters of a callable (``self`` excluded), or ``None`` if opaque."""
    try:
        signature = inspect.signature(target)
    except (TypeError, ValueError):  # pragma: no cover - exotic callables
        return None
    return list(signature.parameters.values())


def _accepts_argument(target) -> bool:
    """Whether ``target`` can be called with one positional argument."""
    parameters = _parameters(target)
    if parameters is None:
        return False
    return any(
        parameter.kind in _POSITIONAL_KINDS
        or parameter.kind is inspect.Parameter.VAR_POSITIONAL
        for parameter in parameters
    )


def _requires_argument(target) -> bool:
    """Whether ``target`` cannot be called with no arguments at all.

    This separates a connector factory (``(config) -> connector``) from a
    registration hook (``() -> None``) arriving through the same entry point.
    """
    parameters = _parameters(target)
    if parameters is None:
        return False
    return any(
        parameter.default is inspect.Parameter.empty
        and parameter.kind in _POSITIONAL_KINDS
        for parameter in parameters
    )


def _register(name: str, factory: Callable, *, source=None) -> None:
    """Register a plugin factory, tolerating a name that already exists."""
    if name in connector_names():
        logger.warning("Connector plugin %r replaces an existing registration", name)

    display_name = getattr(source, "display_name", None)
    settings_schema = getattr(source, "settings_schema", None)
    register_connector(
        name,
        factory,
        replace=True,
        kind="plugin",
        display_name=display_name if isinstance(display_name, str) else None,
        settings_schema=settings_schema if isinstance(settings_schema, list) else None,
    )


# --------------------------------------------------------------------------
# Directory discovery
# --------------------------------------------------------------------------


def _iter_plugin_files() -> Iterator[Path]:
    """Yield candidate plugin modules in the connectors directory."""
    directory = plugin_directory()
    try:
        entries = sorted(directory.glob("*.py"))
    except OSError as exc:  # pragma: no cover - unreadable directory
        logger.warning("Could not scan connector plugin directory %s: %s", directory, exc)
        return
    for path in entries:
        # Skip dunder/private modules so helpers can live beside plugins.
        if path.name.startswith("_") or not path.is_file():
            continue
        yield path


def _load_plugin_file(path: Path) -> None:
    """Execute one plugin module by path so its registrations run."""
    module_name = f"{_MODULE_PREFIX}.{path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load connector plugin from {path}")

    module = importlib.util.module_from_spec(spec)
    # Present in sys.modules before execution so dataclasses, pickling and
    # self-referential imports inside the plugin resolve.
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(module_name, None)
        raise


__all__ = [
    "ENTRY_POINT_GROUP",
    "PLUGIN_DIR_NAME",
    "describe_plugins",
    "load_connector_plugins",
    "plugin_directory",
    "reset_plugins",
]

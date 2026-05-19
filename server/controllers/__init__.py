"""Controllers package facade.

This package now owns the former ``server/controllers.py`` implementation.
The facade re-exports the existing API surface so imports like
``import controllers`` keep working, including internal helpers used by tests.
"""

from importlib import import_module

_common = None


def _get_common():
    global _common
    if _common is None:
        _common = import_module(".common", __name__)
    return _common


def __getattr__(name: str):
    if name.startswith("__"):
        raise AttributeError(name)
    value = getattr(_get_common(), name)
    globals()[name] = value
    return value


def __dir__():
    names = {name for name in globals() if not name.startswith("__")}
    if _common is not None:
        names.update(name for name in dir(_common) if not name.startswith("__"))
    return sorted(names)


__all__: list[str] = []

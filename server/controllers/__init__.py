"""Controllers package facade.

This package now owns the former ``server/controllers.py`` implementation.
The facade re-exports the existing API surface so imports like
``import controllers`` keep working, including internal helpers used by tests.
"""

from . import common as _common

# Re-export every non-dunder name from the shared implementation module so the
# historical `import controllers` call sites and tests continue to work.
for _name in dir(_common):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(_common, _name)

__all__ = [name for name in globals() if not name.startswith("__")]

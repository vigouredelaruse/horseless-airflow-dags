"""Top-level import shim for local development.

When the project is not installed, Python will look for packages at the
repository root. This shim makes `import horseless_airflow_dags` work by
inserting `src/horseless_airflow_dags` into the package `__path__` when
present.
"""
import os

_HERE = os.path.dirname(__file__)
# src layout relative to repository root
_SRC_pkg = os.path.normpath(os.path.join(_HERE, "..", "src", "horseless_airflow_dags"))

if os.path.isdir(_SRC_pkg) and _SRC_pkg not in __path__:
    __path__.insert(0, _SRC_pkg)

__all__ = ["dags"]

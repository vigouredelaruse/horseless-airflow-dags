"""Expose project-level `dags/` modules under
`horseless_airflow_dags.dags` during development/editable installs.

This file appends the repository `dags` directory to the package
`__path__` so import machinery can locate the existing DAG modules.
"""
import os

# Locate the repository root (two levels up from this file in src layout)
_here = os.path.dirname(__file__)
_repo_root = os.path.abspath(os.path.join(_here, "..", "..", ".."))
_external_dags = os.path.join(_repo_root, "dags")

if os.path.isdir(_external_dags) and _external_dags not in __path__:
    __path__.insert(0, _external_dags)

__all__ = []

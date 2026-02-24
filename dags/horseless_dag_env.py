"""
horseless_dag_env.py
~~~~~~~~~~~~~~~~~~~~
Shared DAG-level constants and helpers for all horseless Airflow DAGs.

This file is excluded from Airflow's DAG scanner via ``dags/.airflowignore``
so it will never produce "no DAGs found" warnings or import errors when
Airflow walks the dags directory.  It IS importable by any DAG file because
the ``dags/`` folder is on ``sys.path`` for all Airflow components
(scheduler, workers, webserver, triggerer).

All code here runs at **DAG parse time** (module-level import in the worker /
scheduler process, NOT inside a virtualenv subprocess).  The only external
dependency is ``airflow.sdk``, which is always present in the Airflow worker
image.

Usage in a DAG::

    from horseless_dag_env import DEFAULT_ARGS, VENV_REQUIREMENTS, VENV_PIP_OPTIONS, build_venv_env_vars

    default_args  = DEFAULT_ARGS
    _VENV_REQUIREMENTS = VENV_REQUIREMENTS
    _VENV_PIP_OPTIONS  = VENV_PIP_OPTIONS
    _VENV_ENV_VARS     = build_venv_env_vars()               # base vars
    _VENV_ENV_VARS     = build_venv_env_vars(include_redis=True)  # + Redis transport vars
"""
from __future__ import annotations

from datetime import timedelta

from airflow.sdk import Variable

# ---------------------------------------------------------------------------
# Default task arguments — identical for every DAG in this collection.
# ---------------------------------------------------------------------------

DEFAULT_ARGS: dict = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Virtualenv package spec — shared by all @task.virtualenv tasks.
# ---------------------------------------------------------------------------

VENV_REQUIREMENTS: list[str] = ["horseless-repotracker"]

VENV_PIP_OPTIONS: list[str] = [
    "--extra-index-url",
    "https://pkgs.dev.azure.com/wizardcontroller/MetOffice/_packaging/public/pypi/simple/",
]

# ---------------------------------------------------------------------------
# Environment variable builder
# ---------------------------------------------------------------------------

def build_venv_env_vars(*, include_redis: bool = False) -> dict[str, str]:
    """Build the env-var dict forwarded to every ``@task.virtualenv`` subprocess.

    All values are resolved by ``Variable.get()`` at **DAG parse time** from
    the Airflow Variables KV store.  ``env_vars`` is NOT in ``template_fields``
    on ``PythonVirtualenvOperator`` in Airflow 3.x, so Jinja
    ``{{ var.value.X }}`` strings would be forwarded as literal strings to the
    subprocess.  Resolving here guarantees the subprocess receives the actual
    values stored in the Airflow Variables UI / API.

    Tokens (``GITHUB_TOKEN``, ``GITHUB_TOKEN_SECHELE``, ``HF_TOKEN``) are
    called without ``default=`` so that a missing KV entry raises ``KeyError``
    at parse time rather than silently running with an empty token.

    Args:
        include_redis: When ``True``, include Redis Pub/Sub transport variables.
            These must be present in the Airflow Variables KV store.

    Returns:
        A ``dict[str, str]`` suitable for passing directly to
        ``env_vars=`` on ``@task.virtualenv``.
    """
    env_vars: dict[str, str] = {
        # PostgreSQL connection
        "PG_HOST":     Variable.get("PG_HOST",     default="picok8s.dubridge.ataxlab.com"),
        "PG_PORT":     Variable.get("PG_PORT",     default="32432"),
        "PG_DBNAME":   Variable.get("PG_DBNAME",   default="horseless_repotracker_tests"),
        "PG_USER":     Variable.get("PG_USER",     default="postgres"),
        "PG_PASSWORD": Variable.get("PG_PASSWORD", default="postgres"),
        "DB_ENABLED":  Variable.get("DB_ENABLED",  default="true"),
        # GitHub HTTP transport — tokens have no default (fail loudly if absent)
        "GITHUB_TOKEN":                        Variable.get("GITHUB_TOKEN"),
        "GITHUB_TOKEN_SECHELE":                Variable.get("GITHUB_TOKEN_SECHELE"),
        "GITHUB_CORE_RATE_LIMIT_RPS":          Variable.get("GITHUB_CORE_RATE_LIMIT_RPS",          default="4"),
        "GITHUB_SEARCH_RATE_LIMIT_RPS":        Variable.get("GITHUB_SEARCH_RATE_LIMIT_RPS",        default="4"),
        "GITHUB_CONCURRENCY":                  Variable.get("GITHUB_CONCURRENCY",                  default="4"),
        "GITHUB_MAX_RETRIES":                  Variable.get("GITHUB_MAX_RETRIES",                  default="6"),
        "GITHUB_BACKOFF_MIN_SECONDS":          Variable.get("GITHUB_BACKOFF_MIN_SECONDS",          default="10"),
        "GITHUB_BACKOFF_MAX_SECONDS":          Variable.get("GITHUB_BACKOFF_MAX_SECONDS",          default="120"),
        "GITHUB_BACKOFF_JITTER_SECONDS":       Variable.get("GITHUB_BACKOFF_JITTER_SECONDS",       default=".5"),
        "GITHUB_REQUEST_TIMEOUT_SECONDS":      Variable.get("GITHUB_REQUEST_TIMEOUT_SECONDS",      default="30"),
        "GITHUB_REQUEST_SPACING_SECONDS":      Variable.get("GITHUB_REQUEST_SPACING_SECONDS",      default="0"),
        "GITHUB_WORKER_START_STAGGER_SECONDS": Variable.get("GITHUB_WORKER_START_STAGGER_SECONDS", default="1"),
        # ML / embedding — HF_TOKEN has no default (fail loudly if absent)
        "EMBEDDING_MODEL": Variable.get("EMBEDDING_MODEL", default="all-MiniLM-L6-v2"),
        "EMBEDDING_DIMS":  Variable.get("EMBEDDING_DIMS",  default="384"),
        "HF_TOKEN":        Variable.get("HF_TOKEN"),
        # Threading / parallelism
        "OMP_NUM_THREADS":          Variable.get("OMP_NUM_THREADS",        default="4"),
        "MKL_NUM_THREADS":          Variable.get("MKL_NUM_THREADS",        default="4"),
        "OPENBLAS_NUM_THREADS":     Variable.get("OPENBLAS_NUM_THREADS",   default="4"),
        "NUMEXPR_NUM_THREADS":      Variable.get("NUMEXPR_NUM_THREADS",    default="4"),
        "PYTORCH_NUM_THREADS":      Variable.get("PYTORCH_NUM_THREADS",    default="4"),
        "TOKENIZERS_PARALLELISM":   Variable.get("TOKENIZERS_PARALLELISM", default="false"),
    }

    if include_redis:
        env_vars.update({
            # Redis Pub/Sub transport — must be set in the Airflow Variables KV store.
            "REDIS_PUBSUB_HOST":                     Variable.get("REDIS_PUBSUB_HOST",                     default="localhost"),
            "REDIS_PUBSUB_PORT":                     Variable.get("REDIS_PUBSUB_PORT",                     default="6379"),
            "REDIS_PUBLISH_USERNAME":                Variable.get("REDIS_PUBLISH_USERNAME",                default=""),
            "REDIS_PUBLISH_PASSWORD":                Variable.get("REDIS_PUBLISH_PASSWORD",                default=""),
            "REDIS_PUBSUB_MODELRUN_CHANNEL":          Variable.get("REDIS_PUBSUB_MODELRUN_CHANNEL",          default="modelrun"),
            "REDIS_PUBSUB_SCHEMAOPS_RESET_CHANNEL":  Variable.get("REDIS_PUBSUB_SCHEMAOPS_RESET_CHANNEL",  default="schema_reset"),
        })

    return env_vars

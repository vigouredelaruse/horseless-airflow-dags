from __future__ import annotations

from datetime import timedelta

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, Variable, dag, task

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Shared virtualenv spec
# ---------------------------------------------------------------------------

_VENV_REQUIREMENTS = ["horseless-repotracker"]
_VENV_PIP_OPTIONS = [
    "--extra-index-url",
    "https://pkgs.dev.azure.com/wizardcontroller/MetOffice/_packaging/public/pypi/simple/",
]

# ---------------------------------------------------------------------------
# Environment variables forwarded to every @task.virtualenv subprocess.
# Variable.get() is evaluated at DAG parse time.  env_vars is NOT in
# template_fields on PythonVirtualenvOperator in Airflow 3.x so Jinja
# {{ var.value.X }} would be forwarded as a literal string.
# ---------------------------------------------------------------------------
_VENV_ENV_VARS = {
    # PostgreSQL connection
    "PG_HOST":     Variable.get("PG_HOST",     default_var="picok8s.dubridge.ataxlab.com"),
    "PG_PORT":     Variable.get("PG_PORT",     default_var="32432"),
    "PG_DBNAME":   Variable.get("PG_DBNAME",   default_var="horseless_repotracker_tests"),
    "PG_USER":     Variable.get("PG_USER",     default_var="postgres"),
    "PG_PASSWORD": Variable.get("PG_PASSWORD", default_var="postgres"),
    "DB_ENABLED":  Variable.get("DB_ENABLED",  default_var="true"),
    # GitHub HTTP transport — tokens have no default (intentionally omitted)
    "GITHUB_TOKEN":                        Variable.get("GITHUB_TOKEN"),
    "GITHUB_TOKEN_SECHELE":                Variable.get("GITHUB_TOKEN_SECHELE"),
    "GITHUB_CORE_RATE_LIMIT_RPS":          Variable.get("GITHUB_CORE_RATE_LIMIT_RPS",          default_var="4"),
    "GITHUB_SEARCH_RATE_LIMIT_RPS":        Variable.get("GITHUB_SEARCH_RATE_LIMIT_RPS",        default_var="4"),
    "GITHUB_CONCURRENCY":                  Variable.get("GITHUB_CONCURRENCY",                  default_var="4"),
    "GITHUB_MAX_RETRIES":                  Variable.get("GITHUB_MAX_RETRIES",                  default_var="6"),
    "GITHUB_BACKOFF_MIN_SECONDS":          Variable.get("GITHUB_BACKOFF_MIN_SECONDS",          default_var="10"),
    "GITHUB_BACKOFF_MAX_SECONDS":          Variable.get("GITHUB_BACKOFF_MAX_SECONDS",          default_var="120"),
    "GITHUB_BACKOFF_JITTER_SECONDS":       Variable.get("GITHUB_BACKOFF_JITTER_SECONDS",       default_var=".5"),
    "GITHUB_REQUEST_TIMEOUT_SECONDS":      Variable.get("GITHUB_REQUEST_TIMEOUT_SECONDS",      default_var="30"),
    "GITHUB_REQUEST_SPACING_SECONDS":      Variable.get("GITHUB_REQUEST_SPACING_SECONDS",      default_var="0"),
    "GITHUB_WORKER_START_STAGGER_SECONDS": Variable.get("GITHUB_WORKER_START_STAGGER_SECONDS", default_var="1"),
    # ML / embedding — HF_TOKEN has no default (intentionally omitted)
    "EMBEDDING_MODEL": Variable.get("EMBEDDING_MODEL", default_var="all-MiniLM-L6-v2"),
    "EMBEDDING_DIMS":  Variable.get("EMBEDDING_DIMS",  default_var="384"),
    "HF_TOKEN":        Variable.get("HF_TOKEN"),
    # Threading / parallelism
    "OMP_NUM_THREADS":          Variable.get("OMP_NUM_THREADS",        default_var="4"),
    "MKL_NUM_THREADS":          Variable.get("MKL_NUM_THREADS",        default_var="4"),
    "OPENBLAS_NUM_THREADS":     Variable.get("OPENBLAS_NUM_THREADS",   default_var="4"),
    "NUMEXPR_NUM_THREADS":      Variable.get("NUMEXPR_NUM_THREADS",    default_var="4"),
    "PYTORCH_NUM_THREADS":      Variable.get("PYTORCH_NUM_THREADS",    default_var="4"),
    "TOKENIZERS_PARALLELISM":   Variable.get("TOKENIZERS_PARALLELISM", default_var="false"),
    # Redis Pub/Sub transport
    "REDIS_PUBSUB_HOST":     Variable.get("REDIS_PUBSUB_HOST",     default_var="localhost"),
    "REDIS_PUBSUB_PORT":     Variable.get("REDIS_PUBSUB_PORT",     default_var="6379"),
    "REDIS_PUBLISH_USERNAME": Variable.get("REDIS_PUBLISH_USERNAME", default_var=""),
    "REDIS_PUBLISH_PASSWORD": Variable.get("REDIS_PUBLISH_PASSWORD", default_var=""),
}

# this dag is is meant to be triggered manually in the airflow ui (or by api)
# it constructs a trigger message from the SchemaOperationsMessage DTO and publishes to the 
# Redis channel that the repotracker_schema_reset_handler DAG is watching
@dag(
    dag_id="repotracker_schema_reset_operator",
    default_args=default_args,
    description=(
        "Destructively reset the Repotracker schema for a named database, "
        "dropping all tables and re-creating them from SQLAlchemy models.  "
        "Intended for development/testing use only; use with caution!"
    ),
    schedule=None,
    params={"database_name": ""},
)
def repotracker_schema_reset():
    """Manually-triggered operator DAG that fires a schema reset.

    Constructs a :class:`SchemaOperationsMessage` for the target database and
    publishes it to the ``schema_reset`` Redis Pub/Sub channel.  The
    ``repotracker_schema_reset_handler`` DAG is subscribed to that channel via
    its ``MessageQueueTrigger`` and performs the actual destructive reset.

    Trigger
    -------
    Run from the Airflow UI (or REST API) — ``schedule=None``.  Optionally
    pass ``{"database_name": "<name>"}`` in the DAG-run conf to target a
    specific database; otherwise the ``PG_DBNAME`` Airflow Variable is used.

    Task chain
    ----------
    ``resolve_database_name`` → ``publish_schema_reset_message``
    """

    @task(task_id="resolve_database_name")
    def resolve_database_name(**context) -> str:
        """Return the target database name.

        Precedence:
        1. ``dag_run.conf["database_name"]`` if non-empty.
        2. ``PG_DBNAME`` Airflow Variable (default ``"horseless_repotracker_tests"``).

        Returns:
            The database name to reset.

        Raises:
            ValueError: If neither source yields a non-empty string.
        """
        conf: dict = context.get("dag_run").conf or {}
        database_name: str = (conf.get("database_name") or "").strip()
        if not database_name:
            database_name = Variable.get("PG_DBNAME", default_var="horseless_repotracker_tests")
        if not database_name:
            raise ValueError(
                "database_name is required.  Pass it in dag_run.conf or set "
                "the PG_DBNAME Airflow Variable."
            )
        return database_name

    @task.virtualenv(
        task_id="publish_schema_reset_message",
        requirements=_VENV_REQUIREMENTS,
        pip_install_options=_VENV_PIP_OPTIONS,
        system_site_packages=False,
        env_vars=_VENV_ENV_VARS,
    )
    def publish_schema_reset_message(database_name: str) -> int:
        """Publish a :class:`SchemaOperationsMessage` to the schema_reset channel.

        Constructs a :class:`SchemaOperationsMessage` for *database_name* and
        forwards it to ``RedisTransport.publish_schema_reset``, which writes
        to the hardcoded ``schema_reset`` Pub/Sub channel consumed by the
        ``repotracker_schema_reset_handler`` DAG.

        Args:
            database_name: Target PostgreSQL database to reset.

        Returns:
            Redis subscriber count at publish time (``0`` means no consumer
            was listening; the handler DAG may not have been running).

        Raises:
            redis.RedisError: On any underlying Redis connection or protocol
                error.
        """
        from horseless_repotracker.repotracker.dto import SchemaOperationsMessage
        from horseless_repotracker.repotracker.redistransport import RedisTransport

        msg = SchemaOperationsMessage(database_name=database_name)
        with RedisTransport() as transport:
            subscriber_count = transport.publish_schema_reset(msg)
        return subscriber_count

    # --- task chain -------------------------------------------------------
    db_name = resolve_database_name()
    publish_schema_reset_message(db_name)


repotracker_schema_reset()
    
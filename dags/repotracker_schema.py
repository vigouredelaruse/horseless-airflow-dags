from __future__ import annotations

from datetime import timedelta

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, Variable, dag, task

# ---------------------------------------------------------------------------
# Assets and triggers
# ---------------------------------------------------------------------------

# The schema_reset channel carries serialised SchemaOperationsMessage JSON
# strings published by any producer that wants a destructive schema reset.
schema_reset_trigger = MessageQueueTrigger(
    scheme="redis+pubsub",
    channels=["schema_reset"],
    redis_conn_id="redis_pubsub",
)

schema_reset_asset = Asset(
    name="schema_reset",
    uri="//schema/reset",
    watchers=[AssetWatcher(name="redis_watcher", trigger=schema_reset_trigger)],
)

# ---------------------------------------------------------------------------
# Default task arguments
# ---------------------------------------------------------------------------

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
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    dag_id="repotracker_schema_reset_handler",
    default_args=default_args,
    description=(
        "Destructively reset the Repotracker schema for a named database, "
        "triggered by a SchemaOperationsMessage on the schema_reset Redis Pub/Sub channel."
    ),
    schedule=[schema_reset_asset],
)
def repotracker_schema_reset():
    """Repotracker schema reset DAG.

    Trigger flow
    ------------
    A producer serialises a :class:`SchemaOperationsMessage` to JSON and
    publishes it to the ``schema_reset`` Redis Pub/Sub channel.  Airflow's
    ``MessageQueueTrigger`` fires and creates a DAG run.

    Task chain
    ----------
    ``extract_dto_json``
        →  ``create_database_if_not_exists``
        →  ``drop_and_recreate_schema``

    * **extract_dto_json** — lightweight ``@task`` (no virtualenv overhead)
      that pulls the raw JSON string from the trigger event context and
      passes it downstream as an XCom value.

    * **create_database_if_not_exists** — ``@task.virtualenv`` that
      deserialises the :class:`SchemaOperationsMessage` and calls
      :meth:`PersistenceSQLAlchemy.create_database_from_env` with
      ``database_name`` as the target.  The task is idempotent — if the
      database already exists the call is a no-op.

    * **drop_and_recreate_schema** — ``@task.virtualenv`` that
      unconditionally drops the target database (terminating all existing
      connections first) then creates it fresh and runs
      ``Base.metadata.create_all()`` via the ORM layer.  This is
      deliberately destructive; the trigger chain is the safety gate.
    """

    @task(task_id="extract_dto_json")
    def extract_dto_json(**context) -> str:
        """Extract the SchemaOperationsMessage JSON payload from the trigger.

        The :class:`MessageQueueTrigger` for ``redis+pubsub`` places the
        raw published string in ``asset_event.extra["payload"]["data"]``.

        Returns:
            The raw JSON string published by the producer.

        Raises:
            ValueError: If no trigger events are found or the expected
                ``payload.data`` key is absent from the event extra.
        """
        triggering_events: dict = context.get("triggering_asset_events", {})

        schema_reset_events = None
        for asset_key, events in triggering_events.items():
            key_name = getattr(asset_key, "name", None) or getattr(asset_key, "uri", str(asset_key))
            if "schema_reset" in str(key_name):
                schema_reset_events = events
                break

        if not schema_reset_events:
            raise ValueError(
                "No triggering asset events found for the schema_reset asset. "
                f"Available keys: {list(triggering_events.keys())}"
            )

        latest_event = schema_reset_events[-1]
        extra: dict = getattr(latest_event, "extra", {}) or {}
        payload: dict = extra.get("payload", {})
        if "data" not in payload:
            raise ValueError(
                "Expected 'payload.data' in schema_reset asset event extra. "
                f"Got extra keys: {list(extra.keys())}, "
                f"payload keys: {list(payload.keys())}"
            )
        return payload["data"]

    @task.virtualenv(
        task_id="create_database_if_not_exists",
        requirements=_VENV_REQUIREMENTS,
        pip_install_options=_VENV_PIP_OPTIONS,
        system_site_packages=False,
        env_vars=_VENV_ENV_VARS,
    )
    def create_database_if_not_exists(dto_json: str) -> str:
        """Ensure the target database named in the DTO exists.

        Deserialises the :class:`SchemaOperationsMessage` and calls
        :meth:`PersistenceSQLAlchemy.create_database_from_env` with
        ``database_name`` overriding the ``PG_DBNAME`` env var.  The
        underlying ``create_database()`` implementation checks
        ``pg_database`` before issuing ``CREATE DATABASE`` so this
        task is idempotent.

        Args:
            dto_json: JSON string encoding a :class:`SchemaOperationsMessage`.

        Returns:
            The ``database_name`` from the DTO, forwarded downstream.
        """
        import os
        from horseless_repotracker.repotracker.dto import SchemaOperationsMessage
        from horseless_repotracker.repotracker.persistence_sqlalchemy import PersistenceSQLAlchemy

        msg = SchemaOperationsMessage.from_json(dto_json)

        PersistenceSQLAlchemy.create_database_from_env(
            dbname=msg.database_name,
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            host=os.getenv("PG_HOST"),
            port=int(os.getenv("PG_PORT", "5432")),
        )
        return msg.database_name

    @task.virtualenv(
        task_id="drop_and_recreate_schema",
        requirements=_VENV_REQUIREMENTS,
        pip_install_options=_VENV_PIP_OPTIONS,
        system_site_packages=False,
        env_vars=_VENV_ENV_VARS,
    )
    def drop_and_recreate_schema(database_name: str) -> None:
        """Destructively drop and re-create the target database schema.

        Sequence:
        1. :func:`drop_database` terminates all connections to
           *database_name* and issues ``DROP DATABASE IF EXISTS``.
        2. :func:`create_database` re-creates the empty database.
        3. :class:`PersistenceSQLAlchemy` is instantiated with a URL
           targeting *database_name*.  Its constructor calls
           ``Base.metadata.create_all()`` which applies the full ORM
           schema to the fresh database.

        Args:
            database_name: Name of the PostgreSQL database to drop and
                re-create.  Sourced from the upstream task via XCom.
        """
        import os
        from horseless_repotracker.repotracker.sqlalchemy_model import drop_database, create_database
        from horseless_repotracker.repotracker.persistence_sqlalchemy import PersistenceSQLAlchemy

        user     = os.getenv("PG_USER",     "postgres")
        password = os.getenv("PG_PASSWORD", "")
        host     = os.getenv("PG_HOST",     "localhost")
        port     = int(os.getenv("PG_PORT", "5432"))

        # Step 1: terminate all connections and drop the database.
        drop_database(
            dbname=database_name,
            user=user,
            password=password,
            host=host,
            port=port,
        )

        # Step 2: re-create the empty database.
        create_database(
            dbname=database_name,
            user=user,
            password=password,
            host=host,
            port=port,
        )

        # Step 3: apply the full ORM schema via Base.metadata.create_all().
        db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database_name}"
        orm = PersistenceSQLAlchemy(db_url=db_url)
        orm.shutdown()

    # --- task chain -------------------------------------------------------
    dto_json = extract_dto_json()
    db_name  = create_database_if_not_exists(dto_json)
    drop_and_recreate_schema(db_name)


repotracker_schema_reset()

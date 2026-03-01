from __future__ import annotations

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, Variable, dag, task
from horseless_dag_env import DEFAULT_ARGS, VENV_REQUIREMENTS, VENV_PIP_OPTIONS, build_venv_env_vars

# ---------------------------------------------------------------------------
# Shared constants — defined in horseless_dag_env.py (ignored by DAG scanner)
# ---------------------------------------------------------------------------

default_args       = DEFAULT_ARGS
_VENV_REQUIREMENTS = VENV_REQUIREMENTS
_VENV_PIP_OPTIONS  = VENV_PIP_OPTIONS
_VENV_ENV_VARS     = build_venv_env_vars(include_redis=True)

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
            database_name = Variable.get("PG_DBNAME", default="horseless_repotracker")
        if not database_name:
            raise ValueError(
                "database_name is required.  Pass it in dag_run.conf or set "
                "the PG_DBNAME Airflow Variable."
            )
        return database_name

    @task.kubernetes(
        task_id="publish_schema_reset_message",
        image="thehorselessnewspaper/horseless-repotracker@sha256:c9d9674c791fbf77b8bb75cef8adaa6f381b5f5dbc93761affa69bfb96228b63",
        name="k8s-env-task",
        env_vars=_VENV_ENV_VARS,
        image_pull_policy="IfNotPresent", 
        get_logs=True,
        is_delete_operator_pod=False
    )
    def publish_schema_reset_message(database_name: str) -> None:
        """Publish a :class:`SchemaOperationsMessage` to the schema_reset channel.

        Constructs a :class:`SchemaOperationsMessage` for *database_name* and
        forwards it to ``RedisTransport.publish_schema_reset``, which writes
        to the hardcoded ``schema_reset`` Pub/Sub channel consumed by the
        ``repotracker_schema_reset_handler`` DAG.

        Args:
            database_name: Target PostgreSQL database to reset.

        Returns:
            None.  The Redis subscriber count is logged to stdout rather than
            returned — ``@task.kubernetes`` pods write return values to
            ``/dev/null`` so the return value would be silently discarded.
            A subscriber count of ``0`` means no consumer was listening;
            the handler DAG may not have been running.

        Raises:
            redis.RedisError: On any underlying Redis connection or protocol
                error.
        """
        from horseless_repotracker.repotracker.dto import SchemaOperationsMessage
        from horseless_repotracker.repotracker.redistransport import RedisTransport

        msg = SchemaOperationsMessage(database_name=database_name)
        with RedisTransport() as transport:
            subscriber_count = transport.publish_schema_reset(msg)
        if subscriber_count == 0:
            print(
                f"[publish_schema_reset_message] WARNING: published schema reset for '{database_name}' "
                f"but subscriber_count=0 — handler DAG may not be running"
            )
        else:
            print(
                f"[publish_schema_reset_message] published schema reset for '{database_name}'; "
                f"subscriber_count={subscriber_count}"
            )

    # --- task chain -------------------------------------------------------
    db_name = resolve_database_name()
    publish_schema_reset_message(db_name)


repotracker_schema_reset()

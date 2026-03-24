from __future__ import annotations

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, Variable, dag, task
from horseless_dag_env import DEFAULT_ARGS, VENV_REQUIREMENTS, VENV_PIP_OPTIONS, build_venv_env_vars

# ---------------------------------------------------------------------------
# Assets and triggers
# ---------------------------------------------------------------------------

# The schema_reset channel carries serialised SchemaOperationsMessage JSON
# strings published by any producer that wants a destructive schema reset.
# Channel name is read from the Airflow Variables KV store
# (key: REDIS_PUBSUB_SCHEMAOPS_RESET_CHANNEL); defaults to "reset_schema" to
# match the live Airflow Variable value.
_SCHEMA_RESET_CHANNEL = Variable.get("REDIS_PUBSUB_SCHEMAOPS_RESET_CHANNEL", default="reset_schema")

schema_reset_trigger = MessageQueueTrigger(
    scheme="redis+pubsub",
    channels=[_SCHEMA_RESET_CHANNEL],
    redis_conn_id="critical_redis",
)

schema_reset_asset = Asset(
    name="schema_reset",
    uri="//schema/reset",
    watchers=[AssetWatcher(name="redis_schema_reset_watcher", trigger=schema_reset_trigger)],
)

# ---------------------------------------------------------------------------
# Shared constants — defined in horseless_dag_env.py (ignored by DAG scanner)
# ---------------------------------------------------------------------------

default_args       = DEFAULT_ARGS
_VENV_REQUIREMENTS = VENV_REQUIREMENTS
_VENV_PIP_OPTIONS  = VENV_PIP_OPTIONS
_VENV_ENV_VARS     = build_venv_env_vars()


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
    catchup=False,
    is_paused_upon_creation=False,
)
def repotracker_schema_reset_handler():
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
        →  ``create_materialized_views``

    * **extract_dto_json** — lightweight ``@task`` (no virtualenv overhead)
      that pulls the raw JSON string from the trigger event context and
      pushes it to XCom.  This is the *sole* reliable XCom source for all
      downstream Kubernetes tasks — ``@task.kubernetes`` pods write their
      return values to ``/dev/null`` so K8s-to-K8s XCom chaining is not
      used.

    * **create_database_if_not_exists** — ``@task.kubernetes`` that
      deserialises the :class:`SchemaOperationsMessage` and calls
      :meth:`PersistenceSQLAlchemy.create_database_from_env` with
      ``database_name`` as the target.  Idempotent.  Returns ``None``;
      ordering enforced via an explicit ``>>`` edge.

    * **drop_and_recreate_schema** — ``@task.kubernetes`` that
      unconditionally drops the target database (terminating all existing
      connections first) then creates it fresh and runs
      ``Base.metadata.create_all()`` via the ORM layer.  This is
      deliberately destructive; the trigger chain is the safety gate.
      Returns ``None``.

    * **create_materialized_views** — ``@task.kubernetes`` that creates
      all materialised views after the schema is in place.  Returns
      ``None``.
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

    @task.kubernetes(
        task_id="create_database_if_not_exists",
        image="localhost:32000/horseless-repotracker:latest",        
        name="k8s-env-task",
        env_vars=_VENV_ENV_VARS,
        image_pull_policy="Always",
        startup_timeout_seconds=600, 
        get_logs=True,
        is_delete_operator_pod=False
    )
    def create_database_if_not_exists(dto_json: str) -> None:
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
            None.  Ordering relative to the next task is enforced via an
            explicit ``>>`` edge; the ``@task.kubernetes`` script runner
            discards return values to ``/dev/null`` so XCom from this pod
            is not used.
        """
        import os
        from horseless_repotracker.repotracker.dto import SchemaOperationsMessage
        from horseless_repotracker.repotracker.persistence_sqlalchemy import PersistenceSQLAlchemy

        msg = SchemaOperationsMessage.from_json(dto_json)
        print(f"[create_database_if_not_exists] target database: {msg.database_name}")

        PersistenceSQLAlchemy.create_database_from_env(
            dbname=msg.database_name,
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            host=os.getenv("PG_HOST"),
            port=int(os.getenv("PG_PORT", "5432")),
        )
        print(f"[create_database_if_not_exists] database '{msg.database_name}' is ready")

    @task.kubernetes(
        task_id="drop_and_recreate_schema",
        image="localhost:32000/horseless-repotracker:latest",        name="k8s-env-task",
        env_vars=_VENV_ENV_VARS,
        image_pull_policy="IfNotPresent", 
        get_logs=True,
        is_delete_operator_pod=False
    )
    def drop_and_recreate_schema(dto_json: str) -> None:
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
            dto_json: JSON string encoding a :class:`SchemaOperationsMessage`.
                Sourced directly from ``extract_dto_json`` via its plain
                ``@task`` XCom (not from the upstream Kubernetes pod return
                value, which is unreliable when the task runner writes its
                output to ``/dev/null``).
        """
        import os
        from horseless_repotracker.repotracker.dto import SchemaOperationsMessage
        from horseless_repotracker.repotracker.sqlalchemy_model import drop_database, create_database
        from horseless_repotracker.repotracker.persistence_sqlalchemy import PersistenceSQLAlchemy

        msg = SchemaOperationsMessage.from_json(dto_json)
        database_name = msg.database_name

        user     = os.getenv("PG_USER",     "postgres")
        password = os.getenv("PG_PASSWORD", "postgres")  # match build_venv_env_vars default; "" is falsy
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
        print(f"[drop_and_recreate_schema] schema reset complete for '{database_name}'")

    @task.kubernetes(
        task_id="create_materialized_views",
        image="localhost:32000/horseless-repotracker:latest",        name="k8s-env-task",
        env_vars=_VENV_ENV_VARS,
        image_pull_policy="IfNotPresent", 
        get_logs=True,
        is_delete_operator_pod=False
    )
    def create_materialized_views(dto_json: str) -> None:
        """Create all materialised views and their unique indexes.

        This task runs after ``drop_and_recreate_schema`` so the underlying
        hypertables and ORM tables already exist.  All DDL statements use
        ``IF NOT EXISTS`` / ``CREATE UNIQUE INDEX IF NOT EXISTS`` so the task
        is idempotent and safe to re-run.

        Args:
            dto_json: JSON string encoding a :class:`SchemaOperationsMessage`.
                Sourced directly from ``extract_dto_json`` via its plain
                ``@task`` XCom for the same reason as ``drop_and_recreate_schema``
                — K8s pod return values are discarded to ``/dev/null``.

        Views created
        -------------
        **B1 — mv_event_counts_by_issue_bucket**
            Weekly event-count buckets for open/closed/updated transitions with
            LAG-derived derivatives.  Feeds the M_state matrix input.

        **B2 — mv_user_repo_activity**
            Aggregated issue + comment activity per (user, repo, model_run).
            Feeds the M_user_repo bipartite matrix input.

        **B3 — mv_issue_label_incidence**
            Binary incidence of (issue, label) pairs per model run.
            Feeds the M_issue_label matrix input.

        **mv_analysis_ready**
            Wide row joining all six per-stage enrichment artifact hypertables
            on ``(issue_id, model_run_id, sample_time)``.  This is the sole
            source for the SVD community-detection pipeline after enrichment;
            no wide DataFrame is assembled from individual enrichment stages.
        """
        # Delegate to library helper so DAG stays a thin facade.
        import os
        from horseless_repotracker.repotracker.dto import SchemaOperationsMessage
        from horseless_repotracker.repotracker.schema import create_materialized_views as _create_mvs

        msg = SchemaOperationsMessage.from_json(dto_json)
        database_name = msg.database_name

        user = os.getenv("PG_USER", "postgres")
        password = os.getenv("PG_PASSWORD", "postgres")  # match build_venv_env_vars default; "" is falsy
        host = os.getenv("PG_HOST", "localhost")
        port = int(os.getenv("PG_PORT", "5432"))

        _create_mvs(database_name, user=user, password=password, host=host, port=port)

    # --- task chain -------------------------------------------------------
    # NOTE: All downstream Kubernetes tasks receive dto_json directly from the
    # plain @task extract_dto_json (reliable XCom) rather than chaining on
    # @task.kubernetes return values.  The Kubernetes task runner writes its
    # output to /dev/null, so pod return values are never pushed to XCom and
    # arrive as None in downstream tasks.  Explicit ordering edges preserve
    # the required create-before-drop-before-views sequence.
    dto_json   = extract_dto_json()
    db_created = create_database_if_not_exists(dto_json)
    schema     = drop_and_recreate_schema(dto_json)
    mv_task    = create_materialized_views(dto_json)
    db_created >> schema >> mv_task


repotracker_schema_reset_handler()

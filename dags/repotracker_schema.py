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
# (key: REDIS_PUBSUB_SCHEMAOPS_RESET_CHANNEL); defaults to "schema_reset" to
# match RedisTransport._SCHEMA_RESET_CHANNEL.
_SCHEMA_RESET_CHANNEL = Variable.get("REDIS_PUBSUB_SCHEMAOPS_RESET_CHANNEL", default="schema_reset")

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
        system_site_packages=True,
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
        system_site_packages=True,
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

    @task.virtualenv(
        task_id="create_materialized_views",
        requirements=_VENV_REQUIREMENTS,
        pip_install_options=_VENV_PIP_OPTIONS,
        system_site_packages=True,
        env_vars=_VENV_ENV_VARS,
    )
    def create_materialized_views(database_name: str) -> None:
        """Create all materialised views and their unique indexes.

        This task runs after ``drop_and_recreate_schema`` so the underlying
        hypertables and ORM tables already exist.  All DDL statements use
        ``IF NOT EXISTS`` / ``CREATE UNIQUE INDEX IF NOT EXISTS`` so the task
        is idempotent and safe to re-run.

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
        import os
        import psycopg2

        user     = os.getenv("PG_USER",     "postgres")
        password = os.getenv("PG_PASSWORD", "")
        host     = os.getenv("PG_HOST",     "localhost")
        port     = int(os.getenv("PG_PORT", "5432"))

        conn = psycopg2.connect(
            dbname=database_name, user=user, password=password, host=host, port=port
        )
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                # --------------------------------------------------------
                # B1 — mv_event_counts_by_issue_bucket
                # --------------------------------------------------------
                cur.execute("""
                    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_event_counts_by_issue_bucket AS
                    WITH event_counts AS (
                        SELECT
                            issue_github_id                       AS issue_id,
                            model_run_id,
                            date_trunc('week', sample_time)       AS week_bucket,
                            COUNT(*) FILTER (WHERE event_type IN ('opened', 'reopened'))
                                                                  AS event_count_open,
                            COUNT(*) FILTER (WHERE event_type = 'closed')
                                                                  AS event_count_closed,
                            COUNT(*) FILTER (WHERE event_type = 'updated')
                                                                  AS event_count_updated
                        FROM issue_timeline_state_events
                        WHERE issue_github_id IS NOT NULL
                        GROUP BY
                            issue_github_id,
                            model_run_id,
                            date_trunc('week', sample_time)
                    )
                    SELECT
                        issue_id,
                        model_run_id,
                        week_bucket,
                        event_count_open,
                        event_count_closed,
                        event_count_updated,
                        LAG(event_count_open)
                            OVER (PARTITION BY issue_id, model_run_id
                                  ORDER BY week_bucket)           AS prev_event_count_open,
                        LAG(event_count_closed)
                            OVER (PARTITION BY issue_id, model_run_id
                                  ORDER BY week_bucket)           AS prev_event_count_closed,
                        LAG(event_count_updated)
                            OVER (PARTITION BY issue_id, model_run_id
                                  ORDER BY week_bucket)           AS prev_event_count_updated,
                        event_count_open
                            - LAG(event_count_open)
                              OVER (PARTITION BY issue_id, model_run_id
                                    ORDER BY week_bucket)         AS event_derivative_open,
                        event_count_closed
                            - LAG(event_count_closed)
                              OVER (PARTITION BY issue_id, model_run_id
                                    ORDER BY week_bucket)         AS event_derivative_closed,
                        event_count_updated
                            - LAG(event_count_updated)
                              OVER (PARTITION BY issue_id, model_run_id
                                    ORDER BY week_bucket)         AS event_derivative_updated
                    FROM event_counts;
                """)
                cur.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "uq_mv_event_counts_bucket "
                    "ON mv_event_counts_by_issue_bucket (issue_id, model_run_id, week_bucket);"
                )

                # --------------------------------------------------------
                # B2 — mv_user_repo_activity
                # --------------------------------------------------------
                cur.execute("""
                    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_user_repo_activity AS
                    SELECT
                        i.author_id                           AS user_id,
                        i.repo_id                             AS repo_id,
                        i.model_run_id,
                        COUNT(DISTINCT i.id)                  AS issue_count,
                        COUNT(DISTINCT c.id)                  AS comment_count,
                        COUNT(DISTINCT i.id)
                            + COUNT(DISTINCT c.id)            AS total_activity_weight
                    FROM issues i
                    LEFT JOIN issue_comments c
                           ON c.issue_id    = i.id
                          AND c.model_run_id= i.model_run_id
                    WHERE i.author_id IS NOT NULL
                      AND i.repo_id IS NOT NULL
                    GROUP BY i.author_id, i.repo_id, i.model_run_id;
                """)
                cur.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "uq_mv_user_repo_activity "
                    "ON mv_user_repo_activity (user_id, repo_id, model_run_id);"
                )

                # --------------------------------------------------------
                # B3 — mv_issue_label_incidence
                # --------------------------------------------------------
                cur.execute("""
                    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_issue_label_incidence AS
                    SELECT
                        il.issue_id,
                        il.label_id,
                        i.model_run_id,
                        1 AS incidence
                    FROM issue_labels il
                    JOIN issues i ON il.issue_id = i.id
                    WHERE i.model_run_id IS NOT NULL;
                """)
                cur.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "uq_mv_issue_label_incidence "
                    "ON mv_issue_label_incidence (issue_id, label_id, model_run_id);"
                )

                # --------------------------------------------------------
                # mv_analysis_ready — wide join across all enrichment artifact
                # hypertables.  Embedding vector columns are intentionally
                # excluded: they are accessed directly from issue_text_embeddings
                # during SVD rather than duplicated in the wide matview.
                # --------------------------------------------------------
                cur.execute("""
                    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_analysis_ready AS
                    SELECT
                        d.issue_id,
                        d.model_run_id,
                        d.repo,
                        d.sample_time,
                        -- Stage 1: nyquist flux + derivatives
                        d.divergence,
                        d.curl,
                        d.magnitude,
                        d.derivative_open,
                        d.derivative_closed,
                        d.derivative_updated,
                        d.resample_bin_open,
                        d.resample_bin_closed,
                        d.resample_bin_updated,
                        d.updated_minus_created_sec,
                        -- Stage 2: cross-repo
                        x.derivative_cross_mentions,
                        x.mention_count,
                        x.cross_mention_target_count,
                        -- Stage 5: physics integrals
                        p.displacement_open,
                        p.velocity_open,
                        p.acceleration_open,
                        p.k_est_open,
                        p.force_open,
                        p.displacement_closed,
                        p.velocity_closed,
                        p.acceleration_closed,
                        p.k_est_closed,
                        p.force_closed,
                        p.displacement_net,
                        p.velocity_net,
                        p.acceleration_net,
                        p.force_net,
                        p.derivative_net,
                        p.vorticity_traditional,
                        p.vorticity_curl,
                        p.vorticity_combined,
                        p.corr_disp_vel,
                        p.corr_vel_force,
                        p.corr_disp_force,
                        p.corr_k_vel,
                        p.corr_v_a,
                        -- Stage 6: vector coordinates
                        v.orthogonal_repo_coord,
                        v.vector_x,
                        v.vector_y,
                        v.vector_magnitude,
                        v.norm_0,
                        v.norm_1,
                        v.norm_2,
                        v.x_coord,
                        -- Stage 7/8/9: partial + spatial derivatives
                        pd.partial_dderivative_open_dt,
                        pd.partial_dderivative_closed_dt,
                        pd.partial_dderivative_updated_dt,
                        pd.partial_dderivative_open_drepo,
                        pd.partial_dderivative_closed_drepo,
                        pd.partial_dderivative_updated_drepo,
                        pd.derivative_open_spatial_grad,
                        pd.derivative_closed_spatial_grad,
                        pd.derivative_updated_spatial_grad,
                        -- Stage 3: scalar scores only (vector cols excluded)
                        te.complexity_score,
                        te.repo_topic_diversity
                    FROM issue_basic_derivatives d
                    LEFT JOIN issue_cross_repo_derivatives  x
                           ON x.issue_id    = d.issue_id
                          AND x.model_run_id= d.model_run_id
                          AND x.sample_time = d.sample_time
                    LEFT JOIN issue_physics_features        p
                           ON p.issue_id    = d.issue_id
                          AND p.model_run_id= d.model_run_id
                          AND p.sample_time = d.sample_time
                    LEFT JOIN issue_vector_coordinates      v
                           ON v.issue_id    = d.issue_id
                          AND v.model_run_id= d.model_run_id
                          AND v.sample_time = d.sample_time
                    LEFT JOIN issue_derivative_features     pd
                           ON pd.issue_id    = d.issue_id
                          AND pd.model_run_id= d.model_run_id
                          AND pd.sample_time = d.sample_time
                    LEFT JOIN issue_text_embeddings         te
                           ON te.issue_id    = d.issue_id
                          AND te.model_run_id= d.model_run_id;
                """)
                cur.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "uq_mv_analysis_ready "
                    "ON mv_analysis_ready (issue_id, model_run_id, sample_time);"
                )

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # --- task chain -------------------------------------------------------
    dto_json = extract_dto_json()
    db_name  = create_database_if_not_exists(dto_json)
    schema   = drop_and_recreate_schema(db_name)
    create_materialized_views(db_name)


repotracker_schema_reset_handler()

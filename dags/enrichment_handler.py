"""
enrichment_handler.py
~~~~~~~~~~~~~~~~~~~~~
Airflow DAG that enriches a completed GitHub model run.

Trigger flow
------------
1. ``github_ingester`` refreshes the B1/B2/B3 materialised views after
   ingestion and then calls ``RedisTransport.publish_enrichment_trigger(
   model_run_id)`` which publishes ``{"model_run_id": <int>}`` JSON to the
   ``modelrun_enriched`` Redis Pub/Sub channel.
2. This DAG's ``MessageQueueTrigger`` fires and creates a DAG run.

Task chain
----------
``extract_model_run_id``
    →  ``run_enrichment_pipeline``
    →  ``refresh_analysis_ready_view``

``extract_model_run_id``
    Lightweight ``@task`` (no virtualenv).  Pulls the ``model_run_id``
    integer from the trigger event payload.

``run_enrichment_pipeline``
    ``@task.virtualenv``.  Loads the raw issues DataFrame for the given
    ``model_run_id`` (A1 extraction query), runs the full
    ``EnrichmentPipeline`` stage by stage, and writes each stage's output
    columns to the corresponding per-stage artifact hypertable:

    ============  ==============================
    Stage         Artifact table
    ============  ==============================
    1 (nyquist)   ``issue_basic_derivatives``
    2 (xrepo)     ``issue_cross_repo_derivatives``
    3 (embed)     ``issue_text_embeddings``
    4 (metadata)  ``github_repository_events`` *
    5 (physics)   ``issue_physics_features``
    6 (coords)    ``issue_vector_coordinates``
    7/8/9 (deriv) ``issue_derivative_features``
    ============  ==============================

    \\* Stage 4 is wired into the pipeline (``_stage_repository_metadata``
    is called) and enriches the DataFrame with GitHub-API-sourced vectors
    and scalars.  However, the per-repo persist path to
    ``github_repository_events`` is **not yet implemented** — the Stage 4
    columns are consumed by downstream stages but no rows are written to
    the hypertable.  A dedicated ``repository_metadata_enricher`` DAG
    planned for the next iteration will handle that write path.

    Analysis-ready rows are materialized from those tables via
    ``mv_analysis_ready``; the enrichment task does NOT write a wide
    feature table.

``refresh_analysis_ready_view``
    ``@task.virtualenv``.  Runs
    ``REFRESH MATERIALIZED VIEW CONCURRENTLY mv_analysis_ready``
    so the SVD / community-detection pipeline sees the new enrichment
    artifacts.

New contradictions introduced (scheduled for follow-up)
---------------------------------------------------------
* **XCom size** — ``run_enrichment_pipeline`` passes ``model_run_id`` (int)
  via XCom; no DataFrame is transferred.  All inter-task state lives in the
  artifact hypertables.  This is by design.

* **Missing DB writer helpers** — the artifact write logic in
  ``run_enrichment_pipeline`` uses raw ``psycopg2`` ``execute_values``
  calls.  A future refactor should add ORM-backed ``ArtifactWriterORM``
  helpers inside the ``horseless-repotracker`` package so the DAG task
  stays thin.

* **stage_4_repository_metadata** — Stage 4 (``_stage_repository_metadata``)
  is now wired into the production handler.  It enriches the DataFrame with
  GitHub API metadata vectors and scalars consumed by the spatial-derivative
  stages.  The persist path to ``github_repository_events`` is intentionally
  deferred: a dedicated ``repository_metadata_enricher`` DAG will aggregate
  per-repo rows and write them to that hypertable as a separate concern.

* **embedding_dims mismatch** — the vector columns in
  ``issue_text_embeddings`` are defined with ``EMBEDDING_DIMS`` read at
  import time.  If the DB was created with a different dimensionality than
  the worker's ``EMBEDDING_DIMS`` env var, inserts will fail.  The schema
  reset handler and the worker must share the same value.
"""
from __future__ import annotations

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, Variable, dag, task
from horseless_dag_env import DEFAULT_ARGS, VENV_REQUIREMENTS, VENV_PIP_OPTIONS, build_venv_env_vars

# ---------------------------------------------------------------------------
# Assets and triggers
# ---------------------------------------------------------------------------

_ENRICHMENT_CHANNEL = Variable.get(
    "REDIS_PUBSUB_ENRICHMENT_CHANNEL", default="modelrun_enriched"
)

enrichment_trigger = MessageQueueTrigger(
    scheme="redis+pubsub",
    channels=[_ENRICHMENT_CHANNEL],
    redis_conn_id="critical_redis",
)

enrichment_asset = Asset(
    name="model_run_enriched",
    uri="//repotracker/enrichment/trigger",
    watchers=[AssetWatcher(name="enrichment_watcher", trigger=enrichment_trigger)],
)

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

default_args       = DEFAULT_ARGS
_VENV_REQUIREMENTS = VENV_REQUIREMENTS
_VENV_PIP_OPTIONS  = VENV_PIP_OPTIONS
_VENV_ENV_VARS     = build_venv_env_vars(include_redis=False)   # no redis needed in worker


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    dag_id="enrichment_handler",
    default_args=default_args,
    description=(
        "Run the enrichment pipeline for a completed ingestion run and "
        "write per-stage artifacts to TimescaleDB hypertables."
    ),
    schedule=[enrichment_asset],
    catchup=False,
    is_paused_upon_creation=False,
)
def enrichment_handler():
    """Enrichment handler DAG.

    See module docstring for architecture details.
    """

    @task(task_id="extract_model_run_id")
    def extract_model_run_id(**context) -> int:
        """Extract the model_run_id from the enrichment trigger event.

        The ``redis+pubsub`` ``MessageQueueTrigger`` wraps the published
        string as ``extra["payload"]["data"]``.  The producer
        (``publish_enrichment_trigger``) serialises
        ``{"model_run_id": <int>}`` JSON.

        Returns:
            The integer ``model_run_id`` from the trigger payload.

        Raises:
            ValueError: If no trigger events are found or the payload
                structure is unexpected.
        """
        import json

        triggering_events: dict = context.get("triggering_asset_events", {})

        enrichment_events = None
        for asset_key, events in triggering_events.items():
            key_name = getattr(asset_key, "name", None) or getattr(asset_key, "uri", str(asset_key))
            if "enriched" in str(key_name) or "enrichment" in str(key_name):
                enrichment_events = events
                break

        if not enrichment_events:
            raise ValueError(
                "No triggering asset events found for the enrichment asset. "
                f"Available keys: {list(triggering_events.keys())}"
            )

        latest_event = enrichment_events[-1]
        extra: dict = getattr(latest_event, "extra", {}) or {}
        payload: dict = extra.get("payload", {})
        if "data" not in payload:
            raise ValueError(
                f"Expected 'payload.data' in enrichment event extra. "
                f"Got extra keys: {list(extra.keys())}, "
                f"payload keys: {list(payload.keys())}"
            )
        data = json.loads(payload["data"])
        return int(data["model_run_id"])

    @task.kubernetes(
        task_id="run_enrichment_pipeline",
        image="localhost:32000/horseless-repotracker:latest",
        name="k8s-env-task",
        env_vars=_VENV_ENV_VARS,
        image_pull_policy="IfNotPresent",
        startup_timeout_seconds=600,
        get_logs=True,
        is_delete_operator_pod=False,
    )
    def run_enrichment_pipeline(model_run_id: int) -> int:
        """Delegate to :class:`EnrichmentRunner` — run all 9 enrichment stages.

        Loads ``mv_issues_enrichment_input``, runs stages 1-9 of
        :class:`EnrichmentPipeline`, bulk-inserts per-stage artifacts into
        the corresponding TimescaleDB hypertables, and returns
        *model_run_id* unchanged for downstream XCom forwarding.

        The full stage logic lives in
        ``horseless_repotracker.repotracker.enrichment_runner.EnrichmentRunner``.
        All configuration is read from environment variables via
        :meth:`EnrichmentRunner.from_env`.

        Args:
            model_run_id: The ``model_run.id`` to enrich.

        Returns:
            The same ``model_run_id`` for XCom forwarding.
        """
        from horseless_repotracker.repotracker.enrichment_runner import EnrichmentRunner

        runner = EnrichmentRunner.from_env()
        return runner.run(model_run_id)

    @task.kubernetes(
        task_id="refresh_analysis_ready_view",
        image="localhost:32000/horseless-repotracker:latest",
        name="k8s-env-task",
        env_vars=_VENV_ENV_VARS,
        image_pull_policy="IfNotPresent",
        startup_timeout_seconds=600,
        get_logs=True,
        is_delete_operator_pod=False,
    )
    def refresh_analysis_ready_view(model_run_id: int) -> int:
        """Refresh ``mv_analysis_ready`` after all artifact tables are populated.

        Uses ``REFRESH MATERIALIZED VIEW CONCURRENTLY`` so existing readers
        are not blocked.  The unique index ``uq_mv_analysis_ready`` on
        ``(issue_id, model_run_id, created_at)`` must exist (created by the
        schema reset handler) for ``CONCURRENTLY`` to work.

        Args:
            model_run_id: Forwarded model run id (passed through unchanged).

        Returns:
            The same ``model_run_id``.
        """
        import logging
        import os

        import psycopg2

        logger = logging.getLogger(__name__)

        user     = os.getenv("PG_USER",     "postgres")
        password = os.getenv("PG_PASSWORD", "")
        host     = os.getenv("PG_HOST",     "localhost")
        port     = int(os.getenv("PG_PORT", "5432"))
        dbname   = os.getenv("PG_DBNAME",   "postgres")

        conn = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )
        conn.autocommit = True   # REFRESH CONCURRENTLY cannot run in a transaction block
        try:
            with conn.cursor() as cur:
                logger.info("Refreshing mv_analysis_ready...")
                cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_analysis_ready;")
                logger.info("mv_analysis_ready refreshed.")
        finally:
            conn.close()

        return model_run_id

    # -----------------------------------------------------------------------
    # Task chain
    # -----------------------------------------------------------------------
    model_run_id     = extract_model_run_id()
    enriched_run_id  = run_enrichment_pipeline(model_run_id)
    refresh_analysis_ready_view(enriched_run_id)


dag = enrichment_handler()

if __name__ == "__main__":
    dag.test()

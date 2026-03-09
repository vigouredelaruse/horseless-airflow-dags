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
    5 (physics)   ``issue_physics_features``
    6 (coords)    ``issue_vector_coordinates``
    7/8/9 (deriv) ``issue_derivative_features``
    ============  ==============================

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

* **stage_4_repository_metadata** — Stage 4 enriches repositories with
  GitHub API metadata and embedding vectors.  The output is written to the
  ``github_repository_events`` hypertable (already in ORM tier 3a).  This
  stage is currently SKIPPED in the production enrichment handler because
  ``GithubRepositoryEvents`` inserts require the GitHub API and can be
  batched separately.  A dedicated ``repository_metadata_enricher`` DAG
  should be added to handle this.

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
        """Load issues, run all enrichment stages, write per-stage artifacts.

        A1 extraction
        ~~~~~~~~~~~~~
        Loads the canonical issues DataFrame for ``model_run_id`` by joining
        ``issues``, ``repositories``, ``users``, and ``labels``.  The result
        matches the column contract expected by ``EnrichmentPipeline``.

        Stage execution
        ~~~~~~~~~~~~~~~
        Runs each pipeline stage via the stage-specific private methods
        (``_stage_basic_derivatives``, etc.) using ``asyncio.run()`` rather
        than calling the full ``enrich_dataframe()`` to enable per-stage
        artifact writes between stages.

        Artifact writes
        ~~~~~~~~~~~~~~~
        After each stage executes, the newly produced columns are extracted
        from the accumulated DataFrame and bulk-inserted into the
        corresponding artifact hypertable using ``psycopg2.extras.execute_values``
        with ``ON CONFLICT DO NOTHING`` for idempotent re-runs.

        Args:
            model_run_id: The ``model_run.id`` to enrich.

        Returns:
            The same ``model_run_id`` for XCom forwarding.
        """
        import asyncio
        import json
        import logging
        import os
        from datetime import datetime, timezone

        import numpy as np
        import pandas as pd
        import psycopg2
        import psycopg2.extras

        from horseless_repotracker.repotracker.enrichment_pipeline import EnrichmentPipeline
        from horseless_repotracker.repotracker.persistence_sqlalchemy import PersistenceSQLAlchemy

        logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

        # ------------------------------------------------------------------
        # DB connection helpers
        # ------------------------------------------------------------------
        user     = os.getenv("PG_USER",     "postgres")
        password = os.getenv("PG_PASSWORD", "")
        host     = os.getenv("PG_HOST",     "localhost")
        port     = int(os.getenv("PG_PORT", "5432"))
        dbname   = os.getenv("PG_DBNAME",   "postgres")

        def get_conn():
            return psycopg2.connect(
                dbname=dbname, user=user, password=password, host=host, port=port
            )

        # ------------------------------------------------------------------
        # A1: Load canonical issues DataFrame
        # ------------------------------------------------------------------
        logger.info("Loading issues for model_run_id=%d", model_run_id)
        conn = get_conn()
        try:
            df = pd.read_sql(
                """
                SELECT
                    i.id,
                    i.number,
                    i.title,
                    i.body,
                    i.state,
                    i.state_reason,
                    i.locked,
                    i.html_url,
                    i.author_association,
                    i.comments_count,
                    i.created_at,
                    i.updated_at,
                    i.closed_at,
                    i.author_id,
                    i.repo_id,
                    i.model_run_id,
                    r.full_name            AS repo,
                    r.language             AS repo_language,
                    r.stargazers_count     AS repo_stargazers_count,
                    r.forks_count          AS repo_forks_count,
                    r.open_issues_count    AS repo_open_issues_count,
                    r.description          AS repo_description,
                    COALESCE(
                        (
                            SELECT json_agg(l.name)
                            FROM issue_labels il
                            JOIN labels l ON l.id = il.label_id
                                         AND l.model_run_id = i.model_run_id
                            WHERE il.issue_id    = i.id
                              AND il.model_run_id = i.model_run_id
                        ),
                        '[]'
                    )::text                AS labels_json
                FROM issues i
                JOIN repositories r
                  ON r.github_id    = i.repo_id
                 AND r.model_run_id  = i.model_run_id
                WHERE i.model_run_id = %(model_run_id)s
                ORDER BY i.created_at ASC NULLS LAST
                """,
                conn,
                params={"model_run_id": model_run_id},
                parse_dates=["created_at", "updated_at", "closed_at"],
            )
        finally:
            conn.close()

        if df.empty:
            logger.warning("No issues found for model_run_id=%d — enrichment skipped.", model_run_id)
            return model_run_id

        # Convert labels_json string column to list
        df["labels"] = df["labels_json"].apply(
            lambda v: json.loads(v) if pd.notna(v) else []
        )
        df = df.drop(columns=["labels_json"])

        # NOTE: schema drift: use `created_at` as the canonical sample time
        # Historically code used `sample_time`; treat `created_at` as the
        # blanket replacement wherever `sample_time` was expected.

        logger.info("Loaded %d issue rows for enrichment.", len(df))

        # ------------------------------------------------------------------
        # Pipeline instance (GitHub token for Stage 4 / cross-repo HTTP
        # calls; text embeddings enabled by default)
        # ------------------------------------------------------------------
        github_token = os.getenv("GITHUB_TOKEN", "")
        pipeline = EnrichmentPipeline(
            github_token=github_token,
            use_text_embeddings=True,
            embedding_model=os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2"),
            num_proc=int(os.getenv("OMP_NUM_THREADS", "4")),
        )

        # Helper: collect new columns produced by a stage
        def new_cols(df_before: pd.DataFrame, df_after: pd.DataFrame) -> list[str]:
            return [c for c in df_after.columns if c not in df_before.columns]

        def df_to_safe(df: pd.DataFrame, tz_cols: list[str]) -> pd.DataFrame:
            """Coerce tz-naive datetimes to UTC-aware for TimescaleDB."""
            out = df.copy()
            for col in tz_cols:
                if col in out.columns:
                    s = pd.to_datetime(out[col], errors="coerce")
                    if s.dt.tz is None:
                        s = s.dt.tz_localize("UTC")
                    else:
                        s = s.dt.tz_convert("UTC")
                    out[col] = s
            return out

        # ------------------------------------------------------------------
        # Bulk-insert helper
        # ------------------------------------------------------------------
        def bulk_insert(table: str, records: list[dict]) -> None:
            if not records:
                logger.warning("No records to insert for table=%s", table)
                return
            cols = list(records[0].keys())
            col_str = ", ".join(f'"{c}"' for c in cols)
            values = [
                tuple(
                    (v.tolist() if isinstance(v, np.ndarray) else v)
                    for v in row.values()
                )
                for row in records
            ]
            sql = (
                f"INSERT INTO {table} ({col_str}) VALUES %s "
                "ON CONFLICT DO NOTHING"
            )
            conn = get_conn()
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(cur, sql, values, page_size=500)
                conn.commit()
                logger.info("Inserted %d rows into %s.", len(values), table)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

        # ------------------------------------------------------------------
        # Stage 1 — basic derivatives (nyquist_api)
        # ------------------------------------------------------------------
        logger.info("Stage 1: basic derivatives")
        df_s0 = df.copy()
        df_s1 = asyncio.run(pipeline._stage_basic_derivatives(df_s0))

        stage1_cols = [
            "derivative_open", "derivative_closed", "derivative_updated",
            "divergence", "curl", "magnitude",
            "resample_bin_open", "resample_bin_closed", "resample_bin_updated",
            "updated_minus_created_sec",
        ]

        def make_stage1_records(row):
            return {
                "created_at":              row.get("sample_time") or row.get("created_at"),
                "issue_id":                 int(row["id"]) if pd.notna(row.get("id")) else None,
                "model_run_id":             int(model_run_id),
                "repo":                     row.get("repo"),
                "divergence":               _f(row.get("divergence")),
                "curl":                     _f(row.get("curl")),
                "magnitude":                _f(row.get("magnitude")),
                "derivative_open":          _f(row.get("derivative_open")),
                "derivative_closed":        _f(row.get("derivative_closed")),
                "derivative_updated":       _f(row.get("derivative_updated")),
                "resample_bin_open":        _f(row.get("resample_bin_open")),
                "resample_bin_closed":      _f(row.get("resample_bin_closed")),
                "resample_bin_updated":     _f(row.get("resample_bin_updated")),
                "updated_minus_created_sec": _f(row.get("updated_minus_created_sec")),
            }

        def _f(v):
            """Convert numpy float / nan to Python float or None."""
            if v is None:
                return None
            try:
                fv = float(v)
                return None if np.isnan(fv) or np.isinf(fv) else fv
            except (TypeError, ValueError):
                return None

        df_s1_tz = df_to_safe(df_s1, ["created_at"])
        records_s1 = [
            make_stage1_records(row)
            for _, row in df_s1_tz.iterrows()
            if pd.notna(row.get("id"))
        ]
        bulk_insert("issue_basic_derivatives", records_s1)

        # ------------------------------------------------------------------
        # Stage 2 — cross-repo derivatives
        # ------------------------------------------------------------------
        logger.info("Stage 2: cross-repo derivatives")
        df_s2 = asyncio.run(pipeline._stage_cross_repo_derivatives(df_s1))

        def make_stage2_records(row):
            return {
                "created_at":               row.get("sample_time") or row.get("created_at"),
                "issue_id":                  int(row["id"]) if pd.notna(row.get("id")) else None,
                "model_run_id":              int(model_run_id),
                "repo":                      row.get("repo"),
                "derivative_cross_mentions": _f(row.get("derivative_cross_mentions")),
                "mention_count":             _f(row.get("mention_count")),
                "cross_mention_target_count": _f(row.get("cross_mention_target_count")),
            }

        df_s2_tz = df_to_safe(df_s2, ["created_at"])
        records_s2 = [
            make_stage2_records(row)
            for _, row in df_s2_tz.iterrows()
            if pd.notna(row.get("id"))
            and any(pd.notna(row.get(c)) for c in
                    ["derivative_cross_mentions", "mention_count", "cross_mention_target_count"])
        ]
        bulk_insert("issue_cross_repo_derivatives", records_s2)

        # ------------------------------------------------------------------
        # Stage 3 — text embeddings
        # ------------------------------------------------------------------
        logger.info("Stage 3: text embeddings")
        df_s3 = asyncio.run(pipeline._stage_text_embeddings(df_s2))
        df_s3_tz = df_to_safe(df_s3, ["created_at"])

        embed_cols = [
            "issue_body_embedding", "issue_title_embedding",
            "issue_language_embedding", "issue_topic_embedding",
            "repo_license_embedding",
        ]
        scalar3_cols = [
            "complexity_score", "repo_topic_diversity",
        ]
        jsonb3_cols = [
            "language_distribution", "topic_distribution", "repo_language_encoding",
        ]

        def make_stage3_records(row):
            rec = {
                "created_at":  row.get("created_at"),
                "issue_id":     int(row["id"]) if pd.notna(row.get("id")) else None,
                "model_run_id": int(model_run_id),
                "repo":         row.get("repo"),
            }
            for col in embed_cols:
                v = row.get(col)
                if v is not None and not (isinstance(v, float) and np.isnan(v)):
                    rec[col] = list(v) if hasattr(v, "__iter__") and not isinstance(v, str) else None
                else:
                    rec[col] = None
            for col in scalar3_cols:
                rec[col] = _f(row.get(col))
            for col in jsonb3_cols:
                v = row.get(col)
                if isinstance(v, (dict, list)):
                    rec[col] = json.dumps(v)
                elif isinstance(v, str):
                    rec[col] = v
                else:
                    rec[col] = None
            return rec

        # Deduplicate on (issue_id) — text embeddings are per-issue, not per-bucket
        seen_ids: set[int] = set()
        records_s3 = []
        for _, row in df_s3_tz.iterrows():
            iid = row.get("id")
            if not pd.notna(iid):
                continue
            iid_int = int(iid)
            if iid_int in seen_ids:
                continue
            seen_ids.add(iid_int)
            records_s3.append(make_stage3_records(row))
        bulk_insert("issue_text_embeddings", records_s3)

        # ------------------------------------------------------------------
        # Stage 5 — physics features
        # ------------------------------------------------------------------
        logger.info("Stage 5: physics features")
        df_s5 = asyncio.run(pipeline._stage_physics_features(df_s3))
        df_s5_tz = df_to_safe(df_s5, ["created_at"])

        physics_cols = [
            "displacement_open", "velocity_open", "acceleration_open", "k_est_open", "force_open",
            "velocity_integrated_open",
            "displacement_closed", "velocity_closed", "acceleration_closed", "k_est_closed",
            "force_closed", "velocity_integrated_closed",
            "displacement_net", "velocity_net", "acceleration_net", "k_est_net", "force_net",
            "derivative_net",
            "vorticity_traditional", "vorticity_curl", "vorticity_combined",
            "corr_disp_vel", "corr_vel_force", "corr_disp_force", "corr_k_vel", "corr_v_a",
        ]

        def make_stage5_records(row):
            rec = {
                "created_at":  row.get("sample_time") or row.get("created_at"),
                "issue_id":     int(row["id"]) if pd.notna(row.get("id")) else None,
                "model_run_id": int(model_run_id),
                "repo":         row.get("repo"),
            }
            for col in physics_cols:
                rec[col] = _f(row.get(col))
            return rec

        records_s5 = [
            make_stage5_records(row)
            for _, row in df_s5_tz.iterrows()
            if pd.notna(row.get("id"))
            and any(_f(row.get(c)) is not None for c in physics_cols)
        ]
        bulk_insert("issue_physics_features", records_s5)

        # ------------------------------------------------------------------
        # Stage 6 — vector coordinates
        # ------------------------------------------------------------------
        logger.info("Stage 6: vector coordinates")
        df_s6 = asyncio.run(pipeline._stage_vector_coordinates(df_s5))
        df_s6_tz = df_to_safe(df_s6, ["created_at"])

        coord_cols = [
            "vector_x", "vector_y", "vector_magnitude",
            "norm_0", "norm_1", "norm_2", "x_coord",
        ]

        def make_stage6_records(row):
            # Normalise the duplicate column name from the pipeline
            coord = _f(
                row.get("orthogonal_repo_coordinate")
                or row.get("orthogonal_repo_coord")
            )
            rec = {
                "created_at":           row.get("sample_time") or row.get("created_at"),
                "issue_id":              int(row["id"]) if pd.notna(row.get("id")) else None,
                "model_run_id":          int(model_run_id),
                "repo":                  row.get("repo"),
                "orthogonal_repo_coord": coord,
            }
            for col in coord_cols:
                rec[col] = _f(row.get(col))
            return rec

        records_s6 = [
            make_stage6_records(row)
            for _, row in df_s6_tz.iterrows()
            if pd.notna(row.get("id"))
        ]
        bulk_insert("issue_vector_coordinates", records_s6)

        # ------------------------------------------------------------------
        # Stages 7 / 8 / 9 — derivative features (partial + spatial)
        # ------------------------------------------------------------------
        logger.info("Stage 7: partial derivatives")
        df_s7 = asyncio.run(pipeline._stage_partial_derivatives(df_s6))
        logger.info("Stage 8: spatial derivatives")
        df_s8 = asyncio.run(pipeline._stage_spatial_derivatives(df_s7))
        logger.info("Stage 9: partial derivatives (time only)")
        df_s9 = asyncio.run(pipeline._stage_partial_derivatives_time(df_s8))
        df_s9_tz = df_to_safe(df_s9, ["created_at"])

        deriv_cols = [
            "partial_dderivative_open_dt", "partial_dderivative_closed_dt",
            "partial_dderivative_updated_dt",
            "partial_dderivative_open_drepo", "partial_dderivative_closed_drepo",
            "partial_dderivative_updated_drepo",
            "derivative_open_spatial_grad", "derivative_closed_spatial_grad",
            "derivative_updated_spatial_grad",
        ]

        def make_stage789_records(row):
            rec = {
                "created_at":  row.get("sample_time") or row.get("created_at"),
                "issue_id":     int(row["id"]) if pd.notna(row.get("id")) else None,
                "model_run_id": int(model_run_id),
                "repo":         row.get("repo"),
            }
            for col in deriv_cols:
                rec[col] = _f(row.get(col))
            return rec

        records_s789 = [
            make_stage789_records(row)
            for _, row in df_s9_tz.iterrows()
            if pd.notna(row.get("id"))
            and any(_f(row.get(c)) is not None for c in deriv_cols)
        ]
        bulk_insert("issue_derivative_features", records_s789)

        logger.info("Enrichment pipeline complete for model_run_id=%d.", model_run_id)
        return model_run_id

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

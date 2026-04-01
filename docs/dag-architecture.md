# DAG Architecture — horseless-airflow-dags

> **Living document.** Update this file whenever connections, channels, or DAG task chains change in deployment.

## Deployment context (as of 2026-02-25)

| Component | Value |
|---|---|
| Airflow version | 3.1.7 |
| API server | `http://airflow-api.dubridge.ataxlab.com` (`/api/v2`) |
| Redis broker | `critical-redis.dubridge.ataxlab.com:30379` (db 0) |
| Redis `modelrun` subscribers | 1 (Airflow triggerer) |
| Redis `reset_schema` subscribers | 1 (Airflow triggerer) |
| Redis `modelrun_enriched` subscribers | 1 (Airflow triggerer) |
| PostgreSQL | `timescale.dubridge.ataxlab.com:32432` |
| Target database | `horseless_repotracker_tests` |
| Airflow connection (Redis) | `critical_redis` (`login=default`) |
| Airflow connection (PG) | `timescaledb` |

---

## DAGs in scope

| DAG | Trigger | Purpose |
|---|---|---|
| `repotracker_schema_reset_operator` | Manual (`schedule=None`) | Publishes a `SchemaOperationsMessage` to trigger a destructive schema reset |
| `repotracker_schema_reset_handler` | `schema_reset` Asset (Redis pub/sub `reset_schema` channel) | Drops and re-creates the target PostgreSQL database schema and all materialised views |
| `github_ingester` | `model_run` Asset (Redis pub/sub `modelrun` channel) | Persists a `ModelRunDTO`, streams GitHub data into PostgreSQL, refreshes ingestion views, and publishes the enrichment trigger |
| `enrichment_handler` | `model_run_enriched` Asset (Redis pub/sub `modelrun_enriched` channel) | Runs the 9-stage enrichment pipeline and refreshes `mv_analysis_ready` |
| `modelrun_starter` | Manual (`schedule=None`) | UI-facing starter DAG — publishes a `ModelRunDTO` to the `modelrun` channel to kick off `github_ingester` |

---

## Interaction diagram

```mermaid
sequenceDiagram
    autonumber

    participant Starter as modelrun_starter<br/>(Manual DAG)
    participant OpDAG   as repotracker_schema_reset_operator
    participant Redis   as Redis Pub/Sub<br/>critical-redis:30379
    participant Trig    as Airflow Triggerer<br/>MessageQueueTrigger
    participant GI      as github_ingester
    participant EH      as enrichment_handler
    participant SR      as repotracker_schema_reset_handler
    participant Lib     as horseless-repotracker<br/>(@task.kubernetes pod)
    participant GH      as GitHub REST API
    participant PG      as PostgreSQL / TimescaleDB<br/>horseless_repotracker_tests

    %%─── github_ingester flow ────────────────────────────────────────────────
    rect rgb(230, 240, 255)
        note over Starter, PG: github_ingester — triggered by modelrun channel

        Starter   ->> Lib    : publish_modelrun()  [@task.kubernetes]<br/>ModelRunDTO → RedisTransport.publish_model_run_dto()
        Lib       ->> Redis  : PUBLISH modelrun {ModelRunDTO JSON}
        Redis    -->> Trig   : message event  (channel: modelrun)
        Trig      ->> GI     : create DAG run  (model_run Asset event)

        GI        ->> GI     : extract_dto_json()  [@task]<br/>reads extra.payload.data from asset event context

        GI        ->> Lib    : persist_model_run(dto_json)  [@task.kubernetes]<br/>ModelRunDTO.from_json() → ModelRun → ModelRunParameter → SpectralConfig
        Lib       ->> PG     : INSERT model_run, model_run_parameter, spectral_config
        PG       -->> Lib    : model_run_id
        Lib      -->> GI     : model_run_id  (XCom, do_xcom_push=True)

        GI        ->> Lib    : ingest_repositories(model_run_id)  [@task.kubernetes]<br/>GET /repos/{owner}/{repo} per params.repos → UPSERT repositories
        Lib       ->> GH     : GET /repos/{owner}/{repo}
        GH       -->> Lib    : repository metadata JSON
        Lib       ->> PG     : UPSERT repositories

        GI        ->> Lib    : ingest_repository_owners(model_run_id)  [@task.kubernetes]<br/>RepositoryOwnerIngestor per repository
        Lib       ->> GH     : GET /users/{login} or /orgs/{login}
        GH       -->> Lib    : owner profile JSON
        Lib       ->> PG     : UPSERT users / organizations

        GI        ->> Lib    : ingest_issues(model_run_id)  [@task.kubernetes]<br/>IssueIngestor.stream() per repository
        Lib       ->> GH     : paginated GET /repos/{owner}/{repo}/issues
        GH       -->> Lib    : issue pages
        Lib       ->> PG     : UPSERT users, labels, issues  (per-issue transaction)

        GI        ->> Lib    : refresh_materialized_views(model_run_id)  [@task.kubernetes]<br/>REFRESH CONCURRENTLY A1/B1/B2/B3
        Lib       ->> PG     : REFRESH MATERIALIZED VIEW CONCURRENTLY<br/>mv_issues_enrichment_input, mv_event_counts_by_issue_bucket,<br/>mv_user_repo_activity, mv_issue_label_incidence
        PG       -->> GI     : views updated

        GI        ->> Lib    : publish_enrichment_trigger(model_run_id)  [@task.kubernetes]<br/>publish_enrichment_trigger() + publish_gpu_enrichment_trigger()
        Lib       ->> Redis  : PUBLISH modelrun_enriched  {model_run_id JSON}
        Lib       ->> Redis  : PUBLISH modelrun_enriched_gpu  {ModelRunDTO JSON}
        Redis    -->> Trig   : message event  (channel: modelrun_enriched)
        Trig      ->> EH     : create DAG run  (model_run_enriched Asset event)
    end

    %%─── enrichment_handler flow ─────────────────────────────────────────────
    rect rgb(220, 255, 220)
        note over EH, PG: enrichment_handler — triggered by modelrun_enriched channel

        EH        ->> EH     : extract_model_run_id()  [@task]<br/>reads model_run_id from asset event extra.payload.data

        EH        ->> Lib    : run_enrichment_pipeline(model_run_id)  [@task.kubernetes]<br/>EnrichmentRunner.from_env().run(model_run_id)
        Lib       ->> PG     : SELECT FROM mv_issues_enrichment_input  (A1)
        Lib       ->> Lib    : stages 1–9 of EnrichmentPipeline
        Lib       ->> PG     : INSERT issue_basic_derivatives, issue_cross_repo_derivatives,<br/>issue_text_embeddings, issue_physics_features,<br/>issue_vector_coordinates, issue_derivative_features
        PG       -->> Lib    : done
        Lib      -->> EH     : model_run_id  (XCom)

        EH        ->> Lib    : refresh_analysis_ready_view(model_run_id)  [@task.kubernetes]<br/>REFRESH MATERIALIZED VIEW CONCURRENTLY mv_analysis_ready
        Lib       ->> PG     : REFRESH MATERIALIZED VIEW CONCURRENTLY mv_analysis_ready
        PG       -->> EH     : analysis-ready view updated
    end

    %%─── schema reset flow ───────────────────────────────────────────────────
    rect rgb(255, 240, 230)
        note over OpDAG, PG: schema reset — operator → handler via reset_schema channel

        OpDAG     ->> OpDAG  : resolve_database_name()  [@task]<br/>conf["database_name"] or PG_DBNAME Variable
        OpDAG     ->> Lib    : publish_schema_reset_message(db_name)  [@task.kubernetes]<br/>SchemaOperationsMessage → RedisTransport.publish_schema_reset()
        Lib       ->> Redis  : PUBLISH reset_schema {SchemaOperationsMessage JSON}

        Redis    -->> Trig   : message event  (channel: reset_schema)
        Trig      ->> SR     : create DAG run  (schema_reset Asset event)

        SR        ->> SR     : extract_dto_json()  [@task]<br/>reads extra.payload.data → dto_json  (XCom)

        SR        ->> Lib    : create_database_if_not_exists(dto_json)  [@task.kubernetes]<br/>PersistenceSQLAlchemy.create_database_from_env()
        Lib       ->> PG     : CREATE DATABASE IF NOT EXISTS {database_name}
        PG       -->> SR     : ok  (ordering edge: db_created >> schema)

        SR        ->> Lib    : drop_and_recreate_schema(dto_json)  [@task.kubernetes]<br/>drop_database() → create_database() → Base.metadata.create_all()
        Lib       ->> PG     : TERMINATE connections → DROP DATABASE → CREATE DATABASE
        Lib       ->> PG     : CREATE TABLE ...  (all SQLAlchemy ORM models)
        PG       -->> SR     : schema ready  (ordering edge: schema >> mv_task)

        SR        ->> Lib    : create_materialized_views(dto_json)  [@task.kubernetes]<br/>create_materialized_views() — B1/B2/B3 + mv_analysis_ready
        Lib       ->> PG     : CREATE MATERIALIZED VIEW IF NOT EXISTS  (all views + unique indexes)
        PG       -->> SR     : views created
    end
```

---

## PostgreSQL schema summary

### Regular tables (ORM-managed)

| Table | Notes |
|---|---|
| `model_run` | Top-level run record; `status`, `started_at` |
| `model_run_parameter` | Repos, dates, keyword, token — 1:1 with `model_run` |
| `spectral_config` | Nyquist sampling config — 1:1 with `model_run_parameter` |
| `repositories` | GitHub repo metadata; upserted per run |
| `users` | GitHub user records |
| `issues` | GitHub issues |
| `labels` | Issue labels |
| `issue_labels` | M:N join |
| `issue_comments` | Issue comment bodies |
| `pull_requests` | PR metadata |
| `commits`, `commit_comments` | Commit data |
| `milestones`, `organizations`, `teams` | Metadata |
| `reactions`, `review_requests`, `pr_reviews`, `pr_review_comments` | PR review data |
| `deployments`, `licenses` | Repository metadata |
| `model_assets`, `model_logs`, `model_run_actions`, `model_run_actors`, `model_run_events`, `model_run_triggers` | Run lifecycle tracking |

### TimescaleDB hypertables (time-partitioned, 1 dimension each)

#### Ingestion event tables

| Hypertable | Compression |
|---|---|
| `github_repository_events` | off |
| `issue_timeline_comment_events` | off |
| `issue_timeline_commit_events` | off |
| `issue_timeline_connected_events` | off |
| `issue_timeline_cross_reference_events` | off |
| `issue_timeline_project_events` | off |
| `issue_timeline_ref_push_events` | off |
| `issue_timeline_rename_events` | off |
| `issue_timeline_review_events` | off |
| `issue_timeline_reviewed_events` | off |
| `issue_timeline_state_events` | off |
| `issue_timeline_transferred_events` | off |

#### Enrichment artifact tables (written by `enrichment_handler`)

| Hypertable | Stage | Notes |
|---|---|---|
| `issue_basic_derivatives` | 1 (nyquist) | Basic Nyquist-derived features per issue |
| `issue_cross_repo_derivatives` | 2 (xrepo) | Cross-repository interaction derivatives |
| `issue_text_embeddings` | 3 (embed) | Sentence-transformer embedding vectors (`EMBEDDING_DIMS` dimensions) |
| `issue_physics_features` | 5 (physics) | Physics-inspired structural features |
| `issue_vector_coordinates` | 6 (coords) | Spatial coordinates for community detection |
| `issue_derivative_features` | 7–9 (deriv) | Higher-order derivative features |

### Materialised views

| View | Alias | Refreshed by | Notes |
|---|---|---|---|
| `mv_issues_enrichment_input` | A1 | `github_ingester.refresh_materialized_views` | Wide enrichment-input join; source for `EnrichmentRunner` |
| `mv_event_counts_by_issue_bucket` | B1 | `github_ingester.refresh_materialized_views` | Weekly event-count buckets with LAG-derived state derivatives |
| `mv_user_repo_activity` | B2 | `github_ingester.refresh_materialized_views` | Aggregated activity per (user, repo, model_run) |
| `mv_issue_label_incidence` | B3 | `github_ingester.refresh_materialized_views` | Binary (issue, label) incidence matrix per model run |
| `mv_analysis_ready` | — | `enrichment_handler.refresh_analysis_ready_view` | Wide join of all 6 enrichment artifact tables; sole source for SVD/community-detection |

---

## Airflow connection registry

| `conn_id` | Type | Host | Port | Login | Used by |
|---|---|---|---|---|---|
| `critical_redis` | redis | `critical-redis.dubridge.ataxlab.com` | 30379 | `default` | `MessageQueueTrigger` in `github_ingester`, `repotracker_schema_reset_handler`, `enrichment_handler` |
| `schema_redis` | redis | `critical-redis.dubridge.ataxlab.com` | 30379 | `default` | reserved |
| `timescaledb` | postgres | `timescale.dubridge.ataxlab.com` | 32432 | `postgres` | direct PG tooling |

## Airflow Variables — Redis transport (resolved at DAG parse time)

| Variable | Live value | Consumer |
|---|---|---|
| `REDIS_PUBSUB_HOST` | `critical-redis.dubridge.ataxlab.com` | `RedisTransport` in `@task.kubernetes` pods |
| `REDIS_PUBSUB_PORT` | `30379` | `RedisTransport` |
| `REDIS_PUBLISH_USERNAME` | `default` | `RedisTransport` |
| `REDIS_PUBLISH_PASSWORD` | `(secret)` | `RedisTransport` |
| `REDIS_PUBSUB_MODELRUN_CHANNEL` | `modelrun` | `github_ingester` trigger |
| `REDIS_PUBSUB_SCHEMAOPS_RESET_CHANNEL` | `reset_schema` | `repotracker_schema_reset_handler` trigger |
| `REDIS_PUBSUB_ENRICHMENT_CHANNEL` | `modelrun_enriched` | `enrichment_handler` trigger; `github_ingester.publish_enrichment_trigger` |
| `REDIS_PUBSUB_ENRICHMENT_GPU_CHANNEL` | `modelrun_enriched_gpu` | `github_ingester.publish_enrichment_trigger` (GPU consumers) |

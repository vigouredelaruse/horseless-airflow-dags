# Airflow Variables

All Airflow Variables used by `horseless-airflow-dags`, with their current values.
Variables marked **🔐 Secret** contain credentials — rotate them if this file is
committed to a repository with broad access.

Variables are consumed in two ways:
- **DAG parse-time** — `Variable.get()` is called at module level in a DAG file or
  in `horseless_dag_env.py`.  The scheduler must re-parse the DAG for a value change
  to take effect in trigger subscriptions or env-var injection.
- **Task run-time** — `Variable.get()` is called inside a `@task` function body.
  Changes take effect on the next DAG run without a scheduler restart.

---

## 1  Pub/Sub Channel Names

These variables name the Redis Pub/Sub channels used by `MessageQueueTrigger`
definitions and by `RedisTransport` inside Kubernetes pod tasks.

### `REDIS_PUBSUB_MODELRUN_CHANNEL`

| Field   | Value      |
|---------|------------|
| Value   | `modelrun` |
| Secret  | No         |
| Default | `modelrun` |

Channel on which `RedisTransport.publish_model_run_dto()` publishes serialised
`ModelRunDTO` JSON.  The `github_ingester` DAG's `MessageQueueTrigger` subscribes
to this channel via the `critical_redis` connection.

---

### `REDIS_PUBSUB_ENRICHMENT_CHANNEL`

| Field   | Value               |
|---------|---------------------|
| Value   | `modelrun_enriched` |
| Secret  | No                  |
| Default | `modelrun_enriched` |

Channel on which `RedisTransport.publish_enrichment_trigger(model_run_id)` publishes
an enrichment trigger payload at the end of `github_ingester`.  The
`enrichment_handler` DAG subscribes to this channel.

---

### `REDIS_PUBSUB_SCHEMAOPS_RESET_CHANNEL`

| Field   | Value          |
|---------|----------------|
| Value   | `reset_schema` |
| Secret  | No             |
| Default | `reset_schema` |

Channel on which `RedisTransport.publish_schema_reset()` publishes
`SchemaOperationsMessage` JSON.  The `repotracker_schema_reset_handler` DAG subscribes
to this channel.

---

### `REDIS_PUBSUB_ENRICHMENT_GPU_CHANNEL`

| Field   | Value                   |
|---------|-------------------------|
| Value   | `modelrun_enriched_gpu` |
| Secret  | No                      |
| Default | `modelrun_enriched_gpu` |

GPU enrichment trigger channel.  Used by `github_ingester`'s
`publish_enrichment_trigger` task when GPU-accelerated enrichment workers are present.

---

## 2  PostgreSQL Connection

Injected as environment variables into every Kubernetes pod task by
`build_venv_env_vars()` in `horseless_dag_env.py`.

### `PG_HOST`

| Field   | Value                            |
|---------|----------------------------------|
| Value   | `appliance.dubridge.ataxlab.com` |
| Secret  | No                               |
| Default | `appliance.dubridge.ataxlab.com` |

### `PG_PORT`

| Field   | Value   |
|---------|---------|
| Value   | `32433` |
| Secret  | No      |
| Default | `32433` |

### `PG_DBNAME`

| Field   | Value                      |
|---------|----------------------------|
| Value   | `horseless_repotracker`    |
| Secret  | No                         |
| Default | `horseless-repotracker`    |

Also used at task run-time by `repotracker_schema_reset_operator` as a fallback
database name when none is supplied in `dag_run.conf`.

### `PG_USER`

| Field   | Value      |
|---------|------------|
| Value   | `postgres` |
| Secret  | No         |
| Default | `postgres` |

### `PG_PASSWORD` 🔐

| Field   | Value      |
|---------|------------|
| Value   | `postgres` |
| Secret  | Yes        |
| Default | `postgres` |

### `DB_ENABLED`

| Field   | Value  |
|---------|--------|
| Value   | `true` |
| Secret  | No     |
| Default | `true` |

---

## 3  GitHub API

### `GITHUB_TOKEN` 🔐

| Field       | Value                                                                           |
|-------------|---------------------------------------------------------------------------------|
| Value       | `` *(redacted — set in Airflow Variables UI)* |
| Description | root github token                                                               |
| Secret      | Yes                                                                             |
| Default     | *(none — missing key raises `KeyError` at parse time)*                          |

Primary GitHub personal access token.  Used by all repository ingestion tasks.

### `GITHUB_TOKEN_SECHELE` 🔐

| Field       | Value                                                                                       |
|-------------|----------------------------------------------------------------------------------------------|
| Value       | `` *(redacted — set in Airflow Variables UI)* |
| Description | aux token                                                                                   |
| Secret      | Yes                                                                                         |
| Default     | *(none — missing key raises `KeyError` at parse time)*                                      |

Auxiliary GitHub personal access token for a second account, used to stay under
per-user rate limits during concurrent ingestion.

### `GITHUB_CORE_RATE_LIMIT_RPS`

| Field   | Value |
|---------|-------|
| Value   | `4`   |
| Secret  | No    |
| Default | `4`   |

### `GITHUB_SEARCH_RATE_LIMIT_RPS`

| Field   | Value |
|---------|-------|
| Value   | `4`   |
| Secret  | No    |
| Default | `4`   |

### `GITHUB_CONCURRENCY`

| Field   | Value |
|---------|-------|
| Value   | `4`   |
| Secret  | No    |
| Default | `4`   |

### `GITHUB_MAX_RETRIES`

| Field   | Value |
|---------|-------|
| Value   | `6`   |
| Secret  | No    |
| Default | `6`   |

### `GITHUB_BACKOFF_MIN_SECONDS`

| Field   | Value |
|---------|-------|
| Value   | `10`  |
| Secret  | No    |
| Default | `10`  |

### `GITHUB_BACKOFF_MAX_SECONDS`

| Field   | Value |
|---------|-------|
| Value   | `120` |
| Secret  | No    |
| Default | `120` |

### `GITHUB_BACKOFF_JITTER_SECONDS`

| Field   | Value |
|---------|-------|
| Value   | `.5`  |
| Secret  | No    |
| Default | `.5`  |

### `GITHUB_REQUEST_TIMEOUT_SECONDS`

| Field   | Value |
|---------|-------|
| Value   | `30`  |
| Secret  | No    |
| Default | `30`  |

### `GITHUB_REQUEST_SPACING_SECONDS`

| Field   | Value |
|---------|-------|
| Value   | `0`   |
| Secret  | No    |
| Default | `0`   |

### `GITHUB_WORKER_START_STAGGER_SECONDS`

| Field   | Value |
|---------|-------|
| Value   | `1`   |
| Secret  | No    |
| Default | `1`   |

---

## 4  ML / Embedding

### `EMBEDDING_MODEL`

| Field   | Value               |
|---------|---------------------|
| Value   | `all-MiniLM-L6-v2`  |
| Secret  | No                  |
| Default | `all-MiniLM-L6-v2`  |

HuggingFace sentence-transformer model identifier used by the embedding stage of
`EnrichmentPipeline`.

### `EMBEDDING_DIMS`

| Field   | Value |
|---------|-------|
| Value   | `384` |
| Secret  | No    |
| Default | `384` |

Output dimensionality of `EMBEDDING_MODEL`.  Must match the column widths of
`issue_text_embeddings`.  Changing this requires a schema reset.

### `HF_TOKEN` 🔐

| Field   | Value                                      |
|---------|--------------------------------------------|
| Value   | `` *(redacted — set in Airflow Variables UI)* |
| Secret  | Yes                                        |
| Default | *(none — missing key raises `KeyError` at parse time)* |

HuggingFace API token.  Required to download gated models.

---

## 5  Redis Pub/Sub Transport

Injected into Kubernetes pod tasks that run `RedisTransport` (i.e. tasks with
`build_venv_env_vars(include_redis=True)`).

### `REDIS_PUBSUB_HOST`

| Field   | Value                                  |
|---------|----------------------------------------|
| Value   | `critical-redis.dubridge.ataxlab.com`  |
| Secret  | No                                     |
| Default | `localhost`                            |

### `REDIS_PUBSUB_PORT`

| Field   | Value   |
|---------|---------|
| Value   | `30379` |
| Secret  | No      |
| Default | `6379`  |

### `REDIS_PUBLISH_USERNAME`

| Field   | Value     |
|---------|-----------|
| Value   | `default` |
| Secret  | No        |
| Default | *(empty)* |

### `REDIS_PUBLISH_PASSWORD` 🔐

| Field   | Value      |
|---------|------------|
| Value   | `Pa55w0rd` |
| Secret  | Yes        |
| Default | *(empty)*  |

---

## 6  Threading / Parallelism

All four values are `4`; `TOKENIZERS_PARALLELISM` is `false`.

| Variable                    | Value   | Default |
|-----------------------------|---------|---------|
| `OMP_NUM_THREADS`           | `4`     | `4`     |
| `MKL_NUM_THREADS`           | `4`     | `4`     |
| `OPENBLAS_NUM_THREADS`      | `4`     | `4`     |
| `NUMEXPR_NUM_THREADS`       | `4`     | `4`     |
| `PYTORCH_NUM_THREADS`       | `4`     | `4`     |
| `TOKENIZERS_PARALLELISM`    | `false` | `false` |

---

## Summary Table

| Variable | Value | 🔐 | Category |
|----------|-------|----|----------|
| `REDIS_PUBSUB_MODELRUN_CHANNEL` | `modelrun` | | Pub/Sub |
| `REDIS_PUBSUB_ENRICHMENT_CHANNEL` | `modelrun_enriched` | | Pub/Sub |
| `REDIS_PUBSUB_SCHEMAOPS_RESET_CHANNEL` | `reset_schema` | | Pub/Sub |
| `REDIS_PUBSUB_ENRICHMENT_GPU_CHANNEL` | `modelrun_enriched_gpu` | | Pub/Sub |
| `PG_HOST` | `appliance.dubridge.ataxlab.com` | | PostgreSQL |
| `PG_PORT` | `32433` | | PostgreSQL |
| `PG_DBNAME` | `horseless_repotracker` | | PostgreSQL |
| `PG_USER` | `postgres` | | PostgreSQL |
| `PG_PASSWORD` | `postgres` | 🔐 | PostgreSQL |
| `DB_ENABLED` | `true` | | PostgreSQL |
| `GITHUB_TOKEN` | `` | 🔐 | GitHub |
| `GITHUB_TOKEN_SECHELE` | `` | 🔐 | GitHub |
| `GITHUB_CORE_RATE_LIMIT_RPS` | `4` | | GitHub |
| `GITHUB_SEARCH_RATE_LIMIT_RPS` | `4` | | GitHub |
| `GITHUB_CONCURRENCY` | `4` | | GitHub |
| `GITHUB_MAX_RETRIES` | `6` | | GitHub |
| `GITHUB_BACKOFF_MIN_SECONDS` | `10` | | GitHub |
| `GITHUB_BACKOFF_MAX_SECONDS` | `120` | | GitHub |
| `GITHUB_BACKOFF_JITTER_SECONDS` | `.5` | | GitHub |
| `GITHUB_REQUEST_TIMEOUT_SECONDS` | `30` | | GitHub |
| `GITHUB_REQUEST_SPACING_SECONDS` | `0` | | GitHub |
| `GITHUB_WORKER_START_STAGGER_SECONDS` | `1` | | GitHub |
| `EMBEDDING_MODEL` | `all-MiniLM-L6-v2` | | ML |
| `EMBEDDING_DIMS` | `384` | | ML |
| `HF_TOKEN` | `` | 🔐 | ML |
| `REDIS_PUBSUB_HOST` | `critical-redis.dubridge.ataxlab.com` | | Redis Transport |
| `REDIS_PUBSUB_PORT` | `30379` | | Redis Transport |
| `REDIS_PUBLISH_USERNAME` | `default` | | Redis Transport |
| `REDIS_PUBLISH_PASSWORD` | `Pa55w0rd` | 🔐 | Redis Transport |
| `OMP_NUM_THREADS` | `4` | | Threading |
| `MKL_NUM_THREADS` | `4` | | Threading |
| `OPENBLAS_NUM_THREADS` | `4` | | Threading |
| `NUMEXPR_NUM_THREADS` | `4` | | Threading |
| `PYTORCH_NUM_THREADS` | `4` | | Threading |
| `TOKENIZERS_PARALLELISM` | `false` | | Threading |

---

## Appendix A — Direct Variable Usage in DAG Source Files

This appendix maps every `Variable.get()` call that appears **directly in DAG files**
(and `horseless_dag_env.py`, which lives in `dags/` and is called at DAG parse time)
to the exact location.  Usage inside the `horseless_repotracker` package (installed
into Kubernetes pod virtualenvs) is **not** listed here.

---

### A.1  `github_ingester.py` — module level (parse time)

```python
# dags/github_ingester.py  (line 16)
_MODELRUN_CHANNEL = Variable.get("REDIS_PUBSUB_MODELRUN_CHANNEL", default="modelrun")

model_run_trigger = MessageQueueTrigger(
    scheme="redis+pubsub",
    channels=[_MODELRUN_CHANNEL],
    redis_conn_id="critical_redis",
)
```

**Variables read:** `REDIS_PUBSUB_MODELRUN_CHANNEL`

This call happens at DAG parse time.  Changing the variable requires a scheduler
DAG-file re-parse to update the trigger subscription.

---

### A.2  `enrichment_handler.py` — module level (parse time)

```python
# dags/enrichment_handler.py  (line 74)
_ENRICHMENT_CHANNEL = Variable.get(
    "REDIS_PUBSUB_ENRICHMENT_CHANNEL", default="modelrun_enriched"
)

enrichment_trigger = MessageQueueTrigger(
    scheme="redis+pubsub",
    channels=[_ENRICHMENT_CHANNEL],
    redis_conn_id="critical_redis",
)
```

**Variables read:** `REDIS_PUBSUB_ENRICHMENT_CHANNEL`

Same parse-time caveat as A.1.

---

### A.3  `repotracker_schema.py` — module level (parse time)

```python
# dags/repotracker_schema.py  (line 16)
_SCHEMA_RESET_CHANNEL = Variable.get(
    "REDIS_PUBSUB_SCHEMAOPS_RESET_CHANNEL", default="reset_schema"
)

schema_reset_trigger = MessageQueueTrigger(
    scheme="redis+pubsub",
    channels=[_SCHEMA_RESET_CHANNEL],
    redis_conn_id="critical_redis",
)
```

**Variables read:** `REDIS_PUBSUB_SCHEMAOPS_RESET_CHANNEL`

---

### A.4  `reset_repotracker_schema.py` — inside `resolve_database_name` task (run time)

```python
# dags/reset_repotracker_schema.py  (inside resolve_database_name @task)
conf: dict = context.get("dag_run").conf or {}
database_name: str = (conf.get("database_name") or "").strip()
if not database_name:
    database_name = Variable.get("PG_DBNAME", default="horseless_repotracker")
```

**Variables read:** `PG_DBNAME`

Unlike the channel variables above, this call executes at **task run time**, so
changes take effect on the next DAG run without a restart.

---

### A.5  `horseless_dag_env.py` — `build_venv_env_vars()` (parse time, called by all DAGs)

`horseless_dag_env.py` lives in `dags/` and is imported at module level by every
DAG that uses `build_venv_env_vars()`.  All `Variable.get()` calls inside it
therefore execute at DAG parse time.

**Base variables (always included):**

| Variable key | env var forwarded to pod |
|--------------|--------------------------|
| `PG_HOST` | `PG_HOST` |
| `PG_PORT` | `PG_PORT` |
| `PG_DBNAME` | `PG_DBNAME` |
| `PG_USER` | `PG_USER` |
| `PG_PASSWORD` | `PG_PASSWORD` |
| `DB_ENABLED` | `DB_ENABLED` |
| `GITHUB_TOKEN` | `GITHUB_TOKEN` |
| `GITHUB_TOKEN_SECHELE` | `GITHUB_TOKEN_SECHELE` |
| `GITHUB_CORE_RATE_LIMIT_RPS` | `GITHUB_CORE_RATE_LIMIT_RPS` |
| `GITHUB_SEARCH_RATE_LIMIT_RPS` | `GITHUB_SEARCH_RATE_LIMIT_RPS` |
| `GITHUB_CONCURRENCY` | `GITHUB_CONCURRENCY` |
| `GITHUB_MAX_RETRIES` | `GITHUB_MAX_RETRIES` |
| `GITHUB_BACKOFF_MIN_SECONDS` | `GITHUB_BACKOFF_MIN_SECONDS` |
| `GITHUB_BACKOFF_MAX_SECONDS` | `GITHUB_BACKOFF_MAX_SECONDS` |
| `GITHUB_BACKOFF_JITTER_SECONDS` | `GITHUB_BACKOFF_JITTER_SECONDS` |
| `GITHUB_REQUEST_TIMEOUT_SECONDS` | `GITHUB_REQUEST_TIMEOUT_SECONDS` |
| `GITHUB_REQUEST_SPACING_SECONDS` | `GITHUB_REQUEST_SPACING_SECONDS` |
| `GITHUB_WORKER_START_STAGGER_SECONDS` | `GITHUB_WORKER_START_STAGGER_SECONDS` |
| `EMBEDDING_MODEL` | `EMBEDDING_MODEL` |
| `EMBEDDING_DIMS` | `EMBEDDING_DIMS` |
| `HF_TOKEN` | `HF_TOKEN` |
| `OMP_NUM_THREADS` | `OMP_NUM_THREADS` |
| `MKL_NUM_THREADS` | `MKL_NUM_THREADS` |
| `OPENBLAS_NUM_THREADS` | `OPENBLAS_NUM_THREADS` |
| `NUMEXPR_NUM_THREADS` | `NUMEXPR_NUM_THREADS` |
| `PYTORCH_NUM_THREADS` | `PYTORCH_NUM_THREADS` |
| `TOKENIZERS_PARALLELISM` | `TOKENIZERS_PARALLELISM` |

**Redis transport variables (`include_redis=True` — default):**

| Variable key | env var forwarded to pod |
|--------------|--------------------------|
| `REDIS_PUBSUB_HOST` | `REDIS_PUBSUB_HOST` |
| `REDIS_PUBSUB_PORT` | `REDIS_PUBSUB_PORT` |
| `REDIS_PUBLISH_USERNAME` | `REDIS_PUBLISH_USERNAME` |
| `REDIS_PUBLISH_PASSWORD` | `REDIS_PUBLISH_PASSWORD` |
| `REDIS_PUBSUB_MODELRUN_CHANNEL` | `REDIS_PUBSUB_MODELRUN_CHANNEL` |
| `REDIS_PUBSUB_SCHEMAOPS_RESET_CHANNEL` | `REDIS_PUBSUB_SCHEMAOPS_RESET_CHANNEL` |
| `REDIS_PUBSUB_ENRICHMENT_CHANNEL` | `REDIS_PUBSUB_ENRICHMENT_CHANNEL` |
| `REDIS_PUBSUB_ENRICHMENT_GPU_CHANNEL` | `REDIS_PUBSUB_ENRICHMENT_GPU_CHANNEL` |

> Note: `enrichment_handler.py` calls `build_venv_env_vars(include_redis=False)`;
> its pod tasks do not receive Redis transport variables.

**DAGs that call `build_venv_env_vars()`:**

| DAG file | `include_redis` |
|----------|-----------------|
| `github_ingester.py` | `True` (default) |
| `enrichment_handler.py` | `False` |
| `repotracker_schema.py` | `True` (default) |
| `reset_repotracker_schema.py` | `True` (default) |

---

*Source of truth: `dags/airflow_variables.json` and DAG source files.*
*Generated: 2026-03-24.*

# Airflow Connections

All Airflow Connections used by `horseless-airflow-dags`, with their current
configuration values.  Connections marked **🔐 Secret** contain credentials —
rotate them if this file is committed to a repository with broad access.

---

## `critical_redis`

| Field       | Value                                  |
|-------------|----------------------------------------|
| **Conn ID** | `critical_redis`                       |
| **Type**    | Redis                                  |
| **Host**    | `critical-redis.dubridge.ataxlab.com`  |
| **Port**    | `30379`                                |
| **Schema / DB** | `0`                                |
| **Login**   | `default`                              |
| **Password** 🔐 | `Pa55w0rd`                        |
| **Extra**   | *(none)*                               |

The sole Redis connection used for all `MessageQueueTrigger` Pub/Sub subscriptions.
Every DAG that subscribes to a Redis channel references this connection by ID.
The host, port, username, and password mirror the `REDIS_PUBSUB_*` and
`REDIS_PUBLISH_*` Airflow Variables (see [airflow-variables.md](airflow-variables.md)).

**Channels routed through this connection:**

| Channel variable | Live channel name | Direction | DAG |
|-----------------|-------------------|-----------|-----|
| `REDIS_PUBSUB_MODELRUN_CHANNEL` | `modelrun` | Subscribe | `github_ingester` |
| `REDIS_PUBSUB_ENRICHMENT_CHANNEL` | `modelrun_enriched` | Subscribe | `enrichment_handler` |
| `REDIS_PUBSUB_SCHEMAOPS_RESET_CHANNEL` | `reset_schema` | Subscribe | `repotracker_schema_reset_handler` |

> **Publish vs. subscribe.**  The triggerer process uses `critical_redis` to receive
> messages.  The *publish* side (Kubernetes pod tasks running `RedisTransport`) does
> **not** use this Airflow Connection object.  Instead the pods receive Redis
> credentials through environment variables injected by `build_venv_env_vars(include_redis=True)`
> — see [airflow-variables.md §5](airflow-variables.md).

---

## Summary Table

| Conn ID | Type | Host | Port | DB | Used By |
|---------|------|------|------|----|---------|
| `critical_redis` | Redis | `critical-redis.dubridge.ataxlab.com` | `30379` | `0` | `github_ingester`, `enrichment_handler`, `repotracker_schema_reset_handler` |

---

## Appendix A — Direct `conn_id` Usage in DAG Source Files

Every hard-coded `redis_conn_id=` reference in the `dags/` directory.  Usage inside
the `horseless_repotracker` package is **not** listed here.

---

### A.1  `github_ingester.py` — module level (parse time)

```python
# dags/github_ingester.py  (line 19)
model_run_trigger = MessageQueueTrigger(
    scheme="redis+pubsub",
    channels=[_MODELRUN_CHANNEL],
    redis_conn_id="critical_redis",
)
```

Subscribes to the `REDIS_PUBSUB_MODELRUN_CHANNEL` channel.  The trigger drives the
`model_run_asset` `AssetWatcher` that schedules every run of `github_ingester`.

---

### A.2  `enrichment_handler.py` — module level (parse time)

```python
# dags/enrichment_handler.py  (line 100)
enrichment_trigger = MessageQueueTrigger(
    scheme="redis+pubsub",
    channels=[_ENRICHMENT_CHANNEL],
    redis_conn_id="critical_redis",
)
```

Subscribes to the `REDIS_PUBSUB_ENRICHMENT_CHANNEL` channel.  The trigger drives
the `enrichment_asset` `AssetWatcher` that schedules every run of
`enrichment_handler`.

---

### A.3  `repotracker_schema.py` — module level (parse time)

```python
# dags/repotracker_schema.py  (line 19)
schema_reset_trigger = MessageQueueTrigger(
    scheme="redis+pubsub",
    channels=[_SCHEMA_RESET_CHANNEL],
    redis_conn_id="critical_redis",
)
```

Subscribes to the `REDIS_PUBSUB_SCHEMAOPS_RESET_CHANNEL` channel.  The trigger
drives the `schema_reset_asset` `AssetWatcher` that schedules every run of
`repotracker_schema_reset_handler`.

---

## Appendix B — Connections NOT modelled as Airflow Connection objects

The following service connectivity exists in the deployment but is **not** configured
as an Airflow Connection.  It is documented here so operators know where to update
credentials.

### B.1  Celery/Airflow internal broker (Redis)

| Field | Value |
|-------|-------|
| URL   | `redis://:Pa55W0rd@picok8s.dubridge.ataxlab.com:30379` |
| Purpose | Airflow task queue broker (Celery executor) |
| Configured in | `airflow-current-values.yaml` → `data.brokerUrl` |

This is a separate Redis instance from `critical_redis`.  It is used by the Airflow
scheduler and workers for task queueing, not for application Pub/Sub messaging.

### B.2  PostgreSQL (Repotracker database)

| Field | Value | Source |
|-------|-------|--------|
| Host  | `appliance.dubridge.ataxlab.com` | `PG_HOST` Airflow Variable |
| Port  | `32433` | `PG_PORT` Airflow Variable |
| Database | `horseless_repotracker` | `PG_DBNAME` Airflow Variable |
| User  | `postgres` | `PG_USER` Airflow Variable |
| Password 🔐 | `postgres` | `PG_PASSWORD` Airflow Variable |

Credentials are injected into Kubernetes pod tasks as environment variables by
`build_venv_env_vars()`.  There is no Airflow `postgres` Connection object for this
database; the `horseless_repotracker` package reads the env vars directly.

### B.3  Redis Pub/Sub transport (pod-side publish path)

| Field | Value | Source |
|-------|-------|--------|
| Host  | `critical-redis.dubridge.ataxlab.com` | `REDIS_PUBSUB_HOST` Variable |
| Port  | `30379` | `REDIS_PUBSUB_PORT` Variable |
| Username | `default` | `REDIS_PUBLISH_USERNAME` Variable |
| Password 🔐 | `Pa55w0rd` | `REDIS_PUBLISH_PASSWORD` Variable |

Used by `RedisTransport` inside Kubernetes pod tasks to *publish* trigger messages.
Injected by `build_venv_env_vars(include_redis=True)`.  This is the same Redis
server as `critical_redis` above — only the path to the credentials differs
(env vars vs. Airflow Connection object).

### B.4  HuggingFace model registry

| Field | Value | Source |
|-------|-------|--------|
| Token 🔐 | *(redacted — set in Airflow Variables UI as `HF_TOKEN`)* | `HF_TOKEN` Variable |

Injected into enrichment pod tasks as the `HF_TOKEN` environment variable.  Used by
the `sentence-transformers` library to download gated models.

### B.5  Azure DevOps package feed (pip)

| Field | Value | Source |
|-------|-------|--------|
| Index URL | `https://pkgs.dev.azure.com/wizardcontroller/MetOffice/_packaging/public/pypi/simple/` | `airflow-current-values.yaml` `env.PIP_EXTRA_INDEX_URL` |

Used at image build time and at Kubernetes pod virtualenv creation time to install
`horseless-repotracker` from the private Azure DevOps feed.  No credentials are
required because the feed is public.

---

*Source of truth: `dags/*.py`, `airflow-current-values.yaml`, `airflow_variables.json`.*
*Generated: 2026-03-24.*

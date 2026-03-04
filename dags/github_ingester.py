from __future__ import annotations

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, Variable, dag, task
from horseless_dag_env import DEFAULT_ARGS, VENV_REQUIREMENTS, VENV_PIP_OPTIONS, build_venv_env_vars

# ---------------------------------------------------------------------------
# Assets and triggers
# ---------------------------------------------------------------------------

# The model_run channel carries serialised ModelRunDTO JSON strings published
# by RedisTransport.publish_model_run_dto() on the producer side.
# Channel name is read from the Airflow Variables KV store (key: REDIS_PUBSUB_MODELRUN_CHANNEL).
_MODELRUN_CHANNEL = Variable.get("REDIS_PUBSUB_MODELRUN_CHANNEL", default="modelrun")

model_run_trigger = MessageQueueTrigger(
    scheme="redis+pubsub",
    channels=[_MODELRUN_CHANNEL],
    redis_conn_id="critical_redis",
)

model_run_asset = Asset(
    name="model_run",
    uri="//githubapi/firehose/model_run",
    watchers=[AssetWatcher(name="redis_watcher", trigger=model_run_trigger)],
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
    dag_id="github_ingester",
    default_args=default_args,
    is_paused_upon_creation=False,
    description="Ingest a GitHub model run triggered by a ModelRunDTO on Redis Pub/Sub.",
    schedule=[model_run_asset],
)
def github_ingester():
    """GitHub ingestion DAG.

    Trigger flow
    ------------
    1. A producer calls ``RedisTransport.publish_model_run_dto(dto)`` which
       serialises a :class:`ModelRunDTO` to JSON and publishes it to the
       ``modelrun`` Redis Pub/Sub channel.
    2. Airflow's ``MessageQueueTrigger`` fires, creating a DAG run.  The
       trigger event payload (the raw JSON string) is available in the task
       context under ``triggering_asset_events``.

    Task chain
    ----------
    ``extract_dto_json``  →  ``persist_model_run``

    * **extract_dto_json** — lightweight ``@task`` (no virtualenv overhead)
      that extracts the raw JSON string from the Airflow trigger context and
      passes it downstream via XCom.

    * **persist_model_run** — ``@task.virtualenv`` that deserialises the JSON
      into a :class:`ModelRunDTO` and persists the three-layer entity chain:
      ``ModelRun`` → ``ModelRunParameter`` → ``SpectralConfig``.  Returns
      the newly created ``model_run_id`` for use by downstream tasks.

    * **ingest_repositories** — ``@task.virtualenv`` that accepts the
      ``model_run_id`` produced by ``persist_model_run``, loads the
      corresponding :class:`ModelRunParameter`, then streams each repository
      in ``params.repos`` through the GitHub API and runs
      :class:`IssueIngestor` for each one, persisting all resulting entities.
    """

    @task(task_id="extract_dto_json")
    def extract_dto_json(**context) -> str:
        """Extract the ModelRunDTO JSON payload from the trigger event context.

        When the ``model_run_asset`` fires, Airflow populates
        ``context["triggering_asset_events"]`` with a dict keyed by
        :class:`Asset`.  The :class:`MessageQueueTrigger` for ``redis+pubsub``
        places the raw published string in ``asset_event.extra["message"]``.

        Returns:
            The raw JSON string that was published by the producer.

        Raises:
            ValueError: If no trigger events are found for the model_run asset,
                or if the expected message key is absent from the event extra.
        """
        triggering_events: dict = context.get("triggering_asset_events", {})

        # Locate events for the model_run_asset regardless of key type.
        model_run_events = None
        for asset_key, events in triggering_events.items():
            key_name = getattr(asset_key, "name", None) or getattr(asset_key, "uri", str(asset_key))
            if "model_run" in str(key_name):
                model_run_events = events
                break

        if not model_run_events:
            raise ValueError(
                "No triggering asset events found for the model_run asset. "
                f"Available keys: {list(triggering_events.keys())}"
            )

        # The MessageQueueTrigger for redis+pubsub wraps the full Redis
        # pub/sub envelope as extra["payload"].  The actual serialised
        # ModelRunDTO string is at extra["payload"]["data"], matching the
        # validated wire format:
        #   { "payload": { "type": "message", "channel": "modelrun",
        #                  "data": "{...ModelRunDTO JSON...}" } }
        latest_event = model_run_events[-1]
        extra: dict = getattr(latest_event, "extra", {}) or {}
        payload: dict = extra.get("payload", {})
        if "data" not in payload:
            raise ValueError(
                f"Expected 'payload.data' in asset event extra. "
                f"Got extra keys: {list(extra.keys())}, "
                f"payload keys: {list(payload.keys())}"
            )
        return payload["data"]

    @task.kubernetes(
        task_id="persist_model_run",
        image="thehorselessnewspaper/horseless-repotracker@sha256:3e7706e7709beb835664e613a72d3215c529013c16ab2b2f3c2dbefa4eef2bb0",
        name="k8s-env-task",
        env_vars=_VENV_ENV_VARS,
        image_pull_policy="IfNotPresent", 
        startup_timeout_seconds=600,
        get_logs=True,
        is_delete_operator_pod=False,
    )
    def persist_model_run(dto_json: str) -> None:
        """Deserialise a ModelRunDTO JSON string and persist the entity chain.

        Persists:
        * :class:`ModelRun` — the top-level run record with status
          ``"initialized"``, stamped with ``started_at = utcnow()``.
        * :class:`ModelRunParameter` — the user-supplied run parameters
          (repos, dates, keyword, etc.) linked 1:1 to the ``ModelRun``.
        * :class:`SpectralConfig` — the Nyquist/spectral sampling config
          linked 1:1 to the ``ModelRunParameter``.

        All three writes use the async ORM helpers (``ModelRunParameterORM``,
        ``SpectralConfigORM``) and run inside a single ``asyncio.run()`` call.
        The PostgreSQL connection URL is resolved via
        ``PersistenceSQLAlchemy.get_async_postgres_db_url()`` which reads
        ``PG_HOST / PG_PORT / PG_DBNAME / PG_USER / PG_PASSWORD`` from the
        Airflow worker environment.

        Args:
            dto_json: JSON string produced by ``ModelRunDTO.to_json()``.

        Returns:
            The ``model_run.id`` of the newly created (or updated) run.
        """
        import asyncio
        import json
        import os
        from datetime import datetime

        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

        from horseless_repotracker.repotracker.dto import ModelRunDTO
        from horseless_repotracker.repotracker.orm import (
            ModelRunORM,
            ModelRunParameterORM,
            SpectralConfigORM,
        )
        from horseless_repotracker.repotracker.persistence_sqlalchemy import PersistenceSQLAlchemy
        from horseless_repotracker.repotracker.sqlalchemy_model import (
            ModelRun,
            ModelRunParameter,
            SpectralConfig,
        )

        dto = ModelRunDTO.from_json(dto_json)

        async def _persist() -> int:
            url = PersistenceSQLAlchemy.get_async_postgres_db_url()
            engine = create_async_engine(url, echo=False)
            sf = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

            try:
                # ----------------------------------------------------------
                # 1. ModelRun — create via ModelRunORM helper
                # ----------------------------------------------------------
                model_run_orm = ModelRunORM(sf)

                # Map DTO fields into ModelRun where names match the table columns.
                mr_table = ModelRun.__table__
                model_run_kwargs = {}
                for col in mr_table.columns:
                    if col.name == "xmin":
                        continue
                    if col.name == "started_at":
                        model_run_kwargs["started_at"] = datetime.utcnow()
                        continue
                    # Only copy values where the DTO exposes the same attribute name.
                    if hasattr(dto, col.name):
                        model_run_kwargs[col.name] = getattr(dto, col.name)

                # Ensure the parameters JSON contains the canonical form-data keys.
                model_run_kwargs.setdefault("parameters", {
                    "repos": dto.repos,
                    "start_date": dto.start_date,
                    "end_date": dto.end_date,
                    "keyword": dto.keyword,
                })
                model_run_kwargs.setdefault("status", "initialized")
                model_run_kwargs.setdefault("notes", f"Model run for {dto.model_name}")

                model_run = ModelRun(**model_run_kwargs)
                model_run_id: int = await model_run_orm.upsert(model_run)

                # ----------------------------------------------------------
                # 2. ModelRunParameter
                # ----------------------------------------------------------
                param_orm = ModelRunParameterORM(sf)
                param = ModelRunParameter(
                    model_run_id=model_run_id,
                    repos=dto.repos,
                    start_date=dto.start_date,
                    end_date=dto.end_date,
                    model_name=dto.model_name,
                    token=dto.token,
                    keyword=dto.keyword,
                    output_dir=dto.output_dir,
                    reset_if_exists=dto.reset_if_exists,
                )
                param_id: int = await param_orm.upsert(param)

                # ----------------------------------------------------------
                # 3. SpectralConfig
                # ----------------------------------------------------------
                sc_orm = SpectralConfigORM(sf)
                sc_cfg = dto.spectral_config

                # Build SpectralConfig from DTO by mapping matching field names.
                sc_table = SpectralConfig.__table__
                spectral_kwargs = {"model_run_parameter_id": param_id}
                for col in sc_table.columns:
                    if col.name in ("id", "xmin", "model_run_parameter_id"):
                        continue
                    if hasattr(sc_cfg, col.name):
                        spectral_kwargs[col.name] = getattr(sc_cfg, col.name)

                spectral = SpectralConfig(**spectral_kwargs)
                await sc_orm.upsert(spectral)

                return model_run_id

            finally:
                await engine.dispose()

        model_run_id = asyncio.run(_persist())
        print(f"[persist_model_run] upserted model_run_id={model_run_id}")
        
        # Write to XCom for Kubernetes pod-to-pod communication
        os.makedirs('/airflow/xcom', exist_ok=True)
        with open('/airflow/xcom/return.json', 'w') as f:
            json.dump(model_run_id, f)
        
        return model_run_id

    @task.kubernetes(
        task_id="ingest_repositories",
        image="thehorselessnewspaper/horseless-repotracker@sha256:3e7706e7709beb835664e613a72d3215c529013c16ab2b2f3c2dbefa4eef2bb0",
        name="k8s-env-task",
        env_vars=_VENV_ENV_VARS,
        image_pull_policy="IfNotPresent", 
        startup_timeout_seconds=600,
        get_logs=True,
        is_delete_operator_pod=False,
    )
    def ingest_repositories(dto_json: str) -> list:
        """Stream repositories from the ModelRunParameter and ingest issues.

        For each ``owner/repo`` string in :attr:`ModelRunParameter.repos`:

        1. Fetches authoritative repository metadata from the GitHub REST API
           (``GET /repos/{owner}/{repo}``) and upserts a :class:`Repository`
           row via :class:`RepositoryORM`.
        2. Streams :class:`IssueFetchResult` objects from
           :class:`IssueIngestor` and persists each ``User``, ``Label``,
           and ``Issue`` row via PostgreSQL ``ON CONFLICT DO UPDATE``
           upserts.

        Args:
            model_run_id: The ``model_run.id`` returned by ``persist_model_run``.

        Returns:
            List of fully-qualified repository names that were successfully
            ingested (``["owner/repo", ...]``).
        """
        import asyncio
        import json
        import logging
        import os
        from datetime import datetime

        import aiohttp
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

        from horseless_repotracker.repotracker.dto import ModelRunDTO
        from horseless_repotracker.repotracker.github_api import GitHubAPI
        from horseless_repotracker.repotracker.ingestion import IssueIngestor
        from horseless_repotracker.repotracker.orm import ModelRunORM, ModelRunParameterORM, RepositoryORM
        from horseless_repotracker.repotracker.persistence_sqlalchemy import PersistenceSQLAlchemy
        from horseless_repotracker.repotracker.sqlalchemy_model import Issue, Label, Repository, User, ModelRun
        logger = logging.getLogger(__name__)

        async def _stream_repositories(repos, token, sf, model_run_id):
            """Yield a persisted :class:`Repository` ORM for each owner/repo string."""
            repo_orm_helper = RepositoryORM(sf)
            api = GitHubAPI(token=token)
            async with aiohttp.ClientSession() as session:
                for repo_full_name in repos:
                    owner, repo_name = repo_full_name.strip().split("/", 1)
                    details = await api.get_repository_details(session, owner, repo_name)
                    github_id = details.get("id")
                    if github_id is None:
                        raise ValueError(
                            f"GitHub API returned no numeric id for {repo_full_name!r}"
                        )
                    repo = Repository(
                        github_id=github_id,
                        model_run_id=model_run_id,
                        name=details.get("name", repo_name),
                        full_name=details.get("full_name", repo_full_name),
                        html_url=details.get("html_url"),
                        url=details.get("url"),
                        description=details.get("description"),
                        private=details.get("private"),
                        fork=details.get("fork"),
                        default_branch=details.get("default_branch"),
                        stargazers_count=details.get("stargazers_count"),
                        open_issues_count=details.get("open_issues_count"),
                        owner_type=details.get("owner", {}).get("type"),
                        owner_id=details.get("owner", {}).get("id"),
                    )
                    await repo_orm_helper.upsert(repo)
                    logger.info("Upserted repository %s (github_id=%s)", repo_full_name, github_id)
                    yield repo

        async def _ingest() -> list:
            url = PersistenceSQLAlchemy.get_async_postgres_db_url()
            engine = create_async_engine(url, echo=False)
            sf = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

            try:
                # Derive model_run_id by upserting/read ModelRun from the DTO.
                dto = ModelRunDTO.from_json(dto_json)
                # Upsert ModelRun minimally to obtain id (idempotent).
                mr_table = ModelRun.__table__
                model_run_kwargs = {}
                for col in mr_table.columns:
                    if col.name == "xmin":
                        continue
                    if col.name == "started_at":
                        model_run_kwargs["started_at"] = datetime.utcnow()
                        continue
                    if hasattr(dto, col.name):
                        model_run_kwargs[col.name] = getattr(dto, col.name)

                model_run = ModelRun(**model_run_kwargs)
                model_run_id: int = await ModelRunORM(sf).upsert(model_run)

                param_orm = ModelRunParameterORM(sf)
                params = await param_orm.get_by_model_run(model_run_id)
                # If parameters missing (persist_model_run may not have executed),
                # fall back to DTO fields.
                if params is None:
                    token: str = dto.token or os.environ.get("GITHUB_TOKEN", "")
                    repos: list = dto.repos if isinstance(dto.repos, list) else list(dto.repos)
                else:
                    token: str = params.token or os.environ.get("GITHUB_TOKEN", "")
                    repos: list = params.repos if isinstance(params.repos, list) else list(params.repos)

                ingestor = IssueIngestor(github_token=token)
                ingested: list = []

                async for repository in _stream_repositories(repos, token, sf, model_run_id):
                    ingested.append(repository)

                return ingested

            finally:
                await engine.dispose()

        repositories = asyncio.run(_ingest())
        print(f"[ingest_repositories] ingested {len(repositories)} repositories")
        
        # Write to XCom for Kubernetes pod-to-pod communication
        repo_list = [repo.full_name for repo in repositories]
        os.makedirs('/airflow/xcom', exist_ok=True)
        with open('/airflow/xcom/return.json', 'w') as f:
            json.dump(repo_list, f)
        
        return repo_list

    @task.kubernetes(
        task_id="ingest_issues",
        image="thehorselessnewspaper/horseless-repotracker@sha256:3e7706e7709beb835664e613a72d3215c529013c16ab2b2f3c2dbefa4eef2bb0",
        name="k8s-env-task",
        env_vars=_VENV_ENV_VARS,
        image_pull_policy="IfNotPresent",
        startup_timeout_seconds=600,
        get_logs=True,
        is_delete_operator_pod=False,
    )
    def ingest_issues(dto_json: str, repositories: list) -> None:
        """Ingest issues for the repositories ingested by the previous task.

        For each repository ingested by ``ingest_repositories``, streams issues
        through :class:`IssueIngestor` and persists them to PostgreSQL.

        Args:
            dto_json: JSON-serialized ModelRunDTO containing token and date range.
            repositories: List of repository full_name strings (e.g., ["owner/repo1", "owner/repo2"]).
        """
        import asyncio
        import json
        import logging
        import os
        from datetime import datetime

        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

        from horseless_repotracker.repotracker.dto import ModelRunDTO
        from horseless_repotracker.repotracker.ingestion import IssueIngestor
        from horseless_repotracker.repotracker.orm import (
            IssueORM,
            LabelORM,
            ModelRunORM,
            RepositoryORM,
            UserORM,
        )
        from horseless_repotracker.repotracker.persistence_sqlalchemy import PersistenceSQLAlchemy
        from horseless_repotracker.repotracker.sqlalchemy_model import Issue, Label, ModelRun, Repository, User

        logger = logging.getLogger(__name__)

        async def _ingest_issues():
            # Deserialize DTO to get token, model_run_id, and date range
            dto = ModelRunDTO.from_json(dto_json)
            token = dto.token or os.environ.get("GITHUB_TOKEN", "")
            start_date = dto.start_date
            end_date = dto.end_date
            keyword = dto.keyword

            # Set up database connection
            url = PersistenceSQLAlchemy.get_async_postgres_db_url()
            engine = create_async_engine(url, echo=False)
            sf = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

            try:
                # Derive model_run_id from DTO (may need to upsert ModelRun)
                mr_table = ModelRun.__table__
                model_run_kwargs = {}
                for col in mr_table.columns:
                    if col.name == "xmin":
                        continue
                    if col.name == "started_at":
                        model_run_kwargs["started_at"] = datetime.utcnow()
                        continue
                    if hasattr(dto, col.name):
                        model_run_kwargs[col.name] = getattr(dto, col.name)
                model_run = ModelRun(**model_run_kwargs)
                model_run_id = await ModelRunORM(sf).upsert(model_run)

                # Set up ORM helpers
                repo_orm = RepositoryORM(sf)
                issue_orm = IssueORM(sf)
                user_orm = UserORM(sf)
                label_orm = LabelORM(sf)

                # Create IssueIngestor
                ingestor = IssueIngestor(github_token=token)

                # Process each repository
                for repo_full_name in repositories:  # repositories is a list of strings
                    logger.info("Starting issue ingestion for repository: %s", repo_full_name)

                    # Look up the Repository object from the database
                    repository = await repo_orm.get_by_full_name(repo_full_name, model_run_id)
                    if repository is None:
                        logger.warning(
                            "Repository %s not found in database for model_run_id=%d, skipping",
                            repo_full_name,
                            model_run_id,
                        )
                        continue

                    # Stream issues using IssueIngestor
                    issue_count = 0
                    async for issue_result in ingestor.stream(
                        repository=repository,
                        model_run_id=model_run_id,
                        start_date=start_date,
                        end_date=end_date,
                        keyword=keyword,
                    ):
                        # Persist users first (foreign key dependency)
                        for user in issue_result.users:
                            await user_orm.upsert(user)

                        # Persist labels
                        for label in issue_result.labels:
                            await label_orm.upsert(label)

                        # Persist the issue
                        await issue_orm.upsert(issue_result.issue)

                        issue_count += 1
                        if issue_count % 100 == 0:
                            logger.info("Ingested %d issues from %s", issue_count, repo_full_name)

                    logger.info(
                        "Completed issue ingestion for %s: %d issues total",
                        repo_full_name,
                        issue_count,
                    )

            finally:
                await engine.dispose()

        asyncio.run(_ingest_issues())
        print("[ingest_issues] completed issue ingestion")
        return None     
    
    
    @task.kubernetes(
        task_id="refresh_materialized_views",
        image="thehorselessnewspaper/horseless-repotracker@sha256:3e7706e7709beb835664e613a72d3215c529013c16ab2b2f3c2dbefa4eef2bb0",
        name="k8s-env-task",
        env_vars=_VENV_ENV_VARS,
        image_pull_policy="IfNotPresent",
        startup_timeout_seconds=600,
        get_logs=True,
        is_delete_operator_pod=False,
    )
    def refresh_materialized_views(dto_json: str) -> None:
        """REFRESH the three ingestion-side materialised views.

        Runs after ingestion completes so that B1/B2/B3 reflect the newly
        persisted issues, issue-timeline events, labels, and comments.
        ``CONCURRENTLY`` is used so that existing consumers can read from the
        views without an exclusive lock; this requires that each view already
        have a unique index (created by the schema reset handler).

        Views refreshed
        ---------------
        * ``mv_event_counts_by_issue_bucket`` (B1)
        * ``mv_user_repo_activity``           (B2)
        * ``mv_issue_label_incidence``         (B3)

        Note: ``mv_analysis_ready`` is NOT refreshed here because the
        per-stage artifact tables are still empty at this point.  It is
        refreshed at the end of the enrichment handler DAG.

        Args:
            model_run_id: Forwarded from ``ingest_repositories``; passed
                through unchanged so the downstream task can use it.

        Returns:
            The same ``model_run_id`` for XCom forwarding.
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
        conn.autocommit = True   # REFRESH CONCURRENTLY cannot run in a transaction
        try:
            with conn.cursor() as cur:
                for view in (
                    "mv_event_counts_by_issue_bucket",
                    "mv_user_repo_activity",
                    "mv_issue_label_incidence",
                ):
                    logger.info("Refreshing materialised view: %s", view)
                    cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view};")
                    logger.info("Refreshed: %s", view)
        finally:
            conn.close()

        print("[refresh_materialized_views] refreshed ingestion views")
        return None

    @task.kubernetes(
        task_id="publish_enrichment_trigger",
        image="thehorselessnewspaper/horseless-repotracker@sha256:3e7706e7709beb835664e613a72d3215c529013c16ab2b2f3c2dbefa4eef2bb0",
        name="k8s-env-task",
        env_vars=build_venv_env_vars(include_redis=True),
        image_pull_policy="IfNotPresent",
        startup_timeout_seconds=600,
        get_logs=True,
        is_delete_operator_pod=False,
    )
    def publish_enrichment_trigger(dto_json: str) -> None:
        """Publish ``model_run_id`` to the enrichment trigger channel.

        Fires after B1/B2/B3 materialised views have been refreshed, so the
        enrichment handler DAG starts with consistent matrix inputs.

        The :class:`RedisTransport` reads the channel name from the
        ``REDIS_PUBSUB_ENRICHMENT_CHANNEL`` environment variable
        (default ``"modelrun_enriched"``).  The Airflow Variable of the same
        name must be set to the same value as the enrichment handler DAG's
        ``MessageQueueTrigger`` subscription.

        Args:
            model_run_id: The id of the completed model run.

        Returns:
            The number of Redis Pub/Sub subscribers that received the message.
        """
        import asyncio
        import logging
        from datetime import datetime

        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

        from horseless_repotracker.repotracker.dto import ModelRunDTO
        from horseless_repotracker.repotracker.orm import ModelRunORM
        from horseless_repotracker.repotracker.persistence_sqlalchemy import PersistenceSQLAlchemy
        from horseless_repotracker.repotracker.redistransport import RedisTransport
        from horseless_repotracker.repotracker.sqlalchemy_model import ModelRun
        
        logger = logging.getLogger(__name__)
        dto = ModelRunDTO.from_json(dto_json)
        model_run_id = dto.model_run_id
        # If DTO has no explicit model_run_id, attempt to upsert/read it.
        if not model_run_id:
            # Minimal upsert to obtain id
            url = PersistenceSQLAlchemy.get_async_postgres_db_url()
            engine = create_async_engine(url, echo=False)
            sf = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
            async def _get_id():
                mr_table = ModelRun.__table__
                model_run_kwargs = {}
                for col in mr_table.columns:
                    if col.name == "xmin":
                        continue
                    if col.name == "started_at":
                        model_run_kwargs["started_at"] = datetime.utcnow()
                        continue
                    if hasattr(dto, col.name):
                        model_run_kwargs[col.name] = getattr(dto, col.name)
                model_run = ModelRun(**model_run_kwargs)
                return await ModelRunORM(sf).upsert(model_run)
            try:
                model_run_id = asyncio.run(_get_id())
            finally:
                asyncio.run(engine.dispose())

        transport = RedisTransport()
        count = transport.publish_enrichment_trigger(model_run_id)
        logger.info(
            "Published enrichment trigger model_run_id=%d to %d subscriber(s).",
            model_run_id,
            count,
        )
        print(f"[publish_enrichment_trigger] published model_run_id={model_run_id} subscriber_count={count}")
        
        count = transport.publish_gpu_enrichment_trigger(model_run_id)
        logger.info(
            "Published GPU enrichment trigger model_run_id=%d to %d subscriber(s).",
            model_run_id,
            count,
        )
        print(f"[publish_gpu_enrichment_trigger] published model_run_id={model_run_id} subscriber_count={count}")
        return None

    # -----------------------------------------------------------------------
    # Task chain
    # -----------------------------------------------------------------------
    dto_json = extract_dto_json()
    # Fan-out dto_json to Kubernetes tasks. Persist/ingest/refresh/publish
    # do not rely on K8s-to-K8s XCom return values — they are discarded.
    model_run_id = persist_model_run(dto_json)
    repositories = ingest_repositories(dto_json)
    issues = ingest_issues(dto_json, repositories)
    refreshed = refresh_materialized_views(dto_json)
    published = publish_enrichment_trigger(dto_json)

    # Enforce ordering explicitly via task edges
    model_run_id >> repositories >> issues >> refreshed >> published


github_ingester()

from __future__ import annotations

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, Variable, dag, task
import os

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

# Image configuration: allow overriding registry (e.g. microk8s private registry)
REPO_IMAGE_NAME = "horseless-repotracker"
REPO_IMAGE_SHA = "sha256:bb90d28f943d91256b87c353d3bc5d94ab080e4719851ff02d1cc8e28072bf45"
MICROK8S_REGISTRY = os.getenv("MICROK8S_REGISTRY", "docker-registry.dubridge.ataxlab.com")
REPO_IMAGE = f"{MICROK8S_REGISTRY}/{REPO_IMAGE_NAME}@{REPO_IMAGE_SHA}"


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
        image="localhost:32000/horseless-repotracker:latest",
        name="k8s-env-task",
        env_vars=_VENV_ENV_VARS,
        image_pull_policy="Always", 
        startup_timeout_seconds=600,
        get_logs=True,
        is_delete_operator_pod=False,
        do_xcom_push=True,
    )
    def persist_model_run(dto_json: str) -> int:
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
        
        # @task.kubernetes automatically handles XCom push via TaskFlow API
        # No manual XCom write needed - the return value is automatically pushed
        return model_run_id

    @task.kubernetes(
        task_id="ingest_repositories",
        image="localhost:32000/horseless-repotracker:latest",
        name="k8s-env-task",
        env_vars=_VENV_ENV_VARS,
        image_pull_policy="IfNotPresent", 
        startup_timeout_seconds=600,
        get_logs=True,
        is_delete_operator_pod=False,
        do_xcom_push=True,
    )
    def ingest_repositories(model_run_id: int) -> None:
        """Stream repositories from the ModelRunParameter and ingest issues.
        
        Args:
            model_run_id: The model_run.id returned by persist_model_run via XCom.

        For each ``owner/repo`` string in :attr:`ModelRunParameter.repos`:

        1. Fetches authoritative repository metadata from the GitHub REST API
           (``GET /repos/{owner}/{repo}``) and upserts a :class:`Repository`
           row via :class:`RepositoryORM`.
        2. Streams :class:`IssueFetchResult` objects from
           :class:`IssueIngestor` and persists each ``User``, ``Label``,
           and ``Issue`` row via PostgreSQL ``ON CONFLICT DO UPDATE``
           upserts.

        Returns:
            None. Repositories are persisted to the database; downstream tasks
            should query via ``get_repositories_for_model_run(model_run_id)``.
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
        
        print(f"[ingest_repositories] received model_run_id={model_run_id} via XCom")

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
                # Read the persisted ModelRunParameter to obtain token/repos.
                param_orm = ModelRunParameterORM(sf)
                params = await param_orm.get_by_model_run(model_run_id)
                if params is None:
                    # If parameters missing, log and skip ingestion
                    print(f"[ingest_repositories] No ModelRunParameter found for model_run_id={model_run_id}; skipping")
                    return []
                token: str = params.token or os.environ.get("GITHUB_TOKEN", "")
                repos: list = params.repos if isinstance(params.repos, list) else list(params.repos)

                print(f"[ingest_repositories] repos={repos}, token={'<set>' if token else '<empty>'}")
                ingestor = IssueIngestor(github_token=token)
                ingested: list = []

                async for repository in _stream_repositories(repos, token, sf, model_run_id):
                    ingested.append(repository)
                    print(f"[ingest_repositories] streamed repository: {repository.full_name}")

                print(f"[ingest_repositories] total ingested: {len(ingested)} repositories")
                return ingested

            finally:
                await engine.dispose()

        repositories = asyncio.run(_ingest())
        print(f"[ingest_repositories] ingested {len(repositories)} repositories")
        # Repositories are persisted to the DB by the upsert logic above.
        # Do not rely on fragile K8s-to-K8s XCom propagation — downstream
        # tasks should read the repository list from the DB via the
        # repotracker.messaging helper.
        return None

    @task.kubernetes(
        task_id="ingest_repository_owners",
        image="localhost:32000/horseless-repotracker:latest",
        name="k8s-env-task",
        env_vars=_VENV_ENV_VARS,
        image_pull_policy="IfNotPresent",
        startup_timeout_seconds=600,
        get_logs=True,
        is_delete_operator_pod=False,
    )
    def ingest_repository_owners(model_run_id: int) -> None:
        """Fetch and persist repository owner profiles (User or Organization).

        Runs after `ingest_repositories`. Reads the authoritative repository
        list from the DB via `repotracker.messaging.get_repositories_for_model_run`
        and for each repository calls the RepositoryOwnerIngestor to fetch and
        persist the owner row.
        
        Args:
            model_run_id: The model_run.id received via XCom.
        """
        import asyncio
        import logging
        import os

        import aiohttp
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

        from horseless_repotracker.repotracker.dto import ModelRunDTO
        from horseless_repotracker.repotracker.orm import (
            UserORM,
            OrganizationORM,
            ModelRunParameterORM,
        )
        from horseless_repotracker.repotracker.ingestion import RepositoryOwnerIngestor
        from horseless_repotracker.repotracker.messaging import get_repositories_for_model_run
        from horseless_repotracker.repotracker.persistence_sqlalchemy import PersistenceSQLAlchemy
        from horseless_repotracker.repotracker.sqlalchemy_model import Repository

        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        print(f"[ingest_repository_owners] received model_run_id={model_run_id} via XCom")

        async def _ingest_owners():
            # DB async engine for ORM upserts
            url = PersistenceSQLAlchemy.get_async_postgres_db_url()
            engine = create_async_engine(url, echo=False)
            sf = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

            try:
                user_orm = UserORM(sf)
                org_orm = OrganizationORM(sf)

                # Read token from persisted parameters
                param_orm = ModelRunParameterORM(sf)
                params = await param_orm.get_by_model_run(model_run_id)
                if params is None:
                    print(f"[ingest_repository_owners] No ModelRunParameter for model_run_id={model_run_id}; skipping")
                    return
                token = params.token or os.environ.get("GITHUB_TOKEN", "")
                print(f"[ingest_repository_owners] token={'<set>' if token else '<empty>'}")
                ingestor = RepositoryOwnerIngestor(token=token)

                repos = get_repositories_for_model_run(model_run_id)
                print(f"[ingest_repository_owners] found {len(repos) if repos else 0} repositories for model_run_id={model_run_id}")
                if not repos:
                    logger.info("No repositories found for model_run_id=%s; skipping owner ingestion", model_run_id)
                    return

                owner_count = 0
                async with aiohttp.ClientSession() as session:
                    for repo_full in repos:
                        # Try to look up repository row to ensure we have coordinates
                        # We rely on Repository.full_name (owner/repo) to derive owner login
                        try:
                            result = await ingestor.ingest(session, Repository(full_name=repo_full), model_run_id, user_orm, org_orm)
                            if result:
                                owner_count += 1
                                print(f"[ingest_repository_owners] ingested owner for {repo_full}: {result.login if hasattr(result, 'login') else result}")
                        except Exception as e:
                            logger.exception("Failed to ingest owner for repository %s", repo_full)
                            print(f"[ingest_repository_owners] ERROR ingesting owner for {repo_full}: {e}")

                print(f"[ingest_repository_owners] ingested {owner_count} owners")
            finally:
                await engine.dispose()

        asyncio.run(_ingest_owners())
        print("[ingest_repository_owners] completed owner ingestion")
        return None

    @task.kubernetes(
        task_id="ingest_issues",
        image="localhost:32000/horseless-repotracker:latest",
        name="k8s-env-task",
        env_vars=_VENV_ENV_VARS,
        image_pull_policy="IfNotPresent",
        startup_timeout_seconds=600,
        get_logs=True,
        is_delete_operator_pod=False,
    )
    def ingest_issues(model_run_id: int) -> None:
        """Ingest issues for the repositories ingested by the previous task.

        For each repository ingested by ``ingest_repositories``, streams issues
        through :class:`IssueIngestor` and persists them to PostgreSQL.

        Args:
            model_run_id: The model_run.id received via XCom.
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
        # Use direct package exports from the installed horseless_repotracker
        # distribution. Do not perform any dynamic resolution here; allow
        # ImportError to propagate if the runtime package layout is wrong.
        from horseless_repotracker.repotracker.orm import (
            IssueORM,
            LabelORM,
            ModelRunORM,
            RepositoryORM,
            ModelRunParameterORM,
        )
        from horseless_repotracker.repotracker.messaging import (
            get_repositories_for_model_run,
        )

        from horseless_repotracker.repotracker.persistence_sqlalchemy import PersistenceSQLAlchemy
        from horseless_repotracker.repotracker.sqlalchemy_model import Issue, Label, ModelRun, Repository, User

        logger = logging.getLogger(__name__)
        
        print(f"[ingest_issues] received model_run_id={model_run_id} via XCom")

        async def _ingest_issues():
            # Read token and date range from persisted parameters
            url = PersistenceSQLAlchemy.get_async_postgres_db_url()
            engine = create_async_engine(url, echo=False)
            sf = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

            # Load params
            param_orm = ModelRunParameterORM(sf)
            params = await param_orm.get_by_model_run(model_run_id)
            if params is None:
                print(f"[ingest_issues] No ModelRunParameter found for model_run_id={model_run_id}; skipping")
                await engine.dispose()
                return
            token = params.token or os.environ.get("GITHUB_TOKEN", "")
            start_date = params.start_date
            end_date = params.end_date
            keyword = params.keyword

            try:
                # Set up ORM helpers
                repo_orm = RepositoryORM(sf)
                issue_orm = IssueORM(sf)
                label_orm = LabelORM(sf)

                # Create IssueIngestor
                ingestor = IssueIngestor(github_token=token)

                # Read the authoritative repository list from the DB.
                repositories = get_repositories_for_model_run(model_run_id)

                # Process each repository
                for repo_full_name in repositories:
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
                        # Persist all entities for this issue in a SINGLE transaction
                        # to ensure atomicity and proper FK constraint ordering
                        async with sf() as session:
                            async with session.begin():
                                # 1. Persist users first (foreign key dependency)
                                for user in issue_result.users:
                                    table = User.__table__
                                    # Only include non-None values to avoid constraint violations
                                    values = {col.name: val for col in table.columns if (val := getattr(user, col.name, None)) is not None}
                                    stmt = pg_insert(table).values(**values)
                                    # Only update columns that have non-None values in the new data
                                    update_cols = {c.name: stmt.excluded[c.name] for c in table.columns 
                                                   if c.name not in ("github_id", "model_run_id") 
                                                   and getattr(user, c.name, None) is not None}
                                    stmt = stmt.on_conflict_do_update(index_elements=["github_id", "model_run_id"], set_=update_cols)
                                    await session.execute(stmt)

                                # 2. Persist labels (using ORM with existing session)
                                for label in issue_result.labels:
                                    table = Label.__table__
                                    values = {col.name: getattr(label, col.name) for col in table.columns}
                                    stmt = pg_insert(table).values(**values)
                                    # Labels use (id, model_run_id) as the composite key
                                    update_cols = {c.name: stmt.excluded[c.name] for c in table.columns 
                                                   if c.name not in ("id", "model_run_id")}
                                    stmt = stmt.on_conflict_do_update(index_elements=["id", "model_run_id"], set_=update_cols)
                                    await session.execute(stmt)

                                # 3. Persist the issue (using ORM with existing session)
                                issue_table = Issue.__table__
                                issue_values = {col.name: getattr(issue_result.issue, col.name) for col in issue_table.columns}
                                issue_stmt = pg_insert(issue_table).values(**issue_values)
                                # Issue composite PK is (id, model_run_id)
                                issue_update_cols = {c.name: issue_stmt.excluded[c.name] for c in issue_table.columns 
                                                     if c.name not in ("id", "model_run_id")}
                                issue_stmt = issue_stmt.on_conflict_do_update(
                                    index_elements=["id", "model_run_id"], 
                                    set_=issue_update_cols
                                )
                                await session.execute(issue_stmt)
                                # Transaction commits when context exits

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
        image="localhost:32000/horseless-repotracker:latest",
        name="k8s-env-task",
        env_vars=_VENV_ENV_VARS,
        image_pull_policy="IfNotPresent",
        startup_timeout_seconds=600,
        get_logs=True,
        is_delete_operator_pod=False,
    )
    def refresh_materialized_views(model_run_id: int) -> None:
        """REFRESH the three ingestion-side materialised views.

        Runs after ingestion completes so that B1/B2/B3 reflect the newly
        persisted issues, issue-timeline events, labels, and comments.
        ``CONCURRENTLY`` is used so that existing consumers can read from the
        views without an exclusive lock; this requires that each view already
        have a unique index (created by the schema reset handler).

        Views refreshed
        ---------------
        * ``mv_issues_enrichment_input``      (A1)
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
                    "mv_issues_enrichment_input",
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
        image="localhost:32000/horseless-repotracker:latest",
        name="k8s-env-task",
        env_vars=build_venv_env_vars(include_redis=True),
        image_pull_policy="IfNotPresent",
        startup_timeout_seconds=600,
        get_logs=True,
        is_delete_operator_pod=False,
    )
    def publish_enrichment_trigger(model_run_id: int) -> None:
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
        import os
        from datetime import datetime

        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

        from horseless_repotracker.repotracker.dto import ModelRunDTO
        from horseless_repotracker.repotracker.orm import ModelRunORM
        from horseless_repotracker.repotracker.persistence_sqlalchemy import PersistenceSQLAlchemy
        from horseless_repotracker.repotracker.redistransport import RedisTransport
        from horseless_repotracker.repotracker.sqlalchemy_model import ModelRun
        
        logger = logging.getLogger(__name__)
        
        print(f"[publish_enrichment_trigger] received model_run_id={model_run_id} via XCom")
        
        if not model_run_id:
            # Minimal upsert to obtain id. Create the async engine inside
            # the coroutine so creation and disposal happen on the same
            # event loop. This avoids attaching Futures to a different loop
            # when disposing the engine from a separate ``asyncio.run`` call.
            async def _get_id():
                url = PersistenceSQLAlchemy.get_async_postgres_db_url()
                engine = create_async_engine(url, echo=False)
                sf = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
                try:
                    mr_table = ModelRun.__table__
                    model_run_kwargs = {}
                    # No DTO present; perform no-op and return None
                    return None
                finally:
                    await engine.dispose()

            model_run_id = asyncio.run(_get_id())

        import os
        
        transport = RedisTransport()
        count = transport.publish_enrichment_trigger(model_run_id)
        logger.info(
            "Published enrichment trigger model_run_id=%d to %d subscriber(s).",
            model_run_id,
            count,
        )
        print(f"[publish_enrichment_trigger] published model_run_id={model_run_id} subscriber_count={count}")
        
        # Get GPU enrichment channel from environment
        gpu_channel = os.getenv("REDIS_PUBSUB_GPU_ENRICHMENT_CHANNEL", "modelrun_enriched_gpu")
        # Build a full ModelRunDTO from persisted parameters for GPU consumers.
        async def _build_dto():
            url = PersistenceSQLAlchemy.get_async_postgres_db_url()
            engine = create_async_engine(url, echo=False)
            sf = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
            try:
                from horseless_repotracker.repotracker.orm import ModelRunParameterORM
                from horseless_repotracker.repotracker.sqlalchemy_model import SpectralConfig as SpectralConfigModel
                from sqlalchemy import select
                from horseless_repotracker.repotracker.dto import SpectralConfigDTO, ModelRunDTO as MRDTO

                param_orm = ModelRunParameterORM(sf)
                params = await param_orm.get_by_model_run(model_run_id)
                if params is None:
                    return None

                # Attempt to load spectral config if present
                sc_row = None
                async with sf() as session:
                    res = await session.execute(select(SpectralConfigModel).where(SpectralConfigModel.model_run_parameter_id == params.id))
                    sc_row = res.scalar_one_or_none()

                sc_dto = None
                if sc_row is not None:
                    sc_dict = {c.name: getattr(sc_row, c.name) for c in SpectralConfigModel.__table__.columns if c.name not in ("id", "model_run_parameter_id")}
                    try:
                        sc_dto = SpectralConfigDTO.from_dict(sc_dict)
                    except Exception:
                        sc_dto = SpectralConfigDTO(**sc_dict)

                return MRDTO(
                    repos=params.repos,
                    start_date=params.start_date,
                    end_date=params.end_date,
                    model_name=params.model_name,
                    token=params.token,
                    keyword=params.keyword,
                    output_dir=params.output_dir,
                    reset_if_exists=params.reset_if_exists,
                    model_run_id=model_run_id,
                    spectral_config=sc_dto,
                )
            finally:
                await engine.dispose()

        dto = asyncio.run(_build_dto())
        if dto is None:
            logger.error("ModelRunParameter missing for model_run_id=%s — failing fast", model_run_id)
            raise RuntimeError(f"Missing ModelRunParameter for model_run_id={model_run_id}")

        count = transport.publish_gpu_enrichment_trigger(gpu_enrichment_channel=gpu_channel, model_run_dto=dto)
        logger.info(
            "Published GPU enrichment trigger model_run_id=%d to %d subscriber(s) on channel=%s.",
            model_run_id,
            count,
            gpu_channel,
        )
        print(f"[publish_gpu_enrichment_trigger] published model_run_id={model_run_id} subscriber_count={count} channel={gpu_channel}")
        return None

    # -----------------------------------------------------------------------
    # Task chain - TaskFlow API with automatic XCom handling
    # -----------------------------------------------------------------------
    dto_json = extract_dto_json()
    # persist_model_run returns int, which is automatically pushed to XCom
    # All downstream tasks receive model_run_id automatically via TaskFlow
    model_run_id = persist_model_run(dto_json)
    repositories = ingest_repositories(model_run_id)
    repository_owners = ingest_repository_owners(model_run_id)
    issues = ingest_issues(model_run_id)
    refreshed = refresh_materialized_views(model_run_id)
    published = publish_enrichment_trigger(model_run_id)

    # Enforce ordering explicitly via task edges
    # Ensure repository owners are ingested after repositories and before issues
    model_run_id >> repositories >> repository_owners >> issues >> refreshed >> published


# Instantiate the DAG object so it can be executed or tested from the CLI/IDE.
dag = github_ingester()

if __name__ == "__main__":
    # Run the DAG in-process for local debugging. This executes all tasks
    # serially in a single Python process and will fail-fast on errors.
    dag.test()

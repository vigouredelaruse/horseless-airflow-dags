from __future__ import annotations

from airflow.models.param import Param
from airflow.sdk import dag, task

from horseless_dag_env import DEFAULT_ARGS, VENV_REQUIREMENTS, VENV_PIP_OPTIONS, build_venv_env_vars


@dag(
    dag_id="modelrun_starter",
    default_args=DEFAULT_ARGS,
    description="Trigger a ModelRunDTO by publishing it to Redis Pub/Sub via RedisTransport.",
    schedule=None,
    params={
        "model_run": Param(
            {
                "repos": ["dotnet/aspire"],
                "start_date": "2026-01-01",
                "end_date": "2026-01-07",
                "model_name": "example-run",
                "token": None,
                "keyword": "",
                "output_dir": "example-run-outputs",
                "reset_if_exists": True,
                "model_run_id": None,
                "spectral_config": {
                    "base_sample_rate": "1h",
                    "window_size": 40,
                    "step_size": 5,
                    "target_samples": 5000,
                    "frame_budget": 499,
                    "playback_seconds": 30,
                    "min_interval": None,
                    "max_interval": None,
                    "smooth_window": None,
                },
            },
            type="object",
            description="ModelRunDTO payload (editable JSON).",
        )
    },
)
def modelrun_starter():
    """DAG that publishes a ModelRunDTO to the configured Redis channel.

    The UI will render `params.model_run` as an editable JSON object because
    we use `Param(..., type='object')`. The task below constructs a
    `ModelRunDTO` and calls `RedisTransport.publish_model_run_dto()`.
    """

    @task.kubernetes(
        task_id="publish_modelrun",
        image="thehorselessnewspaper/horseless-repotracker@sha256:ae758925e003993c1f665dbf1e8f00de7ec54dd99b0ed533a2f6d9bb5c7e2ad1",
        name="modelrun_starter",   
        get_logs=True,
        startup_timeout_seconds=300,
        is_delete_operator_pod=False,        
        image_pull_policy="IfNotPresent",
        env_vars=build_venv_env_vars(include_redis=True),
    )
    def publish_modelrun(**context) -> int:
        from horseless_repotracker.repotracker.dto import ModelRunDTO, SpectralConfigDTO
        from horseless_repotracker.repotracker.redistransport.redis_transport import RedisTransport

        params = context.get("params", {})
        model_run_payload = params.get("model_run", {})

        # Ensure spectral_config is a SpectralConfigDTO
        sc = model_run_payload.get("spectral_config")
        if isinstance(sc, dict):
            model_run_payload["spectral_config"] = SpectralConfigDTO.from_dict(sc)

        # Construct DTO (expects repos as list, etc.)
        dto = ModelRunDTO(
            repos=model_run_payload.get("repos", []),
            start_date=model_run_payload.get("start_date", ""),
            end_date=model_run_payload.get("end_date", ""),
            model_name=model_run_payload.get("model_name", ""),
            token=model_run_payload.get("token"),
            keyword=model_run_payload.get("keyword", ""),
            output_dir=model_run_payload.get("output_dir"),
            reset_if_exists=model_run_payload.get("reset_if_exists", True),
            model_run_id=model_run_payload.get("model_run_id"),
            spectral_config=model_run_payload.get("spectral_config"),
        )

        transport = RedisTransport()
        count = transport.publish_model_run_dto(dto)
        transport.close()
        return count

    publish_modelrun()


modelrun_starter()

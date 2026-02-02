"""Sentinel-2 L1C ingestion DAG using TaskFlow API."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task

# The task runs ingestion inside an isolated virtualenv; do not import
# project packages at DAG-parse time (they may not be installed on the
# scheduler/worker image). The virtualenv operator will install the
# required wheel listed in `requirements`.

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="sentinel2_l1c_ingestion",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@monthly",
    catchup=False,
    render_template_as_native_obj=True,
    params={
        # Editable DAG parameter; the UI exposes `ingest_defaults` as a
        # JSON-like object. Runs can override by passing a DagRun.conf
        # with key `ingest`.
        "ingest_defaults": {
            "endpoint": "https://earth-search.aws.element84.com/v1",
            "collections": ["sentinel-2-l1c"],
            "query": {"eo:cloud_cover": {"lt": 10}},
            "groupby": "solar_day",
            "bbox": [13.0, 45.0, 13.5, 45.5],
            "date_range": "2023-12-01/2023-12-31",
            "resolution": 10,
            "chunks": {"x": 2048, "y": 2048},
            "bands": [
                "B02",
                "B03",
                "B04",
                "B08",
                "B11",
                "B12",
                "B05",
                "B06",
                "B07",
                "B8A",
                "B01",
                "wvp",
                "B10",
            ],
        }
    },
)
def sentinel2_l1c_ingestion() -> None:
    """Define the Sentinel-2 L1C ingestion DAG using TaskFlow tasks."""

    @task.virtualenv(
        task_id="ingest_13_bands",
        requirements=[
            "pystac-client",
            "odc-stac",
            "horseless-atmospheric-correction==0.0.4",
        ],
        pip_install_options=[
            "--extra-index-url",
            "https://pkgs.dev.azure.com/wizardcontroller/MetOffice/_packaging/public/pypi/simple/",
        ],
        system_site_packages=False,
    )
    def ingest_13_bands(ingest: dict[str, Any]) -> str:
        """Run ingestion inside a task-scoped virtualenv."""
        if not isinstance(ingest, dict):
            ingest = {}

        # imports inside task to avoid DAG-parse failures
        from pystac_client import Client
        import odc.stac

        try:
            from horseless_atmospheric_correction.ingest.models.msg_ingest_sentinel import (
                IngestSentineMessage,
            )
        except Exception:
            raise ImportError(
                "Failed to import IngestSentineMessage from "
                "horseless_atmospheric_correction. Ensure the package is "
                "installed in the task virtualenv."
            ) from None

        msg: Any
        if IngestSentineMessage and isinstance(ingest, dict):
            msg = IngestSentineMessage(**ingest)
        else:
            class SimpleMsg:
                """Fallback message container for ingestion settings."""

                pass

            msg = SimpleMsg()
            for key, value in ingest.items():
                setattr(msg, key, value)

        catalog = Client.open(getattr(msg, "endpoint", None))
        search = catalog.search(
            collections=getattr(msg, "collections", None) or ["sentinel-2-l1c"],
            bbox=getattr(msg, "bbox", None),
            datetime=getattr(msg, "date_range", None),
            query=getattr(msg, "query", None),
        )
        ds = odc.stac.load(
            search.items(),
            bands=getattr(msg, "bands", None),
            bbox=getattr(msg, "bbox", None),
            resolution=getattr(msg, "resolution", None),
            groupby=getattr(msg, "groupby", None),
            chunks=getattr(msg, "chunks", None),
        )
        print(f"Successfully ingested {len(ds.data_vars)} spectral bands.")
        return "Ingestion Complete"

    ingest_13_bands(
        ingest="{{ dag_run.conf.get('ingest', params.ingest_defaults) }}",
    )


dag = sentinel2_l1c_ingestion()


def test_dag(execution_date: pendulum.DateTime) -> None:
    """Run the DAG in test mode using a compatibility wrapper.

    Different Airflow versions accept different keyword names for the
    `DAG.test()` helper. Try the common variants in order and fall back
    to the no-argument form.
    """
    try:
        # Preferred API in older code
        dag.test(execution_date=execution_date)
        return
    except TypeError:
        pass

    try:
        # Some Airflow versions use `start_date` as the keyword
        dag.test(start_date=execution_date)
        return
    except TypeError:
        pass

    # Final fallback: call without arguments (runtime will choose dates)
    dag.test()
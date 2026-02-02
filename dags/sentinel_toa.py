from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
import pendulum
from datetime import timedelta

# Use the packaged business logic wrapper which constructs the message
# from primitive arguments (safe to pass via Airflow `op_kwargs`).
from horseless_atmospheric_correction.ingest.aws_ingester import ingest_sentinel_wrapper

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sentinel2_l1c_ingestion',
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule='@monthly',
    catchup=False,
) as dag:

    # Primary task: run ingestion in an isolated virtualenv so task-specific
    # Python dependencies do not need to be installed on the worker image.
    
    # Alternative: use PythonVirtualenvOperator to install task-specific
    # Python packages in an isolated venv. This avoids building custom images
    # for every dependency set. Use when packages are pure-Python and have
    # no system-level binary dependencies.
    def _virtualenv_ingest(endpoint, bbox, date_range, collections=None, query=None, bands=None, resolution=10, groupby='solar_day', chunks=None):
        # imports inside function so they run inside the virtualenv
        from pystac_client import Client
        import odc.stac

        catalog = Client.open(endpoint)
        search = catalog.search(collections=collections or ["sentinel-2-l1c"], bbox=bbox, datetime=date_range, query=(query or {"eo:cloud_cover": {"lt": 10}}))
        ds = odc.stac.load(
            search.items(),
            bands=bands,
            bbox=bbox,
            resolution=resolution,
            groupby=groupby,
            chunks=chunks,
        )
        print(f"Successfully ingested {len(ds.data_vars)} spectral bands.")
        return "Ingestion Complete"

    venv_task = PythonVirtualenvOperator(
        task_id='ingest_13_bands',
        python_callable=_virtualenv_ingest,
        requirements=['pystac-client', 'odc-stac'],
        system_site_packages=False,
        op_kwargs={
            'endpoint': 'https://earth-search.aws.element84.com/v1',
            'bbox': [13.0, 45.0, 13.5, 45.5],
            'date_range': '2023-12-01/2023-12-31',
        },
    )


def test_dag(execution_date):
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
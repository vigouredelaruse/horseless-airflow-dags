from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
import pendulum
from datetime import timedelta

# The task runs ingestion inside an isolated virtualenv; do not import
# project packages at DAG-parse time (they may not be installed on the
# scheduler/worker image). The virtualenv operator will install the
# required wheel listed in `requirements`.

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
    params={
        # Editable DAG parameter; the UI exposes `ingest_defaults` as a
        # JSON-like object. Runs can override by passing a DagRun.conf
        # with key `ingest`.
        'ingest_defaults': {
            'endpoint': 'https://earth-search.aws.element84.com/v1',
            'collections': ['sentinel-2-l1c'],
            'query': {'eo:cloud_cover': {'lt': 10}},
            'groupby': 'solar_day',
            'bbox': [13.0, 45.0, 13.5, 45.5],
            'date_range': '2023-12-01/2023-12-31',
            'resolution': 10,
            'chunks': {'x': 2048, 'y': 2048},
            'bands': [
                'blue', 'green', 'red', 'nir', 'swir16', 'swir22',
                'rededge1', 'rededge2', 'rededge3', 'nir08',
                'coastal', 'water', 'cirrus',
            ],
        }
    },
) as dag:

    # Primary task: run ingestion in an isolated virtualenv so task-specific
    # Python dependencies do not need to be installed on the worker image.
    
    # Alternative: use PythonVirtualenvOperator to install task-specific
    # Python packages in an isolated venv. This avoids building custom images
    # for every dependency set. Use when packages are pure-Python and have
    # no system-level binary dependencies.
    def _virtualenv_ingest(ingest: dict):
        # imports inside function so they run inside the virtualenv
        from pystac_client import Client
        import odc.stac

        # Build the typed message inside the venv using the installed package
        try:
            from horseless_atmospheric_correction.ingest.models.msg_ingest_sentinel import IngestSentineMessage
        except Exception:
            # Fallback: accept dict-like ingest input as-is if package import fails
            IngestSentineMessage = None

        if IngestSentineMessage and isinstance(ingest, dict):
            msg = IngestSentineMessage(**ingest)
        else:
            # If the dataclass isn't available, treat `ingest` as a simple namespace
            class SimpleMsg:
                pass

            msg = SimpleMsg()
            for k, v in (ingest or {}).items():
                setattr(msg, k, v)

        catalog = Client.open(msg.endpoint)
        search = catalog.search(
            collections=getattr(msg, 'collections', None) or ['sentinel-2-l1c'],
            bbox=getattr(msg, 'bbox', None),
            datetime=getattr(msg, 'date_range', None),
            query=getattr(msg, 'query', None),
        )
        ds = odc.stac.load(
            search.items(),
            bands=getattr(msg, 'bands', None),
            bbox=getattr(msg, 'bbox', None),
            resolution=getattr(msg, 'resolution', None),
            groupby=getattr(msg, 'groupby', None),
            chunks=getattr(msg, 'chunks', None),
        )
        print(f"Successfully ingested {len(ds.data_vars)} spectral bands.")
        return "Ingestion Complete"

    venv_task = PythonVirtualenvOperator(
        task_id='ingest_13_bands',
        python_callable=_virtualenv_ingest,
        requirements=[
                'pystac-client',
                'odc-stac',
                'pendulum',  
                'horseless-atmospheric-correction==0.0.2'],
        index_urls=['https://pkgs.dev.azure.com/wizardcontroller/MetOffice/_apis/packaging/feeds/29c04fda-7517-4d3b-872e-1134a0ecf4da/pypi/simple/'],
        system_site_packages=False,
        # Pass a templated `ingest` dict; runs may override via DagRun.conf['ingest']
        op_kwargs={
            'ingest': "{{ dag_run.conf.get('ingest', params.ingest_defaults) }}",
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
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
from datetime import timedelta
from pystac_client import Client
import odc.stac

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

    ingest_task = PythonOperator(
        task_id='ingest_13_bands',
        python_callable=ingest_sentinel_wrapper,
        op_kwargs={
            'endpoint': 'https://earth-search.aws.element84.com/v1',
            'bbox': [13.0, 45.0, 13.5, 45.5],
            'date_range': '2023-12-01/2023-12-10', 
            'collections': ['sentinel-2-l1c'],
            'query': {'eo:cloud_cover': {'lt': 10}},
            'bands': [ "blue",
            "green",
            "red",
            "nir",
            "swir16",
            "swir22",
            "rededge1",
            "rededge2",
            "rededge3",
            "nir08",
            "coastal",
            "water",
            "cirrus"],
            'resolution': 10,
            'groupby': 'solar_day',
            'chunks': {'x': 2048, 'y': 2048},
        }
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
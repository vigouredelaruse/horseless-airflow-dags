from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pystac_client import Client
import odc.stac

def ingest_sentinel_l1c(message: IngestSentineMessage):
    # Connect to the Copernicus Data Space or AWS Earth Search STAC API
    catalog = Client.open("https://earth-search.aws.element84.com/v1")
    
    # Define search parameters
    bbox = [13.0, 45.0, 13.5, 45.5]  # Example: [min_lon, min_lat, max_lon, max_lat]
    date_range = "2023-12-01/2023-12-31"
    
    # Search for L1C products
    search = catalog.search(
        collections=["sentinel-2-l1c"],
        bbox=bbox,
        datetime=date_range,
        query={"eo:cloud_cover": {"lt": 10}}
    )
    
    # Load all 13 spectral bands
    # Bands: B01, B02, B03, B04, B05, B06, B07, B08, B8A, B09, B10, B11, B12
    bands = ["blue", "green", "red", "nir", "swir16", "swir22", "rededge1", "rededge2", "rededge3", "nir08", "coastal", "water", "cirrus"]
    
    ds = odc.stac.load(
        search.items(),
        bands=bands,
        bbox=bbox,
        resolution=10,  # Resamples all bands to 10m resolution
        groupby="solar_day",
        chunks={"x": 2048, "y": 2048}
    )
    
    print(f"Successfully ingested {len(ds.data_vars)} spectral bands.")
    return "Ingestion Complete"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sentinel2_l1c_ingestion',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@monthly',
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_13_bands',
        python_callable=ingest_sentinel_l1c
    )
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.orm import sessionmaker
from common.airflow.assets import PostgresTable
from airflow.sdk import Asset

from horseless_repotracker.repotracker.github_api import GitHubAPI
from horseless_repotracker.repotracker.github_timeline_api import GitHubTimelineAPI
from horseless_repotracker.repotracker.sqlalchemy_model import User

redis_trigger = MessageQueueTrigger(
    scheme="redis+pubsub", 
    channels=["modelrun", "repository", "issue", "issuecomment", "timeline_event"], 
    redis_conn_id="critical_redis"
)

repositories_asset = Asset(name="repositories", 
                           uri="//githubapi/firehose/repositories",
                           watchers=[AssetWatcher(name="redis_watcher", 
                           trigger=redis_trigger)])
                           
issues_asset = Asset(name="issues", 
                     uri="//githubapi/firehose/issues",
                     watchers=[AssetWatcher(name="redis_watcher", 
                     trigger=redis_trigger)])

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="github_ingester",
    default_args=default_args, 
    description="A DAG to ingest GitHub data into Postgres",
    schedule=[issues_asset, repositories_asset]
    )    
def github_ingester():
    """DAG to ingest GitHub data into Postgres using SQLAlchemy ORM."""
    
    @task.virtualenv(
        task_id="ingest_github_data", 
        requirements=[
            "sqlalchemy",
            "psycopg2-binary",
            "horseless-repotracker",
            "common-airflow",
            "airflow-sdk"
        ],
        pip_install_options=[
            "--extra-index-url",
            "https://pkgs.dev.azure.com/wizardcontroller/MetOffice/_packaging/public/pypi/simple/",
        ],
    )
    def insert_user_with_orm():
        # 1. Initialize the hook with your UI Connection ID
        pg_hook = PostgresHook(postgres_conn_id='horseless_repotrackerdb')
        
        # 2. Get the raw SQLAlchemy engine
        engine = pg_hook.get_sqlalchemy_engine()
        
        # 3. Bind the engine to a session
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # 4. Use your model as usual
            # new_user = User(id=1, login='new_user', name='New User')
            # session.add(new_user)
            # session.commit()
            
            # query users
            users = session.query(User).all()
            for user in users:
                print(f"User ID: {user.id}, Login: {user.login}, Name: {user.name}")
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

"""
Hello World DAG

A simple starter DAG that demonstrates basic Airflow concepts.
This DAG runs daily and prints hello world messages.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def print_hello():
    """Print a hello world message."""
    print("Hello World from Airflow!")
    return "Hello World!"


def print_date(**context):
    """Print the DAG execution date."""
    execution_date = context.get('logical_date') or context.get('execution_date')
    print(f"DAG execution date: {execution_date}")
    print(f"Current time: {datetime.now()}")
    return str(execution_date)


# Define the DAG
with DAG(
    'hello_world',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule='@daily',  # Run once a day
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['example', 'hello-world'],
) as dag:

    # Task 1: Print hello using Python
    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello,
    )

    # Task 2: Print current date using Python
    date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )

    # Task 3: Print hello using Bash
    bash_hello_task = BashOperator(
        task_id='bash_hello',
        bash_command='echo "Hello from Bash!"',
    )

    # Task 4: Print completion message
    completion_task = BashOperator(
        task_id='completion',
        bash_command='echo "DAG execution completed successfully!"',
    )

    # Define task dependencies
    # hello_task and date_task run in parallel, then bash_hello_task, then completion_task
    [hello_task, date_task] >> bash_hello_task >> completion_task

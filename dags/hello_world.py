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


def print_date():
    """Print the current date and time."""
    current_time = datetime.now()
    print(f"Current date and time: {current_time}")
    return str(current_time)


# Define the DAG
with DAG(
    'hello_world',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
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

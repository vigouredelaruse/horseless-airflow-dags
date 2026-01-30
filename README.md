# horseless-airflow-dags

This repository contains Apache Airflow DAGs configured for GitHub DAG sync.

## Repository Structure

```
.
├── dags/               # Airflow DAG files
│   └── hello_world.py  # Starter hello-world DAG
├── .gitignore
└── README.md
```

## DAGs

### hello_world.py

A simple starter DAG that demonstrates basic Airflow concepts:
- **Schedule**: Daily
- **Description**: Prints hello world messages using both Python and Bash operators
- **Tasks**:
  - `say_hello`: Prints "Hello World from Airflow!" using PythonOperator
  - `print_date`: Prints current date and time using PythonOperator
  - `bash_hello`: Prints "Hello from Bash!" using BashOperator
  - `completion`: Prints completion message using BashOperator

The tasks are configured to run with dependencies: `say_hello` and `print_date` run in parallel, followed by `bash_hello`, and finally `completion`.

## Using with Airflow

### GitHub DAG Sync

This repository is designed to work with Airflow's GitHub DAG sync feature. Configure your Airflow instance to sync from this repository:

1. In your Airflow configuration, set up Git-sync or use the built-in DAG sync feature
2. Point to this repository URL
3. Set the `dags/` folder as your DAG directory
4. Airflow will automatically sync and load DAGs from the `dags/` folder

### Local Development

To test DAGs locally:

```bash
# Install Apache Airflow
pip install apache-airflow

# Set AIRFLOW_HOME (optional)
export AIRFLOW_HOME=~/airflow

# Initialize the database
airflow db init

# Copy DAGs to your Airflow DAGs folder
cp -r dags/* $AIRFLOW_HOME/dags/

# Start the web server
airflow webserver --port 8080

# Start the scheduler (in another terminal)
airflow scheduler
```

## Adding New DAGs

1. Create a new Python file in the `dags/` directory
2. Define your DAG using the Airflow DAG API
3. Commit and push to the repository
4. Airflow will automatically pick up the new DAG

## Best Practices

- Keep DAG files in the `dags/` directory
- Use meaningful DAG IDs and descriptions
- Set appropriate `start_date` and `schedule_interval`
- Use `catchup=False` for most DAGs to avoid backfilling
- Add tags to categorize your DAGs
- Document your DAGs with docstrings and comments
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG setup
with DAG(
    dag_id="etl_pipeline",
    description="Simple ETL pipeline: extract → transform → load",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # Step 1: Extract
    extract = BashOperator(
        task_id="extract",
        bash_command="python /project/etl/extract.py"
    )

    # Step 2: Transform
    transform = BashOperator(
        task_id="transform",
        bash_command="python /project/etl/transform.py"
    )

    # Step 3: Load into SQLite
    load = BashOperator(
        task_id="load",
        bash_command="python /project/etl/load.py"
    )

    # Pipeline sequence
    extract >> transform >> load

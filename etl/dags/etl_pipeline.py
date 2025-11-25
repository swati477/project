from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# Define base project path (make it relative to Airflow DAG folder)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

with DAG(
    dag_id="etl_pipeline",
    description="ETL pipeline: extract → transform → load",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # Step 1: Extract
    extract = BashOperator(
        task_id="extract",
        bash_command=f"python {os.path.join(BASE_DIR, '../etl/extract.py')}"
    )

    # Step 2: Transform
    transform = BashOperator(
        task_id="transform",
        bash_command=f"python {os.path.join(BASE_DIR, '../etl/transform.py')}"
    )

    # Step 3: Load into SQLite
    load = BashOperator(
        task_id="load",
        bash_command=f"python {os.path.join(BASE_DIR, '../etl/load.py')}"
    )

    # Define pipeline sequence
    extract >> transform >> load

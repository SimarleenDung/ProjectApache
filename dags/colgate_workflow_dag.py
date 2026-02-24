from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from pathlib import Path
import sys
import os


# Import scripts
from utils.gcs_to_sf_utils import load_all_files
from utils.api_to_gcs_utils import fetch_kaggle_dataset
from utils.landing_to_archive_utils import move_files_to_archive
from config import GCS_BUCKET_NAME
SAVE_PATH = Path("/opt/airflow/data/kaggle_dataset")


# DAG

with DAG(
    dag_id="Colgate_workflow_DAG",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")

    # FETCH KAGGLE DATA 
    fetch_task = PythonOperator(
        task_id="kaggle_to_gcp",
        python_callable=fetch_kaggle_dataset
    )
    
    # LOAD KAGGLE DATA TO SF
    load_task = PythonOperator(
        task_id="gcp_to_snowflake",
        python_callable=load_all_files
    )
    
    archive_task = PythonOperator(
    task_id="archive_files",
    python_callable=move_files_to_archive,
    op_kwargs={
        "bucket_name": GCS_BUCKET_NAME,
        "landing_prefix": "landing/",
        "archive_prefix": "archive/"
    }
)

    end = DummyOperator(task_id="end")

    

start >> fetch_task >> load_task >> archive_task >> end

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule
from pathlib import Path
import sys
import os


# Import scripts
from utils.gcs_to_sf_utils import load_single_file
from utils.api_to_gcs_utils import (download_kaggle_zip, extract_and_upload)
from utils.landing_to_archive_utils import move_files_to_archive
from config import GCS_BUCKET_NAME , GCS_FILE_TABLE_CONFIG
SAVE_PATH = Path("/opt/airflow/data/kaggle_temp")


FILE_CONFIGS = GCS_FILE_TABLE_CONFIG

FILE_NAMES = [
        "olist_customers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "product_category_name_translation.csv"
 
    ]

# DAG

with DAG(
    dag_id="Colgate_workflow_DAG",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")

    # FETCH KAGGLE DATA TO GCS
    with TaskGroup("api_to_gcp") as api_to_gcp:

        download_zip = PythonOperator(
            task_id="download_zip",
            python_callable=download_kaggle_zip
        )

        upload_tasks = []

        for file in FILE_NAMES:
            upload = PythonOperator(
                task_id=f"upload_{file.replace('.csv','')}",
                python_callable=extract_and_upload,
                op_args=[file]
            )

            download_zip >> upload

            upload_tasks.append(upload)
        
    # LOAD GCS FILES TO SF
    with TaskGroup("gcp_to_snowflake") as gcp_to_sf:

        load_tasks = []

        for file_conf in FILE_CONFIGS:

            file_name = file_conf["gcs_path"].split("/")[-1]

            load = PythonOperator(
                task_id=f"load_{file_name.replace('.csv','')}",
                python_callable=load_single_file,
                op_args=[file_conf]  
            )

            load_tasks.append(load)
    
    archive_task = PythonOperator(
    task_id="archive_files",
    python_callable=move_files_to_archive,
    trigger_rule=TriggerRule.NONE_FAILED,
    op_kwargs={
        "bucket_name": GCS_BUCKET_NAME,
        "landing_prefix": "landing/",
        "archive_prefix": "archive/"
    }
)

    end = DummyOperator(task_id="end")

    

start >> api_to_gcp >> gcp_to_sf >> archive_task >> end
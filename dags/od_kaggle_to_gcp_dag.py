from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from pathlib import Path
import sys
import os
 
# # Add python_script folder to path (optional if Airflow doesn't auto-detect)
# SCRIPT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "python_script"))
# sys.path.append(SCRIPT_PATH)
 
# Import scripts
from od_kaggle_v2 import fetch_kaggle_dataset
SAVE_PATH = Path("/opt/airflow/data/kaggle_dataset")
# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="od_kaggle_to_gcp_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
 
    start = DummyOperator(task_id="start")
 
    # ---------- FETCH KAGGLE DATA ----------
    fetch_task = PythonOperator(
        task_id="fetch_kaggle_dataset",
        python_callable=fetch_kaggle_dataset
    )
 
    end = DummyOperator(task_id="end")
 
start >> fetch_task >> end
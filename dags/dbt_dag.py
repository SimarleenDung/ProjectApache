from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
 
with DAG(
    dag_id="dbt_single_model",
    start_date=datetime(2026, 1, 22),
    schedule_interval=None,
    catchup=False,
) as dag:
 
    run_one_model = BashOperator(
        task_id="run_my_model",
        bash_command="""
        cd /opt/airflow/demo_dbt/dbt_project && dbt run \
        --select trialmodel \
        --profiles-dir /opt/airflow/demo_dbt/dbt_project"""
    )
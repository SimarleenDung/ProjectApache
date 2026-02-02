# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime
 
# with DAG(
#     dag_id="dbt_single_model",
#     start_date=datetime(2026, 1, 22),
#     schedule_interval=None,
#     catchup=False,
# ) as dag:
 
#     run_one_model = BashOperator(
#         task_id="run_my_model",
#         bash_command="""
#         cd /opt/airflow/demo_dbt/dbt_project && dbt run \
#         --select trialmodel \
#         --profiles-dir /opt/airflow/demo_dbt/dbt_project"""
#     )


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import json

def create_dbt_command(selected_customer=None):
    base_cmd = (
        "cd /opt/airflow/demo_dbt/dbt_project && "
        "dbt run --select trialmodel "
        "--profiles-dir /opt/airflow/demo_dbt/dbt_project"
    )

    if selected_customer and selected_customer != "None":
        vars_json = json.dumps(
            {"selected_customer": selected_customer}
        ).replace('"', '\\"')
        base_cmd += f' --vars "{vars_json}"'

    return base_cmd

with DAG(
    dag_id="dbt_single_flag",
    start_date=datetime(2026, 1, 22),
    schedule_interval=None,
    catchup=False,
) as dag:

    Start = EmptyOperator(task_id="Start")
    End = EmptyOperator(task_id="End")

    run_dbt = BashOperator(
        task_id="run_trialmodel",
        bash_command=create_dbt_command(
            selected_customer="{{ dag_run.conf.get('selected_customer') }}"
        )
    )

    # Dependencies
    Start >> run_dbt >> End
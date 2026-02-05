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
from airflow.models.param import Param
from datetime import datetime
import json

# def create_dbt_command(selected_customer=None):
#     base_cmd = (
#         "cd /opt/airflow/demo_dbt/dbt_project && "
#         "dbt run --select trialmodel "
#         "--profiles-dir /opt/airflow/demo_dbt/dbt_project"
#     )

    # if selected_customer:  # First check: Does it have a value?
    #     if selected_customer != "None":  # Second check: Is it not the string "None"?
    #         vars_json = json.dumps(
    #             {"selected_customer": selected_customer}
    #         ).replace('"', '\\"')
    #         base_cmd += f' --vars "{vars_json}"'

    # return base_cmd

    # if selected_customer:
    #     # Handle both list and single string values

    #     if isinstance(selected_customer, str):
    #         if selected_customer not in ["None", "all", ""]:
    #             vars_json = json.dumps(
    #                 {"selected_customer": selected_customer}
    #             ).replace('"', '\\"')
    #             base_cmd += f' --vars "{vars_json}"'

    #     elif isinstance(selected_customer, list) and len(selected_customer) > 0:
    #         # Filter out any "all" or "None" values
    #         filtered_customers = [c for c in selected_customer if c not in ["all", "None", ""]]
    #         if filtered_customers:
    #             vars_json = json.dumps(
    #                 {"selected_customer": filtered_customers}
    #             ).replace('"', '\\"')
    #             base_cmd += f' --vars "{vars_json}"'

    # return base_cmd

with DAG(
    dag_id="dbt_single_flag",
    start_date=datetime(2026, 1, 22),
    schedule_interval=None,
    catchup=False,
    params={
        "selected_customer": Param(
            default="all",
            type=["null", "string", "array"],
            description="Enter 'all' or array of emails: ['alice@test.com', 'bob@test.com']",
        ),
    },
) as dag:

    Start = EmptyOperator(task_id="Start")
    End = EmptyOperator(task_id="End")

    # run_dbt = BashOperator(
    #     task_id="run_trialmodel",
    #     bash_command=create_dbt_command(
    #         #selected_customer="{{ dag_run.conf.get('selected_customer') }}"
    #         selected_customer="{{ params.selected_customer }}"
    #     )
    # )

    # Build the command directly in bash_command with Jinja templating
    run_dbt = BashOperator(
        task_id="run_trialmodel",
        bash_command="""
        cd /opt/airflow/demo_dbt/dbt_project && \
        {% set customer = params.selected_customer %}
        {% if customer == "all" or customer == "None" or customer == "" or customer is none %}
        dbt run --select trialmodel --profiles-dir /opt/airflow/demo_dbt/dbt_project
        {% else %}
        dbt run --select trialmodel --profiles-dir /opt/airflow/demo_dbt/dbt_project --vars '{"selected_customer": {{ customer | tojson }} }'
        {% endif %}
        """
    )

    # Dependencies
    Start >> run_dbt >> End
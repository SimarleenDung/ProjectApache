# importing the libraries
from airflow import DAG
from datetime import datetime, timezone
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pyarrow import parquet as pq

FILES_CONFIG = {
    "customer_sample_data": "CUSTOMER",
    "orders_sample_data": "ORDERS",
    "part_sample_data": "PART",
    "region_sample_data": "REGION",
    "supplier_sample_data": "SUPPLIER",
}

# defining the function that manipulates data and converts it to parquet
def _csv_to_parquet(file_name, **context):

    csv_path = f"/opt/airflow/data_external/landing/{file_name}.csv"
    parquet_path = f"/opt/airflow/data_external/formatted/{file_name}.parquet"

    data = pd.read_csv(csv_path)

    # Fill nulls only for string columns
    for col in data.columns:
        if data[col].dtype == "object":
            data[col] = data[col].fillna("UNKNOWN")

    # Add the metadata columns
    data["DAG_ID"] = context["dag"].dag_id
    data["LOAD_TIME"] = datetime.now(timezone.utc)

    # Save as parquet
    data.to_parquet(parquet_path, index=False)

def _load_into_snowflake(file_name, table_name):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    parquet_file = f"/opt/airflow/data_external/formatted/{file_name}.parquet"


    with hook.get_conn() as conn:
        with conn.cursor() as cs:

            # 2. Upload Parquet to stage
            cs.execute(f"""
                PUT file://{parquet_file}
                @airflow_db.stage.my_internal_stage
                OVERWRITE = TRUE;
            """)

            # 3. COPY into table
            cs.execute(f"""
                COPY INTO airflow_db.stage.{table_name}
                FROM @airflow_db.stage.my_internal_stage
                FILE_FORMAT = (TYPE = PARQUET)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PATTERN = '.*{file_name}.*';
            """)

# defining the DAG
with DAG("second_uc_trial", start_date=datetime(2025,12,1), schedule_interval="@daily", catchup=False) as dag:

    # defining the start node
    Start=EmptyOperator(
        task_id="Start"
    )

    # defining the end node
    End=EmptyOperator(
        task_id="End"
    )

    for file_name, table_name in FILES_CONFIG.items():

        # defining first task that calls the function which manipulates data and converts it to parquet
        csv_to_parquet=PythonOperator(
            task_id=f"csv_to_parquet_{file_name}",
            python_callable=_csv_to_parquet,
            op_kwargs={"file_name": file_name}
        )

        # defining second task that loads the dataset into snowflake
        load_into_snowflake=PythonOperator(
            task_id=f"load_to_snowflake_{file_name}",
            python_callable=_load_into_snowflake,
            op_kwargs={
                "file_name": file_name,
                "table_name": table_name
            }
        )

        Start >> csv_to_parquet >> load_into_snowflake >> End

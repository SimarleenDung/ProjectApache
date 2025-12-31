# importing the libraries
from airflow import DAG
from datetime import datetime, timezone
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pyarrow import parquet as pq

# defining the function that manipulates data and converts it to parquet
def _csv_to_parquet(**context):
    data=pd.read_csv("/opt/airflow/data_external/landing/use_case_one_dataset.csv")

    # Capitalize all column names
    data.columns = data.columns.str.upper()

    # Add the metadata columns
    data["DAG_ID"] = context["dag"].dag_id
    data["LOAD_TIME"] = datetime.now(timezone.utc)

    # Handle nulls
    data = data.fillna("UNKNOWN")
    data = data.astype(str)

    # converting and storing as parquet
    data.to_parquet("/opt/airflow/data_external/formatted/use_case_one_dataset.parquet", index=False)

def _load_into_snowflake():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    parquet_file = "/opt/airflow/data_external/formatted/use_case_one_dataset.parquet"

    # Read Parquet schema to get column names dynamically
    table = pq.read_table(parquet_file)
    cols = table.schema.names

    # Build dynamic CREATE TABLE statement
    column_defs = []
    for col in cols:
        if col == "LOAD_TIME":
            column_defs.append(f"{col} TIMESTAMP_NTZ")
        else:
            column_defs.append(f"{col} STRING")

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS airflow_db.stage.MY_TABLE (
            {", ".join(column_defs)}
        );
    """

    with hook.get_conn() as conn:
        with conn.cursor() as cs:

            # 1. Create table if not exists
            cs.execute(create_sql)

            # 2. Upload Parquet to stage
            cs.execute(f"""
                PUT file://{parquet_file}
                @airflow_db.stage.my_internal_stage
                OVERWRITE = TRUE;
            """)

            # 3. COPY into table
            cs.execute("""
                COPY INTO airflow_db.stage.MY_TABLE
                FROM @airflow_db.stage.my_internal_stage
                FILE_FORMAT = (TYPE = PARQUET)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PATTERN = '.*use_case_one_dataset.*';;
            """)

# defining the DAG
with DAG("first_uc_trial", start_date=datetime(2025,12,1), schedule_interval="@daily", catchup=False) as dag:

    # defining the start node
    Start=EmptyOperator(
        task_id="Start"
    )

    # defining first task that calls the function which manipulates data and converts it to parquet
    csv_to_parquet=PythonOperator(
        task_id="csv_to_parquet",
        python_callable=_csv_to_parquet
    )

    # defining second task that loads the dataset into snowflake
    load_into_snowflake=PythonOperator(
        task_id="load_into_snowflake",
        python_callable=_load_into_snowflake
    )

    # defining the end node
    End=EmptyOperator(
        task_id="End"
    )

    Start >> csv_to_parquet >> load_into_snowflake >> End

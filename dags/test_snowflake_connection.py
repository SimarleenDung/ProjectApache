from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
 
def test_snowflake_connection():
    # The connection ID must match what you defined in Airflow Connections UI
    conn_id = "snowflake_conn"
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
 
    # Example query to test connection
    sql = "SELECT CURRENT_VERSION()"
 
    result = hook.get_first(sql)
    print(f"✅ Snowflake connection successful. Current version: {result[0]}")
 
# Define the DAG
with DAG(
    dag_id="test_snowflake_connection",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=["snowflake", "test"],
) as dag:
 
    test_connection = PythonOperator(
        task_id="check_snowflake_connection",
        python_callable=test_snowflake_connection,
    )
 
    test_connection
 
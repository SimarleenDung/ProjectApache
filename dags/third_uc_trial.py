from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import os
 
from utils.parquet_utils import csv_to_parquet
from utils.snowflake_utils import truncate_and_load
from utils.file_discovery_utils import get_all_new_files, update_processed_files
from utils.file_router_utils import resolve_table_from_filename

# Schema change detection task
@task
def detect_schema_change(files):
    """
    files: list of new files discovered
    Returns: list of files with schema changes
    """
    import pandas as pd
    files_with_schema_change = []

    for f in files:
        table_name = resolve_table_from_filename(f)
        table_name_fs = table_name.lower()
        
        # Build the REAL parquet path
        parquet_file = os.path.join(
            "/opt/airflow/data_external/formatted",
            table_name_fs,
            os.path.basename(f).replace(".csv", ".parquet")
        )

        if not os.path.exists(parquet_file):
            raise FileNotFoundError(f"Parquet file not found: {parquet_file}")
        
        df_new = pd.read_parquet(parquet_file) # or pd.read_parquet(f) if Parquet
        new_schema = list(df_new.columns)

        # Fetch reference schema from Airflow Variable
        ref_schema_var = f"schema_{table_name}"
        ref_schema = Variable.get(ref_schema_var, default_var=None)
        if ref_schema:
            ref_schema = ref_schema.split(",")

        if ref_schema != new_schema:
            files_with_schema_change.append(f)
            Variable.set(ref_schema_var, ",".join(new_schema))  # update stored schema

    return files_with_schema_change

with DAG(
    dag_id="third_uc_trial",
    start_date=datetime(2025, 12, 1),
    schedule_interval=None,
    catchup=False,
    #on_failure_callback=on_dag_failure_callback,
    tags=["dynamic-mapping", "snowflake", "vault", "email", "ingestion"],
) as dag:
 
    Start = EmptyOperator(task_id="Start")
    End = EmptyOperator(task_id="End")

    # Discover new files
    @task
    def discover_new_files():
        from airflow.operators.python import get_current_context
        ti = get_current_context()["ti"]

        get_all_new_files(ti=ti)
        files = ti.xcom_pull(key="new_files") or []

        valid_files = []
        skipped_files = []

        for f in files:
            try:
                # resolve table using filename without extension
                resolve_table_from_filename(f.replace(".csv", ""))
                valid_files.append(f)  # keep the .csv extension here
            except ValueError:
                skipped_files.append(f)

        ti.xcom_push(key="skipped_files", value=skipped_files)

        # persist ONLY flat strings for later bookkeeping
        ti.xcom_push(key="processed_candidates", value=valid_files)

        return valid_files
   
    new_files_list = discover_new_files()

    with TaskGroup(group_id="convert_files") as convert_files:
        # CSV → Parquet (parallel and safely)
        @task

        def csv_to_parquet_task(file_name, **context):

            print(f"CSV → Parquet for file: {file_name}")
            csv_to_parquet(file_name=file_name, **context)
    
        parquet_tasks = csv_to_parquet_task.expand(file_name=new_files_list)
    
        # Group files by table
        @task
        def group_files_by_table(files):
            grouped = {}
            for f in files:
                table = resolve_table_from_filename(f)
                grouped.setdefault(table, []).append(f)
            return list(grouped.values())
    
        grouped_files = group_files_by_table(new_files_list)
 
    with TaskGroup(group_id="load_to_snowflake") as load_to_snowflake:
        # Sequential Snowflake load PER TABLE
        @task
        def load_files_sequentially(files):
            """
            files: list of files for ONE table
            Alphabetical order → last file wins
            """
        
            files = sorted(files) # to explicitly order all file before loading
    
            print("Loading files in this table:")
            for f in files:
                print(f"   ➜ {f}")
        
            for f in files:
                truncate_and_load(file_name=f)
    
        load_per_table = load_files_sequentially.expand(
        files=grouped_files
        )
 
    # Mark processed files
    @task
    def mark_files_loaded():
        from airflow.operators.python import get_current_context
        ti = get_current_context()["ti"]
        update_processed_files(ti=ti)
        return ti.xcom_pull(key="processed_candidates")  # RETURN loaded files for schema check

    loaded_files = mark_files_loaded()  # Task reference
    
    # Schema detection runs AFTER all files are loaded
    schema_changed_files = detect_schema_change(loaded_files)

    # DAG Flow
 
    Start >> new_files_list
    new_files_list >> convert_files
    convert_files >> load_per_table
    load_per_table >> loaded_files
    loaded_files >> schema_changed_files >> End
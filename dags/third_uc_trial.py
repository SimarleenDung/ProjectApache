from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import os
#from airflow.operators.email import EmailOperator          
#from airflow.operators.python import PythonOperator        
#from airflow.utils.db import provide_session
#from airflow.models import XCom
 
from utils.parquet_utils import csv_to_parquet
from utils.snowflake_utils import truncate_and_load
from utils.file_discovery_utils import get_all_new_files, update_processed_files
from utils.file_router_utils import resolve_table_from_filename

# Read alert email ONCE at top level 
#ALERT_EMAIL = Variable.get("alert_email")

# Email + XCom failure callbacks

#def on_task_failure_callback(context):
    #"""Collect info of failed tasks in XCom"""
    #ti = context['ti']
    #dag_name = context['dag'].dag_id
    #run_id = dag_name

    #print(f"Task {ti.task_id} failed. Saving info to XCom.")

    #ti.xcom_push(
        #key=f"failed_task_info_{run_id}",
        #value={
           # 'dag_id': dag_name,
           # 'task_id': ti.task_id,
           # 'log_url': ti.log_url
        #}
    #)


#@provide_session
#def on_dag_failure_callback(session=None, **context):
    #"""Aggregate failed tasks from XCom and send Gmail email"""
    #dag_name = context['dag'].dag_id
    #run_id = dag_name
    #execution_date = context['execution_date']
   # email_recipients = [Variable.get("alert_email")]

    # Query failed tasks from XCom
    #failed_tasks = session.query(XCom).filter(
      #  XCom.dag_id == dag_name,
        #XCom.key == f"failed_task_info_{run_id}",
       # XCom.execution_date == execution_date
   # ).all()

    #print(f"Found {len(failed_tasks)} failed tasks for DAG {dag_name}.")

   # if not failed_tasks:
        #return

    # Construct HTML message
    ##msg = """
#<h3>The following tasks failed in the DAG:</h3>
#<table border="1" style="border-collapse: collapse; width: 100%;">
#<tr>
#<th>DAG ID</th>
#<th>Task ID</th>
#<th>Log Link</th>
#</tr>
#"""
 #   for task in failed_tasks:
  #      task_info = task.value
   #     msg += f"""
#<tr>
#<td>{task_info['dag_id']}</td>
#<td>{task_info['task_id']}</td>
#<td><a href="{task_info['log_url']}">Log Link</a></td>
#</tr>
#"""
 #   msg += "</table>"

    # Send Gmail email
  #  email = EmailOperator(
   #     task_id="failure_notification",
    #    to=email_recipients,
     #   subject=f"DAG Failure Notification for {dag_name}",
      #  html_content=msg,
       # conn_id="smtp_gmail"   
    #)
    #email.execute(context=context)

    # Cleanup XCom entries
    #session.query(XCom).filter(
    #    XCom.dag_id == dag_name,
    #    XCom.key == f"failed_task_info_{run_id}"
    #).delete()

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

# Email sender for schema change
#def send_schema_change_email(files_with_change, **context):
   # if not files_with_change:
       # return
    #email_recipients = [Variable.get("alert_email")]
    #file_list_html = "<br>".join(files_with_change)

   # email = EmailOperator(
       # task_id="schema_change_email",
        #to=email_recipients,
        #subject="Schema Change Detected in Incoming Files",
       # html_content=f"""
       # <h3>Schema change detected in the following files:</h3>
       # {file_list_html}
       # """,
       # conn_id="smtp_gmail"
   #)
   # email.execute(context=context)

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
 
    # Detect schema changes
    #schema_changed_files = detect_schema_change(new_files_list)

    # Send email if schema changed
    #@task
    #def send_schema_change_email_task(files_with_change):
       # if files_with_change:
          #  send_schema_change_email(files_with_change)
       # else:
           # print("No schema changes detected — skipping email.")
        #return "done"

    #send_schema_change_task = send_schema_change_email_task(schema_changed_files)


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
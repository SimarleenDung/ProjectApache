from airflow.models import Variable
import json
import os
 
def get_all_new_files(ti, **context):

    LANDING_PATH = "/opt/airflow/data_external/landing"
    all_files = []

    for root, _, files in os.walk(LANDING_PATH):

        # skip files in the landing root folder
        if root == LANDING_PATH:
            continue  # skip files directly under landing

        for f in files:
            if f.endswith(".csv"):
                # store relative path: customer/file.csv
                rel_path = os.path.relpath(
                    os.path.join(root, f),
                    LANDING_PATH
                )
                all_files.append(rel_path)

    # pull processed_files from Variable, fallback to empty list ---
    raw_processed_files = json.loads(
        Variable.get("processed_files", default_var="[]")
    )
    processed_files = []
    for item in raw_processed_files:
        if isinstance(item, str):
            processed_files.append(item)

    new_files = sorted(set(all_files) - set(processed_files))

    ti.xcom_push(key="new_files", value=new_files)
 
 
def update_processed_files(ti, **context):
    """
    Persist processed files across DAG runs using Airflow Variables
    """

    raw_new_files = ti.xcom_pull(key="new_files") or []

    # sanitize new files
    new_files = []
    for item in raw_new_files:
        if isinstance(item, str):
            new_files.append(item)
        elif isinstance(item, dict) and "value" in item:
            new_files.append(item["value"])

    # sanitize already persisted files
    raw_processed_files = json.loads(
        Variable.get("processed_files", default_var="[]")
    )

    processed_files = []
    for item in raw_processed_files:
        if isinstance(item, str):
            processed_files.append(item)

    # merge + dedupe safely
    updated_files = sorted(set(processed_files + new_files))

    Variable.set("processed_files", json.dumps(updated_files))
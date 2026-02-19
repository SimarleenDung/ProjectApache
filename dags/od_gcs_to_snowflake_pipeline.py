import os
import tempfile
import logging

import hvac
import pandas as pd
import snowflake.connector

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from config import GCS_BUCKET_NAME, GCS_FILE_TABLE_CONFIG

log = logging.getLogger(__name__)


def _get_vault_client() -> hvac.Client:
    vault_addr = Variable.get("VAULT_ADDR", default_var="http://vault:8200")
    token      = Variable.get("VAULT_TOKEN")
    return hvac.Client(url=vault_addr, token=token)


def get_snowflake_creds_from_vault() -> dict:
    client = _get_vault_client()
    secret = client.secrets.kv.v2.read_secret_version(
        mount_point="snowflake-secrets",
        path="snowflake-secrets",
    )
    data = secret["data"]["data"]
    
    # Combine organization and account for Python connector
    account = f"{data['organization_name']}-{data['account_name']}"
    
    return {
        "account":   account,  # "cahtfiw-jb32837"
        "user":      data["user"],
        "password":  data["password"],
        "warehouse": data["warehouse"],
        "database":  data["database"],
        "schema":    data["schema"],
        "role":      data["role"],
    }


def get_gcp_sa_from_vault() -> dict:
    client = _get_vault_client()
    secret = client.secrets.kv.v2.read_secret_version(
        mount_point="gcp-secrets",
        path="gcp-secrets",
    )
    return secret["data"]["data"]


def read_csv_from_gcs(bucket_name: str, gcs_path: str) -> pd.DataFrame:
    from google.oauth2 import service_account
    from google.cloud import storage

    sa_info     = get_gcp_sa_from_vault()
    credentials = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    gcs_client = storage.Client(credentials=credentials, project=sa_info["project_id"])
    blob       = gcs_client.bucket(bucket_name).blob(gcs_path)

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        blob.download_to_filename(tmp.name)
        df = pd.read_csv(tmp.name)

    os.unlink(tmp.name)
    log.info("Read %d rows from gs://%s/%s", len(df), bucket_name, gcs_path)
    return df


def upsert_to_snowflake(
    cursor,
    df:        pd.DataFrame,
    table:     str,
    merge_key: str,
    columns:   list,
) -> None:
    stage_name = f"@%{table}"

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, newline=""
    ) as tmp:
        df[columns].to_csv(tmp.name, index=False, header=True)
        tmp_path = tmp.name

    cursor.execute(f"PUT file://{tmp_path} {stage_name} OVERWRITE = TRUE")
    os.unlink(tmp_path)
    log.info("Staged %d rows to %s", len(df), stage_name)

    col_positions  = {col: f"${i+1}" for i, col in enumerate(columns)}
    update_cols    = [c for c in columns if c != merge_key]
    update_clause  = ",\n            ".join(
        f"target.{c} = staged.{c}" for c in update_cols
    )
    insert_cols    = ", ".join(columns)
    insert_vals    = ", ".join(f"staged.{c}" for c in columns)
    staged_select  = ", ".join(
        f"{pos} AS {col}" for col, pos in col_positions.items()
    )

    merge_sql = f"""
        MERGE INTO {table} AS target
        USING (
            SELECT {staged_select}
            FROM {stage_name}
            (FILE_FORMAT => (
                TYPE                         = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER                  = 1
            ))
        ) AS staged
        ON target.{merge_key} = staged.{merge_key}
        WHEN MATCHED THEN
            UPDATE SET
            {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
    """
    cursor.execute(merge_sql)

    result = cursor.fetchone()
    log.info(
        "MERGE into %s — rows inserted: %s, rows updated: %s",
        table,
        result[0] if result else "?",
        result[1] if result else "?",
    )

    cursor.execute(f"REMOVE {stage_name}")


def load_all_files(**context):
    file_config = GCS_FILE_TABLE_CONFIG
    bucket_name = GCS_BUCKET_NAME

    log.info("Starting pipeline — %d file(s) to process", len(file_config))

    sf_config = get_snowflake_creds_from_vault()
    conn      = snowflake.connector.connect(**sf_config)
    cursor    = conn.cursor()

    try:
        for entry in file_config:
            gcs_path  = entry["gcs_path"]
            table     = entry["table"]
            merge_key = entry["merge_key"]
            columns   = entry["columns"]

            log.info(
                "Processing: gs://%s/%s → %s (merge key: %s)",
                bucket_name, gcs_path, table, merge_key,
            )

            df = read_csv_from_gcs(bucket_name, gcs_path)

            missing = [c for c in columns if c not in df.columns]
            if missing:
                raise ValueError(
                    f"[{table}] Columns missing from CSV: {missing}. "
                    f"Available columns: {list(df.columns)}"
                )

            upsert_to_snowflake(cursor, df, table, merge_key, columns)

            log.info("✓ Done: %s → %s", gcs_path, table)

    except Exception:
        log.exception("Pipeline failed")
        raise

    finally:
        cursor.close()
        conn.close()


default_args = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
}

with DAG(
    dag_id="gcs_to_snowflake_pipeline",
    default_args=default_args,
    description="Load CSVs from GCS → Snowflake (upsert) via Vault",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["snowflake", "gcs", "upsert"],
) as dag:

    load_task = PythonOperator(
        task_id="load_all_gcs_files_to_snowflake",
        python_callable=load_all_files,
    )
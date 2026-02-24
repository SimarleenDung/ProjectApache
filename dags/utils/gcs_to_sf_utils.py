import os
import json
import tempfile
import logging

import hvac
import pandas as pd
import snowflake.connector

from airflow.models import Variable
from datetime import datetime, timedelta
from typing import Union, List

from airflow.exceptions import AirflowSkipException

from config import GCS_BUCKET_NAME, GCS_FILE_TABLE_CONFIG
from utils.vault_utils import (get_snowflake_creds_from_vault,get_gcp_sa_from_vault)
log = logging.getLogger(__name__)



def read_csv_from_gcs(bucket_name: str, gcs_path: str):
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



def append_to_snowflake(cursor, df: pd.DataFrame, table: str,columns: list,):

    stage_name = f"@%{table}"

    # Write dataframe to temporary CSV
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, newline=""
    ) as tmp:
        df[columns].to_csv(tmp.name, index=False, header=True)
        tmp_path = tmp.name

    # Upload file to table stage
    cursor.execute(f"PUT file://{tmp_path} {stage_name} OVERWRITE = TRUE")
    os.unlink(tmp_path)

    log.info("Staged %d rows to %s", len(df), stage_name)

    # Map $1, $2, etc. to actual column names
    col_positions = {col: f"${i+1}" for i, col in enumerate(columns)}

    staged_select = ", ".join(
        f"{pos} AS {col}" for col, pos in col_positions.items()
    )

    # COPY INTO (Append Only)
    copy_sql = f"""
        INSERT INTO {table} ({', '.join(columns)})
        SELECT {staged_select}
        FROM {stage_name}
        (FILE_FORMAT => 'CSV_FORMAT')
    """

    cursor.execute(copy_sql)

    log.info("Inserted %d rows into %s", len(df), table)

    # Clean up stage
    cursor.execute(f"REMOVE {stage_name}")



def load_single_file(file_config):

    bucket_name = GCS_BUCKET_NAME
    gcs_path    = file_config["gcs_path"]
    table       = file_config["table"]
    columns     = file_config["columns"]

    log.info("Processing: gs://%s/%s → %s", bucket_name, gcs_path, table)

    # File missing → SKIP
    try:
        df = read_csv_from_gcs(bucket_name, gcs_path)
    except Exception:
        log.warning("File not found: %s — skipping", gcs_path)
        raise AirflowSkipException(f"{gcs_path} not found")

    # Column mismatch → FAIL (this task only)
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(
            f"[{table}] Column mismatch. Missing: {missing}. "
            f"Available: {list(df.columns)}"
        )

    sf_config = get_snowflake_creds_from_vault()
    conn      = snowflake.connector.connect(**sf_config)
    cursor    = conn.cursor()

    try:
        append_to_snowflake(cursor, df, table, columns)
        log.info("✓ Loaded %s into %s", gcs_path, table)
    finally:
        cursor.close()
        conn.close()


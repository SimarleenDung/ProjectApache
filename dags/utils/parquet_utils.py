from datetime import datetime, timezone
import pandas as pd
import os

# Define the landing and formatted paths
LANDING_PATH = "/opt/airflow/data_external/landing"
FORMATTED_PATH = "/opt/airflow/data_external/formatted"

def csv_to_parquet(file_name, **context):
    """
    file_name is a RELATIVE path from landing root
    Example: customer/customer_test_v01.csv
    """

    csv_path = os.path.join(LANDING_PATH, file_name)

    parquet_path = os.path.join(FORMATTED_PATH, file_name.replace(".csv", ".parquet"))

    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path)

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("UNKNOWN")

    df["DAG_ID"] = context["dag"].dag_id
    df["LOAD_TIME"] = datetime.now(timezone.utc)

    df.to_parquet(parquet_path, index=False)
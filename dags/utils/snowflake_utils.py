import snowflake.connector
from utils.vault_utils import get_snowflake_creds_from_vault
from utils.file_router_utils import resolve_table_from_filename
 
 
def truncate_and_load(file_name):
    table_name = resolve_table_from_filename(file_name)
    stage_name = f"{table_name}_STAGE"
    
    print(f"Loading file {file_name} into table {table_name}")

    parquet_file = f"/opt/airflow/data_external/formatted/{file_name.replace('.csv', '.parquet')}"

    creds = get_snowflake_creds_from_vault()

    with snowflake.connector.connect(**creds) as conn:
        with conn.cursor() as cs:

            if table_name == "PART":
                cs.execute(f"""
                    TRUNCATE TABLE {creds['database']}.{creds['schema']}.{table_name}
                """)

                cs.execute(f"""
                    REMOVE @{creds['database']}.{creds['schema']}.{stage_name}
                """)

            cs.execute(f"""
                PUT file://{parquet_file}
                @{creds['database']}.{creds['schema']}.{stage_name}
                OVERWRITE = TRUE
            """)

            cs.execute(f"""
                COPY INTO {creds['database']}.{creds['schema']}.{table_name}
                FROM @{creds['database']}.{creds['schema']}.{stage_name}
                FILE_FORMAT=(TYPE=PARQUET)
                MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
            """)
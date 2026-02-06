import pandas as pd
import snowflake.connector
import tempfile
import os

# Your public GCS file URL
file_url = 'https://storage.googleapis.com/olist_project_rs/Landing/Chocolate%20Sales%20(2).csv'

# Snowflake configuration
SNOWFLAKE_CONFIG = {
    'user': 'SIMARLEENDUNGAIRFLOW',
    'password': 'GodisGreat2807',
    'account': 'CAHTFIW-JB32837',
    'warehouse': 'COMPUTE_WH',
    'database': 'AIRFLOW_DB',
    'schema': 'STAGE'
}

RAW_TABLE = 'raw_chocolate_sales'

def load_public_gcs_to_snowflake():
    """Load data from public GCS URL to Snowflake"""
    
    print(f"Reading data from GCS...")
    df = pd.read_csv(file_url)
    print(f" Loaded {len(df)} rows, {len(df.columns)} columns")
    print(f"Columns: {list(df.columns)}")
    
    # Clean the data
    print("\nCleaning data...")
    # Remove $ and commas from Amount column
    if 'Amount' in df.columns:
        df['Amount'] = df['Amount'].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False)
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
    
    # Clean column names (replace spaces with underscores)
    df.columns = df.columns.str.replace(' ', '_').str.lower()
    print(f"Cleaned columns: {list(df.columns)}")
    
    print("\nConnecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    
    try:
        # Create table if it doesn't exist
        print(f"Creating table {RAW_TABLE}...")
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
            sales_person VARCHAR(255),
            country VARCHAR(255),
            product VARCHAR(255),
            date VARCHAR(50),
            amount FLOAT,
            boxes_shipped INTEGER
        )
        """
        cursor.execute(create_table_sql)
        print("Table created/verified")
        
        # Save cleaned DataFrame to temp CSV file
        print(f"\nPreparing data for upload...")
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as tmp:
            df.to_csv(tmp.name, index=False, header=False)
            tmp_path = tmp.name
        
        # Upload file to stage
        print("Uploading to Snowflake stage...")
        cursor.execute(f"PUT file://{tmp_path} @%{RAW_TABLE}")
        
        # Copy data into table
        print(f"Loading data into {RAW_TABLE}...")
        cursor.execute(f"""
            COPY INTO {RAW_TABLE}
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 0
            )
            ON_ERROR = 'CONTINUE'
            PURGE = TRUE
        """)
        
        # Clean up temp file
        os.unlink(tmp_path)
        
        # # Verify row count
        # cursor.execute(f"SELECT COUNT(*) FROM {RAW_TABLE}")
        # count = cursor.fetchone()[0]
        # print(f"Successfully loaded {count} rows to {RAW_TABLE}")
        
        # Show sample data
        print("\nSample data from Snowflake:")
        cursor.execute(f"SELECT * FROM {RAW_TABLE} LIMIT 5")
        for row in cursor:
            print(row)
        
    except Exception as e:
        print(f" Error: {e}")
        raise
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    load_public_gcs_to_snowflake()
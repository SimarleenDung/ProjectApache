def fetch_kaggle_dataset():

    import os
    import zipfile
    import json
    from pathlib import Path
    from utils.vault_utils import (get_kaggle_creds_from_vault,get_gcp_sa_from_vault)
    
    from google.cloud import storage
    from google.oauth2 import service_account

    
    # CONFIG
 
    DATASET = "olistbr/brazilian-ecommerce"

    FILE_NAMES = [
        "olist_customers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "product_category_name_translation.csv"
    ]

    BUCKET_NAME = "olist_project_rs"
    GCS_FOLDER = "landing/"

    # Temporary path inside Airflow worker
    SAVE_PATH = Path("/opt/airflow/data/kaggle_temp")
    SAVE_PATH.mkdir(parents=True, exist_ok=True)

   
    # KAGGLE AUTH (From Vault)
  
    kaggle_creds = get_kaggle_creds_from_vault()

    os.environ["KAGGLE_USERNAME"] = kaggle_creds["username"]
    os.environ["KAGGLE_KEY"] = kaggle_creds["key"]

    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()

 
    # DOWNLOAD DATASET ZIP
   
    api.dataset_download_files(
        DATASET,
        path=str(SAVE_PATH),
        unzip=False,
        force=True
    )

    dataset_zip = SAVE_PATH / "brazilian-ecommerce.zip"

    if not dataset_zip.exists():
        raise FileNotFoundError(f"{dataset_zip} not downloaded!")

    print("Dataset zip downloaded successfully.")

 
    # GCP AUTH (From Vault)
    
    gcp_creds = get_gcp_sa_from_vault()

    
    if isinstance(gcp_creds, str):
        gcp_creds = json.loads(gcp_creds)

    credentials = service_account.Credentials.from_service_account_info(
        gcp_creds
    )

    storage_client = storage.Client(
        credentials=credentials,
        project=gcp_creds["project_id"]
    )

    bucket = storage_client.bucket(BUCKET_NAME)

    print("Authenticated with GCP successfully.")

    # EXTRACT + UPLOAD TO GCS
 
    with zipfile.ZipFile(dataset_zip, 'r') as zip_ref:
        for file in FILE_NAMES:
            try:
                # Extract locally
                zip_ref.extract(file, SAVE_PATH)
                local_file_path = SAVE_PATH / file
                print(f"Extracted: {file}")

                # Upload to GCS landing folder
                blob = bucket.blob(f"{GCS_FOLDER}{file}")
                blob.upload_from_filename(str(local_file_path))
                print(f"Uploaded to GCS: {file}")

                # Remove extracted file after upload
                local_file_path.unlink()

            except KeyError:
                print(f"{file} not found inside zip!")

    # Remove zip file
    dataset_zip.unlink()

    # Cleanup Kaggle env vars
    os.environ.pop("KAGGLE_USERNAME", None)
    os.environ.pop("KAGGLE_KEY", None)

    print("Kaggle dataset successfully uploaded to GCS landing folder.")

# def fetch_kaggle_dataset():
 
#     import os
#     import zipfile
#     import json
#     from pathlib import Path
#     from utils.vault_utils import get_kaggle_creds_from_vault
#     from od_gcs_to_snowflake_pipeline import get_gcp_sa_from_vault
    
   
 
#     from google.cloud import storage
#     from google.oauth2 import service_account
 
#     # ----------------------------
#     # CONFIG
#     # ----------------------------
#     DATASET = "olistbr/brazilian-ecommerce"
 
#     FILE_NAMES = [
#         "olist_customers_dataset.csv",
#         "olist_geolocation_dataset.csv",
#         "olist_orders_dataset.csv",
#         "olist_order_items_dataset.csv",
#         "olist_order_payments_dataset.csv",
#         "olist_order_reviews_dataset.csv",
#         "product_category_name_translation.csv"

#     ]
 
#     BUCKET_NAME = "olist_project_rs"
#     GCS_FOLDER = "landing/"
 
#     # Temporary path inside Airflow worker
#     SAVE_PATH = Path("/opt/airflow/data/kaggle_temp")
#     SAVE_PATH.mkdir(parents=True, exist_ok=True)
 
#     # ----------------------------
#     # KAGGLE AUTH (From Vault)
#     # ----------------------------
#     kaggle_creds = get_kaggle_creds_from_vault()
 
#     os.environ["KAGGLE_USERNAME"] = kaggle_creds["username"]
#     os.environ["KAGGLE_KEY"] = kaggle_creds["key"]
 
#     from kaggle.api.kaggle_api_extended import KaggleApi
 
#     api = KaggleApi()
#     api.authenticate()
 
#     # ----------------------------
#     # DOWNLOAD DATASET ZIP
#     # ----------------------------
#     api.dataset_download_files(
#         DATASET,
#         path=str(SAVE_PATH),
#         unzip=False,
#         force=True
#     )
 
#     dataset_zip = SAVE_PATH / "brazilian-ecommerce.zip"
 
#     if not dataset_zip.exists():
#         raise FileNotFoundError(f"{dataset_zip} not downloaded!")
 
#     print("Dataset zip downloaded successfully.")
 
#     # ----------------------------
#     # GCP AUTH (From Vault)
#     # ----------------------------
#     gcp_creds = get_gcp_sa_from_vault()
 
#     # If returned as string → convert to dict
#     if isinstance(gcp_creds, str):
#         gcp_creds = json.loads(gcp_creds)
 
#     credentials = service_account.Credentials.from_service_account_info(
#         gcp_creds
#     )
 
#     storage_client = storage.Client(
#         credentials=credentials,
#         project=gcp_creds["project_id"]
#     )
 
#     bucket = storage_client.bucket(BUCKET_NAME)
 
#     print("Authenticated with GCP successfully.")
 
#     # ----------------------------
#     # EXTRACT + UPLOAD TO GCS
#     # ----------------------------
#     with zipfile.ZipFile(dataset_zip, 'r') as zip_ref:
#         for file in FILE_NAMES:
#             try:
#                 # Extract locally
#                 zip_ref.extract(file, SAVE_PATH)
#                 local_file_path = SAVE_PATH / file
#                 print(f"Extracted: {file}")
 
#                 # Upload to GCS landing folder
#                 blob = bucket.blob(f"{GCS_FOLDER}{file}")
#                 blob.upload_from_filename(str(local_file_path))
#                 print(f"Uploaded to GCS: {file}")
 
#                 # Remove extracted file after upload
#                 local_file_path.unlink()
 
#             except KeyError:
#                 print(f"{file} not found inside zip!")
 
#     # Remove zip file
#     dataset_zip.unlink()
 
#     # Cleanup Kaggle env vars
#     os.environ.pop("KAGGLE_USERNAME", None)
#     os.environ.pop("KAGGLE_KEY", None)
 
#     print("Kaggle dataset successfully uploaded to GCS landing folder.")
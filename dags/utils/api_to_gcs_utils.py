#This function downloads the kaggle dataset as a zip file
def download_kaggle_zip():
    import os
    from pathlib import Path
    from utils.vault_utils import get_kaggle_creds_from_vault
   

    DATASET = "olistbr/brazilian-ecommerce"
    SAVE_PATH = Path("/opt/airflow/data/kaggle_temp")
    SAVE_PATH.mkdir(parents=True, exist_ok=True)

    kaggle_creds = get_kaggle_creds_from_vault()

    os.environ["KAGGLE_USERNAME"] = kaggle_creds["username"]
    os.environ["KAGGLE_KEY"] = kaggle_creds["key"]

    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files(
        DATASET,
        path=str(SAVE_PATH),
        unzip=False,
        force=True
    )

    print("ZIP downloaded successfully")

#This function extracts and uploads the specified files from the zip file of dataset to GCS
def extract_and_upload(file_name):

    import json
    import zipfile
    from pathlib import Path
    from utils.vault_utils import get_gcp_sa_from_vault
    from google.cloud import storage
    from google.oauth2 import service_account

    BUCKET_NAME = "olist_project_rs"
    GCS_FOLDER = "landing/"
    SAVE_PATH = Path("/opt/airflow/data/kaggle_temp")
    dataset_zip = SAVE_PATH / "brazilian-ecommerce.zip"

    if not dataset_zip.exists():
        raise FileNotFoundError("ZIP file not found")

    # GCP Auth
    gcp_creds = get_gcp_sa_from_vault()

    if isinstance(gcp_creds, str):
        gcp_creds = json.loads(gcp_creds)

    credentials = service_account.Credentials.from_service_account_info(gcp_creds)

    storage_client = storage.Client(
        credentials=credentials,
        project=gcp_creds["project_id"]
    )

    bucket = storage_client.bucket(BUCKET_NAME)

    # Extract only that file
    with zipfile.ZipFile(dataset_zip, 'r') as zip_ref:
        zip_ref.extract(file_name, SAVE_PATH)

    local_file_path = SAVE_PATH / file_name

    blob = bucket.blob(f"{GCS_FOLDER}{file_name}")
    blob.upload_from_filename(str(local_file_path))

    local_file_path.unlink()

    print(f"{file_name} uploaded successfully")


def delete_kaggle_zip():
    from pathlib import Path

    SAVE_PATH = Path("/opt/airflow/data/kaggle_temp")
    dataset_zip = SAVE_PATH / "brazilian-ecommerce.zip"

    if dataset_zip.exists():
        dataset_zip.unlink()
        print("ZIP file deleted successfully")    
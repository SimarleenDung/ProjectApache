def move_files_to_archive(bucket_name: str, landing_prefix: str, archive_prefix: str):

    from google.cloud import storage
    from google.oauth2 import service_account
    from utils.vault_utils import get_gcp_sa_from_vault
    import logging

    log = logging.getLogger(__name__)

    #Fetch GCP service account JSON from Vault
    gcp_creds = get_gcp_sa_from_vault()

    #Build credentials object explicitly
    credentials = service_account.Credentials.from_service_account_info(
        gcp_creds
    )

    #Create client using credentials
    client = storage.Client(
        credentials=credentials,
        project=gcp_creds["project_id"]
    )

    bucket = client.bucket(bucket_name)
    blobs = list(client.list_blobs(bucket_name, prefix=landing_prefix))

    if not blobs:
        log.info("No files found in landing folder.")
        return

    log.info("Found %d file(s) to archive.", len(blobs))

    for blob in blobs:
        if blob.name.endswith("/"):
            continue

        filename = blob.name.split("/")[-1]
        new_blob_name = f"{archive_prefix.rstrip('/')}/{filename}"

        bucket.copy_blob(blob, bucket, new_blob_name)
        blob.delete()

        log.info("Moved %s → %s", blob.name, new_blob_name)

    log.info("All landing files moved to archive successfully.")
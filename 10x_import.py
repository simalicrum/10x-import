"""
10x_import.py

Prefect flow for copying 10x genomics data blobs from a source Azure storage account (SCRNA)
into a destination Azure storage account (ATLAS), using blob URLs and a JSON manifest.

Environment variables required:
    ATLAS_STORAGE_CONNECTION_STRING: Connection string for the destination storage account.
    DROPBOX_URL: Base URL for the destination container.
    SCRNA_STORAGE_CONNECTION_STRING: Connection string for the source storage account.
    SCRNA_URL: Base URL for the source container.
    SOURCE_JSON_FILEPATH: Path to the JSON file listing blobs to copy.
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone  # For SAS token expiry
from urllib.parse import urlparse

from azure.storage.blob import (
    BlobSasPermissions,  # For specifying SAS permissions
    BlobServiceClient,
    generate_blob_sas,  # For generating SAS tokens
)
from dotenv import load_dotenv
from prefect import flow, task
from prefect.futures import wait
from prefect.logging import get_run_logger

# Load environment variables from .env file
load_dotenv()

# Storage configuration from environment variables
ATLAS_STORAGE_CONNECTION_STRING = os.getenv("ATLAS_STORAGE_CONNECTION_STRING")
DROPBOX_URL = os.getenv("DROPBOX_URL")
SCRNA_STORAGE_CONNECTION_STRING = os.getenv("SCRNA_STORAGE_CONNECTION_STRING")
SCRNA_URL = os.getenv("SCRNA_URL")

SOURCE_JSON_FILEPATH = os.getenv("SOURCE_JSON_FILEPATH")

# Load the JSON manifest containing the files to copy
with open(SOURCE_JSON_FILEPATH) as f:
    data = json.load(f)


@task(tags=["copy_blob"], retries=3, retry_delay_seconds=10)
def copy_blob(source, destination):
    """
    Copy a blob from the source URL to the destination URL using Azure Blob Storage SDK.
    Generates a SAS token for the source blob and uses it to upload to the destination.

    Args:
        source (str): Source blob URL (without SAS).
        destination (str): Destination blob URL.
    """
    logger = get_run_logger()
    logger.info(f"Copying blob from {source} to {destination}")

    # Create blob service clients for source and destination
    source_blob_service_client = BlobServiceClient.from_connection_string(
        SCRNA_STORAGE_CONNECTION_STRING
    )
    dest_blob_service_client = BlobServiceClient.from_connection_string(
        ATLAS_STORAGE_CONNECTION_STRING
    )

    # Parse source blob URL to extract container and blob name
    source_url = urlparse(source)
    source_container = source_url.path.split("/")[1]
    source_blob = source_url.path.split("/")[2:]
    source_blob = "/".join(source_blob)
    source_blob_client = source_blob_service_client.get_blob_client(
        container=source_container, blob=source_blob
    )

    # Parse destination blob URL to extract container and blob name
    destination_url = urlparse(destination)
    destination_container = destination_url.path.split("/")[1]
    destination_blob = destination_url.path.split("/")[2:]
    destination_blob = "/".join(destination_blob)
    destination_blob_client = dest_blob_service_client.get_blob_client(
        container=destination_container, blob=destination_blob
    )

    # Generate SAS token for the source blob (valid for 1 hour)
    sas_token = generate_blob_sas(
        account_name=source_blob_client.account_name,
        container_name=source_blob_client.container_name,
        blob_name=source_blob_client.blob_name,
        account_key=source_blob_service_client.credential.account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(timezone.utc) + timedelta(hours=1),
    )

    # Construct the source URL with SAS token
    source_url_with_sas = f"{source_blob_client.url}?{sas_token}"

    # Upload the blob from the source URL to the destination using server-side copy
    copy_id = destination_blob_client.start_copy_from_url(source_url_with_sas)
    logger.info(f"Started copy operation with copy_id: {copy_id}")

    # Wait for the copy to complete

    while True:
        props = destination_blob_client.get_blob_properties()
        status = props.copy.status
        logger.info(f"Copy status for {destination_blob_client.blob_name}: {status}")
        if status == "success":
            logger.info(f"Copy completed for {destination_blob_client.blob_name}")
            break
        elif status == "pending":
            time.sleep(5)
        else:
            logger.error(
                f"Copy failed for {destination_blob_client.blob_name} with status: {status}"
            )
            raise Exception(
                f"Copy failed for {destination_blob_client.blob_name} with status: {status}"
            )


@flow
def copy_10x():
    """
    Prefect flow to copy all 10x genomics data blobs listed in the JSON manifest
    from the SCRNA storage account to the ATLAS storage account.
    """
    logger = get_run_logger()
    logger.info("Starting copy of 10x data")

    copies = []  # List to hold futures for all copy tasks

    # Iterate over all experiments and their file paths in the manifest
    for xp, file_paths in data.items():
        for file_path in file_paths:
            source = f"{SCRNA_URL}/{file_path}"
            destination = f"{DROPBOX_URL}/{xp}/{os.path.basename(file_path)}"
            # Submit the copy_blob task asynchronously
            future = copy_blob.submit(source, destination)
            copies.append(future)

    # Wait for all copy tasks to complete
    wait(copies)

    logger.info("Copy of 10x data completed")


if __name__ == "__main__":
    # Run the Prefect flow if this script is executed directly
    copy_10x()

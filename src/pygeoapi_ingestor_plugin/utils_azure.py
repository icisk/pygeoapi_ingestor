# from azure.storage.blob import BlobServiceClient
import os
from logging import getLogger
import requests
logger = getLogger(__name__)
# Define your storage account name and SAS token
CONTAINER_NAME = os.getenv("CONTAINER_NAME", "")
DATALAKE_STORAGE_ACCOUNT_NAME = os.getenv("DATALAKE_STORAGE_ACCOUNT_NAME", "")
SAS_TOKEN = os.getenv("SAS_TOKEN", "")
FOLDER_NAME = os.getenv("FOLDER_NAME", "")

def upload_file_to_azure(filename, sub_folder=None):
    just_filename = os.path.basename(filename)
    if sub_folder:
        blob_name = f"{FOLDER_NAME}/{sub_folder}/{just_filename}"
    else:
        blob_name = f"{FOLDER_NAME}/{just_filename}"
    
    # Construct the URL
    url = f"https://{DATALAKE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER_NAME}/{blob_name}?{SAS_TOKEN}"

    # Read file content
    with open(filename, "rb") as file_data:
        file_content = file_data.read()

    # Define headers
    headers = {
        "x-ms-blob-type": "BlockBlob",
        "Content-Length": str(len(file_content))
    }

    # Make the REST API request
    response = requests.put(url, headers=headers, data=file_content)

    if response.status_code == 201:
        logger.debug(f"File {blob_name} uploaded successfully!")
    else:
        logger.error(f"Failed to upload file {blob_name}: {response.text}")

# Example usage
# upload_file_to_azure("yourfile.txt")

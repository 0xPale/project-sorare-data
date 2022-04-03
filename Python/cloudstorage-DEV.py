from google.cloud import bigquery
from google.cloud import storage
import os
import getpass

from variables import outputFolderLocal, outputFolderCloud, googleAppCredentialsCloud,\
    outputCSVPlayer, googleAppCredentialsLocal, googleAppCredentialsCloud

if getpass.getuser() == "benjamin":
    googleAppCredentials = googleAppCredentialsLocal
    outputFolder = outputFolderLocal
else:
    googleAppCredentials = googleAppCredentialsCloud
    outputFolder = outputFolderCloud

# This is a way to set the environment variable for the Google Cloud API.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= googleAppCredentials

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

upload_blob("sorare-data", outputFolder + outputCSVPlayer + "player.csv", "player.csv")
upload_blob("sorare-data", outputFolder + outputCSVPlayer + "allSo5Scores.csv", "allSo5Scores.csv")
upload_blob("sorare-data", outputFolder + outputCSVPlayer + "cardSupply.csv", "cardSupply.csv")
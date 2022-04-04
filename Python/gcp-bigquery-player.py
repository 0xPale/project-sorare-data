from google.cloud import bigquery
from google.cloud import storage
import os
import getpass

from variables import outputFolderLocal, outputFolderCloud, googleAppCredentialsCloud,\
    outputCSVPlayer, googleAppCredentialsLocal, googleAppCredentialsCloud

from functions import upload_blob

# This is a way to set the environment variable for the Google Cloud API.
if getpass.getuser() == "benjamin":
    googleAppCredentials = googleAppCredentialsLocal
    outputFolder = outputFolderLocal
else:
    googleAppCredentials = googleAppCredentialsCloud
    outputFolder = outputFolderCloud

# This is a way to set the environment variable for the Google Cloud API.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= googleAppCredentials

# Uploading the files to the cloud.
upload_blob("sorare-data", outputFolder + outputCSVPlayer + "player.csv", "player.csv")
upload_blob("sorare-data", outputFolder + outputCSVPlayer + "allSo5Scores.csv", "allSo5Scores.csv")
upload_blob("sorare-data", outputFolder + outputCSVPlayer + "cardSupply.csv", "cardSupply.csv")


########################################################################################################################
# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "sorare-data-341411.sorare.player"

job_config = bigquery.LoadJobConfig(
    autodetect=True,
    field_delimiter=";",
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)

uri = "gs://sorare-data/player.csv"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))
from google.cloud import bigquery
import os
import getpass

from variables import googleAppCredentialsCloud, googleAppCredentialsLocal

# This is a way to set the environment variable for the Google Cloud API.
if getpass.getuser() == "benjamin":
    googleAppCredentials = googleAppCredentialsLocal
else:
    googleAppCredentials = googleAppCredentialsCloud

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
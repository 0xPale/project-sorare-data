from google.cloud import bigquery
import os
import getpass
import glob

from variables import outputFolderLocal, outputFolderCloud, outputCSVSubscription, \
    googleAppCredentialsLocal, googleAppCredentialsCloud,\
    bucket_name, database_name, schema_name

from functions import list_blobs_with_prefix, move_blob, upload_blob, load_csv_file

# This is a way to set the environment variable for the Google Cloud API.
if getpass.getuser() == "benjamin":
    googleAppCredentials = googleAppCredentialsLocal
    outputFolder = outputFolderLocal
else:
    googleAppCredentials = googleAppCredentialsCloud
    outputFolder = outputFolderCloud

# This is a way to set the environment variable for the Google Cloud API.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= googleAppCredentials

try:
    # Moving the files from the subscription folder to the subscription-done folder.
    for blob in list_blobs_with_prefix(bucket_name, prefix="subscription"):
        move_blob(bucket_name, blob.name, bucket_name, "done/" + blob.name)
except:
    print("No subscription files to move.")

# A way to upload the files to the Google Cloud Storage.
for f in glob.glob(outputFolder + outputCSVSubscription + "*.csv"):
    upload_blob(bucket_name, f, os.path.basename(f))

########################################################################################################################

# Sorting the list of blobs by name.
blobs = list_blobs_with_prefix(bucket_name, prefix="subscription")
#blobs.sort() #AttributeError: 'HTTPIterator' object has no attribute 'sort'

# A way to select the right files to be loaded.
for blob in blobs:
    if "subscriptionCard" in blob.name:
        blob_card = blob.name
    elif "subscriptionTransfer" in blob.name:
        blob_transfer = blob.name
    else:
        pass

# Defining the location of the files to be loaded.
try:
    uri_card = "gs://" + bucket_name + "/" + blob_card #subscriptionCard_DATETIME.csv
except:
    pass
try:
    uri_transfer = "gs://" + bucket_name + "/" + blob_transfer #subscriptionTransfer_DATETIME.csv
except:
    pass

# A way to set the name of the table.
table_id_card = database_name + "." + schema_name + "." + "card"
table_id_transfer = database_name + "." + schema_name + "." + "transfer"

# Setting the parameters for the load job.
job_config_card = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("card_slug", "STRING"),
        bigquery.SchemaField("card_name", "STRING"),
        bigquery.SchemaField("player_slug", "STRING"),
        bigquery.SchemaField("card_rarity", "STRING"),
        bigquery.SchemaField("card_season_startYear", "INTEGER"),
        bigquery.SchemaField("extracted_at", "TIMESTAMP"),
    ],
    field_delimiter=";",
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)

job_config_transfer = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("card_slug", "STRING"),
        bigquery.SchemaField("manager_slug", "STRING"),
        bigquery.SchemaField("manager_nickname", "STRING"),
        bigquery.SchemaField("transfer_date", "TIMESTAMP"),
        bigquery.SchemaField("transfer_type", "STRING"),
        bigquery.SchemaField("transfer_priceETH", "FLOAT"),
        bigquery.SchemaField("transfer_priceFiat_usd", "FLOAT"),
        bigquery.SchemaField("extracted_at", "TIMESTAMP"),
    ],
    field_delimiter=";",
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)

#Card csv loading
# This is a way to avoid errors when the files are not available.
try:
    load_csv_file(uri_card, table_id_card, job_config_card)
except:
    pass
#Transfer csv loading
try:
    load_csv_file(uri_transfer, table_id_transfer, job_config_transfer)
except:
    pass
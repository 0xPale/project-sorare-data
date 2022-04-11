from google.cloud import bigquery
import os
import getpass

from variables import googleAppCredentialsCloud, googleAppCredentialsLocal, bucket_name, database_name, schema_name
from functions import load_csv_file, list_blobs_with_prefix

# This is a way to set the environment variable for the Google Cloud API.
if getpass.getuser() == "benjamin":
    googleAppCredentials = googleAppCredentialsLocal
else:
    googleAppCredentials = googleAppCredentialsCloud

# This is a way to set the environment variable for the Google Cloud API.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= googleAppCredentials

########################################################################################################################

# Defining the location of the files to be loaded.
uri_card = "gs://" + bucket_name + "/" + "card.csv"
uri_transfer = "gs://" + bucket_name + "/" + "transfer.csv"


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
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
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
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)

#Card csv loading
load_csv_file(uri_card, table_id_card, job_config_card)
#Transfer csv loading
load_csv_file(uri_transfer, table_id_transfer, job_config_transfer)

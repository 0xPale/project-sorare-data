from google.cloud import bigquery
import os
import getpass

from variables import googleAppCredentialsCloud, googleAppCredentialsLocal
from functions import load_csv_file

# This is a way to set the environment variable for the Google Cloud API.
if getpass.getuser() == "benjamin":
    googleAppCredentials = googleAppCredentialsLocal
else:
    googleAppCredentials = googleAppCredentialsCloud

# This is a way to set the environment variable for the Google Cloud API.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= googleAppCredentials

########################################################################################################################

uri_player = "gs://sorare-data/player.csv"
uri_cardSupply = "gs://sorare-data/cardSupply.csv"
uri_score = "gs://sorare-data/allSo5Scores.csv"

# A way to set the name of the table.
table_id_player = "sorare-data-341411.sorare.player"
table_id_cardSupply = "sorare-data-341411.sorare.cardSupply"
table_id_score = "sorare-data-341411.sorare.score"

# Setting the parameters for the load job.
job_config_player = bigquery.LoadJobConfig(
    autodetect=True,
    field_delimiter=";",
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)

job_config_cardSupply = bigquery.LoadJobConfig(
    autodetect=True,
    field_delimiter=";",
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)

job_config_score = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("player_slug", "STRING"),
        bigquery.SchemaField("game_fixture_slug", "STRING"),
        bigquery.SchemaField("game_fixture_gameWeek", "FLOAT"),
        bigquery.SchemaField("game_fixture_eventType", "STRING"),
        bigquery.SchemaField("game_date", "TIMESTAMP"),
        bigquery.SchemaField("game_competition_slug", "STRING"),
        bigquery.SchemaField("game_homeTeam_slug", "STRING"),
        bigquery.SchemaField("game_homeGoals", "FLOAT"),
        bigquery.SchemaField("game_awayTeam_slug", "STRING"),
        bigquery.SchemaField("game_awayGoals", "FLOAT"),
        bigquery.SchemaField("gameweek_score", "FLOAT"),
        bigquery.SchemaField("decisive_score", "FLOAT"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("stat", "STRING"),
        bigquery.SchemaField("statValue", "FLOAT"),
        bigquery.SchemaField("points", "FLOAT"),
        bigquery.SchemaField("score", "FLOAT"),
    ],
    field_delimiter=";",
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)

#Player csv loading
load_csv_file(uri_player, table_id_player, job_config_player)
#cardSupply csv loading
load_csv_file(uri_cardSupply, table_id_cardSupply, job_config_cardSupply)
#Scores csv loading
load_csv_file(uri_score, table_id_score, job_config_score)
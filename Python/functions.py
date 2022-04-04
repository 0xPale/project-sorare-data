from google.cloud import bigquery
from google.cloud import storage

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


def load_csv_file(uri, table_id, job_config):
    """Load a csv to google big query."""
    # The path of the csv file in GCS bucket
    # uri = "gs://your-bucket-name/filename.csv"
    # The table in which the file needs to be uploaded
    # table_id = "database.schema.table"
    # The job configuration
    # job_config = bigquery.LoadJobConfig()

    bigquery_client = bigquery.Client()

    load_job = bigquery_client.load_table_from_uri(
    uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bigquery_client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows.".format(
            destination_table.num_rows
        )
    )
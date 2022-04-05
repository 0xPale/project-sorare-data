import os
import getpass
import glob

from variables import outputFolderLocal, outputFolderCloud,\
    outputCSVSubscription, googleAppCredentialsLocal, googleAppCredentialsCloud, bucket_name

from functions import list_blobs_with_prefix, move_blob, upload_blob

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
        move_blob(bucket_name, blob.name, bucket_name + "done/" + blob.name)
except:
    print("No subscription files to move.")

# A way to upload the files to the Google Cloud Storage.
for f in glob.glob(outputFolder + outputCSVSubscription + "*.csv"):
    upload_blob(bucket_name, f, os.path.basename(f))
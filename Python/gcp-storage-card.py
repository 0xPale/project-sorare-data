import os
import getpass
import glob

from variables import outputFolderLocal, outputFolderCloud,\
    outputCardCSV, googleAppCredentialsLocal, googleAppCredentialsCloud, bucket_name

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

# A way to upload the files to the Google Cloud Storage.
for f in glob.glob(outputFolder + outputCardCSV + "*.csv"):
    upload_blob(bucket_name, f, os.path.basename(f))
import os
import getpass

from variables import outputFolderLocal, outputFolderCloud,\
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


bucket_name = "sorare-data"

# Uploading the files to the cloud.
upload_blob(bucket_name, outputFolder + outputCSVPlayer + "player.csv", "player.csv")
upload_blob(bucket_name, outputFolder + outputCSVPlayer + "allSo5Scores.csv", "allSo5Scores.csv")
#upload_blob(bucket_name, outputFolder + outputCSVPlayer + "lastSo5Scores.csv", "lastSo5Scores.csv")
upload_blob(bucket_name, outputFolder + outputCSVPlayer + "cardSupply.csv", "cardSupply.csv")
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
import json
import pandas as pd
import glob
import shutil
import getpass
from datetime import datetime
from variables import outputFolderLocal, outputFolderCloud, outputCSVSubscription, outputJSONSubscription

# This is a way to set the output folder depending on the user.
#     If the user is benjamin, then the output folder is the local one.
#     If the user is not benjamin, then the output folder is the cloud one.
if getpass.getuser() == "benjamin":
    outputFolder = outputFolderLocal
else:
    outputFolder = outputFolderCloud

# Reading all the files in the folder and then it is creating a dataframe for each file.
# Then it is concatenating all the dataframes into one.
read_files = glob.glob(outputFolder + outputJSONSubscription + "*.json")
# Sorting the files in the folder by the date of the file.
read_files.sort()
# Removing the last element of the list.
read_files.pop()

df = pd.DataFrame()

# Reading all the files in the folder and then it is creating a dataframe for each file.
# Then it is concatenating all the dataframes into one.
for file in read_files:
    # Creating a list of dictionaries.
    raw_data = []  
    # Reading the file line by line and then it is appending the data into a list.
    with open(file, 'r') as current_file:
        for line in current_file:
            raw_data.append(json.loads(line))
    # Concatenating the dataframes into one.
    df_curr = pd.json_normalize(raw_data, sep="_")
    df = pd.concat([df, df_curr], sort=True)
    # Moving the file to the done folder.
    shutil.move(file, outputFolder + outputJSONSubscription + "done/")

# Dropping all the rows that have a NaN value in the column
# message_result_data_publicMarketWasUpdated_transfer_transfer_type.
df = df.dropna(subset=['message_result_data_publicMarketWasUpdated_transfer_transfer_type'])

df = df.reindex(columns=[
    "message_result_data_publicMarketWasUpdated_card_slug",
    "message_result_data_publicMarketWasUpdated_transfer_sorareAccount_manager_slug",
    "message_result_data_publicMarketWasUpdated_transfer_sorareAccount_manager_nickname",
    "message_result_data_publicMarketWasUpdated_transfer_transfer_date",
    "message_result_data_publicMarketWasUpdated_transfer_transfer_type",
    "message_result_data_publicMarketWasUpdated_transfer_transfer_priceETH",
    "message_result_data_publicMarketWasUpdated_transfer_transfer_priceFiat_usd"])

df = df.rename(columns={
    "message_result_data_publicMarketWasUpdated_card_slug": "card_slug",
    "message_result_data_publicMarketWasUpdated_transfer_sorareAccount_manager_slug": "manager_slug",
    "message_result_data_publicMarketWasUpdated_transfer_sorareAccount_manager_nickname": "manager_nickname",
    "message_result_data_publicMarketWasUpdated_transfer_transfer_date": "transfer_date",
    "message_result_data_publicMarketWasUpdated_transfer_transfer_type": "transfer_type",
    "message_result_data_publicMarketWasUpdated_transfer_transfer_priceETH": "transfer_priceETH",
    "message_result_data_publicMarketWasUpdated_transfer_transfer_priceFiat_usd": "transfer_priceFiat_usd"})

#Export csv
df.to_csv(outputFolder + outputCSVSubscription + "subscriptionTransfer_" + datetime.now().isoformat() + ".csv", sep=";", index= False)
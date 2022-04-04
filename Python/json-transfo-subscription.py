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

# Moving the files to the done folder.
for f in glob.glob(outputFolder + outputCSVSubscription + "*.csv"):
    shutil.move(f, outputFolder + outputCSVSubscription + "done/")

# Creating a timestamp of the current date and time.
extractionDate = datetime.now().isoformat()

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

# Adding a new column to the dataframe with the date of the extraction.
df['extracted_at'] = extractionDate

# Creating a new dataframe with the columns that we want.
df_card = df.reindex(columns=[
    "message_result_data_publicMarketWasUpdated_card_slug",
    "message_result_data_publicMarketWasUpdated_card_name",
    "message_result_data_publicMarketWasUpdated_player_slug",
    "message_result_data_publicMarketWasUpdated_card_rarity",
    "message_result_data_publicMarketWasUpdated_card_season_startYear",
    "extracted_at"
    ])

# Renaming the columns.
df_card = df_card.rename(columns={
    "message_result_data_publicMarketWasUpdated_card_slug": "card_slug",
    "message_result_data_publicMarketWasUpdated_card_name": "card_name",
    "message_result_data_publicMarketWasUpdated_player_slug": "player_slug",
    "message_result_data_publicMarketWasUpdated_card_rarity": "card_rarity",
    "message_result_data_publicMarketWasUpdated_card_season_startYear": "card_season_startYear"
    })

df_transfer = df.reindex(columns=[
    "message_result_data_publicMarketWasUpdated_card_slug",
    "message_result_data_publicMarketWasUpdated_transfer_sorareAccount_manager_slug",
    "message_result_data_publicMarketWasUpdated_transfer_sorareAccount_manager_nickname",
    "message_result_data_publicMarketWasUpdated_transfer_transfer_date",
    "message_result_data_publicMarketWasUpdated_transfer_transfer_type",
    "message_result_data_publicMarketWasUpdated_transfer_transfer_priceETH",
    "message_result_data_publicMarketWasUpdated_transfer_transfer_priceFiat_usd",
    "extracted_at"
    ])

df_transfer = df_transfer.rename(columns={
    "message_result_data_publicMarketWasUpdated_card_slug": "card_slug",
    "message_result_data_publicMarketWasUpdated_transfer_sorareAccount_manager_slug": "manager_slug",
    "message_result_data_publicMarketWasUpdated_transfer_sorareAccount_manager_nickname": "manager_nickname",
    "message_result_data_publicMarketWasUpdated_transfer_transfer_date": "transfer_date",
    "message_result_data_publicMarketWasUpdated_transfer_transfer_type": "transfer_type",
    "message_result_data_publicMarketWasUpdated_transfer_transfer_priceETH": "transfer_priceETH",
    "message_result_data_publicMarketWasUpdated_transfer_transfer_priceFiat_usd": "transfer_priceFiat_usd"
    })

#Export csv
df_card.to_csv(outputFolder + outputCSVSubscription + "subscriptionCard_" + extractionDate + ".csv", sep=";", index= False)
df_transfer.to_csv(outputFolder + outputCSVSubscription + "subscriptionTransfer_" + extractionDate + ".csv", sep=";", index= False)
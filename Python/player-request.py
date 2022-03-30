from datetime import datetime
import requests
import json
import time
import pandas as pd
#Parameters from variables
from variables import outputFolder, outputCSV, outputJSON, endpoint, headers, queryPlayer

#Time
start_time = time.time()

# Reading the csv file with the player data and creating a dataframe with the data.
df_player = pd.read_csv(outputFolder + outputCSV + "player_deduplicate.csv", sep=";")

# Removing duplicates from the list of players.
playerList = list(dict.fromkeys(df_player["player_slug"].tolist()))
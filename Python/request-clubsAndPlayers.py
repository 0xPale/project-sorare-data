from datetime import datetime
import requests
import json
import time
import os
import getpass
import pandas as pd
#Parameters from variables
from variables import outputFolderLocal, outputFolderCloud, outputClubsAndPlayers, endpoint, headers, queryClub, queryActivePlayers

if getpass.getuser() == "benjamin":
    outputFolder = outputFolderLocal
else:
    outputFolder = outputFolderCloud

try:
    os.remove(outputFolder + outputClubsAndPlayers + "playersFromClubs.csv")
    print("Previous file removed")
except:
    pass

#Time
start_time = time.time()

################## Get list of clubs ready ##################

# A request to the endpoint with the query and the headers.
r = requests.post(endpoint, json={"query": queryClub}, headers=headers)
if r.status_code == 200: #success
#Transforme l'output de la commande requests en format json pour être exploité
    raw_data = r.json()
else:
    raise Exception(f"Query failed to run with a {r.status_code} + {r.headers}.")

# Creating a list of clubs from the dataframe.
dic_clubs = raw_data["data"]["clubsReady"]
df_clubs = pd.DataFrame(dic_clubs)
clubs = df_clubs["slug"].tolist()

########################################################################################################################
################## Get list of active players from clubs ready ##################

for club in clubs:
    r = requests.post(endpoint, json={"query": queryActivePlayers, "variables": {"clubSlug": club}}, headers=headers)
    if r.status_code == 200: #success
        raw_data = r.json()
        time.sleep(3)
    elif r.status_code == 429: #Time out
      time.sleep(60)
      print("Status code 429 - Waited for 30s for iteration ")
    elif r.status_code == 502: #Time out
      time.sleep(60)
      print("Status code 502 - Waited for 30s for iteration ")
    else:
      raise Exception(f"Query failed to run with a {r.status_code} + {r.headers}.")

    # A way to check if the club has a next page of players. If it has, it will print the club name
    # and the word "has next page: True".
    #         If it doesn't, it will pass.
    if raw_data["data"]["club"]["activePlayers"]["pageInfo"]["hasNextPage"] == True:
        print(club + " has next page: True ")
    else:
        pass

    # Creating a dataframe from the list of players and then exporting it to a csv file.
    dic_players = raw_data["data"]["club"]["activePlayers"]["nodes"]
    df_players = pd.DataFrame(dic_players)
    df_players.to_csv(outputFolder + outputClubsAndPlayers + "playersFromClubs.csv", index=False, mode='a', header=not os.path.exists(outputFolder + outputClubsAndPlayers + "playersFromClubs.csv"))

#Time
print("--- %s seconds ---" % (time.time() - start_time))
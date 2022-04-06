from datetime import datetime
import requests
import json
import time
import pandas as pd
import getpass
#Parameters from variables
from variables import outputFolderLocal, outputFolderCloud, outputClubsAndPlayers, outputJSONPlayerLast, endpoint, headers, queryPlayerLast5

if getpass.getuser() == "benjamin":
    outputFolder = outputFolderLocal
else:
    outputFolder = outputFolderCloud

#Time
start_time = time.time()

# Reading the csv file with the player data and creating a dataframe with the data.
df_player = pd.read_csv(outputFolder + outputClubsAndPlayers + "playersFromClubs.csv", sep=";")

# Removing duplicates from the list of players.
playerList = list(dict.fromkeys(df_player["slug"].tolist()))

# Counting the number of players in the list of players.
lenPlayerList = len(playerList)

sliceStart = 0
sliceEnd = 100

# A loop that will run until sliceEnd is smaller than the length of the list of players.
while sliceEnd < lenPlayerList:

    # Slicing the list of players to get the first 100 players, then the next 100 players, etc.
    playerListSliced = playerList[sliceStart:sliceEnd]

    # Sending a request to the API to get the data.
    r = requests.post(endpoint, json={"query": queryPlayerLast5, "variables": {"playerList": playerListSliced}}, headers=headers)
    if r.status_code == 200: #success
        # Getting the data from the API and storing it in a dictionary.
        raw_data = r.json()
        dic = raw_data["data"]["players"]

        # This is a way to write a json file.
        with open(outputFolder + outputJSONPlayerLast +'lastPlayerScore_' + str(sliceStart) + '.json', 'w') as json_file:
            json.dump(dic, json_file)
    
        # Adding 100 to the sliceStart and sliceEnd variables.
        sliceStart = sliceStart + 100
        sliceEnd = sliceEnd + 100
        time.sleep(5)
    
    elif r.status_code == 429: #Time out
      time.sleep(60)
      print("Status code 429 - Waited for 30s")
  
    elif r.status_code == 502: #Time out
      time.sleep(60)
      print("Status code 502 - Waited for 30s")

    # This is a way to handle errors. If the request is not successful, the code will raise an
    # exception.
    else:
        raise Exception(f"Query failed to run with a {r.status_code} + {r.headers}.")

# Out of the loop, we restart for the last slice of players until the end of the list.
playerListSliced = playerList[sliceStart:lenPlayerList+1]

r = requests.post(endpoint, json={"query": queryPlayerLast5, "variables": {"playerList": playerListSliced}}, headers=headers)
if r.status_code == 200: #success
    raw_data = r.json()
    dic = raw_data["data"]["players"]
    with open(outputFolder + outputJSONPlayerLast + 'lastPlayerScore_' + str(sliceStart) + '.json', 'w') as json_file:
        json.dump(dic, json_file)

else:
    raise Exception(f"Query failed to run with a {r.status_code} + {r.headers}.")

#Time
print("--- %s seconds ---" % (time.time() - start_time))
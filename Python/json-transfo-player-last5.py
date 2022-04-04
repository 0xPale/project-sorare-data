import json
import pandas as pd
import time
import glob
import os
import getpass
from datetime import datetime
from variables import outputFolderLocal, outputFolderCloud, outputCSVPlayer, outputJSONPlayerLast

if getpass.getuser() == "benjamin":
    outputFolder = outputFolderLocal
else:
    outputFolder = outputFolderCloud

# Deleting the files in the folder.
for f in glob.glob(outputFolder + outputCSVPlayer + "*.csv"):
    os.remove(f)

# Creating a date and time for the extraction.
extractionDate = datetime.now().isoformat()

# Reading all the files in the folder.
read_files = glob.glob(outputFolder + outputJSONPlayerLast + "*.json")

# Reading all the files in the folder and then it is creating a dataframe for each file.
# Then it is concatenating all the dataframes into one.
for file in read_files:     

    #Time
    start_time = time.time()
    df = pd.DataFrame()
    df_cardSupply = pd.DataFrame()
    df_player = pd.DataFrame()
    df_lastSo5Scores = pd.DataFrame()

    with open(file, 'r') as current_file:
        current_object = json.load(current_file)
    df = pd.json_normalize(current_object, sep="_")

    ####################################################################################################################################

    #On commence par créer les dataframes correspondant à chaque subset en sélectionnant uniquement les colonnes utiles

    # A way to reindex the columns of the dataframe.
    df_player = df.reindex(columns=[
        "player_slug", "player_name", "age", "birthDate", "position", "followers", "status_playingStatus",
        "club_slug", "club_code", "club_name", "club_league_slug", "club_league_name", "club_country_slug", "club_country_code",
        "stats_appearances", "stats_minutesPlayed", "stats_assists", "stats_goals", "stats_yellowCards", "stats_redCards", "stats_substituteIn", "stats_substituteOut", 
        "status_lastFiveSo5Appearances", "status_lastFiveSo5AverageScore", "status_lastFifteenSo5Appearances", "status_lastFifteenSo5AverageScore"
        ])

    df_cardSupply = df.reindex(columns=["player_slug", "cardSupply"])

    df_lastSo5Scores = df.reindex(columns=["player_slug", "so5Scores"])

    ####################################################################################################################################
    #On explode les colonne qui contiennent une liste de dict, ce qui nous permet d'obtenir une nouvelle ligne de données par dict de la liste
    #On reset au passage l'index pour qu'il soit correct de 0 à n-1

    # Exploding the column cardSupply into several rows.
    df_cardSupply = df_cardSupply.explode("cardSupply", ignore_index=True)

    # Exploding the column allSo5Scores_nodes into several rows.
    df_lastSo5Scores = df_lastSo5Scores.explode("so5Scores", ignore_index=True)

    ####################################################################################################################################

    #See https://stackoverflow.com/questions/38231591/split-explode-a-column-of-dictionaries-into-separate-columns-with-pandas/63311361#63311361
    #On applique de nouveau un normalize sur la colonne qu'on a explode pour obtenir une colonne par key du dict et on join sur le dataframe initial

    # Creating a new column for each key of the dict in the column allSo5Scores_nodes.
    df_cardSupply = df_cardSupply.join(pd.json_normalize(df_cardSupply.cardSupply, sep="_"))
    df_lastSo5Scores = df_lastSo5Scores.join(pd.json_normalize(df_lastSo5Scores.so5Scores, sep="_"))

    # Renaming the column score to gameweek_score.
    df_lastSo5Scores = df_lastSo5Scores.rename(columns={"score": "gameweek_score"})


    # Creating a new column for each key of the dict in the column allSo5Scores_nodes.
    df_lastSo5Scores = df_lastSo5Scores.explode("detailedScore", ignore_index=True)
    df_lastSo5Scores = df_lastSo5Scores.join(pd.json_normalize(df_lastSo5Scores.detailedScore, sep="_"))

    ####################################################################################################################################
    #On drop la colonne qui contenait le dict de valeurs / de keys et qui est maintenant inutile puis on export sous csv
    # Dropping the column and player_cardSupply and player_allSo5Scores_nodes. On drop aussi les colonnes syétamtiquement vides.

    # Dropping the column cardSupply and allSo5Scores_nodes.
    df_cardSupply = df_cardSupply.drop(columns=["cardSupply"])
    df_lastSo5Scores = df_lastSo5Scores.drop(columns=["so5Scores", "detailedScore", "game_fixture"])

    # A way to reindex the columns of the dataframe.
    df_cardSupply = df_cardSupply.reindex(columns=["player_slug","cardSupply_season", "cardSupply_limited","cardSupply_rare","cardSupply_superRare","cardSupply_unique"])
    df_lastSo5Scores = df_lastSo5Scores.reindex(columns=[
        "player_slug","game_fixture_slug","game_fixture_gameWeek","game_fixture_eventType", "game_date", 
        "game_competition_slug", "game_homeTeam_slug", "game_homeGoals", "game_awayTeam_slug", "game_awayGoals",
        "gameweek_score", "decisive_score", "category", "stat", "statValue", "points", "score"])
    
    # Adding a new column to the dataframe with the date of the extraction.
    df_player['extracted_at'] = extractionDate
    df_cardSupply['extracted_at'] = extractionDate
    df_lastSo5Scores['extracted_at'] = extractionDate

    #Export csv
    df_player.to_csv(outputFolder + outputCSVPlayer + "player.csv", sep=";", index= False, mode='a', header=not os.path.exists(outputFolder + outputCSVPlayer + "player.csv"))
    df_cardSupply.to_csv(outputFolder + outputCSVPlayer + "cardSupply.csv", sep=";", index= False, mode='a', header=not os.path.exists(outputFolder + outputCSVPlayer + "cardSupply.csv"))
    df_lastSo5Scores.to_csv(outputFolder + outputCSVPlayer + "lastSo5Scores.csv", sep=";", index= False, mode='a', header=not os.path.exists(outputFolder + outputCSVPlayer + "lastSo5Scores.csv"))

    #Time
    print("--- %s seconds ---" % (time.time() - start_time))
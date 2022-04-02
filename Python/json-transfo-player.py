import json
import pandas as pd
import time
import glob
import pickle
import os
from pathlib import Path
from variables import outputFolder, outputCSV, outputJSONPlayer

# Reading all the files in the folder.
read_files = glob.glob(outputFolder + outputJSONPlayer + "*.json")

# Reading all the files in the folder and then it is creating a dataframe for each file.
# Then it is concatenating all the dataframes into one.
for file in read_files:     

    #Time
    start_time = time.time()
    df = pd.DataFrame()
    df_cardSupply = pd.DataFrame()
    df_player = pd.DataFrame()
    df_allSo5Scores = pd.DataFrame()

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

    df_allSo5Scores = df.reindex(columns=["player_slug", "allSo5Scores_nodes"])

    ####################################################################################################################################
    #On explode les colonne qui contiennent une liste de dict, ce qui nous permet d'obtenir une nouvelle ligne de données par dict de la liste
    #On reset au passage l'index pour qu'il soit correct de 0 à n-1

    # Exploding the column cardSupply into several rows.
    df_cardSupply = df_cardSupply.explode("cardSupply", ignore_index=True)

    # Exploding the column allSo5Scores_nodes into several rows.
    df_allSo5Scores = df_allSo5Scores.explode("allSo5Scores_nodes", ignore_index=True)

    ####################################################################################################################################

    #See https://stackoverflow.com/questions/38231591/split-explode-a-column-of-dictionaries-into-separate-columns-with-pandas/63311361#63311361
    #On applique de nouveau un normalize sur la colonne qu'on a explode pour obtenir une colonne par key du dict et on join sur le dataframe initial

    # Creating a new column for each key of the dict in the column allSo5Scores_nodes.
    df_cardSupply = df_cardSupply.join(pd.json_normalize(df_cardSupply.cardSupply, sep="_"))
    df_allSo5Scores = df_allSo5Scores.join(pd.json_normalize(df_allSo5Scores.allSo5Scores_nodes, sep="_"))

    # Renaming the column score to gameweek_score.
    df_allSo5Scores = df_allSo5Scores.rename(columns={"score": "gameweek_score"})


    # Creating a new column for each key of the dict in the column allSo5Scores_nodes.
    df_allSo5Scores = df_allSo5Scores.explode("detailedScore", ignore_index=True)
    df_allSo5Scores = df_allSo5Scores.join(pd.json_normalize(df_allSo5Scores.detailedScore, sep="_"))

    ####################################################################################################################################
    #On drop la colonne qui contenait le dict de valeurs / de keys et qui est maintenant inutile puis on export sous csv
    # Dropping the column and player_cardSupply and player_allSo5Scores_nodes. On drop aussi les colonnes syétamtiquement vides.

    # Dropping the column cardSupply and allSo5Scores_nodes.
    df_cardSupply = df_cardSupply.drop(columns=["cardSupply"])
    df_allSo5Scores = df_allSo5Scores.drop(columns=["allSo5Scores_nodes", "detailedScore", "game_fixture"])

# A way to reindex the columns of the dataframe.
    df_cardSupply = df_cardSupply.reindex(columns=["player_slug","cardSupply_season", "cardSupply_limited","cardSupply_rare","cardSupply_superRare","cardSupply_unique"])
    df_allSo5Scores = df_allSo5Scores.reindex(columns=[
        "player_slug","game_fixture_slug","game_fixture_gameWeek","game_fixture_eventType", "game_date", 
        "game_competition_slug", "game_homeTeam_slug", "game_homeGoals", "game_awayTeam_slug", "game_awayGoals",
        "gameweek_score", "decisive_score", "category", "stat", "statValue", "points", "score"])

    #Export csv
    df_player.to_csv(outputFolder + outputCSV + "player.csv", sep=";", index= False, mode='a', header=not os.path.exists(outputFolder + outputCSV + "player.csv"))
    df_cardSupply.to_csv(outputFolder + outputCSV + "cardSupply.csv", sep=";", index= False, mode='a', header=not os.path.exists(outputFolder + outputCSV + "cardSupply.csv"))
    df_allSo5Scores.to_csv(outputFolder + outputCSV + "allSo5Scores.csv", sep=";", index= False, mode='a', header=not os.path.exists(outputFolder + outputCSV + "allSo5Scores.csv"))

    #Time
    print("--- %s seconds ---" % (time.time() - start_time))
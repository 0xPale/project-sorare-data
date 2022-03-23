import json
import pandas as pd
import time
import glob
from pathlib import Path
from variables import outputFolder, outputCSV, outputJSON

# Storing the current time in seconds since the Epoch.
start_time = time.time()

# Reading all the files in the folder and subfolders.
read_files = glob.glob(outputFolder + outputJSON + "*.json")

# It creates a new dataframe.
df = pd.DataFrame()

# Reading the files and creating a dataframe for each file.
for f in read_files:
    datetimeFromFilename = Path(f).stem.split("_")[-1] #Path().stem give the filename without extension and then we return the last element [-1] of the list created by the split
    with open(f, 'r') as current_file:
        #raw = current_file.read()
        #current_object = json.loads(raw)
        current_object = json.load(current_file)
    df_curr = pd.json_normalize(current_object, sep="_")
    df_curr['extracted_at'] = datetimeFromFilename
    df = pd.concat([df, df_curr])

#On remet la date extracted_at qui indique quand est-ce que la ligne a été extracted depuis l'API en format datetime
df["extracted_at"] = pd.to_datetime(df["extracted_at"])

#On normalise le json pour le stocker dans un dataframe
#df=pd.json_normalize(json_list, sep="_")

####################################################################################################################################
####################################################################################################################################

#On commence par créer les dataframes correspondant à chaque subset en sélectionnant uniquement les colonnes utiles

#On crée le premier output de sortie (la première table) qui ne contiendra que les data core de type dimension pour les cards
df_card = df[["card_slug", "card_name", "card_rarity", "card_season_startYear", "player_slug", "extracted_at"]]

#Deuxième dataframe de type core pour la liste des players et leurs info (pour éviter de les répéter sur toutes les cards)
#df_player --> regexr.com/6girp
# Creating a new dataframe with only the columns that match the regex.
df_player = df.filter(regex="player_name|player_slug|player_position|player_age|player_birthDate|player_appearances|player_followers|player_club.*|player_stat.*|extracted_at", axis=1)

#df_userOwnersWithRate --> regexr.com/6gira
# It creates a new dataframe with only the columns that match the regex.
df_transfer = df[["card_slug", "transfer", "extracted_at"]]

#df_cardSupply
df_cardSupply = df[["player_slug", "player_cardSupply", "extracted_at"]]

#player_allSo5Scores_nodes
df_allSo5Scores = df[["player_slug", "player_allSo5Scores_nodes", "extracted_at"]]

####################################################################################################################################
#On explode les colonne qui contiennent une liste de dict, ce qui nous permet d'obtenir une nouvelle ligne de données par dict de la liste
#On reset au passage l'index pour qu'il soit correct de 0 à n-1

# A way to create a new dataframe from a column that contains a list of dictionaries.
df_transfer = df_transfer.explode("transfer", ignore_index=True)

df_cardSupply = df_cardSupply.explode("player_cardSupply", ignore_index=True)

df_allSo5Scores = df_allSo5Scores.explode("player_allSo5Scores_nodes", ignore_index=True)

####################################################################################################################################

#See https://stackoverflow.com/questions/38231591/split-explode-a-column-of-dictionaries-into-separate-columns-with-pandas/63311361#63311361
#On applique de nouveau un normalize sur la colonne qu'on a explode pour obtenir une colonne par key du dict et on join sur le dataframe initial
# A way to create a new dataframe from a column that contains a list of dictionaries.
df_transfer = df_transfer.join(pd.json_normalize(df_transfer.transfer, sep="_"))

df_cardSupply = df_cardSupply.join(pd.json_normalize(df_cardSupply.player_cardSupply, sep="_"))

df_allSo5Scores = df_allSo5Scores.join(pd.json_normalize(df_allSo5Scores.player_allSo5Scores_nodes, sep="_"))

####################################################################################################################################
#On drop la colonne qui contenait le dict de valeurs / de keys et qui est maintenant inutile puis on export sous csv
# Dropping the column transfer and player_cardSupply and player_allSo5Scores_nodes.
df_transfer.drop(columns=['transfer'], inplace=True)
df_cardSupply.drop(columns=['player_cardSupply'], inplace=True)
df_allSo5Scores.drop(columns=['player_allSo5Scores_nodes'], inplace=True)

#On drop les valeurs vides de transfert qui sont inutiles
# It drops the rows where the transfer_type is NaN.
df_transfer.dropna(subset=['transfer_type'], inplace=True)

#On drop les duplicate qui peuvent apparaître dans les data liées aux players (car elles vont être dupliquées pour chaque card de ce player)
# It drops the duplicate rows.
df_player.drop_duplicates(inplace=True)
df_cardSupply.drop_duplicates(inplace=True)
df_allSo5Scores.drop_duplicates(inplace=True)

#Export csv
# A way to save the dataframe in a csv file.
df_card.to_csv(outputFolder + outputCSV + "card.csv", sep=";", index= False)
df_player.to_csv(outputFolder + outputCSV + "player.csv", sep=";", index= False)
df_transfer.to_csv(outputFolder + outputCSV + "transfer.csv", sep=";", index= False)
df_cardSupply.to_csv(outputFolder + outputCSV + "cardSupply.csv", sep=";", index= False)
df_allSo5Scores.to_csv(outputFolder + outputCSV + "allSo5Scores.csv", sep=";", index= False)


#Time
print("--- %s seconds ---" % (time.time() - start_time))
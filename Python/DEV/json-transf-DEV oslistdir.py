import json
import pandas as pd
import time
import glob
import os
from variables import output_folder

#Time
start_time = time.time()

# returns a list with all the files in source
folder = os.listdir(output_folder + "dev/") 
dsStore_exists = ".DS_Store" in folder

if dsStore_exists == True:
    dsStore_index = folder.index(".DS_Store")
    folder.pop(dsStore_index)  
else:
    time.sleep(1)       

df = pd.DataFrame()

#while folder:                             # True if there are any files, False if empty list
for i in range(100):                    # 5 files at a time 
    file = folder[0]                    # select the first file's name
    file_path = output_folder + "dev/" + file    # creates a string - full path to the file                           
    with open(file_path, 'r') as current_file:
        #raw = current_file.read()
        #current_object = json.loads(raw)
        current_object = json.load(current_file)
        df_curr = pd.json_normalize(current_object, sep="_")
        df = pd.concat([df, df_curr])
    folder.pop(0)       # Remove the moved file from the list

#Time
print("--- Complete DataFrame concatened in %s seconds ---" % (time.time() - start_time))

#On normalise le json pour le stocker dans un dataframe
#df=pd.json_normalize(json_list, sep="_")

####################################################################################################################################
####################################################################################################################################

#On commence par créer les dataframes correspondant à chaque subset en sélectionnant uniquement les colonnes utiles

#On crée le premier output de sortie (la première table) qui ne contiendra que les data core de type dimension pour les cards
df_card = df[["card_slug", "card_name", "card_rarity", "card_season_startYear", "player_slug"]]
print("--- df_card in %s seconds ---" % (time.time() - start_time))

#Deuxième dataframe de type core pour la liste des players et leurs info (pour éviter de les répéter sur toutes les cards)
#df_player --> regexr.com/6girp
df_player = df.filter(regex="player_name|player_slug|player_position|player_age|player_birthDate|player_appearances|player_followers|player_club.*|player_stat.*", axis=1)
print("--- df_player in %s seconds ---" % (time.time() - start_time))

#df_userOwnersWithRate --> regexr.com/6gira
df_transfer = df[["card_slug", "transfer"]]
print("--- df_transfer in %s seconds ---" % (time.time() - start_time))

#df_cardSupply
df_cardSupply = df[["player_slug", "player_cardSupply"]]
print("--- df_cardSupply in %s seconds ---" % (time.time() - start_time))

#player_allSo5Scores_nodes
df_allSo5Scores = df[["player_slug", "player_allSo5Scores_nodes"]]
print("--- df_allSo5Scores in %s seconds ---" % (time.time() - start_time))

####################################################################################################################################
#On explode les colonne qui contiennent une liste de dict, ce qui nous permet d'obtenir une nouvelle ligne de données par dict de la liste
#On reset au passage l'index pour qu'il soit correct de 0 à n-1

df_transfer = df_transfer.explode("transfer", ignore_index=True)
print("--- df_transfer exploded in %s seconds ---" % (time.time() - start_time))

df_cardSupply = df_cardSupply.explode("player_cardSupply", ignore_index=True)
print("--- df_cardSupply exploded in %s seconds ---" % (time.time() - start_time))

df_allSo5Scores = df_allSo5Scores.explode("player_allSo5Scores_nodes", ignore_index=True)
print("--- df_allSo5Scores exploded in %s seconds ---" % (time.time() - start_time))

####################################################################################################################################

#See https://stackoverflow.com/questions/38231591/split-explode-a-column-of-dictionaries-into-separate-columns-with-pandas/63311361#63311361
#On applique de nouveau un normalize sur la colonne qu'on a explode pour obtenir une colonne par key du dict et on join sur le dataframe initial
df_transfer = df_transfer.join(pd.json_normalize(df_transfer.transfer, sep="_"))
print("--- df_transfer normalize in %s seconds ---" % (time.time() - start_time))
df_cardSupply = df_cardSupply.join(pd.json_normalize(df_cardSupply.player_cardSupply, sep="_"))
print("--- df_cardSupply normalize in %s seconds ---" % (time.time() - start_time))
df_allSo5Scores = df_allSo5Scores.join(pd.json_normalize(df_allSo5Scores.player_allSo5Scores_nodes, sep="_"))
print("--- df_allSo5Scores normalize in %s seconds ---" % (time.time() - start_time))

####################################################################################################################################
#On drop la colonne qui contenait le dict de valeurs / de keys et qui est maintenant inutile puis on export sous csv
df_transfer.drop(columns=['transfer'], inplace=True)
df_cardSupply.drop(columns=['player_cardSupply'], inplace=True)
df_allSo5Scores.drop(columns=['player_allSo5Scores_nodes'], inplace=True)
print("--- drop columns in %s seconds ---" % (time.time() - start_time))

#On drop les valeurs vides de transfert qui sont inutiles
df_transfer.dropna(subset=['transfer_type'], inplace=True)
print("--- dropna in %s seconds ---" % (time.time() - start_time))

#On drop les duplicate qui peuvent apparaître dans les data liées aux players (car elles vont être dupliquées pour chaque card de ce player)
df_player.drop_duplicates(inplace=True)
df_cardSupply.drop_duplicates(inplace=True)
df_allSo5Scores.drop_duplicates(inplace=True)
print("--- drop duplicates in %s seconds ---" % (time.time() - start_time))

#Export csv
df_card.to_csv(output_folder + "csv/card.csv", sep=";", index= False, mode='a', header=not os.path.exists(output_folder + "csv/card.csv"))
df_player.to_csv(output_folder + "csv/player.csv", sep=";", index= False, mode='a', header=not os.path.exists(output_folder + "csv/player.csv"))
df_transfer.to_csv(output_folder + "csv/transfer.csv", sep=";", index= False, mode='a', header=not os.path.exists(output_folder + "csv/transfer.csv"))
df_cardSupply.to_csv(output_folder + "csv/cardSupply.csv", sep=";", index= False, mode='a', header=not os.path.exists(output_folder + "csv/cardSupply.csv"))
df_allSo5Scores.to_csv(output_folder + "csv/allSo5Scores.csv", sep=";", index= False, mode='a', header=not os.path.exists(output_folder + "csv/allSo5Scores.csv"))


#Time
print("--- %s seconds ---" % (time.time() - start_time))
import json
import pandas as pd
import time
import glob
import pickle
import os
from variables import output_folder

#Time
start_time = time.time()

# returns a list with all the files in source
read_files = glob.glob(output_folder + "json/*.json")
len_read_files = len(read_files)

while read_files:  # True if there are any files, False if empty list

    while_time = time.time()

    df = pd.DataFrame()
    
    for i in range(min(len_read_files, 200)):   # xx files at a time 
        file = read_files[0]     # select the first file's name                      
        with open(file, 'r') as current_file:
            current_object = json.load(current_file)
            df_curr = pd.json_normalize(current_object, sep="_")
            df = pd.concat([df, df_curr])
        read_files.pop(0)       # Remove the moved file from the list
    
    #Store len(list) to avoid errors on the next while if len(list) is < range(xx)
    len_read_files = len(read_files)

    #We save the current list in pickle (binary format used by pyton) to reuse in case of error
    with open(output_folder + "currentReadFilesList/read_files_list", 'wb') as currentReadFilesList:
        pickle.dump(read_files, currentReadFilesList)

    ####################################################################################################################################
    ####################################################################################################################################

    #On commence par créer les dataframes correspondant à chaque subset en sélectionnant uniquement les colonnes utiles

    #On crée le premier output de sortie (la première table) qui ne contiendra que les data core de type dimension pour les cards
    df_card = df[["card_slug", "card_name", "card_rarity", "card_season_startYear", "player_slug"]]

    #Deuxième dataframe de type core pour la liste des players et leurs info (pour éviter de les répéter sur toutes les cards)
    #df_player --> regexr.com/6girp
    df_player = df.filter(regex="player_name|player_slug|player_position|player_age|player_birthDate|player_appearances|player_followers|player_club.*|player_stat.*", axis=1)

    #df_userOwnersWithRate --> regexr.com/6gira
    df_transfer = df[["card_slug", "transfer"]]

    #df_cardSupply
    df_cardSupply = df[["player_slug", "player_cardSupply"]]

    #player_allSo5Scores_nodes
    df_allSo5Scores = df[["player_slug", "player_allSo5Scores_nodes"]]

    ####################################################################################################################################
    #On explode les colonne qui contiennent une liste de dict, ce qui nous permet d'obtenir une nouvelle ligne de données par dict de la liste
    #On reset au passage l'index pour qu'il soit correct de 0 à n-1

    df_transfer = df_transfer.explode("transfer", ignore_index=True)

    df_cardSupply = df_cardSupply.explode("player_cardSupply", ignore_index=True)

    df_allSo5Scores = df_allSo5Scores.explode("player_allSo5Scores_nodes", ignore_index=True)

    ####################################################################################################################################

    #See https://stackoverflow.com/questions/38231591/split-explode-a-column-of-dictionaries-into-separate-columns-with-pandas/63311361#63311361
    #On applique de nouveau un normalize sur la colonne qu'on a explode pour obtenir une colonne par key du dict et on join sur le dataframe initial
    df_transfer = df_transfer.join(pd.json_normalize(df_transfer.transfer, sep="_"))
    df_cardSupply = df_cardSupply.join(pd.json_normalize(df_cardSupply.player_cardSupply, sep="_"))
    df_allSo5Scores = df_allSo5Scores.join(pd.json_normalize(df_allSo5Scores.player_allSo5Scores_nodes, sep="_"))

    ####################################################################################################################################
    #On drop la colonne qui contenait le dict de valeurs / de keys et qui est maintenant inutile puis on export sous csv
    df_transfer.drop(columns=['transfer'], inplace=True)
    df_cardSupply.drop(columns=['player_cardSupply'], inplace=True)
    df_allSo5Scores.drop(columns=['player_allSo5Scores_nodes'], inplace=True)

    ####################################################################################################################################
    #On drop certaines colonnes inutiles
    

    #On drop les valeurs vides de transfert qui sont inutiles
    df_transfer.dropna(subset=['transfer_type'], inplace=True)

    #On drop les duplicate qui peuvent apparaître dans les data liées aux players (car elles vont être dupliquées pour chaque card de ce player)
    df_player = df_player.drop_duplicates()
    df_cardSupply = df_cardSupply.drop_duplicates()
    df_allSo5Scores = df_allSo5Scores.drop_duplicates()

    #Export csv
    df_card.to_csv(output_folder + "csv/card.csv", sep=";", index= False, mode='a', header=not os.path.exists(output_folder + "csv/card.csv"))
    df_player.to_csv(output_folder + "csv/player.csv", sep=";", index= False, mode='a', header=not os.path.exists(output_folder + "csv/player.csv"))
    df_transfer.to_csv(output_folder + "csv/transfer.csv", sep=";", index= False, mode='a', header=not os.path.exists(output_folder + "csv/transfer.csv"))
    df_cardSupply.to_csv(output_folder + "csv/cardSupply.csv", sep=";", index= False, mode='a', header=not os.path.exists(output_folder + "csv/cardSupply.csv"))
    df_allSo5Scores.to_csv(output_folder + "csv/allSo5Scores.csv", sep=";", index= False, mode='a', header=not os.path.exists(output_folder + "csv/allSo5Scores.csv"))

    print("--- %s seconds ---" % (time.time() - while_time))

#Time
print("--- %s seconds ---" % (time.time() - start_time))
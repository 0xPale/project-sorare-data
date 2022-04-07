import json
import pandas as pd
import time
import glob
import pickle
import os
import getpass
from pathlib import Path
from variables import outputFolderLocal, outputFolderCloud, outputCardCSV, outputJSON

# It's checking if the user is benjamin and if yes, then it's using the local folder, otherwise it's
# using the cloud folder.
if getpass.getuser() == "benjamin":
    outputFolder = outputFolderLocal
else:
    outputFolder = outputFolderCloud

#Time
start_time = time.time()

# Deleting the files in the folder.
for f in glob.glob(outputFolder + outputCardCSV + "*.csv"):
    os.remove(f)

# It's asking the user to input a number and then convert it to an integer.
maxRangeInput = input ("Enter a number for file range: ")
maxRange = int(maxRangeInput)

# returns a list with all the files in source
read_files = glob.glob(outputFolder + outputJSON + "*.json")
len_read_files = len(read_files)

while read_files:  # True if there are any files, False python3if empty list

    while_time = time.time()

    df = pd.DataFrame()
    df_card = pd.DataFrame()
    df_transfer = pd.DataFrame()
   
    # A loop that iterates over the files in the list of files to read.
    for i in range(min(len_read_files, maxRange)):   # xx files at a time 
        file = read_files[0]     # select the first file's name
        print(file)
        # It's extracting the date from the filename.
        datetimeFromFilename = Path(file).stem.split("_")[-1] #Path().stem give the filename without extension and then we return the last element [-1] of the list created by the split                    
        with open(file, 'r') as current_file:
            current_object = json.load(current_file)
        df_curr = pd.json_normalize(current_object, sep="_")
        # It's creating a new column called extracted_at and filling it with the date extracted from
        # the filename.
        df_curr['extracted_at'] = datetimeFromFilename
        # It's adding the current dataframe to the previous one.
        df = pd.concat([df, df_curr], sort=True)
        # It removes the first element of the list.
        read_files.pop(0)       # Remove the moved file from the list
    
    #Store len(list) to avoid errors on the next while if len(list) is < range(xx)
    len_read_files = len(read_files)

    #We save the current list in pickle (binary format used by pyton) to reuse in case of error
    # It's saving the current list of files to read in a pickle file.
    with open(outputFolder + "currentReadFilesList/read_files_list", 'wb') as currentReadFilesList:
        pickle.dump(read_files, currentReadFilesList)

    #On remet la date extracted_at qui indique quand est-ce que la ligne a été extracted depuis l'API en format datetime
    # It's converting the extracted_at column from string to datetime.
    df["extracted_at"] = pd.to_datetime(df["extracted_at"])

    ####################################################################################################################################
    #On commence par créer les dataframes correspondant à chaque subset en sélectionnant uniquement les colonnes utiles
    #On crée le premier output de sortie (la première table) qui ne contiendra que les data core de type dimension pour les cards
    df_card = df[["extracted_at", "card_slug", "card_name", "card_rarity", "card_season_startYear", "player_slug"]]
    #df_userOwnersWithRate --> regexr.com/6gira
    df_transfer = df[["extracted_at", "card_slug", "transfer"]]

    ####################################################################################################################################
    #On explode les colonne qui contiennent une liste de dict, ce qui nous permet d'obtenir une nouvelle ligne de données par dict de la liste
    #On reset au passage l'index pour qu'il soit correct de 0 à n-1
    df_transfer = df_transfer.explode("transfer", ignore_index=True)

    ####################################################################################################################################

    #See https://stackoverflow.com/questions/38231591/split-explode-a-column-of-dictionaries-into-separate-columns-with-pandas/63311361#63311361
    #On applique de nouveau un normalize sur la colonne qu'on a explode pour obtenir une colonne par key du dict et on join sur le dataframe initial
    df_transfer = df_transfer.join(pd.json_normalize(df_transfer.transfer, sep="_"))

    ####################################################################################################################################
   #On drop la colonne qui contenait le dict de valeurs / de keys et qui est maintenant inutile puis on export sous csv
    # Dropping the column transfer and player_cardSupply and player_allSo5Scores_nodes. On drop aussi les colonnes syétamtiquement vides.
    df_transfer = df_transfer.drop(columns=['transfer'])

    ####################################################################################################################################
    #On drop certaines colonnes inutiles
    #On drop les valeurs vides de transfert qui sont inutiles
    df_transfer = df_transfer.dropna(subset=['transfer_type'])

    #On drop les duplicate qui peuvent apparaître dans les data liées aux players (car elles vont être dupliquées pour chaque card de ce player)
    # It drops the duplicate rows.
    df_card = df_card.drop_duplicates(subset=["card_slug", "card_name", "player_slug", "card_rarity", "card_season_startYear"])
    df_transfer = df_transfer.drop_duplicates(subset=[
        "card_slug", "transfer_sorareAccount_manager_slug", "transfer_sorareAccount_manager_nickname", 
        "transfer_date", "transfer_type", "transfer_priceETH", "transfer_priceFiat_usd"])
 
    #On met toutes les colonnes dans le bon ordre avant export
    df_card = df_card.reindex(columns=["card_slug", "card_name", "player_slug", "card_rarity", "card_season_startYear", "extracted_at"])
    df_transfer = df_transfer.reindex(columns=[
        "card_slug", "transfer_sorareAccount_manager_slug", "transfer_sorareAccount_manager_nickname", 
        "transfer_date", "transfer_type", "transfer_priceETH", "transfer_priceFiat_usd", "extracted_at"])

    df_card = df_card.astype({'card_season_startYear':'int'})

    #Export csv
    df_card.to_csv(outputFolder + outputCardCSV + "card.csv", sep=";", index= False, mode='a', header=not os.path.exists(outputFolder + outputCardCSV + "card.csv"))
    df_transfer.to_csv(outputFolder + outputCardCSV + "transfer.csv", sep=";", index= False, mode='a', header=not os.path.exists(outputFolder + outputCardCSV + "transfer.csv"))

    print("--- %s seconds ---" % (time.time() - while_time))

#Time
print("--- %s seconds ---" % (time.time() - start_time))
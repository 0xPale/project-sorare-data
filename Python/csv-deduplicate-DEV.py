import pandas as pd
import time
from variables import output_folder

#Time
start_time = time.time()

df_allSo5Scores = pd.read_csv(output_folder + "csv/allSo5Scores.csv", sep=";")

df_allSo5Scores = df_allSo5Scores[pd.to_numeric(df_allSo5Scores['info_game_fixture_gameWeek'], errors='coerce').notnull()] #TEMPORAIRE

df_allSo5Scores = df_allSo5Scores.drop_duplicates() #TEMPORAIRE
df_allSo5Scores.to_csv(output_folder + "csv/allSo5Scores_deduplicate.csv", sep=";", index= False)

exit()

#df_card = pd.read_csv(output_folder + "csv/card.csv", sep=";")
#df_card = df_card.drop_duplicates()
#df_card.to_csv(output_folder + "csv/card_deduplicate.csv", sep=";", index= False)
#print("--- %s seconds ---" % (time.time() - start_time))
############################################################################
df_player = pd.read_csv(output_folder + "csv/player.csv", sep=";")
df_player = df_player.drop_duplicates(subset=['player_slug']) #TEMPORAIRE
df_player.to_csv(output_folder + "csv/player_deduplicate.csv", sep=";", index= False)
print("--- %s seconds ---" % (time.time() - start_time))
############################################################################
#df_transfer = pd.read_csv(output_folder + "csv/transfer.csv", sep=";")
#df_transfer = df_transfer.drop_duplicates()
#df_transfer.to_csv(output_folder + "csv/transfer_deduplicate.csv", sep=";", index= False)
#print("--- %s seconds ---" % (time.time() - start_time))
############################################################################
df_cardSupply = pd.read_csv(output_folder + "csv/cardSupply.csv", sep=";")
df_cardSupply = df_cardSupply.drop_duplicates()
df_cardSupply.to_csv(output_folder + "csv/cardSupply_deduplicate.csv", sep=";", index= False)
print("--- %s seconds ---" % (time.time() - start_time))
############################################################################
df_allSo5Scores = pd.read_csv(output_folder + "csv/allSo5Scores.csv", sep=";")

df_allSo5Scores = df_allSo5Scores[pd.to_numeric(df_allSo5Scores['info_game_fixture_gameWeek'], errors='coerce').notnull()] #TEMPORAIRE

df_allSo5Scores = df_allSo5Scores.drop_duplicates() #TEMPORAIRE
df_allSo5Scores.to_csv(output_folder + "csv/allSo5Scores_deduplicate.csv", sep=";", index= False)

#Time
print("--- %s seconds ---" % (time.time() - start_time))
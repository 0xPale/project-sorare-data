import pandas as pd
import time
from variables import outputFolder, outputCSV

#Time
start_time = time.time()

df_allSo5Scores = pd.read_csv(outputFolder + outputCSV + "allSo5Scores.csv", sep=";")

df_allSo5Scores = df_allSo5Scores[pd.to_numeric(df_allSo5Scores['info_game_fixture_gameWeek'], errors='coerce').notnull()] #TEMPORAIRE

df_allSo5Scores = df_allSo5Scores.drop_duplicates() #TEMPORAIRE
df_allSo5Scores.to_csv(outputFolder + outputCSV + "allSo5Scores_deduplicate.csv", sep=";", index= False)

exit()

#df_card = pd.read_csv(outputFolder + "csv/card.csv", sep=";")
#df_card = df_card.drop_duplicates()
#df_card.to_csv(outputFolder + "csv/card_deduplicate.csv", sep=";", index= False)
#print("--- %s seconds ---" % (time.time() - start_time))
############################################################################
df_player = pd.read_csv(outputFolder + "csv/player.csv", sep=";")
df_player = df_player.drop_duplicates(subset=['player_slug']) #TEMPORAIRE
df_player.to_csv(outputFolder + "csv/player_deduplicate.csv", sep=";", index= False)
print("--- %s seconds ---" % (time.time() - start_time))
############################################################################
#df_transfer = pd.read_csv(outputFolder + "csv/transfer.csv", sep=";")
#df_transfer = df_transfer.drop_duplicates()
#df_transfer.to_csv(outputFolder + "csv/transfer_deduplicate.csv", sep=";", index= False)
#print("--- %s seconds ---" % (time.time() - start_time))
############################################################################
df_cardSupply = pd.read_csv(outputFolder + "csv/cardSupply.csv", sep=";")
df_cardSupply = df_cardSupply.drop_duplicates()
df_cardSupply.to_csv(outputFolder + "csv/cardSupply_deduplicate.csv", sep=";", index= False)
print("--- %s seconds ---" % (time.time() - start_time))
############################################################################
df_allSo5Scores = pd.read_csv(outputFolder + "csv/allSo5Scores.csv", sep=";")

df_allSo5Scores = df_allSo5Scores[pd.to_numeric(df_allSo5Scores['info_game_fixture_gameWeek'], errors='coerce').notnull()] #TEMPORAIRE

df_allSo5Scores = df_allSo5Scores.drop_duplicates() #TEMPORAIRE
df_allSo5Scores.to_csv(outputFolder + "csv/allSo5Scores_deduplicate.csv", sep=";", index= False)

#Time
print("--- %s seconds ---" % (time.time() - start_time))
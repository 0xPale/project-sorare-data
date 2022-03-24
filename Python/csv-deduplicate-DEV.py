import pandas as pd
import time
from variables import outputFolder, outputCSV

#Time
start_time = time.time()

#Deduplicate player
df_player = pd.read_csv(outputFolder + outputCSV + "player.csv", sep=";")
df_player = df_player.drop_duplicates(subset=[
    "player_slug","player_name","player_position","player_age","player_birthDate","player_appearances","player_followers",
    "player_club_country_code","player_club_country_slug","player_club_slug","player_club_code","player_club_name",
    "player_club_league_slug","player_club_league_name","player_stats_appearances","player_stats_assists",
    "player_stats_goals","player_stats_minutesPlayed","player_stats_yellowCards","player_stats_redCards",
    "player_stats_substituteIn","player_stats_substituteOut","player_status_lastFifteenSo5Appearances",
    "player_status_lastFifteenSo5AverageScore","player_status_lastFiveSo5Appearances",
    "player_status_lastFiveSo5AverageScore","player_status_playingStatus"])
df_player.to_csv(outputFolder + outputCSV + "player_deduplicate.csv", sep=";", index= False)
print("Player duplicate --- %s seconds ---" % (time.time() - start_time))

#Deduplicate card supply
df_cardSupply = pd.read_csv(outputFolder + outputCSV + "cardSupply.csv", sep=";")
df_cardSupply = df_cardSupply.drop_duplicates(subset=["player_slug","cardSupply_limited","cardSupply_rare","cardSupply_superRare","cardSupply_unique","cardSupply_season"])
df_cardSupply.to_csv(outputFolder + outputCSV + "cardSupply_deduplicate.csv", sep=";", index= False)
print("Card supply duplicate  --- %s seconds ---" % (time.time() - start_time))

#Deduplicate scores
df_allSo5Scores = pd.read_csv(outputFolder + outputCSV + "allSo5Scores.csv", sep=";")
#df_allSo5Scores = df_allSo5Scores[pd.to_numeric(df_allSo5Scores['info_game_fixture_gameWeek'], errors='coerce').notnull()] #TEMPORAIRE
df_allSo5Scores = df_allSo5Scores.drop_duplicates(
    subset=["player_slug","score","info_game_date","info_game_fixture_eventType","info_game_fixture_slug","info_game_fixture_gameWeek","decisive_score"]) #TEMPORAIRE
df_allSo5Scores.to_csv(outputFolder + outputCSV + "allSo5Scores_deduplicate.csv", sep=";", index= False)

#Time
print("Total time --- %s seconds ---" % (time.time() - start_time))


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
import requests
import json
import pandas as pd
from flatten_json import flatten
import csv

#Parameters for requests
endpoint = f"https://api.sorare.com/graphql"

headers = {
  "Accept-Encoding": "gzip,deflate", 
  "Content-Type": "application/json",
  "Accept": "application/json",
  "Connection": "keep-alive",
  "Origin": "https://api.sorare.com"
  }

#Initial request
query = """
query allCardsWithCursor {
  allCards(rarities: [unique, super_rare, rare, limited], first: 3) {
    nodes {
      cardSlug: slug
      cardName: name
      cardRarity: rarity
      season {
        startYear
      }
      player {
        slug
        name: displayName
        position
        age
        birthDate
        appearances
        followers: subscriptionsCount
        club: activeClub {
          country {
            code
            slug
          }
          slug
          code
          name
          league: domesticLeague {
            slug
            name
          }
        }
        cardSupply {
          cardSupply: season {
            season: startYear
          }
          cardSupply_limited: limited
          cardSupply_rare: rare
          cardSupply_superRare : superRare
          cardSupply_unique: unique
        }
        stats(seasonStartYear: 2021) {
          appearances
          assists
          goals
          minutesPlayed
          yellowCards
          redCards
          substituteIn
          substituteOut
        }
        status {
          lastFifteenSo5Appearances
          lastFifteenSo5AverageScore
          lastFiveSo5Appearances
          lastFiveSo5AverageScore
          playingStatus
        }
        allSo5Scores {
          nodes {
            info: playerGameStats {
              game {
                date
                fixture: so5Fixture {
                  eventType
                  slug
                  gameWeek
                }
              }
            }
            score
            decisive: decisiveScore {
              score: totalScore
            }
          }
        }
      }
      transfer: userOwnersWithRate {
        transfer_date: from
        transfer_type: transferType
        transfer_priceETH: price
        transfer_priceFiat: priceInFiat {
          usd
        }
      }
    }
    allCardsPageInfo: pageInfo {
      allCardsHasPreviousPage: hasPreviousPage
      allCardsHasNextPage: hasNextPage
      allCardsEndCursor: endCursor
    }
  }
}
"""

r = requests.post(endpoint, json={"query": query}, headers=headers)
if r.status_code == 200: #success
  #print(r.json())
  #print(json.dumps(r.json(), indent=2))

  #Transforme l'output de la commande requests en format json pour être exploité
  raw_data = r.json()

  #On récupère le endCursor pour le stocket dans la variable qui sera utilisée dans la boucle
  currentCursor = raw_data["data"]["allCards"]["allCardsPageInfo"]["allCardsEndCursor"]

  #On extrait uniquement la liste des cartes (liste d'arrays qui commence par un [])
  dic = raw_data["data"]["allCards"]["nodes"]
  #On normalise le json pour le stocker dans un dataframe
  df=pd.json_normalize(dic, sep="_")

  ####################################################################################################################################
  ####################################################################################################################################

  #On crée le premier output de sortie (la première table) qui ne contiendra que les data core de type dimension pour les cards
  df_card = df[["cardSlug", "cardName", "cardRarity", "season_startYear", "player_slug"]]
  #Export csv
  df_card.to_csv("Python/output/card_explode.csv", sep=";", index= False)

  #Deuxième dataframe de type core pour la liste des players et leurs info (pour éviter de les répéter sur toutes les cards)
  #df_player --> regexr.com/6girp
  df_player = df.filter(regex="player_name|player_slug|player_position|player_age|player_birthDate|player_appearances|player_followers|player_club.*|player_stat.*", axis=1)
  #Export csv
  df_player.to_csv("Python/output/player_explode.csv", sep=";", index= False)

  ####################################################################################################################################
  #On commence par créer les dataframes correspondant à chaque subset en sélectionnant uniquement les colonnes utiles
  
  #df_userOwnersWithRate --> regexr.com/6gira
  df_transfer = df[["cardSlug", "transfer"]]

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

  print(df_transfer)
  print(df_cardSupply)
  print(df_allSo5Scores)

  ####################################################################################################################################

  #See https://stackoverflow.com/questions/38231591/split-explode-a-column-of-dictionaries-into-separate-columns-with-pandas/63311361#63311361
  # replace NaN with '{}' if the column is strings, otherwise replace with {}
  # df.Pollutants = df.Pollutants.fillna('{}')  # if the NaN is in a column of strings
  # df_userOwnersWithRate.userOwnersWithRate = df.userOwnersWithRate.fillna({i: {} for i in df_userOwnersWithRate.index})  # if the column is not strings

  # Convert the column of stringified dicts to dicts
  # skip this line, if the column contains dicts
  #df.Pollutants = df.Pollutants.apply(literal_eval)

  # normalize the column of dictionaries and join it to df
  #df = df.join(pd.json_normalize(df.Pollutants))

  # drop Pollutants
  #df.drop(columns=['Pollutants'], inplace=True)

  #On applique de nouveau un normalize sur la colonne qu'on a explode pour obtenir une colonne par key du dict et on join sur le dataframe initial
  df_transfer = df_transfer.join(pd.json_normalize(df_transfer.transfer, sep="_"))
  df_cardSupply = df_cardSupply.join(pd.json_normalize(df_cardSupply.player_cardSupply, sep="_"))
  df_allSo5Scores = df_allSo5Scores.join(pd.json_normalize(df_allSo5Scores.player_allSo5Scores_nodes, sep="_"))

  ####################################################################################################################################
  #On drop la colonne qui contenait le dict de valeurs / de keys et qui est maintenant inutile puis on export sous csv
  df_transfer.drop(columns=['transfer'], inplace=True)
  df_cardSupply.drop(columns=['player_cardSupply'], inplace=True)
  df_allSo5Scores.drop(columns=['player_allSo5Scores_nodes'], inplace=True)

  #On drop les valeurs vides de transfert qui sont inutiles
  df_transfer.dropna(subset=['transfer_type'], inplace=True)

  #Export csv
  df_transfer.to_csv("Python/output/transfer_explode.csv", sep=";", index= False)
  df_cardSupply.to_csv("Python/output/cardSupply_explode.csv", sep=";", index= False)
  df_allSo5Scores.to_csv("Python/output/allSo5Scores_explode.csv", sep=";", index= False)
  
else:
    raise Exception(f"Query failed to run with a {r.status_code}.")

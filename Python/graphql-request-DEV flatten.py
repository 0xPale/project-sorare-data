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
query allCardsWithCursorTEST {
  allCards(rarities: [unique, super_rare, rare, limited], first: 10) {
    nodes {
      cardSlug: slug
      cardName: name
      rarity
      season {
        cardSeason: startYear
      }
      player {
        playerSlug: slug
        playerName: displayName
        playerPosition: position
        playerAge: age
        playerBirthDate: birthDate
        playerAppearances: appearances
        club: activeClub {
          clubCountry: country {
            countryCode: code
            country: slug
          }
          clubSlug: slug
          clubCode: code
          clubName: name
        }
        cardSupply {
          cardSeason: season {
            seasonStartYear: startYear
          }
          cardLimitedNb: limited
          cardrareNb: rare
          cardSuperareNb: superRare
          cardUniqueNb: unique
        }
        soFScores: so5Scores(last: 5) {
          score
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
          lastFifteenSoFAppearances: lastFifteenSo5Appearances
          lastFifteenSoFAverageScore: lastFifteenSo5AverageScore
          lastFiveSoFAppearances: lastFiveSo5Appearances
          lastFiveSoFAverageScore: lastFiveSo5AverageScore
          playingStatus
        }
        allSoFScores: allSo5Scores {
          nodes {
            playerGameStats {
              game {
                date
              }
            }
            score
          }
        }
      }
      userOwnersWithRate {
        ownerFrom: from
        transferType
        transferPriceETH: price
        transferPriceUSD: priceInFiat {
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

  #Utilisation de la fonction flatten puis store dans un DataFrame pour manipulation
  dic_flattened = [flatten(d) for d in dic]
  df = pd.DataFrame(dic_flattened)


  #Drop des colonnes "vides" qui contiennent en fait soit une liste vide [], soit un NaN --> ce sont les colonnes d'ouverture des listes --> il n'y en a qu'une
  df = df.drop(columns=['userOwnersWithRate'])

  #On crée le premier output de sortie (la première table) qui ne contiendra que les data core de type dimension
  df_core_OLD = df[[
    "cardSlug"
    , "cardName"
    , "rarity"
    , "season_cardSeason"
    , "player_playerName"
    , "player_playerSlug"
    , "player_playerPosition"
    , "player_playerAge"
    , "player_playerBirthDate"
    , "player_playerAppearances"
    , "player_club_clubCountry_countryCode"
    , "player_club_clubCountry_country"
    , "player_club_clubSlug"
    , "player_club_clubCode"
    , "player_club_clubName"
    , "player_stats_appearances"
    , "player_stats_assists"
    , "player_stats_goals"
    , "player_stats_minutesPlayed"
    , "player_stats_yellowCards"
    , "player_stats_redCards"
    , "player_stats_substituteIn"
    , "player_stats_substituteOut"
    , "player_status_lastFifteenSoFAppearances"
    , "player_status_lastFifteenSoFAverageScore"
    , "player_status_lastFiveSoFAppearances"
    , "player_status_lastFiveSoFAverageScore"
    , "player_status_playingStatus"
    ]]

  #On crée le premier output de sortie (la première table) qui ne contiendra que les data core de type dimension en utilisant un filter + regex
  #qui ne retourne que les noms de colonnes ne contenant pas de chiffres, càd tout sauf les colonnes provenant des listes d'éléments
  df_card = df[["cardSlug", "cardName", "rarity", "season_cardSeason", "player_playerSlug"]]
  #Export csv
  df_card.to_csv("Python/output/card.csv", sep=";", index= False)

  #Deuxième dataframe de type core pour la liste des players et leurs info (pour éviter de les répéter sur toutes les cards)
  #regexr.com/6girp
  df_player = df.filter(regex="player_player.*|player_club.*|player_stat.*", axis=1)
  #Export csv
  df_player.to_csv("Python/output/player.csv", sep=";", index= False)

  #MELTING
  #Melting des colonnes par rapport aux valeurs fixes et uniques (dimensions) pour passer les colonnes en ligne --> on passe les data core en id_vars
  #On commence par le melting des userOwnersWithRate

  #On commence par créer le dataframe correspondant en sélectionnant uniquement les colonnes utiles
  #regexr.com/6gira
  df_userOwnersWithRate = df.filter(regex="cardSlug|userOwnersWithRate_[0-9]*._(.*.)", axis=1)

  #Puis melting des userOwnersWithRate
  df = df.melt(id_vars=["cardSlug"])
  
  #On renomme la totalité des noms de colonnes présents dans la colonne "variable" créée par le melt() pour enlever les chiffres
  df['userOwnersWithRateNumber'] = df.variable.str.extract(pat='userOwnersWithRate_(\d*)_')
  df['varFormat'] = df.variable.str.extract(pat= 'userOwnersWithRate_[0-9]*._(.*.)')
  
  #On conserve que les colonnes qui nous intéressent + on drop le reste et les doublons
  df = df[["cardSlug", "cardName", "rarity", "season_cardSeason", "userOwnersWithRateNumber", "varFormat", "value"]]
  df = df.dropna(subset=['userOwnersWithRateNumber'])
  #df = df.dropna(subset=['value'])
  df = df.drop_duplicates(["cardSlug", "cardName", "rarity", "season_cardSeason", "varFormat", "value"])

  #Pivot par rapport à l'index de chaque carte
  df = df.pivot(index=["cardSlug", "cardName", "rarity", "season_cardSeason", "userOwnersWithRateNumber"], columns='varFormat', values='value')
  df.reset_index(inplace=True)

  #Export csv
  df.to_csv("Python/output/test_flatten.csv", sep=";", index= False)

else:
    raise Exception(f"Query failed to run with a {r.status_code}.")

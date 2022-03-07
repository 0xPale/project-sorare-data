import requests
import json
import pandas as pd
from flatten_json import flatten
import time

start_time = time.time()

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
  allCards(rarities: [unique, super_rare, rare, limited], slugs: ["albert-rusnak-2021-limited-232", "keven-schlotterbeck-2021-rare-2"]) {
    nodes {
      cardSlug: slug
      cardName: name
      rarity
      season {
        cardSeason: startYear
      }
      player {
        playerName: displayName
        playerSlug: slug
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
        so5Scores(last: 5) {
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
          lastFifteenSo5Appearances
          lastFifteenSo5AverageScore
          lastFiveSo5Appearances
          lastFiveSo5AverageScore
          playingStatus
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

  currentCursor = raw_data["data"]["allCards"]["allCardsPageInfo"]["allCardsEndCursor"]

  #dic = raw_data["data"]["allCards"]["nodes"]
  dic = raw_data["data"]["allCards"]
  
  outputList = dic
  
  result = flatten(dic)
  #print(json.dumps(result, indent=2))
  df = pd.json_normalize(result)
  print(df)

else:
    raise Exception(f"Query failed to run with a {r.status_code}.")


with open('Python/output/data_test_normalize.json', 'w') as json_file:
        json.dump(outputList, json_file, indent=2)

print("--- %s seconds ---" % (time.time() - start_time))
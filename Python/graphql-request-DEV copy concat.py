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
  allCards(rarities: [unique, super_rare, rare, limited]) {
    nodes {
      cardSlug: slug
      cardName: name
      cardPriceRange: priceRange {
        min
        max
      }
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
        cardSupply {
          cardSeason: season {
            seasonStartYear: startYear
          }
          cardLimitedNb: limited
          cardrareNb: rare
          cardSuperareNb: superRare
          cardUniqueNb: unique
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
        allSo5Scores {
          nodes {
            playerGameStats {
              game {
                date
              }
            }
            score
          }
          allScoresPageInfo: pageInfo {
            allScoresHasPreviousPage: hasPreviousPage
            allScoresHasNextPage: hasNextPage
            allScoresendCursor: endCursor
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

  currentCursor = raw_data["data"]["allCards"]["allCardsPageInfo"]["allCardsEndCursor"]

  dic = raw_data["data"]["allCards"]["nodes"]
  
  outputList = dic
  
  print("Initial request success")

else:
    raise Exception(f"Query failed to run with a {r.status_code}.")


#Looping requests
#init
i = 0

while i <= 100:

  query = """
  query allCardsWithCursor ($currentCursor: String) {
  allCards(rarities: [unique, super_rare, rare, limited], after: $currentCursor) {
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
        cardSupply {
          cardSeason: season {
            seasonStartYear: startYear
          }
          cardLimitedNb: limited
          cardrareNb: rare
          cardSuperareNb: superRare
          cardUniqueNb: unique
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

  r = requests.post(endpoint, json={"query": query, "variables": {"currentCursor": str(currentCursor)}}, headers=headers)
  if r.status_code == 200: #success

      #Transforme l'output de la commande requests en format json pour être exploité
      raw_data = r.json()

      #print(raw_data)

      currentCursor = raw_data["data"]["allCards"]["allCardsPageInfo"]["allCardsEndCursor"]

      dic = raw_data["data"]["allCards"]["nodes"]
  
      outputList = outputList + dic
      
      print(str(i) +" request success")
      
      i = i+1

  else:
      raise Exception(f"Query failed to run with a {r.status_code}.")


with open('Python/output/data_test_2.json', 'w') as json_file:
        json.dump(outputList, json_file)

print("--- %s seconds ---" % (time.time() - start_time))
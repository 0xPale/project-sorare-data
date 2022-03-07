import requests
import json

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
  allCards(rarities: [unique, super_rare, rare, limited]) {
    nodes {
      cardSlug: slug
      cardNames: name
      rarity
      season {
        cardSeason: startYear
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

  with open('Python/output/data_test.json', 'w') as json_file:
    json.dump(raw_data, json_file)
  
  print("Initial request success")

else:
    raise Exception(f"Query failed to run with a {r.status_code}.")


#Looping requests
#init
i = 0

while i <= 5:

  query = """
  query allCardsWithCursorTEST ($currentCursor: String) {
    allCards(rarities: [unique, super_rare, rare, limited], after: $currentCursor) {
      nodes {
        cardSlug: slug
        cardNames: name
        rarity
        season {
          cardSeason: startYear
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

      with open("Python/output/data_test_" + str(currentCursor) + ".json", 'w') as json_file:
        json.dump(raw_data, json_file)
      
      print(str(i) +" request success")
      
      i = i+1

  else:
      raise Exception(f"Query failed to run with a {r.status_code}.")

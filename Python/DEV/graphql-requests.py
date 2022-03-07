import requests
import json
import pandas as pd
from flatten_json import flatten

#Parameters for requests
endpoint = f"https://api.sorare.com/graphql"

headers = {
  "Accept-Encoding": "gzip,deflate", 
  "Content-Type": "application/json",
  "Accept": "application/json",
  "Connection": "keep-alive",
  "Origin": "https://api.sorare.com"
  }

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


    #Permet de récupérer le current endCursor
    print(raw_data["data"]["allCards"]["allCardsPageInfo"]["allCardsEndCursor"])

    #Permet de flatten la totalité du json
    flat = flatten(raw_data)

    #Normalisation de l'ouput pour intégration dans un dataframe Python Pandas
    df_data = pd.json_normalize(flat)


    #df_data = df_data.drop(columns=['data_allCards_nodes_[0-9]*._userOwnersWithRate'])
    df_data = df_data[df_data.columns.drop(list(df_data.filter(regex='data_allCards_nodes_._userOwnersWithRate')))]

    #Melting des columns en rows
    df = pd.melt(df_data)
  

    #On renomme la totalité des columns
    df['allCards'] = df.variable.str.extract(pat='nodes_(\d*)_') + df.variable.str.extract(pat='userOwnersWithRate_(\d*)_')
    df['varFormat'] = df.variable.str.extract(pat= 'data_allCards_nodes_[0-9]*._(.*.)')

    print(df)
    
    #On conserve que les columns qui nous intéressent + on drop le reste et les doublons
    df = df[['allCards', 'varFormat', 'value']]
    df = df.dropna(subset=['allCards'])
   # df = df.drop_duplicates()


    

    #Pivot par rapport à l'index de chaque carte
    df = df.pivot(index='allCards', columns='varFormat', values='value')

    #Export csv
    df.to_csv("Python/output/test.csv", sep=";", index= False)

else:
    raise Exception(f"Query failed to run with a {r.status_code}.")

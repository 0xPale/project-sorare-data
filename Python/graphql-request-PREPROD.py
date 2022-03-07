import requests
import json
import time
#Parameters for requests
from variables import endpoint, headers, query
import shutil

#Time
start_time = time.time()

shutil.copyfile('Python/output/currentCursor/currentCursor.txt', 'Python/output/currentCursor/currentCursor_copy002.txt')

#On store la dernière valeur de allCardsEndCursor pour l'utiliser en démarrage de la prochaine exécution
with open('Python/output/currentCursor/currentCursor.txt', 'r') as f:
    currentCursor = f.read()

#Looping requests
#init
i = 2
hasNextPage = True

while i <= 1000 and hasNextPage == True:

  r = requests.post(endpoint, json={"query": query, "variables": {"currentCursor": str(currentCursor)}}, headers=headers)
  if r.status_code == 200: #success
    #Transforme l'output de la commande requests en format json pour être exploité
      raw_data = r.json()

      currentCursor = raw_data["data"]["allCards"]["allCardsPageInfo"]["allCardsEndCursor"]
      hasNextPage = raw_data["data"]["allCards"]["allCardsPageInfo"]["allCardsHasNextPage"]

      dic = raw_data["data"]["allCards"]["nodes"]

      #On dump chaque json obtenu dans un fichier de 50 objets pour une réutilisation + simple + tard
      with open('Python/output/json/data_dump_'+currentCursor+'.json', 'w') as json_file:
        json.dump(dic, json_file)

      #On store la dernière valeur de allCardsEndCursor pour l'utiliser en démarrage de la prochaine exécution
      with open('Python/output/currentCursor/currentCursor.txt', 'w') as f:
          f.write(currentCursor)

      print(str(i) + " request success + hasNextPage = " + str(hasNextPage))

      i = i+1
  
  elif r.status_code == 429: #Time out

      time.sleep(30)
      print("Waited for 30s for iteration " + str(i))
      i = i+1

  else:
      raise Exception(f"Query failed to run with a {r.status_code} + {r.headers}.")

#Time
print("--- %s seconds ---" % (time.time() - start_time))
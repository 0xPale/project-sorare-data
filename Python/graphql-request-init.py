import requests
import json
import time
#Parameters for requests
from variables import endpoint, headers, query

#Time
start_time = time.time()

#Init
r = requests.post(endpoint, json={"query": query}, headers=headers)
if r.status_code == 200: #success

  #Transforme l'output de la commande requests en format json pour être exploité
  raw_data = r.json()

  #On récupère le endCursor pour le stocket dans la variable qui sera utilisée dans la boucle
  currentCursor = raw_data["data"]["allCards"]["allCardsPageInfo"]["allCardsEndCursor"]

  #On extrait uniquement la liste des cartes (liste d'arrays qui commence par un [])
  dic = raw_data["data"]["allCards"]["nodes"]

  #On dump le json obtenu dans un fichier de 50 objets pour une réutilisation + simple + tard
  with open('Python/output/json/data_dump_'+currentCursor+'.json', 'w') as json_file:
        json.dump(dic, json_file)
  
  #On store la dernière valeur de allCardsEndCursor pour l'utiliser en démarrage de la prochaine exécution
  with open('Python/output/currentCursor/currentCursor.txt', 'w') as f:
        f.write(currentCursor)

  print("Initial request success")

else:
    raise Exception(f"Query failed to run with a {r.status_code}.")

#Time
print("--- %s seconds ---" % (time.time() - start_time))
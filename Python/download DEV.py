import requests
from requests.auth import HTTPBasicAuth

url = "https://community.cloud.databricks.com/files/sorare/test.json"
usr = "benja.sicard@gmail.com"
pwd = "r^027hiBEeTT"

#wget.download(url, '/Users/benjamin/Documents/Projets/Sorare-data/Python/output/test.json')

"""

print (len(r.content))
"""
r = requests.get(
    url
    , auth=HTTPBasicAuth(usr, pwd)
    )
with open('/Users/benjamin/Documents/Projets/Sorare-data/Python/output/test2.json', 'wb') as f:
    f.write(r.content) 
import json
import pandas as pd
import time
import glob
from pathlib import Path
from variables import outputFolder, outputCSV, outputJSON

# Storing the current time in seconds since the Epoch.
start_time = time.time()

# Reading all the files in the folder and subfolders.
read_files = glob.glob(outputFolder + "dev/*.json")

# It creates a new dataframe.
df = pd.DataFrame()

# Reading the files and creating a dataframe for each file.
for f in read_files:
    datetimeFromFilename = Path(f).stem.split("_")[-1] #Path().stem give the filename without extension and then we return the last element [-1] of the list created by the split
    print(datetimeFromFilename)

    with open(f, 'r') as current_file:
        #raw = current_file.read()
        #current_object = json.loads(raw)
        current_object = json.load(current_file)
    df_curr = pd.json_normalize(current_object, sep="_")
    df_curr['created_at'] = datetimeFromFilename
    df = pd.concat([df, df_curr])
    
    print(df)


df["created_at"] = pd.to_datetime(df["created_at"])
print(df)
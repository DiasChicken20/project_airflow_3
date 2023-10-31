'''
Kelompok: 4
    - Windriya Rachmasari
    - Restu Diah Pangestika
    - Dias Rantelino
'''



import requests
import json
import pandas as pd
from sqlalchemy import create_engine

url = "https://api.openaq.org/v2/locations/5241"

headers = {"X-API-Key": "95ed1888290e5829910cdc3736948562bc942f0d1ac292c8b3b3cffc48aa72c1"}

response = requests.get(url, headers=headers)

data = response.json()

with open("data3.json", "w") as file:
    json.dump(data,file)


with open("data3.json", "r") as json_file:
    data = json.load(json_file)

df = pd.DataFrame(data)

database_url = 'sqllite:///new_database.db'

engine = create_engine(database_url)

table_name =  'python_script'

df.to_sql(table_name, con=engine, if_exist='replace', index=False)

engine.dispose()



              

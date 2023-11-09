'''
Kelompok: 4
    - Windriya Rachmasari
    - Restu Diah Pangestika
    - Dias Rantelino
'''



import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, String, JSON, DateTime, Boolean, Float, Integer
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    "owner": "kelp_4",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}
    # --Extract--
def extract_url(**content):
    url = 'https://api.openaq.org/v2/locations/5241'
    headers = {'X-API-Key': '95ed1888290e5829910cdc3736948562bc942f0d1ac292c8b3b3cffc48aa72c1'}
    response = requests.get(url, headers=headers)
    data = response.json()
    print(data)
    for row in data['results']:
        print(row)
    

# # Load to local file
    with open('dataset/file.json', 'w') as f:
        json.dump(row, f)

    return content['ti'].xcom_push(key='hole_data', value=f)
# read json file
pd.set_option('display.max_columns', None)
df = pd.read_json('dataset/file.json', lines= True)
    

df = pd.read_json('dataset/file.json', lines= True)

# --Transform--
def transform_data(**content):
    df['entity'] = df['entity'].astype('string')
    df['sources'] = df['sources'].astype('string')
    df['sensorType'] = df['sensorType'].astype('string')
    df['isMobile'] = df['isMobile'].astype('boolean')
    df['isAnalysis'] = df['isAnalysis'].astype('boolean')
    df['measurements'] = df['measurements'].astype('string')
    print(df.dtypes)
    return content['ti'].xcom_push(key='transform_data', value=df)

# --Load--
def load_data():
    df_schema = {
    'id': BigInteger,
    'city': String(100),
    'name': String(100),
    'entity': String(100),
    'country': String(100),
    'sources': String(100),
    'isMobile': Boolean,
    'isAnalysis': Boolean,
    'parameters': JSON,
    'sensorType': String(100),
    'coordinates': JSON,
    'lastUpdated': String(100),
    'measurements': String(100),
    'bounds': JSON,
    'manufacturers': JSON,
}
    user = 'djamier'
    password = 'djamier'
    host = 'localhost'
    database = 'postgres'
    port = '5435' 

    conn_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(conn_string)

    return df.to_sql(name= 'data_json', con=engine, schema= 'public', if_exists='replace', index= None, dtype=df_schema)

kelp_4_dag = DAG(
    dag_id = 'etl_klp_4_dag',
    default_args = default_args,
    start_date = datetime(2023, 11, 7),
    catchup = False,
    schedule = "0 4 * * *"
)


extract_data_task = PythonOperator(
    task_id = 'extract_data_task',
    python_callable = extract_url,
    provide_context = True,
    dag = kelp_4_dag
) 

transform_data_task = PythonOperator(
    task_id = 'transform_data_task',
    python_callable = transform_data,
    provide_context = True,
    dag = kelp_4_dag
)

load_data_task = PythonOperator(
    task_id = 'load_data_task',
    python_callable = load_data,
    provide_context = True,
    dag = kelp_4_dag
)

extract_data_task >> transform_data_task >> load_data_task


              

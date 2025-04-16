import sqlalchemy
import json
from sqlalchemy import create_engine, text
import pandas
import os 
from dotenv import load_dotenv
import requests

load_dotenv()

API_KEY=os.getenv("API_KEY")
print(f"{API_KEY}")
API_URL = f"https://api.openweathermap.org/data/2.5/weather?lat=27.7172&lon=85.3240&appid={API_KEY}"
dbname=os.getenv("dbname")
host=os.getenv("host")
user=os.getenv("user")
password=os.getenv("password")
port=os.getenv("port")

try:
    engine= create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
except Exception as e: 
    print(f"Error: {e}")
  
'''
def fetch_data(API_URL): 
    response=requests.get(API_URL)
    response.raise_for_status()
    data= response.json()
    with open("files/weather_data.json", "w") as jsonfile:
        json.dump(data, jsonfile)
    return data
'''

def create_table(table_name,connection):
    query= text(f'''
                CREATE TABLE IF NOT EXISTS {table_name}(
                    id BIGINT PRIMARY KEY,
                    cod INT,
                    coord JSONB, 
                    weather JSONB, 
                    base VARCHAR(50),
                    main JSONB,
                    visibility BIGINT,
                    wind JSONB,
                    clouds JSONB,
                    dt BIGINT,
                    sys JSONB,
                    timezone INT,
                    name VARCHAR(50)  
                );
                ''') 
    connection.execute(query)

def load_data(api_data,connection,table_name):
    try:
        for record in api_data:
            record['coord']=json.dumps(record['coord'])
            record['weather']=json.dumps(record['weather'])
            record['main']=json.dumps(record['main'])
            record['wind']=json.dumps(record['wind'])
            record['clouds']=json.dumps(record['clouds'])
            record['sys']=json.dumps(record['sys'])
            query= text(f"""
                            INSERT INTO {table_name} 
                            (id, cod, coord, weather, base, main, visibility, wind, clouds,
                            dt, sys, timezone, name) VALUES 
                            (:id, :cod, :coord, :weather, :base, :main, :visibility, :wind, :clouds,
                            :dt, :sys, :timezone, :name) ON CONFLICT (id) DO NOTHING;
                            """) 
            connection.execute(query, record)
    except Exception as e: 
        print(f"Error {e}")
        
def transform_data(): 
    try: 
        df= pandas.read_json("files/weather_data.json")
        print(df.head())

        df['weather_overview']=df['weather'].apply(lambda x: x[0].get('main'))
        df['weather_description']=df['weather'].apply(lambda x: x[0].get('description'))
        df['temperature']=df['main'].apply(lambda x: x.get('temp'))
        df.drop(columns=['weather', 'clouds', 'sys', 'wind', 'main'], inplace=True)
        
        df['coord']= df['coord'].apply(json.dumps)

        print(df.head())
        return df
    except Exception as e:
        print(f"Error: {e}")
        
def load_transformed_data(table_name, connection, df):
    try: 
        df.to_sql(table_name, connection, if_exists='replace', index=False)
    except Exception as e: 
        print(f"Error: {e}")


#api_data= fetch_data(API_URL)

with open("files/weather_data.json", 'r') as file:
    api_data= json.load(file)


with engine.begin() as connection: 
    table_name= "weather_raw"
    transformed_table_name="weather_transformed"
    create_table(table_name, connection)
    load_data(api_data,connection,table_name)
    transformed_df= transform_data(table_name, connection)
    load_transformed_data(transformed_table_name, connection, transformed_df)
    
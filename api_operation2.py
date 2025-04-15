import psycopg2
import pandas
import requests
import json
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import os

API_URL= "https://rickandmortyapi.com/api/character"  

dbname= "rick_and_morty"
host= "localhost"
password= "root"
user= "postgres"
port: 5432

def get_db_connection(): 
    try: 
        dbname= "rick_and_morty"
        host= "localhost"
        password= "root"
        user= "postgres"
        port: 5432
        
        
        return psycopg2.connect(
            dbname=dbname,
            host=host,
            password=password,
            user= user,
            port= port
        )
    except Exception as e: 
        print(f"Error: {e}")

def check_table_exists(table_name):
    conn= get_db_connection()
    cursor= conn.cursor()
    check_table_query='''
    SELECT EXISTS(
        SELECT FROM information_schema.tables
        WHERE table_name= %s
    )
    '''
    cursor.execute(check_table_query, (table_name))
    return cursor.fetchone()[0]
def create_table(table_name): 
    try: 
        conn=get_db_connection()
        cursor= conn.cursor()
        create_table_query= """
                CREATE TABLE IF NOT EXISTS %s (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                status VARCHAR(50),
                species VARCHAR(100),
                type VARCHAR(255),
                gender VARCHAR(50),
                origin JSONB,
                location JSONB,
                image VARCHAR(255),
                episode TEXT[],
                url VARCHAR(255),
                created TIMESTAMP
            )
        """
        cursor.execute(create_table_query, (table_name))
        conn.commit()
    except Exception as e: 
        print(f"Error: {e}")
        
def fetch_api_data(api_url):
    try: 
        response= requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except Exception as e: 
        print(f"Error: {e}")
        
def load_to_postgres(data, table_name): 
    try: 
        conn=get_db_connection()
        cursor=conn.cursor()
        if data()
        

        
api_data= fetch_api_data(API_URL)
create_table("rick_and_morty")
load_to_postgres(api_data, "rick_and_morty")


    

        

import requests
import json
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import os

API_URL = "https://rickandmortyapi.com/api/character"  
TABLE_NAME = "rick_and_morty_characters"
TRANSFORMED_TABLE_NAME = "transformed_characters"
OUTPUT_CSV_FILENAME = "files/transformed_characters.csv"

def get_db_connection():
    try:
        dbname= "rick_and_morty"
        user= "postgres"
        password= "root"
        port= 5432
        host= "localhost"
        return psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def check_table_exists(table_name):
    conn= get_db_connection()
    cursor= conn.cursor()
    check_table_query = '''
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = %s
    );
    '''

    cursor.execute(check_table_query, (table_name,))
    return cursor.fetchone()[0]  

def create_table(table_name):
    try:
        conn= get_db_connection()
        cursor= conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
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
        """)
        conn.commit()
        print(f"Table '{table_name}' created or already exists.")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            
def fetch_api_data(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json().get('results')
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def load_to_postgres(data, table_name):

    try:
        conn= get_db_connection()
        cursor= conn.cursor()
        if data:
            for record in data:
                record['origin'] = json.dumps(record['origin']) if isinstance(record['origin'], dict) else record['origin']
                record['location'] = json.dumps(record['location']) if isinstance(record['location'], dict) else record['location']
                
                columns = ", ".join(record.keys())
                placeholders = ", ".join(["%s"] * len(record))
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING" # Assuming 'id' is a unique key
                values = list(record.values())
                print(f"Printing values: {values}")
                try:
                    cursor.execute(sql, values)
                except psycopg2.Error as e:
                    print(f"Error inserting record: {e}, Record: {record}")
            conn.commit()
            print("Data loaded into PostgreSQL!")
    except psycopg2.Error as e:
        print(f"Error connecting or loading to PostgreSQL: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def transform_data_pandas( table_name):
    try:
        conn= get_db_connection()
        cursor= conn.cursor()
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        print("\nOriginal DataFrame:")
        print(df.head())

        df['origin_name'] = df['origin'].apply(lambda x: x.get('name'))
        df['location_name'] = df['location'].apply(lambda x: x.get('name'))
        df['episode_count'] = df['episode'].apply(len)
        df.drop(columns=['origin', 'location', 'url', 'episode'], inplace=True)
        df['created'] = pd.to_datetime(df['created'])

        print("\nTransformed DataFrame:")
        print(df.head())
        return df
    except psycopg2.Error as e:
        print(f"Error reading data into pandas: {e}")
        return None
    finally:
        if conn:
            conn.close()

def load_transformed_to_postgres(df, table_name):
    if df is not None:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            columns = ", ".join(df.columns)
            placeholders = ", ".join(["%s"] * len(df.columns))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING"

            for _, row in df.iterrows():
                cursor.execute(sql, tuple(row))

            conn.commit()
            print(f"\nTransformed data loaded into '{table_name}' in PostgreSQL.")
        except psycopg2.Error as e:
            print(f"Error loading transformed data to PostgreSQL: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                
def export_to_csv(df, filename):
    if df is not None:
        try:
            df.to_csv(filename, index=False)
            print(f"\nTransformed data exported to '{filename}'.")
        except Exception as e:
            print(f"Error exporting to CSV: {e}")

if __name__ == "__main__":
    api_data = fetch_api_data(API_URL)
    if api_data:
        create_table(TABLE_NAME)
        load_to_postgres(api_data, TABLE_NAME)
        transformed_df = transform_data_pandas(TABLE_NAME)
        if transformed_df is not None:
            load_transformed_to_postgres(transformed_df, TRANSFORMED_TABLE_NAME)
            export_to_csv(transformed_df, OUTPUT_CSV_FILENAME)
import os
from dotenv import load_dotenv
import pandas, sqlalchemy, psycopg2
from sqlalchemy import create_engine, text
import time

load_dotenv()

csv_file_path= 'files/tested_verylarge.csv'

dbname= os.getenv("dbname")
host= os.getenv("host")
password= os.getenv("password")
user= os.getenv("user")
port= os.getenv("port")

engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

def check_exists(connection, tablename):
    query= text(
         '''
        SELECT EXISTS( 
        SELECT FROM information_schema.tables
        WHERE table_name= :tablename
        )
         '''
    )

    result= connection.execute(query, {'tablename' : tablename})
    return result.scalar()

def create_table(connection):
    query = text('''
       CREATE TABLE IF NOT EXISTS titanic_from_csv (
            "PassengerId" BIGINT PRIMARY KEY,
            "Survived" INTEGER,
            "Pclass" INTEGER,
            "Name" TEXT,
            "Sex" VARCHAR(10),
            "Age" FLOAT,
            "SibSp" INTEGER,
            "Parch" INTEGER,
            "Ticket" TEXT,
            "Fare" FLOAT,
            "Cabin" TEXT,
            "Embarked" CHAR(1)
        );
    ''')
    connection.execute(query)

def load_csv(connection, csv_file_path,table_name):
    try:
            read_start_time=time.time()
            df = pandas.read_csv(csv_file_path)
            read_end_time=time.time()
            read_time=read_end_time-read_start_time
            print(f"Time to read csv file: {read_time}")
            
            write_start_time=time.time()
            df.to_sql(name=table_name, con=connection, if_exists='append', index=False, method='multi',chunksize=10000)
            write_end_time=time.time()
            write_time=write_end_time-write_start_time
            print(f"Time to write csv file: {write_time}")
            print(f"Data from '{csv_file_path}' loaded successfully into '{table_name}'.")
    except FileNotFoundError:
            print(f"Error: CSV file not found at '{csv_file_path}'.")
    except Exception as e:
            print(f"Error during CSV loading: {e}")



try: 
    with engine.begin() as connection:
        if not check_exists(connection, "titanic_from_csv"):
            create_table(connection)
            print("Table Created Sucessfully")
        else:
             print("Table already exists")

        load_csv(connection, csv_file_path, "titanic_from_csv")

except Exception as e:
    print(f"Error: {e}")
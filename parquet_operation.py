import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pyarrow.csv as pv
import pyarrow.parquet as pq
import os
import pandas, time

load_dotenv()

csv_file_path = 'files/tested_verylarge.csv'
parquet_file_path = 'files/tested_verylarge.parquet' 

dbname = os.getenv("dbname")
host = os.getenv("host")
password = os.getenv("password")
user = os.getenv("user")
port = os.getenv("port")

try:
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
except Exception as e:
    print(f"Error creating database engine: {e}")
    exit() 

def check_exists(connection, tablename):
    query = text(
         '''
        SELECT EXISTS(
        SELECT FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = :tablename
        )
         '''
    )

    result = connection.execute(query, {'tablename': tablename})
    return result.scalar()

def create_parquet_table(connection):
    table_name = "titanic_from_parquet"
    query = text(f'''
       CREATE TABLE IF NOT EXISTS {table_name} (
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


def load_parquet(connection, parquet_file_path, table_name):
        read_start_time= time.time()
        
        # Read with pandas default engine
        df = pandas.read_parquet(parquet_file_path)  
        read_end_time=time.time()
        read_time= read_end_time-read_start_time
        print(f"Time needed to read with default engine: {read_time}")
        
        # Read with pyarrow
        read_start_time = time.time()
        table = pq.read_table(parquet_file_path)
        read_end_time = time.time()
        read_time = read_end_time - read_start_time
        print(f"Time needed to read with pyarrow: {read_time}")
        
        # Read with fastparquet engine with pandas
        read_start_time = time.time()
        df = pandas.read_parquet(parquet_file_path, engine='fastparquet')
        read_end_time = time.time()
        read_time = read_end_time - read_start_time
        print(f"[fastparquet] Time needed to read: {read_time} seconds")
        
        '''
        write_start_time=time.time()
        df.to_sql(name=table_name, con=connection, if_exists='append', index=False, method='multi', chunksize=10000)
        write_end_time=time.time()
        write_time=write_end_time-write_start_time
        print(f"Time needed to write: {write_time}")
        '''

try:
    with engine.begin() as connection: 
        parquet_table_name = "titanic_from_parquet"
        if not check_exists(connection, parquet_table_name):
            create_parquet_table(connection)
        load_parquet(connection, parquet_file_path, parquet_table_name)

except Exception as e:
    print(f"Error: {e}")

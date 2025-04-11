import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pyarrow.csv as pv
import pyarrow.parquet as pq
import os
import pandas

load_dotenv()

csv_file_path = 'tested.csv'
parquet_file_path = 'tested.parquet' 

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


def convert_csv_to_parquet(csv_file_path, parquet_file_path):
    table = pv.read_csv(csv_file_path)
    pq.write_table(table, parquet_file_path)

def load_parquet(connection, parquet_file_path, table_name):
        df = pandas.read_parquet(parquet_file_path)  
        df.to_sql(name=table_name, con=connection, if_exists='append', index=False, method='multi')

try:
    with engine.begin() as connection: 
        parquet_table_name = "titanic_from_parquet"
        if not check_exists(connection, parquet_table_name):
            create_parquet_table(connection)
        load_parquet(connection, parquet_file_path, parquet_table_name)

except Exception as e:
    print(f"Error: {e}")

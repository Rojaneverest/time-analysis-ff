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
        print(f"Time needed to read with fastparquet: {read_time} seconds")
        
        '''
        write_start_time=time.time()
        df.to_sql(name=table_name, con=connection, if_exists='append', index=False, method='multi', chunksize=10000)
        write_end_time=time.time()
        write_time=write_end_time-write_start_time
        print(f"Time needed to write: {write_time}")
        '''

def visualize_parquet(parquet_file_path):
    try:
        parquet_file = pq.ParquetFile(parquet_file_path)

        print(f"Metadata of '{parquet_file_path}':")
        print("\nSchema:")
        print(parquet_file.schema)

        print("\nFile-level Metadata:")
        print(f"  Format Version: {parquet_file.metadata.format_version}")
        print(f"  Number of Row Groups: {parquet_file.num_row_groups}")
        print(f"  Number of Rows: {parquet_file.metadata.num_rows}")
        print(f"  Created by: {parquet_file.metadata.created_by}")


        for i in range(parquet_file.num_row_groups):
            row_group_metadata = parquet_file.metadata.row_group(i)
            print(f"\nRow Group {i} Metadata:")
            print(f"  Number of Rows: {row_group_metadata.num_rows}")
            print(f"  Total Byte Size: {row_group_metadata.total_byte_size}")
            for j in range(row_group_metadata.num_columns):
                column_metadata = row_group_metadata.column(j)
                print(f"  Column '{parquet_file.schema.names[j]}' Metadata:")
                print(f"    Physical Type: {column_metadata.physical_type}")
                print(f"    Compression: {column_metadata.compression}")
                if column_metadata.statistics:
                    print(f"    Statistics:")
                    if column_metadata.statistics.has_min_max:
                        print(f"      Min: {column_metadata.statistics.min}")
                        print(f"      Max: {column_metadata.statistics.max}")
                    print(f"      Null Count: {column_metadata.statistics.null_count}")

    except Exception as e:
        print(f"An error occurred: {e}")
        
try:
    with engine.begin() as connection: 
        parquet_table_name = "titanic_from_parquet"
        if not check_exists(connection, parquet_table_name):
            create_parquet_table(connection)
        #load_parquet(connection, parquet_file_path, parquet_table_name)
        visualize_parquet(parquet_file_path)

except Exception as e:
    print(f"Error: {e}")

from pyspark.sql import SparkSession
import time
# Create or get the Spark session
spark = SparkSession.builder \
    .appName("ParquetToPostgres") \
    .config("spark.jars", "/Users/rojan/Desktop/data-engineering/titanic-etl/postgresql-42.7.5.jar") \
    .getOrCreate()

# Define your Postgres connection properties
postgres_url = "jdbc:postgresql://localhost:5432/titanic_database"
properties = {
    "user": "postgres",
    "password": "root",
    "driver": "org.postgresql.Driver"
}

# Path to your Parquet file
parquet_file_path = 'files/tested_verylarge.parquet'

start_time_read= time.time()
df = spark.read.parquet(parquet_file_path)
end_time_read= time.time()
time_to_read= end_time_read-start_time_read
print(f"The time to read the parquet file was: {time_to_read}")


start_time_write=time.time()
df.write.jdbc(url=postgres_url, table="titanic_from_parquet", mode="append", properties=properties)
end_time_write=time.time()
time_to_write= end_time_write-start_time_write
print(f"The time to write parquet into db was: {time_to_write}")

print("Parquet data successfully written to PostgreSQL!")



import os
from dotenv import load_dotenv
import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq

# File paths
csv_file_path = "files/tested.csv"
large_csv_file_path = "files/tested_verylarge.csv"
large_parquet_file_path = "files/tested_verylarge.parquet"

try:
    # Read the original CSV file
    df = pd.read_csv(csv_file_path)

    # Create a large DataFrame by duplicating the data 1000 times
    df_large = pd.concat([df] * 10000, ignore_index=True)

    # Write the large DataFrame to a new CSV file
    df_large.to_csv(large_csv_file_path, index=False)
    print(f"Large CSV file created: {large_csv_file_path}, Rows: {len(df_large)}")

    # Convert the large DataFrame to a PyArrow Table and write to Parquet
    table = pv.read_csv(large_csv_file_path)  # Read the large CSV
    pq.write_table(table, large_parquet_file_path, compression='snappy')
    print(f"Large Parquet file created: {large_parquet_file_path}")

except FileNotFoundError as e:
    print(f"Error: File not found - {e}")
except Exception as e:
    print(f"Error during file creation: {e}")
import duckdb
import pandas as pd
import pyarrow.parquet as pq
import os
import numpy as np
# from mapper import map_pyarrow_to_duckdb

csv_file_path = '/Users/kaushikshamantha/Documents/datasets/Parking_Violations_Issued_-_Fiscal_Year_2023_20231208.csv'
chunk_size = 10_000

database_connection = duckdb.connect('database/parking_violations_database.db')

drop_table_query = 'DROP TABLE IF EXISTS parking_violations_raw'
database_connection.execute(drop_table_query)

for file_number, chunk in enumerate(pd.read_csv(csv_file_path, chunksize=chunk_size)):

    parquet_file_path = f'raw_parquet_files/data_chunk_{file_number}.parquet'

    chunk = chunk\
                .astype({"Issuer Squad": str, "Violation Post Code": str})\
                .to_parquet(parquet_file_path, engine='pyarrow', compression='snappy')
    
    # chunk = chunk\
    #             .rename(columns={'Unregistered Vehicle?': 'unregistered vehicle'})\
    #             .astype({"Issuer Squad": str, "Violation Post Code": str})\
    #             .to_parquet(parquet_file_path, engine='pyarrow', compression='snappy')
    
    # chunk.columns = [x.lower().replace(' ', '_') for x in chunk.columns]

    if file_number == 0:
        first_chunk = pq.read_table(parquet_file_path)
        schema = first_chunk.schema

        create_table_query = f"CREATE TABLE parking_violations_raw AS\
                                SELECT * FROM '{parquet_file_path}';"
        database_connection.execute(create_table_query)
        print("Table created in DuckDB")

    else:
        query = f"INSERT INTO parking_violations_raw \
                    SELECT * FROM read_parquet('{parquet_file_path}')"

        database_connection.execute(query)

    os.remove(parquet_file_path)

database_connection.close()

print(file_number)

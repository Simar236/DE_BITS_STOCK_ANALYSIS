import os
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook


def load_csv_files(path: str, table_name: str):
    
    all_dataframes = []

    for filename in os.listdir(path):
        if filename.endswith(".csv"):
            file_path = os.path.join(path, filename)
            df = pd.read_csv(file_path)
            all_dataframes.append(df)
            print(f"Loaded {filename} into DataFrame.")

    combined_dataframe = pd.concat(all_dataframes, ignore_index=True)
    print("Combined DataFrame:")
    print(combined_dataframe)
    insert_dataframe_postgres(combined_dataframe, table_name)

def insert_dataframe_postgres(data, table_name):
    hook = PostgresHook(postgres_conn_id="postgres")
    conn = hook.get_conn()
    cursor = conn.cursor()
    # list of dataframe col
    columns = list(data.columns)
    
    for index, row in data.iterrows():
        #columns string
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Create the SQL statement
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        print(sql)
        
        # Extract the row values as a tuple
        row_values = tuple(row[col] for col in columns)
        
        # Execute the SQL query
        cursor.execute(sql, row_values)
    
    # Commit and close the connection
    conn.commit()
    cursor.close()
    conn.close()

def get_data_from_psql_hw( table_name, start_date, end_date):
    hook = PostgresHook(postgres_conn_id='postgres')
    connection = hook.get_conn()
    cursor = connection.cursor()
                            
    query = f"SELECT * FROM {table_name} a WHERE a.date between '{start_date}' and '{end_date}';"
    cursor.execute(query)

    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results

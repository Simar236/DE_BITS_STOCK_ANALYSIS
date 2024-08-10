import os
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook

def load_csv_files(path: str, table_name_p: str):
    
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

    insert_dataframe_postgres(combined_dataframe, table_name_p)

def insert_dataframe_postgres(data, table_name_p):
    hook = PostgresHook(postgres_conn_id="postgres")
    
    # Use the connection to execute SQL commands
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # Generate SQL for inserting data
    for index, row in data.iterrows():
        sql = f"""
        INSERT INTO {table_name_p} (date, open, high, low, close, adj_close, volume, file_name)
        VALUES ('{row['date']}', {row['open']}, {row['high']}, {row['low']}, {row['close']}, {row['adj_close']}, {row['volume']}, '{row['file_name']}');
        """
        cursor.execute(sql)
    
    # Commit the transaction and close the connection
    conn.commit()
    cursor.close()
    conn.close()

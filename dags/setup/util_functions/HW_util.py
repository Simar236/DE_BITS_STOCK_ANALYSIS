from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime

def get_end_date(date_str):
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    next_year_date = date_obj.replace(year=date_obj.year + 1)
    next_year_date_str = next_year_date.strftime('%Y-%m-%d')
    
    return next_year_date_str

def insert_HW_value(table_name,value,update_flag):
    hook = PostgresHook(postgres_conn_id="postgres")
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    if update_flag:
        sql = f"""
            UPDATE {table_name}
            SET date = '{value}';
        """
    else:
        sql = f"""
            INSERT INTO {table_name} (date)
            VALUES ('{value}');
        """
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

def get_HW_value(table_name, **kwargs):
    hook = PostgresHook(postgres_conn_id='postgres')
    connection = hook.get_conn()
    cursor = connection.cursor()
    
    query = f"SELECT date FROM {table_name};"  
    cursor.execute(query)
    result = cursor.fetchone()
    
    cursor.close()
    connection.close()
    print(result[0])
    kwargs['ti'].xcom_push(key='HW_value', value=str(result[0]))
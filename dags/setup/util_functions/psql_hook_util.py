from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

def execute(query):
    hook = PostgresHook(postgres_conn_id="postgres")
    
    conn = hook.get_conn()
    cursor = conn.cursor()    
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
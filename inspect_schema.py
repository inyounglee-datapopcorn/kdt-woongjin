
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

def check_schema():
    hook = PostgresHook(postgres_conn_id='cometj456_supabase_conn')
    sql = """
    SELECT column_name, data_type, character_maximum_length
    FROM information_schema.columns
    WHERE table_name = 'realtime_subway_positions';
    """
    rows = hook.get_records(sql)
    for row in rows:
        print(f"Column: {row[0]}, Type: {row[1]}, Max Length: {row[2]}")

if __name__ == "__main__":
    try:
        check_schema()
    except Exception as e:
        print(f"Error: {e}")

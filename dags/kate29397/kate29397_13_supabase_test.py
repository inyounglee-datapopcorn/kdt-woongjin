from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum

# Define the DAG
with DAG(
    dag_id='popcorn_13_supabase_test_dag',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule='0 0 * * *',
    catchup=False,
) as dag:

    # 1. Create a test table
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='supabase_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS airflow_test_table (
                id SERIAL PRIMARY KEY,
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    # 2. Insert test data
    insert_data = SQLExecuteQueryOperator(
        task_id='insert_data',
        conn_id='supabase_conn',
        sql="""
            INSERT INTO airflow_test_table (message) 
            VALUES ('Hello from Airflow! Supabase Connection SUCCESS :)');
        """
    )

    create_table >> insert_data

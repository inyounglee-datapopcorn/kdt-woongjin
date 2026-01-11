from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum

# Define the DAG
with DAG(
    dag_id='13_supabase_test_dag',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule_interval=None,
    catchup=False,
    tags=['supabase', 'db', 'test'],
) as dag:

    # 1. Create a test table
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='supabase_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS airflow_test_table (
                id SERIAL PRIMARY KEY,
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    # 2. Insert test data
    insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='supabase_conn',
        sql="""
            INSERT INTO airflow_test_table (message) 
            VALUES ('Hello from Airflow! Supabase Connection SUCCESS :)');
        """
    )

    create_table >> insert_data

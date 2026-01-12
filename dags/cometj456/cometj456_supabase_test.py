from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum

# Define the DAG
with DAG(
    dag_id='cometj456_supabase_test_dag_v2',
    start_date=pendulum.today('Asia/Seoul').add(days=-1), # 한국 시간 기준
    schedule='0 0 * * *',
    catchup=False,
    tags=['cometj456', 'supabase', 'test'],
) as dag:

    # 1. Create a test table
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='cometj456_supabase_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS cometj456_test_table (
                id SERIAL PRIMARY KEY,
                message TEXT,
                created_at TIMESTAMPTZ DEFAULT now()
            );
        """
    )

    # 2. Insert test data
    insert_data = SQLExecuteQueryOperator(
        task_id='insert_data',
        conn_id='cometj456_supabase_conn',
        sql="""
            INSERT INTO cometj456_test_table (message) 
            VALUES ('Hello from Airflow! 한국 시간 테스트입니다 :)');
        """
    )

    create_table >> insert_data

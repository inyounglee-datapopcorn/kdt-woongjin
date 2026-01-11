import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
"""
🔸 SQLExecuteQueryOperator
    - 특정 데이터베이스를 대상으로 SQL 쿼리를 실행할 수 있도록 해주는 오퍼레이터
    - Airflow UI에서 대상 DB에 대한 connection 설정 필요!

🔸 Connection 설정 정보
    - Conn Id : supabase_conn
    - Host : AWS / Supabase 등 외부 DB Host
    - Schema : postgres

https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/_api/airflow/providers/common/sql/operators/sql/index.html#airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator
"""

default_args = dict(
    owner = 'popcorn',
    email = ['datapopcorn@gmail.com'],
    email_on_failure = False,
    retries = 3
    )

with DAG(
    dag_id="08_database_operator_dag",
    start_date=pendulum.datetime(2025, 8, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *",
    tags = ['20250824','INTERMEDIATE'],
    default_args = default_args,
    catchup=False
):
    
    DB_CONNECTION_ID = 'supabase_conn'
    
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id=DB_CONNECTION_ID,
        sql="CREATE TABLE IF NOT EXISTS mytable(id INT, name VARCHAR(10));",
    )
    
    insert_rows = SQLExecuteQueryOperator(
        task_id = "insert_rows",
        conn_id = DB_CONNECTION_ID,
        sql = "INSERT INTO mytable VALUES(1,'Ryan'),(2,'Alice'),(3,'Tom');",
    )

    update_rows = SQLExecuteQueryOperator(
        task_id = "update_rows",
        conn_id = DB_CONNECTION_ID,
        sql = "UPDATE mytable SET NAME='Peter' WHERE id=3;",
    )

    # delete_rows = SQLExecuteQueryOperator(
    #     task_id = "delete_rows",
    #     conn_id = DB_CONNECTION_ID,
    #     sql = "DELETE FROM mytable WHERE id=1",
    #     database = 'airflow',
    #     autocommit=True
    # )


create_table >> insert_rows >> update_rows
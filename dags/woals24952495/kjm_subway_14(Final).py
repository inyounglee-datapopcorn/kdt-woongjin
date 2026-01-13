import logging
import requests
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook 

# ==========================================================
# [설정값 입력 구역]
# ==========================================================
MY_SEOUL_API_KEY = "43434e536b776f6137326e4e664152"  
MY_CONN_ID = "jaemin1077_supabase_conn" 
MY_TABLE_NAME = "realtime_subway_positions_woals"
# ==========================================================

TARGET_LINES = [
    "1호선", "2호선", "3호선", "4호선", "5호선", "6호선", "7호선", "8호선", "9호선",
    "경의중앙선", "공항철도", "경춘선", "수인분당선", "신분당선", "우이신설선", "GTX-A"
]

default_args = dict(
    owner = 'woals24952495',
    email_on_failure = False,
    retries = 1
)

with DAG(
    dag_id="woals24952495_subway_14", 
    start_date=pendulum.today('Asia/Seoul').add(days=-1),
    schedule="*/1 * * * *",  # 실시간 1분 수집
    catchup=False,
    default_args=default_args,
    tags=['subway', 'realtime', 'woals'],
) as dag:

    # 1. 테이블 생성 (없을 경우 실행)
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id=MY_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {MY_TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                line_id VARCHAR(50),
                line_name VARCHAR(50),
                station_id VARCHAR(50),
                station_name VARCHAR(50),
                train_number VARCHAR(50),
                last_rec_date VARCHAR(50),
                last_rec_time TIMESTAMPTZ,
                direction_type INT,
                dest_station_id VARCHAR(50),
                dest_station_name VARCHAR(50),
                train_status INT,
                is_express INT DEFAULT 0,
                is_last_train BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    # 2. 데이터 수집 및 적재 태스크
    @task(task_id='collect_and_insert_subway_data')
    def collect_and_insert_subway_data():
        import pandas as pd
        
        logging.info(f"Connecting to Supabase using: {MY_CONN_ID}")
        
        try:
            hook = PostgresHook(postgres_conn_id=MY_CONN_ID)
            conn = hook.get_sqlalchemy_engine()
        except Exception as e:
            logging.error(f"Connection Error: {e}")
            raise
        
        all_records = []
        
        for line in TARGET_LINES:
            try:
                url = f"http://swopenapi.seoul.go.kr/api/subway/{MY_SEOUL_API_KEY}/json/realtimePosition/1/100/{line}"
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                
                if 'realtimePositionList' in data:
                    items = data['realtimePositionList']
                    logging.info(f"{line}: Found {len(items)} trains")
                    
                    for item in items:
                        record = {
                            "line_id": item.get("subwayId"),
                            "line_name": item.get("subwayNm"),
                            "station_id": item.get("statnId"),
                            "station_name": item.get("statnNm"),
                            "train_number": item.get("trainNo"),
                            "last_rec_date": item.get("lastRecptnDt"),
                            "last_rec_time": pendulum.parse(item.get("recptnDt"), tz='Asia/Seoul') if item.get("recptnDt") else None,
                            "direction_type": int(item.get("updnLine")) if item.get("updnLine") and str(item.get("updnLine")).isdigit() else None,
                            "dest_station_id": item.get("statnTid"),
                            "dest_station_name": item.get("statnTnm"),
                            "train_status": int(item.get("trainSttus")) if item.get("trainSttus") and str(item.get("trainSttus")).isdigit() else None,
                            "is_express": int(item.get("directAt")) if item.get("directAt") and str(item.get("directAt")).isdigit() else 0,
                            "is_last_train": item.get("lstcarAt") == "1"
                        }
                        all_records.append(record)
            except Exception as e:
                logging.error(f"Error fetching data for {line}: {e}")
                continue
                
        if all_records:
            df = pd.DataFrame(all_records)
            df.to_sql(
                MY_TABLE_NAME,
                con=conn,
                if_exists='append',
                index=False,
                method='multi' 
            )
            logging.info(f"Insert completed: {len(all_records)} rows.")
        else:
            logging.info("No records to insert.")

    create_table >> collect_and_insert_subway_data()

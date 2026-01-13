import logging
import requests
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook 
from airflow.providers.slack.hooks.slack import SlackHook 
from airflow.models import Variable

# Configuration
SEOUL_API_KEY = "5064496c74746a673130306b6265574d"  # 실제 운영 시 Variable이나 Connection으로 관리 권장
TARGET_LINES = [
    "1호선", "2호선", "3호선", "4호선", "5호선", 
    "6호선", "7호선", "8호선", "9호선",
    "경의중앙선", "공항철도", "수인분당선", "신분당선"
]

default_args = dict(
    owner = 'airflow',
    email = ['tjgk1203@gmail.com'],
    email_on_failure = False,
    retries = 1
)

def on_success_callback(context):
    dag_id = context['dag'].dag_id
    var_key = f"{dag_id}_success_sent"

    # Variable 조회 (없으면 False로 간주)
    already_sent = Variable.get(var_key, default_var="false")

    if already_sent == "true":
        # 이미 성공 알림을 보냈으면 아무 것도 하지 않음
        return

    # 최초 성공인 경우만 Slack 전송
    slack_hook = SlackHook(slack_conn_id='tjgk1203_slack_conn')
    text = f":train: DAG {dag_id} completed successfully."

    try:
        slack_hook.client.chat_postMessage(
            channel='#bot-playground',
            text=text
        )
        # 성공적으로 보냈으면 Variable 기록
        Variable.set(var_key, "true")
    except Exception as e:
        # Slack 실패 시 DAG 실패로 만들지 않음 (권장)
        logging.error(f"Slack success notification failed: {e}")

def on_failure_callback(context):
    slack_hook = SlackHook(slack_conn_id='tjgk1203_slack_conn')
    text = f":x: DAG *{context['dag'].dag_id}* failed."
    try:
        slack_hook.client.chat_postMessage(channel='#bot-playground', text=text)
    except Exception as e:
        logging.error(f"Failed to send Slack failure notification: {e}")

with DAG(
    dag_id="tjgk1203_14_seoul_subway_monitor",
    start_date=pendulum.today('Asia/Seoul').add(days=-1),
    schedule="*/1 * * * *",  # 1분마다 실행
    catchup=False,
    default_args=default_args,
    tags=['subway', 'project'],
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,

) as dag:

    # 1. 테이블 생성 (없을 경우)
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='tjgk1203_supabase_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS realtime_subway_positions (
                id SERIAL PRIMARY KEY,
                line_id VARCHAR(50),
                line_name VARCHAR(50),
                station_id VARCHAR(50),
                station_name VARCHAR(50),
                train_number VARCHAR(50),
                last_rec_date VARCHAR(50),
                last_rec_time VARCHAR(50),
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
        hook = PostgresHook(postgres_conn_id='tjgk1203_supabase_conn')
        conn = hook.get_sqlalchemy_engine()
        
        all_records = []
        
        for line in TARGET_LINES:
            try:
                # API 호출
                url = f"http://swopenapi.seoul.go.kr/api/subway/{SEOUL_API_KEY}/json/realtimePosition/1/100/{line}"
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                
                # 데이터 파싱
                if 'realtimePositionList' in data:
                    items = data['realtimePositionList']
                    logging.info(f"{line}: Found {len(items)} trains")
                    
                    for item in items:
                        # 매핑
                        record = {
                            "line_id": item.get("subwayId"),
                            "line_name": item.get("subwayNm"),
                            "station_id": item.get("statnId"),
                            "station_name": item.get("statnNm"),
                            "train_number": item.get("trainNo"),
                            "last_rec_date": item.get("lastRecptnDt"),
                            "last_rec_time": item.get("recptnDt"),
                            "direction_type": int(item.get("updnLine")) if item.get("updnLine") and str(item.get("updnLine")).isdigit() else None,
                            "dest_station_id": item.get("statnTid"),
                            "dest_station_name": item.get("statnTnm"),
                            "train_status": int(item.get("trainSttus")) if item.get("trainSttus") and str(item.get("trainSttus")).isdigit() else None,
                            "is_express": int(item.get("directAt")) if item.get("directAt") and str(item.get("directAt")).isdigit() else 0,
                            "is_last_train": item.get("lstcarAt") == "1"
                        }
                        all_records.append(record)
                else:
                    logging.info(f"{line}: No data found")
                    
            except Exception as e:
                logging.error(f"Error fetching data for {line}: {e}")
                continue
                
        # 일괄 적재
        if all_records:
            logging.info(f"Inserting total {len(all_records)} records into Supabase...")
            import pandas as pd
            df = pd.DataFrame(all_records)
            df.to_sql(
                'realtime_subway_positions',
                con=conn,
                if_exists='append',
                index=False,
                method='multi' # 성능 향상을 위해 multi insert
            )
            logging.info("Insert completed.")
        else:
            logging.info("No records to insert.")

    ingestion_task = collect_and_insert_subway_data()

    create_table >> ingestion_task

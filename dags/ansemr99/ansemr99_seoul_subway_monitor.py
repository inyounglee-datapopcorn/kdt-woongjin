import logging
import requests
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook 

# Configuration
SEOUL_API_KEY = "6a77636b51616e7337307370656c4a"  # 실제 운영 시 Variable이나 Connection으로 관리 권장
TARGET_LINES = [
    "1호선", "2호선", "3호선", "4호선", "5호선", 
    "6호선", "7호선", "8호선", "9호선",
    "경의중앙선", "공항철도", "수인분당선", "신분당선"
]

default_args = dict(
    owner = 'ansemr99',
    email = ['ansemr99@gmail.com'],
    email_on_failure = False,
    retries = 1
)

with DAG(
    dag_id="ansemr99_seoul_subway_monitor",
    start_date=pendulum.today('Asia/Seoul').add(days=-1),
    schedule="*/5 * * * *", # 5분마다 실행
    catchup=False,
    default_args=default_args,
    tags=['subway', 'project', 'slack'],
) as dag:

    # 1. 데이터 수집 및 적재 태스크
    @task(task_id='collect_and_insert_subway_data')
    def collect_and_insert_subway_data():
        hook = PostgresHook(postgres_conn_id='ansemr99_supabase_conn')
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
                            "last_rec_time": pendulum.parse(item.get("recptnDt"), tz='Asia/Seoul') if item.get("recptnDt") else None,
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
        record_count = len(all_records)
        if all_records:
            logging.info(f"Inserting total {record_count} records into Supabase...")
            import pandas as pd
            df = pd.DataFrame(all_records)
            df.to_sql(
                'realtime_subway_positions_2',
                con=conn,
                if_exists='append',
                index=False,
                method='multi' # 성능 향상을 위해 multi insert
            )
            logging.info("Insert completed.")
        else:
            logging.info("No records to insert.")
        
        return record_count

    # 2. 슬랙 알림 태스크
    # send_slack_notification = SlackAPIPostOperator(
    #     task_id='send_slack_notification',
    #     slack_conn_id='ansemr99_slack_conn',
    #     channel='#bot-playground',
    #     text=":white_check_mark: *지하철 데이터 적재 완료*\n"
    #          "- 대상 테이블: `realtime_subway_positions_2`\n"
    #          "- 적재된 레코드 수: {{ task_instance.xcom_pull(task_ids='collect_and_insert_subway_data') }}개\n",
    #     username='승우봇'
    # )

    ingestion_task = collect_and_insert_subway_data()

    # ingestion_task >> send_slack_notification

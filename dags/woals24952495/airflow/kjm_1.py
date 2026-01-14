import logging
import requests
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configuration
SEOUL_API_KEY = "43434e536b776f6137326e4e664152" # <--- [변경 필요] 1번 키
TARGET_LINES = [
    "1호선", "2호선", "3호선", "4호선", "5호선",
    "6호선", "7호선", "8호선", "9호선",
    "경의중앙선", "공항철도", "수인분당선", "경춘선"
]
default_args = dict(
    owner = 'woals24952495', # <--- [변경 완료]
    email = ['woals24952495@naver.com'], # <--- [변경 완료] (임의 설정)
    email_on_failure = False,
    retries = 1
) 
with DAG(
    dag_id="woals24952495_subway_test_1800", # <--- [테스트용 변경]
    start_date=pendulum.today('Asia/Seoul').add(days=-1),
    schedule="0-19 18 * * *",  # 18:00 ~ 18:19 매분 실행 (20분간)
    catchup=False,
    default_args=default_args,
    tags=['subway', 'project', 'woals24952495'], # <--- [변경 완료]
) as dag:
    # 2. 데이터 수집 및 적재 태스크
    @task(task_id='collect_and_insert_subway_data')
    def collect_and_insert_subway_data():
        hook = PostgresHook(postgres_conn_id='jaemin1077_supabase_conn') # <--- [변경 완료]
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
                            "line_id": item.get("subwayId"), # 10번째 컬럼으로 추가 (호선 ID)
                            "line_name": item.get("subwayNm"),
                            "station_name": item.get("statnNm"),
                            "up_down": int(item.get("updnLine")) if item.get("updnLine") and str(item.get("updnLine")).isdigit() else None,
                            "is_express": int(item.get("directAt")) if item.get("directAt") and str(item.get("directAt")).isdigit() else 0,
                            "train_code": item.get("trainNo"),
                            "train_status": int(item.get("trainSttus")) if item.get("trainSttus") and str(item.get("trainSttus")).isdigit() else None,
                            "last_rec_time": pendulum.parse(item.get("recptnDt"), tz='Asia/Seoul') if item.get("recptnDt") else None,
                            "dest_station_id": item.get("statnTid"),
                            "dest_station_name": item.get("statnTnm")
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
                'final_realtime_subway',
                con=conn,
                if_exists='append',
                index=False,
                method='multi' # 성능 향상을 위해 multi insert
            )
            logging.info("Insert completed.")
        else:
            logging.info("No records to insert.")
    ingestion_task = collect_and_insert_subway_data()
import logging
import requests
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook 

# ==========================================================
# [설정값 입력 구역] - 실습 시 이 부분을 본인의 정보로 수정하세요!
# ==========================================================
# 1. API 키: 본인의 서울시 공공데이터 API 인증키를 입력하세요.
MY_SEOUL_API_KEY = "43434e536b776f6137326e4e664152"  

# 2. Connection ID: Airflow UI(Admin > Connections)에 등록한 본인의 Supabase 연결 ID를 입력하세요.
MY_CONN_ID = "jaemin1077_supabase_conn" 

# 3. 테이블명: 데이터를 적재할 테이블 이름을 지정하세요.
MY_TABLE_NAME = "realtime_subway_positions_woals"
# ==========================================================

TARGET_LINES = [
    "1호선", "2호선", "3호선", "4호선", "5호선", 
    "6호선", "7호선", "8호선", "9호선",
    "경의중앙선", "공항철도", "수인분당선", "신분당선"
]

default_args = dict(
    owner = 'woals24952495', # 본인의 소유자 이름으로 변경 가능
    email_on_failure = False,
    retries = 1
)

with DAG(
    dag_id="woals24952495_subway_14", 
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Seoul'),
    schedule="*/1 * * * *",  # 1분마다 실행
    catchup=False,
    is_paused_upon_creation=False, # 서버 재시작 시 자동 On 설정
    default_args=default_args,
    tags=['subway', 'project', 'woals'],
) as dag:

    # 1. 테이블 생성 (없을 경우 실행)
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id=MY_CONN_ID, # [확인] 본인의 연결 ID가 사용됨
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
        # [주의] Pandas 라이브러리가 Airflow 환경에 설치되어 있어야 합니다.
        import pandas as pd
        
        logging.info(f"Connecting to Supabase using: {MY_CONN_ID} (Port 6543 confirmed by USER)")
        
        try:
            hook = PostgresHook(postgres_conn_id=MY_CONN_ID) # [확인] 본인의 연결 ID가 사용됨
            conn = hook.get_sqlalchemy_engine()
            logging.info("Connection Engine Created Successfully!")
        except Exception as e:
            logging.error(f"Critical Connection Error (Check Port 6543 & Host): {e}")
            raise
        
        all_records = []
        
        for line in TARGET_LINES:
            try:
                # API 호출 (본인의 API 키 사용)
                url = f"http://swopenapi.seoul.go.kr/api/subway/{MY_SEOUL_API_KEY}/json/realtimePosition/1/100/{line}"
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                
                # 데이터 파싱
                if 'realtimePositionList' in data:
                    items = data['realtimePositionList']
                    logging.info(f"{line}: Found {len(items)} trains")
                    
                    for item in items:
                        # 데이터 매핑
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
        if all_records:
            logging.info(f"Inserting total {len(all_records)} records into Supabase...")
            df = pd.DataFrame(all_records)
            df.to_sql(
                MY_TABLE_NAME, # [확인] 본인이 지정한 테이블명 사용
                con=conn,
                if_exists='append',
                index=False,
                method='multi' 
            )
            logging.info("Insert completed.")
        else:
            logging.info("No records to insert.")

    ingestion_task = collect_and_insert_subway_data()

    create_table >> ingestion_task

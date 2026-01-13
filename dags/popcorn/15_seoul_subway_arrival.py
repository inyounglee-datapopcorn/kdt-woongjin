import logging
import requests
import pendulum
from airflow import DAG
from airflow.sdk import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook 

# Configuration
SEOUL_API_KEY = "5a4c61745532696e393557726c696f"

# 주요 환승역 및 거점 역 (전체 역 조회 시 API 과부하 방지를 위해 주요 역 우선 수집)
TARGET_STATIONS = [
    "서울", "강남", "홍대입구", "잠실", "신도림", "고속터미널", "사당", 
    "건대입구", "노원", "종로3가", "영등포", "여의도", "용산", "왕십리"
]

default_args = dict(
    owner = 'datapopcorn',
    email = ['datapopcorn@gmail.com'],
    email_on_failure = False,
    retries = 1
)

with DAG(
    dag_id="popcorn_15_seoul_subway_arrival",
    start_date=pendulum.today('Asia/Seoul').add(days=-1),
    schedule="*/5 * * * *",  # 5분마다 실행
    catchup=False,
    default_args=default_args,
    tags=['subway', 'realtime', 'arrival'],
) as dag:

    # 1. 테이블 생성
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='teampopcorn_supabase_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS realtime_subway_arrivals (
                id SERIAL PRIMARY KEY,
                station_name VARCHAR(50),
                subway_id VARCHAR(50),
                train_line_nm VARCHAR(100),
                arvl_msg2 VARCHAR(255),
                arvl_msg3 VARCHAR(255),
                arvl_cd VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )
    
    # 2. 데이터 수집 -> 적재
    @task(task_id='fetch_and_insert_arrivals')
    def fetch_and_insert_arrivals():
        hook = PostgresHook(postgres_conn_id='teampopcorn_supabase_conn')
        conn = hook.get_sqlalchemy_engine()
        all_records = []

        for station in TARGET_STATIONS:
            try:
                # 서울역의 경우 '서울' or '서울역' 혼용 가능성 고려, API는 보통 역명 그대로 사용
                # URL Encoding이 필요할 수 있으나 requests가 처리함
                url = f"http://swopenapi.seoul.go.kr/api/subway/{SEOUL_API_KEY}/json/realtimeStationArrival/0/20/{station}"
                
                response = requests.get(url)
                data = response.json()
                
                if 'realtimeArrivalList' in data:
                    for item in data['realtimeArrivalList']:
                        record = {
                            "station_name": item.get("statnNm"),
                            "subway_id": item.get("subwayId"),
                            "train_line_nm": item.get("trainLineNm"), # 도착지 방면 (성수행 - 내선)
                            "arvl_msg2": item.get("arvlMsg2"), # 전역 진입, 2분 후 등
                            "arvl_msg3": item.get("arvlMsg3"), # 현재 역
                            "arvl_cd": item.get("arvlCd"), # 도착 코드
                        }
                        all_records.append(record)
                else:
                    logging.info(f"No arrival data for {station}")

            except Exception as e:
                logging.error(f"Failed to fetch {station}: {e}")
                continue
        
        if all_records:
            import pandas as pd
            df = pd.DataFrame(all_records)
            df.to_sql('realtime_subway_arrivals', con=conn, if_exists='append', index=False, method='multi')
            logging.info(f"Inserted {len(df)} arrival records.")

    create_table >> fetch_and_insert_arrivals()

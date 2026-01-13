from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
import pendulum
import requests
import logging

# [설정값]
MY_TABLE_NAME = "test_kjm_subway" # 현재 코드에서 사용 중인 테이블명
MY_CONN_ID = "jaemin1077_supabase_conn"

# [핵심 로직 1] API 수집 및 데이터 변환
def fetch_and_transform_subway_data(api_key):
    # 1호선~9호선 + 경의중앙, 공항철도, 경춘, 수인분당, 신분당, 우이신설, GTX-A
    target_lines = [
        "1호선", "2호선", "3호선", "4호선", "5호선", "6호선", "7호선", "8호선", "9호선",
        "경의중앙선", "공항철도", "경춘선", "수인분당선", "신분당선", "우이신설선", "GTX-A"
    ]
      
    all_transformed_rows = []

    for line in target_lines:
        # [수정] 0/100 -> 1/100 (서울시 API는 1부터 시작해야 데이터를 줍니다)
        url = f"http://swopenapi.seoul.go.kr/api/subway/{api_key}/json/realtimePosition/1/100/{line}"
        
        logging.info(f"{line} 데이터 수집 중...")
        try:
            response = requests.get(url)
            data = response.json()
            
            if 'realtimePositionList' in data:
                for item in data['realtimePositionList']:
                    row = (
                        item.get('subwayId'),            # line_id
                        item.get('subwayNm'),            # line_name
                        item.get('statnId'),             # station_id
                        item.get('statnNm'),             # station_name
                        item.get('trainNo'),             # train_number
                        item.get('lastRecptnDt'),        # last_rec_date
                        item.get('recptnDt'),            # last_rec_time
                        int(item.get('updnLine', 0)) if str(item.get('updnLine', '0')).isdigit() else 0, # direction_type
                        item.get('statnTid'),            # dest_station_id
                        item.get('statnTnm'),            # dest_station_name
                        item.get('trainSttus'),          # train_status
                        item.get('directAt'),            # is_express
                        item.get('lstcarAt') == '1'      # is_last_train
                    )
                    all_transformed_rows.append(row)
                logging.info(f"{line} 수집 완료: {len(data['realtimePositionList'])}개")
            else:
                logging.warning(f"{line}에 현재 운행 중인 열차가 없습니다.")
        except Exception as e:
            logging.error(f"{line} 수집 중 에러 발생: {e}")
            
    return all_transformed_rows

# [핵심 로직 2] DB 적재
def load_data_to_db(**context):
    rows = context['task_instance'].xcom_pull(task_ids='fetch_and_transform')
    
    if not rows:
        logging.info("적재할 데이터가 없습니다. (API 응답 확인 필요)")
        return

    pg_hook = PostgresHook(postgres_conn_id=MY_CONN_ID)
    
    target_fields = [
        'line_id', 'line_name', 'station_id', 'station_name', 'train_number',
        'last_rec_date', 'last_rec_time', 'direction_type', 'dest_station_id',
        'dest_station_name', 'train_status', 'is_express', 'is_last_train'
    ]
    
    pg_hook.insert_rows(
        table=MY_TABLE_NAME,
        rows=rows,
        target_fields=target_fields
    )
    logging.info(f"성공적으로 {len(rows)}개의 데이터를 '{MY_TABLE_NAME}' 테이블에 적재했습니다.")

# [DAG 정의]
with DAG(
    dag_id='woals24952495_kjm_test1_monitor',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule='*/1 * * * *',
    catchup=False,
    tags=['subway', 'kim_test1', 'realtime_all_lines'],
) as dag:

    # 1. 테이블 생성 태스크 추가
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
                last_rec_time TEXT,
                direction_type INT,
                dest_station_id VARCHAR(50),
                dest_station_name VARCHAR(50),
                train_status VARCHAR(50),
                is_express VARCHAR(50),
                is_last_train BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    # 2. 데이터 수집 및 변환
    task_fetch = PythonOperator(
        task_id='fetch_and_transform',
        python_callable=fetch_and_transform_subway_data,
        op_kwargs={'api_key': '{{ var.value.kjm_subway_api_key }}'}
    )

    # 3. DB 적재
    task_load = PythonOperator(
        task_id='load_to_db',
        python_callable=load_data_to_db
    )

    # 순서: 테이블 생성 -> 수집 -> 적재
    create_table >> task_fetch >> task_load
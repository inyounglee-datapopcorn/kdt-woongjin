import logging
import requests
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# [설정] 본인이 보유한 4개의 API 키 리스트
API_KEYS = [
    "54684a47546f683435384d714c6e71", # 1번 키 (최신)
    "434459696c6f6b6b3130347378545273", # 2번 키
    "6e71466270636b733633654f6b4a7a", # 3번 키
    "43434e536b776f6137326e4e664152"  # 4번 키
]

TARGET_LINES = [
    "1호선", "2호선", "3호선", "4호선", "5호선",
    "6호선", "7호선", "8호선", "9호선",
    "경의중앙선", "공항철도", "수인분당선", "경춘선"
]

default_args = dict(
    owner = 'woals24952495',
    retries = 1,
    retry_delay = pendulum.duration(seconds=30)
) 

with DAG(
    dag_id="woals24952495_subway_smart_collector", # 통합 DAG ID
    start_date=pendulum.datetime(2026, 1, 15, 6, 0, tz='Asia/Seoul'), # 1월 15일 06시 정각에 시작
    schedule="* * * * *",  # 15일 06시부터 중단 없이 매분 계속 실행
    catchup=False,
    default_args=default_args,
    tags=['subway', 'project', 'multi_key'],
) as dag:

    @task(task_id='collect_with_key_rotation')
    def collect_with_key_rotation():
        hook = PostgresHook(postgres_conn_id='jaemin1077_supabase_conn')
        conn = hook.get_sqlalchemy_engine()
        
        all_records = []
        working_key_index = 0 # 사용할 키 인덱스
        
        for line in TARGET_LINES:
            success_for_this_line = False
            
            # 현재 키부터 마지막 키까지 시도
            for i in range(working_key_index, len(API_KEYS)):
                current_key = API_KEYS[i]
                try:
                    url = f"http://swopenapi.seoul.go.kr/api/subway/{current_key}/json/realtimePosition/1/100/{line}"
                    response = requests.get(url)
                    data = response.json()
                    
                    # 용량 초과(ERROR-290) 또는 인증 오류(INFO-100) 체크
                    if 'RESULT' in data and data['RESULT']['CODE'] in ['ERROR-290', 'INFO-100']:
                        logging.warning(f"Key {i+1} failed ({data['RESULT']['CODE']}). Trying next key...")
                        working_key_index = i + 1 # 다음 키로 인덱스 업데이트
                        continue 
                    
                    # 정상 데이터인 경우
                    if 'realtimePositionList' in data:
                        items = data['realtimePositionList']
                        for item in items:
                            all_records.append({
                                "line_id": item.get("subwayId"),
                                "line_name": item.get("subwayNm"),
                                "station_name": item.get("statnNm"),
                                "up_down": int(item.get("updnLine")) if str(item.get("updnLine")).isdigit() else None,
                                "is_express": int(item.get("directAt")) if str(item.get("directAt")).isdigit() else 0,
                                "train_code": item.get("trainNo"),
                                "train_status": int(item.get("trainSttus")) if str(item.get("trainSttus")).isdigit() else None,
                                "last_rec_time": pendulum.parse(item.get("recptnDt"), tz='Asia/Seoul') if item.get("recptnDt") else None,
                                "dest_station_id": item.get("statnTid"),
                                "dest_station_name": item.get("statnTnm")
                            })
                        logging.info(f"{line}: Found {len(items)} trains using Key {i+1}")
                    
                    success_for_this_line = True
                    break # 성공했으므로 다음 호선으로 넘어감
                    
                except Exception as e:
                    logging.error(f"Error with Key {i+1} for {line}: {e}")
                    continue
            
            if not success_for_this_line:
                logging.error(f"!!! CRITICAL: All keys failed for {line} !!!")

        # 데이터 적재
        if all_records:
            import pandas as pd
            df = pd.DataFrame(all_records)
            df.to_sql('final_realtime_subway', con=conn, if_exists='append', index=False, method='multi')
            logging.info(f"Successfully inserted {len(all_records)} records.")
        else:
            logging.warning("No data collected in this run.")

    collect_with_key_rotation()
import logging
import requests
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# [설정] API 키 리스트
API_KEYS = [
    "54684a47546f683435384d714c6e71", # 1번 키
    "434459696c6f6b6b3130347378545273", # 2번 키
    "6e71466270636b733633654f6b4a7a", # 3번 키
    "43434e536b776f6137326e4e664152", # 4번 키
    "5667524e7167757332364c65594648", # 5번 키
    "694a557777776f613238787166786l", # 6번 키
    "4a53747648776f6136334c58506d79", # 7번 키
    "5363645066776f61353564766e6669", # 8번 키
    "55517a4a78776f6134326654477043", # 9번 키
    "5672515469776f6133394a656f6853", # 10번 키
    "7a596d6865776f6132327343445945", # 11번 키
    "7a42726656776f6135384a7443496l"  # 12번 키
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
    dag_id="woals24952495_subway_smart_collector",
    start_date=pendulum.datetime(2026, 1, 15, 5, 30, tz='Asia/Seoul'),
    schedule="* * * * *",
    catchup=False,
    default_args=default_args,
    tags=['subway', 'project', 'debug_mode'],
) as dag:

    @task(task_id='collect_with_key_rotation')
    def collect_with_key_rotation():
        hook = PostgresHook(postgres_conn_id='jaemin1077_supabase_conn')
        conn = hook.get_sqlalchemy_engine()
        all_records = []
        
        # [호선 루프] 매 호선(1호선, 2호선...)을 돌 때마다 여기서 시작하여 API 키가 1번부터 다시 시도됩니다.
        for line in TARGET_LINES:
            success = False
            logging.info(f"--- Fetching {line} ---")
            
            # [키 로테이션 루프] 현재 호선에 대해 성공할 때까지 1번~12번 키를 순서대로 꺼내어 시도합니다.
            for i, key in enumerate(API_KEYS):
                try:
                    url = f"http://swopenapi.seoul.go.kr/api/subway/{key}/json/realtimePosition/1/100/{line}"
                    res = requests.get(url, timeout=10)
                    
                    if res.status_code != 200:
                        logging.warning(f"  [Key {i+1}] HTTP {res.status_code} Error. Trying next...")
                        continue

                    data = res.json()
                    res_code = data.get('RESULT', {}).get('CODE', 'INFO-000')
                    
                    if res_code == 'INFO-000' and 'realtimePositionList' in data:
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
                        logging.info(f"  [Key {i+1}] Success! Found {len(items)} trains.")
                        success = True
                        # [STEP 4] 수집 성공: 다음 호선으로 이동 (불필요한 키 호출 방지로 리소스 최적화)
                        break # [로테이션 중단] 성공했으므로 현재 키 시도를 멈추고 다음 호선으로 넘어갑니다. (다음 호선은 다시 1번 키부터)
                    
                    elif res_code == 'INFO-200':
                        logging.info(f"  [Key {i+1}] No trains on this line. (OK)")
                        success = True
                        break # [로테이션 중단] 운행 열차가 없어도 정상 응답이므로 현재 호선은 종료하고 다음 호선으로 넘어갑니다.
                    
                    else:
                        logging.warning(f"  [Key {i+1}] API Error: {res_code}. Trying next...")
                        continue # [키 전환] API 에러가 났으므로 i+1번째 키를 쓰러 건너뜁니다.

                except Exception as e:
                    logging.error(f"  [Key {i+1}] Unexpected Exception: {e}")
                    continue
            
            if not success:
                logging.error(f"  !!! All keys failed for {line} !!!")

        if all_records:
            import pandas as pd
            df = pd.DataFrame(all_records)
            df.to_sql('final_realtime_subway', con=conn, if_exists='append', index=False, method='multi')
            logging.info(f"Final: Successfully inserted {len(all_records)} records.")
        else:
            logging.error("Final: No data collected from any line using any key.")

    collect_with_key_rotation()
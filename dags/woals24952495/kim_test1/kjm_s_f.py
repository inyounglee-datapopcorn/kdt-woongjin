from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import pendulum
import requests
import logging

# [핵심 로직 1] API 수집 및 데이터 변환 (재민님의 16개 호선 황금 리스트 반영)
def fetch_and_transform_subway_data(api_key):
    # Jinja 템플릿을 통해 전달받은 api_key를 사용합니다. (UI의 Rendered 탭에서 확인 가능)
    
    # 1호선~9호선 + 경의중앙, 공항철도, 경춘, 수인분당, 신분당, 우이신설, GTX-A
    target_lines = [
        "1호선", "2호선", "3호선", "4호선", "5호선", "6호선", "7호선", "8호선", "9호선",
        "경의중앙선", "공항철도", "수인분당선", "신분당선"
    ]
      
    all_transformed_rows = []

    for line in target_lines:
        # 각 호선별로 최대 100개까지 요청 (1호선 등이 길기 때문에 넉넉하게 설정)
        url = f"http://swopenapi.seoul.go.kr/api/subway/{api_key}/json/realtimePosition/0/100/{line}"
        
        logging.info(f"{line} 데이터 수집 중...")
        try:
            response = requests.get(url)
            data = response.json()
            
            if 'realtimePositionList' in data:
                for item in data['realtimePositionList']:
                    # 프롬프트 표준 스키마에 맞춘 데이터 변환 (snake_case)
                    row = (
                        item.get('subwayId'),            # line_id
                        item.get('subwayNm'),            # line_name
                        item.get('statnId'),             # station_id
                        item.get('statnNm'),             # station_name
                        item.get('trainNo'),             # train_number
                        item.get('lastRecptnDt'),        # last_rec_date
                        item.get('recptnDt'),            # last_rec_time
                        int(item.get('updnLine', 0)),    # direction_type (0:상행, 1:하행)
                        item.get('statnTid'),            # dest_station_id
                        item.get('statnTnm'),            # dest_station_name
                        item.get('trainSttus'),          # train_status (0:진입, 1:도착, 2:출발 등)
                        item.get('directAt'),            # is_express (1:급행, 0:아님)
                        item.get('lstcarAt') == '1'      # is_last_train (Boolean 변환)
                    )
                    all_transformed_rows.append(row)
                logging.info(f"{line} 수집 완료: {len(data['realtimePositionList'])}개")
            else:
                logging.warning(f"{line}에 현재 운행 중인 열차가 없습니다.")
        except Exception as e:
            logging.error(f"{line} 수집 중 에러 발생: {e}")
            
    return all_transformed_rows

# [핵심 로직 2] DB 적재 (test_kjm_subway 테이블 사용)
def load_data_to_db(**context):
    # XCom을 통해 수집된 데이터 전달받음
    rows = context['task_instance'].xcom_pull(task_ids='fetch_and_transform')
    
    if not rows:
        logging.info("적재할 데이터가 없습니다.")
        return

    # Supabase(Postgres) 연결 정보 사용
    pg_hook = PostgresHook(postgres_conn_id='jaemin1077_supabase_conn')
    
    # 데이터베이스 표준 스키마 컬럼 정의
    target_fields = [
        'line_id', 'line_name', 'station_id', 'station_name', 'train_number',
        'last_rec_date', 'last_rec_time', 'direction_type', 'dest_station_id',
        'dest_station_name', 'train_status', 'is_express', 'is_last_train'
    ]
    
    # 9시 서버 종료 전 대량 수집을 위해 지정된 테이블에 데이터 삽입
    pg_hook.insert_rows(
        table='test_kjm_subway',
        rows=rows,
        target_fields=target_fields
    )
    logging.info(f"성공적으로 {len(rows)}개의 데이터를 'test_kjm_subway' 테이블에 적재했습니다.")

# [DAG 정의]
with DAG(
    dag_id='woals24952495_kjm_test1_monitor',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule='*/1 * * * *',          # 실시간 분석을 위해 1분 주기로 정밀 수집
    catchup=False,
    tags=['subway', 'kim_test1', 'realtime_all_lines'],
) as dag:

    # 1단계: 모든 호선 데이터 수집 및 변환
    task_fetch = PythonOperator(
        task_id='fetch_and_transform',
        python_callable=fetch_and_transform_subway_data,
        op_kwargs={'api_key': '{{ var.value.kjm_subway_api_key }}'}
    )

    # 2단계: Supabase DB에 최종 적재
    task_load = PythonOperator(
        task_id='load_to_db',
        python_callable=load_data_to_db
    )

    # 작업 순서 정의 (수집 -> 적재)
    task_fetch >> task_load
import logging
import requests
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.models import Variable

# ==========================================================
# [설정값 입력 구역]
# ==========================================================
MY_SEOUL_API_KEY = "43434e536b776f6137326e4e664152"
MY_CONN_ID = "jaemin1077_supabase_conn"
MY_TABLE_NAME = "realtime_subway_positions_woals"
SLACK_CONN_ID = "jaemin1077_slack_conn"
SLACK_CHANNEL = "#bot-playground" 
# ==========================================================

TARGET_LINES = [
    "1호선", "2호선", "3호선", "4호선", "5호선", 
    "6호선", "7호선", "8호선", "9호선",
    "경의중앙선", "공항철도", "수인분당선", "신분당선"
]

# [함수 1] 실패 시 슬랙 알림 (조건 2: 하루에 딱 한 번만 발송)
def on_failure_callback(context):
    """
    실패 시 슬랙 알림을 보냅니다. 
    에어플로우 Variable을 이용해 오늘 이미 알림을 보냈다면 다시 보내지 않습니다.
    """
    today_str = pendulum.now('Asia/Seoul').format('YYYY-MM-DD')
    last_alert_date = Variable.get("woals_last_fail_alert_date", default_var="None")
    
    # 오늘 이미 알림을 보냈다면 함수 종료
    if last_alert_date == today_str:
        logging.info("오늘 이미 실패 알림을 보냈으므로 스킵합니다.")
        return

    fail_message = f"""
    :alert: *지하철 데이터 수집 실패 알림 (오늘의 첫 실패)*
    *작업*: {context['task_instance'].task_id} 가 실패했습니다.
    *시간*: {pendulum.now('Asia/Seoul').to_datetime_string()}
    코드를 확인하거나 Supabase 연결을 점검하세요!
    (이 알림은 오늘 처음 발생한 실패에 대해서만 한 번 발송됩니다.)
    """
    
    try:
        slack_alert = SlackAPIPostOperator(
            task_id='slack_failed_alert',
            slack_conn_id=SLACK_CONN_ID,
            channel=SLACK_CHANNEL,
            text=fail_message
        )
        slack_alert.execute(context=context)
        
        # 알림 발송 후 오늘 날짜를 Variable에 저장
        Variable.set("woals_last_fail_alert_date", today_str)
        logging.info(f"실패 알림 발송 완료 및 날짜 기록: {today_str}")
    except Exception as e:
        logging.error(f"슬랙 알림 발송 중 에러 발생: {e}")

# [함수 2] 하루 한 번 체크 (오전 9시)
def check_daily_report_time(logical_date, **context):
    local_time = logical_date.in_timezone('Asia/Seoul')
    # 오전 9시 정각~5분 사이의 실행 건만 통과시킴
    return local_time.hour == 9 and local_time.minute < 5

with DAG(
    dag_id="woals24952495_subway_slack_final", # 성공한 subway_14의 슬랙 업그레이드 버전
    start_date=pendulum.today('Asia/Seoul').add(days=-1),
    schedule="*/5 * * * *", 
    catchup=False,
    default_args={
        'owner': 'woals24952495',
        'on_failure_callback': on_failure_callback, # 실패 시 즉시 알림
        'retries': 0
    },
    tags=['subway', 'slack', 'upgrade'],
) as dag:

    # 1. 테이블 생성 (subway_14에서 성공한 로직 그대로)
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

    # 2. 데이터 적재 (성공한 Pandas 로직 그대로)
    @task(task_id='collect_and_insert_subway_data')
    def collect_and_insert_subway_data():
        import pandas as pd
        hook = PostgresHook(postgres_conn_id=MY_CONN_ID)
        conn = hook.get_sqlalchemy_engine()
        all_records = []
        
        for line in TARGET_LINES:
            try:
                url = f"http://swopenapi.seoul.go.kr/api/subway/{MY_SEOUL_API_KEY}/json/realtimePosition/1/100/{line}"
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                
                if 'realtimePositionList' in data:
                    for item in data['realtimePositionList']:
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
            df.to_sql(MY_TABLE_NAME, con=conn, if_exists='append', index=False, method='multi')
            return len(all_records)
        return 0

    ingestion_task = collect_and_insert_subway_data()

    # 3. [조건 1] 하루 한 번 성과 리포트 발송 (오전 9시)
    is_daily_report_time = ShortCircuitOperator(
        task_id='is_daily_report_time',
        python_callable=check_daily_report_time
    )

    daily_success_message = SlackAPIPostOperator(
        task_id='daily_success_notification',
        slack_conn_id=SLACK_CONN_ID,
        channel=SLACK_CHANNEL,
        text="""
        :white_check_mark: *지하철 데이터 일일 적재 리포트*
        오늘도 지하철 실시간 데이터가 정상적으로 수집/적재되고 있습니다.
        """
    )

    # 흐름 설정
    create_table >> ingestion_task >> is_daily_report_time >> daily_success_message

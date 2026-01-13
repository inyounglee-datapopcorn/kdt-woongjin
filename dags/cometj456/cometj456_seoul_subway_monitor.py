# -*- coding: utf-8 -*-  # 파일 인코딩 지정(윈도우/한글 로그 안정화)
import logging  # 로그 기록용 모듈
import requests  # HTTP API 호출용 모듈
import pendulum  # 시간대(KST) 처리용 모듈
import pandas as pd  # 데이터프레임/DB 적재(to_sql)용 모듈

from airflow import DAG  # Airflow DAG 정의용
from airflow.sdk import task  # Airflow Task SDK 데코레이터
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator  # SQL 실행 오퍼레이터
from airflow.providers.postgres.hooks.postgres import PostgresHook  # Postgres 연결 훅
from airflow.providers.slack.operators.slack import SlackAPIPostOperator  # Slack 메시지 전송 오퍼레이터


# =========================
# Configuration (상수/설정)
# =========================

SEOUL_API_KEY = "45415767686a656838334a6777656b"  # 서울시 지하철 OPEN API 키(운영은 Variable/Secret 권장)

TARGET_LINES = [  # 조회할 노선 목록
    "1호선", "2호선", "3호선", "4호선", "5호선",
    "6호선", "7호선", "8호선", "9호선",
    "경의중앙선", "공항철도", "수인분당선", "신분당선",
]

SUPABASE_CONN_ID = "cometj456_supabase_conn"  # Supabase(Postgres) Airflow Connection ID
SLACK_CONN_ID = "cometj456_slack_conn"  # Slack Airflow Connection ID
SLACK_CHANNEL = "#bot-playground"  # Slack 채널
SLACK_USERNAME = "Comet_Bot"  # Slack 메시지 발신자명(표시용)

TABLE_NAME = "realtime_subway_positions_v2"  # 적재 대상 테이블명(v2로 통일)

# (주의) schedule="*/30 * * * *"는 30분마다 실행입니다.
SCHEDULE_CRON = "*/30 * * * *"  # 30분마다 실행

default_args = dict(  # DAG 기본 옵션
    owner="cometj456",  # 소유자
    email=["cometj456@gmail.com"],  # 실패 알림 이메일
    email_on_failure=True,  # 실패 시 이메일 발송 여부
    retries=1,  # 재시도 횟수
)

# =========================
# DAG 정의
# =========================
with DAG(
    dag_id="cometj456_seoul_subway_monitor",  # DAG ID
    start_date=pendulum.today("Asia/Seoul").add(days=-1),  # 시작일(어제)
    schedule=SCHEDULE_CRON,  # 스케줄(30분마다)
    catchup=False,  # 과거 스케줄 백필 비활성화
    default_args=default_args,  # 기본 인자
    tags=["subway", "project", "slack"],  # 태그
) as dag:

    # =========================
    # 1) 테이블 생성 태스크
    # =========================
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",  # 태스크 ID
        conn_id=SUPABASE_CONN_ID,  # DB 연결 ID
        sql=f"""
            -- (1) 테이블이 없으면 생성
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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
                created_at TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Seoul')
            );

            -- (2) 이미 존재하던 테이블이라면 길이 부족 이슈 방지 위해 컬럼 확장(안전하게 100으로 통일)
            ALTER TABLE {TABLE_NAME} ALTER COLUMN line_id TYPE VARCHAR(100);
            ALTER TABLE {TABLE_NAME} ALTER COLUMN line_name TYPE VARCHAR(100);
            ALTER TABLE {TABLE_NAME} ALTER COLUMN station_id TYPE VARCHAR(100);
            ALTER TABLE {TABLE_NAME} ALTER COLUMN station_name TYPE VARCHAR(100);
            ALTER TABLE {TABLE_NAME} ALTER COLUMN train_number TYPE VARCHAR(100);
            ALTER TABLE {TABLE_NAME} ALTER COLUMN last_rec_date TYPE VARCHAR(100);
            ALTER TABLE {TABLE_NAME} ALTER COLUMN dest_station_id TYPE VARCHAR(100);
            ALTER TABLE {TABLE_NAME} ALTER COLUMN dest_station_name TYPE VARCHAR(100);
        """,  # 실행할 SQL
    )

    # =========================
    # 2) 데이터 수집 및 적재 태스크
    # =========================
    @task(task_id="collect_and_insert_subway_data_v2")  # 태스크 데코레이터
    def collect_and_insert_subway_data_v2():
        hook = PostgresHook(postgres_conn_id=SUPABASE_CONN_ID)  # Postgres 훅 생성
        engine = hook.get_sqlalchemy_engine()  # SQLAlchemy 엔진 획득

        all_records = []  # 수집 레코드 누적 리스트

        for line in TARGET_LINES:  # 노선별로 API 호출 반복
            try:
                url = f"http://swopenapi.seoul.go.kr/api/subway/{SEOUL_API_KEY}/json/realtimePosition/1/100/{line}"  # API URL 구성
                response = requests.get(url, timeout=10)  # API 호출(타임아웃 10초)
                response.raise_for_status()  # HTTP 에러 발생 시 예외
                data = response.json()  # JSON 파싱

                if "realtimePositionList" in data:  # 데이터 키가 있으면
                    items = data["realtimePositionList"]  # 실제 리스트 추출
                    logging.info(f"{line}: Found {len(items)} trains")  # 노선별 개수 로그

                    for item in items:  # 각 열차 레코드 처리
                        recptn_dt = item.get("recptnDt")  # 수신 시각(문자열) 가져오기

                        record = {  # DB 스키마에 맞게 매핑
                            "line_id": item.get("subwayId"),  # 노선 ID
                            "line_name": item.get("subwayNm"),  # 노선명
                            "station_id": item.get("statnId"),  # 현재역 ID
                            "station_name": item.get("statnNm"),  # 현재역명
                            "train_number": item.get("trainNo"),  # 열차 번호
                            "last_rec_date": item.get("lastRecptnDt"),  # 마지막 수신일자(문자)
                            "last_rec_time": pendulum.parse(recptn_dt, tz="Asia/Seoul") if recptn_dt else None,  # TIMESTAMPTZ
                            "direction_type": int(item.get("updnLine")) if str(item.get("updnLine", "")).isdigit() else None,  # 상/하행 코드
                            "dest_station_id": item.get("statnTid"),  # 종착역 ID
                            "dest_station_name": item.get("statnTnm"),  # 종착역명
                            "train_status": int(item.get("trainSttus")) if str(item.get("trainSttus", "")).isdigit() else None,  # 열차 상태 코드
                            "is_express": int(item.get("directAt")) if str(item.get("directAt", "")).isdigit() else 0,  # 급행 여부(0/1)
                            "is_last_train": item.get("lstcarAt") == "1",  # 막차 여부(문자 "1"이면 True)
                        }
                        all_records.append(record)  # 누적 리스트에 추가
                else:
                    logging.info(f"{line}: No data found")  # 데이터가 없으면 로그만 남김

            except Exception as e:
                logging.error(f"Error fetching data for {line}: {e}")  # 노선별 에러 로그
                continue  # 다음 노선으로 진행

        inserted = 0  # 실제 적재 건수 초기화

        if all_records:  # 수집된 데이터가 있으면
            logging.info(f"Inserting total {len(all_records)} records into {TABLE_NAME}...")  # 적재 전 로그

            df = pd.DataFrame(all_records)  # 데이터프레임 생성

            # (선택) 결측치/타입 안전성 보강: 문자열 컬럼은 문자열로 캐스팅(길이 제한 이슈 감소)
            for col in ["line_id", "line_name", "station_id", "station_name", "train_number", "last_rec_date", "dest_station_id", "dest_station_name"]:
                df[col] = df[col].astype("string")  # 문자열로 변환

            df.to_sql(  # pandas -> DB 적재
                TABLE_NAME,  # 테이블명
                con=engine,  # 엔진
                if_exists="append",  # append 모드
                index=False,  # 인덱스 컬럼 저장 안 함
                method="multi",  # multi insert로 성능 개선
            )

            inserted = len(df)  # 적재한 레코드 수 저장
            logging.info("Insert completed.")  # 완료 로그
        else:
            logging.info("No records to insert.")  # 적재할 데이터 없음 로그

        # Slack용 실행 시각(KST) 문자열 생성
        run_time_kst = pendulum.now("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")  # KST 포맷 문자열

        # Slack 템플릿에서 pendulum 매크로 없이 XCom만 쓰도록 dict 반환
        return {"inserted": inserted, "run_time_kst": run_time_kst}  # XCom으로 반환

    ingestion_task = collect_and_insert_subway_data_v2()  # 태스크 인스턴스 생성

    # =========================
    # 3) 슬랙 알림 태스크
    # =========================
    send_slack_notification_cometj456 = SlackAPIPostOperator(
        task_id="send_slack_notification_cometj456",  # 태스크 ID
        slack_conn_id=SLACK_CONN_ID,  # Slack 연결 ID
        channel=SLACK_CHANNEL,  # 채널
        username=SLACK_USERNAME,  # 발신자 표시명
        text=(  # Slack 메시지 본문(Jinja 템플릿 포함 가능)
            f":pet: *지하철 데이터 적재 완료*\n"  # 제목
            f"- 대상 테이블: `{TABLE_NAME}`\n"  # 테이블명
            "- 적재된 레코드 수: {{ task_instance.xcom_pull(task_ids='collect_and_insert_subway_data_v2')['inserted'] }}개\n"  # XCom inserted
            "- 실행 시각(KST): {{ task_instance.xcom_pull(task_ids='collect_and_insert_subway_data_v2')['run_time_kst'] }}"  # XCom run_time_kst
        ),
    )

    # =========================
    # 태스크 의존성 설정
    # =========================
    create_table >> ingestion_task >> send_slack_notification_cometj456  # 생성 -> 적재 -> 슬랙 순서

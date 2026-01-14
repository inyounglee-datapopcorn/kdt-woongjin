import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# script 파일 경로 추가
sys.path.append(os.path.dirname(__file__))

# Import run_my_crawler
from tlswlgo3_infinite_youtube_script import run_my_crawler

def collect_youtube_data_task(**context):
    import os
    from airflow.sdk import Variable
    
    # 1. 환경변수 우선 확인 (Airflow 설정 방식)
    api_key = os.environ.get('AIRFLOW_VAR_TLSWLGO3_YOUTUBE_APIKEY')
    
    if not api_key:
        try:
            # 2. SDK를 통한 변수 가져오기 (소문자)
            api_key = Variable.get("tlswlgo3_youtube_apikey")
        except Exception:
            try:
                # 3. SDK를 통한 변수 가져오기 (대문자 관례)
                api_key = Variable.get("TLSWLGO3_YOUTUBE_APIKEY")
            except Exception as e:
                # 디버깅을 위해 현재 환경의 Airflow 관련 변수 목록 출력
                airflow_vars = [k for k in os.environ if k.startswith('AIRFLOW_VAR')]
                print(f"에러: 'tlswlgo3_youtube_apikey'를 찾을 수 없습니다.")
                print(f"현재 인식 가능한 환경변수 목록: {airflow_vars}")
                raise e
                
    return run_my_crawler(api_key=api_key)

def load_to_supabase(**context):
    # 이전 태스크(collect_youtube_data)에서 반환한 결과를 XCom으로 가져옴
    results = context['ti'].xcom_pull(task_ids='collect_youtube_data')
    
    if not results:
        print("적재할 데이터가 없습니다.")
        return

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    # 사용자 요청에 따른 conn_id 사용
    pg_hook = PostgresHook(postgres_conn_id='tlswlgo3_supabase_conn')
    
    # 테이블이 없으면 생성
    create_table_query = """
    CREATE TABLE IF NOT EXISTS infinite_challenge_youtube_videos (
        video_id TEXT PRIMARY KEY,
        channel_id TEXT,
        title TEXT,
        description TEXT,
        thumbnail_url TEXT,
        view_count BIGINT,
        like_count BIGINT,
        comment_count BIGINT,
        published_at TIMESTAMP,
        collected_at TIMESTAMP
    );
    """
    pg_hook.run(create_table_query)
    
    # 데이터 삽입을 위한 튜플 리스트 작성
    rows = [
        (
            r['video_id'], 
            r['channel_id'], 
            r['title'], 
            r['description'], 
            r['thumbnail_url'], 
            int(r['view_count']), 
            int(r['like_count']), 
            int(r['comment_count']), 
            r['published_at'], 
            r['collected_at']
        ) 
        for r in results
    ]
    
    # 데이터 적재
    pg_hook.insert_rows(
        table='infinite_challenge_youtube_videos',
        rows=rows,
        target_fields=[
            'video_id', 'channel_id', 'title', 'description', 'thumbnail_url', 
            'view_count', 'like_count', 'comment_count', 'published_at', 'collected_at'
        ],
        replace=True,  # video_id가 같은 데이터가 있으면 업데이트 (UPSERT 효과)
        replace_index=['video_id']
    )
    print(f"{len(rows)}개의 데이터를 Supabase(Postgres)에 적재(Upsert) 완료했습니다.")

default_args = {
    'owner': 'tlswlgo3',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='tlswlgo3_infinite_challenge_youtube_dag',
    default_args=default_args,
    description='유튜브 데이터 1회성 수집 및 Supabase 적재',
    schedule='@once', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    collect_task = PythonOperator(
        task_id='collect_youtube_data',
        python_callable=collect_youtube_data_task,
    )

    load_to_supabase_task = PythonOperator(
        task_id='load_to_supabase',
        python_callable=load_to_supabase,
    )

    # 태스크 순서 설정
    collect_task >> load_to_supabase_task

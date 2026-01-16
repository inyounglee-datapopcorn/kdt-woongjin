from airflow import DAG  # Airflow의 핵심인 DAG(작업 흐름)를 만드는 도구
from airflow.providers.standard.operators.python import PythonOperator  # 파이썬 코드를 실행하는 담당자
from datetime import datetime, timedelta
import sys
import os

# [A] 환경 설정: 모듈을 불러오기 위해 현재 폴더의 위치를 파이썬에게 알려줍니다.
dag_path = os.path.dirname(os.path.abspath(__file__))
if dag_path not in sys.path:
    sys.path.append(dag_path)

from kate29397_naver_utils import NaverBlogCrawler

# [B] 실제 실행될 파이썬 함수 정의
def naver_blog_crawling_task(keywords, ds, conn_id='supabase_conn', **kwargs):
    """네이버 블로그 데이터를 수집하고 결과를 DB에 저장하는 과정"""
    # ds: Airflow에서 제공하는 실행 날짜 (YYYY-MM-DD 형식의 문자열)
    print(f"[*] 작업을 시작합니다: 날짜={ds}, 키워드 목록={keywords}, Connection ID={conn_id}")
    
    try:
        # 도구 준비 (Airflow Connection ID 전달)
        crawler = NaverBlogCrawler(conn_id=conn_id)
        
        # 1. 특정 실행 날짜(ds)에 대한 데이터를 수집하고 DB에 저장합니다.
        # run_aggregation에서 start_date와 end_date를 ds로 설정하여 해당 일자만 수집
        results = crawler.run_aggregation(keywords, start_date=ds, end_date=ds)
        
        if results:
            return f"성공: {ds} 일자 총 {len(results)}건의 데이터 처리 완료!"
        else:
            return f"알림: {ds} 일자에 수집된 데이터가 없습니다."
            
    except Exception as e:
        print(f"[오류] 작업 도중 문제가 생겼습니다: {e}")
        raise e  # 에러가 발생했음을 Airflow에게 알려 작업 실패로 표시하게 합니다.

# [C] 기본 설정: DAG의 기본 속성을 정의합니다.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# [D] DAG 정의: 작업의 이름, 실행 시간(스케줄) 등을 설정합니다.
with DAG(
    'kate29397_naver_blog_crawler_dag',  # Airflow 화면에 나타날 DAG의 고유 이름
    default_args=default_args,
    description='네이버 블로그 트렌드 데이터를 수집하는 자동화 흐름',
    schedule='0 16 * * *',  # 매일 16시마다 자동으로 실행
    catchup=False,
    tags=['네이버', '블로그', '크롤링'],
) as dag:

    # 1번 작업: 네이버 블로그 데이터 수집 (PythonOperator 사용)
    crawl_task = PythonOperator(
        task_id='run_naver_blog_crawling',
        python_callable=naver_blog_crawling_task,
        op_kwargs={
            'ds': '{{ macros.ds_add(ds, -1) }}', # Airflow 실행 일자 기준 전날 (YYYY-MM-DD)
            'keywords': ['두바이쫀득쿠키','두쫀쿠'],  # 수집할 키워드 목록
            'conn_id': 'dubaicookie_supabase_conn', # Airflow에 등록된 커넥션 ID
        },
    )

    # 작업 순서 (단일 작업이므로 그대로 나열)
    crawl_task

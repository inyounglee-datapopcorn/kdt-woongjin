from airflow import DAG  # Airflow의 핵심인 DAG(작업 흐름)를 만드는 도구
from airflow.providers.standard.operators.python import PythonOperator  # 파이썬 코드를 실행하는 담당자
from airflow.providers.smtp.operators.smtp import EmailOperator  # 이메일을 보내는 담당자
from airflow.models import Variable  # Airflow 변수를 관리하는 도구
from datetime import datetime, timedelta
import sys
import os

# 모듈(crawler, database)을 불러오기 위해 현재 폴더의 위치를 파이썬에게 알려줍니다.
dag_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dag_path)

from qoxjf135_crawler import YouTubeTrendCrawler
from qoxjf135_database import SupabaseManager

# [A] 기본 설정: DAG의 기본 속성을 정의합니다 (누가 만들었는지, 실패 시 몇 번 재시도할지 등).
default_args = {
    'owner': 'baeseungjae',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False, # 실패 시 자동 메일 발송 기능 (비활성화)
    'email_on_retry': False,
    'retries': 1,  # 실패하면 1번 더 시도합니다.
    'retry_delay': timedelta(minutes=5),  # 재시도 전 5분 동안 기다립니다.
}

# [B] 실제 실행될 파이썬 함수 정의
def youtube_crawling_task(keyword, total_days, **kwargs):
    """유튜브 크롤링을 하고 결과를 DB에 넣는 일련의 과정"""
    print(f"[*] 작업을 시작합니다: 키워드='{keyword}', 기간={total_days}일")
    
    try:
        # 도구들을 준비합니다.
        crawler = YouTubeTrendCrawler()
        db = SupabaseManager(conn_id='qoxjf135_supabase_conn')
        
        # 1. 유튜브에서 데이터를 가져옵니다.
        summary_df = crawler.get_historical_data(keyword, total_days=total_days)
        
        # 2. 가져온 데이터가 있으면 DB에 하나씩 저장합니다.
        if summary_df is not None:
            print(f"[*] {len(summary_df)}일치의 데이터를 발견하여 저장 중...")
            for _, row in summary_df.iterrows():
                db_data = {
                    "date": row["date"],
                    "keyword": row["keyword"],
                    "video_count": int(row["video_count"]),
                    "total_views": int(row["total_views"]),
                    "total_likes": int(row["total_likes"]),
                    "total_comments": int(row["total_comments"])
                }
                db.insert_daily_trend(db_data)
            return f"성공: {len(summary_df)}일치 데이터 저장 완료!"
        else:
            return "알림: 수집된 데이터가 없습니다."
            
    except Exception as e:
        print(f"[오류] 작업 도중 문제가 생겼습니다: {e}")
        raise e  # 에러가 발생했음을 Airflow에게 알려 작업 실패로 표시하게 합니다.

# [C] DAG 정의: 작업의 이름, 실행 시간(스케줄) 등을 설정합니다.
with DAG(
    'qoxjf135_youtube_crawling_dag',  # Airflow 화면에 나타날 DAG의 고유 이름
    default_args=default_args,
    description='유튜브 데이터를 수집하고 성공 시 이메일을 보내는 자동화 흐름',
    schedule='0 16 * * *',  # 매일 16시마다 자동으로 실행합니다.
    catchup=False,  # 과거의 밀린 작업들은 무시하고 현재부터 실행합니다.
    tags=['유튜브', '크롤링', '알림'],
) as dag:

    # 1번 작업: 유튜브 데이터 수집 (PythonOperator 사용)
    crawl_task = PythonOperator(
        task_id='run_youtube_crawling',  # 태스크의 이름
        python_callable=youtube_crawling_task,  # 실행할 함수 이름
        op_kwargs={
            'keyword': '두바이 쫀득 쿠키',  # 함수에 전달할 검색어
            'total_days': 1   # 테스트를 위해 1일치만 수집합니다.
        },
    )

    # 2번 작업: 성공 시 이메일 발송 (EmailOperator 사용)
    send_email = EmailOperator(
        task_id='send_success_email',
        # 수신자 이메일을 직접 입력(하드코딩)합니다.
        to=['qoxjf135@gmail.com', 'kate29397@gmail.com'], 
        subject='[알림] 유튜브 데이터 수집 완료 (오후 16시 정기 실행)',
        # 발송용 SMTP 서버는 'qoxjf135_gmail_connection' 설정을 사용합니다.
        smtp_conn_id='qoxjf135_gmail_conn',
        html_content="""
        <h3>유튜브 트렌드 수집이 완료되었습니다.</h3>
        <p>데이터가 DB에 안전하게 저장되었습니다.</p>
        <p><b>실행 시간(TS):</b> {{ ts }}</p>
        """,
    )

    # [D] 작업 순서 정하기: 수집(crawl_task)이 끝나면 메일(send_email)을 보냅니다.
    crawl_task >> send_email

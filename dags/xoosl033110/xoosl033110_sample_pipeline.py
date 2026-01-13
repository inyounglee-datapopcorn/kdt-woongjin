    """
    샘플 DAG 파일 - xoosl033110

    이 파일은 다음 규칙을 따릅니다:
    1. dag_id는 반드시 폴더명 'xoosl033110'를 포함해야 합니다
    2. 예시: dag_id="xoosl033110_sample", dag_id="xoosl033110_data_pipeline" 등

    GitHub Actions의 deploy.yml에서 이 규칙을 검증합니다.
    """

    from datetime import datetime, timedelta
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator


    def hello_world():
        """간단한 Python 함수 예제"""
        print("Hello from xoosl033110!")
        print(f"Current time: {datetime.now()}")
        return "Success"


    # DAG의 기본 설정
    default_args = {
        'owner': 'xoosl033110',
        'depends_on_past': False,
        'start_date': datetime(2026, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    # DAG 정의 - ⚠️ dag_id에 반드시 'xoosl033110'가 포함되어야 합니다!
    with DAG(
        dag_id='xoosl033110_sample_pipeline',  # ✅ 폴더명이 포함됨
        default_args=default_args,
        description='Sample DAG for xoosl033110',
        schedule=None,  # 수동 실행만 허용
        catchup=False,
        tags=['xoosl033110', 'sample', 'tutorial'],
    ) as dag:

        # Task 1: Bash 명령어 실행
        start_task = BashOperator(
            task_id='start',
            bash_command='echo "Starting xoosl033110 DAG pipeline..."',
        )

        # Task 2: Python 함수 실행
        hello_task = PythonOperator(
            task_id='say_hello',
            python_callable=hello_world,
        )

        # Task 3: 종료 작업
        end_task = BashOperator(
            task_id='end',
            bash_command='echo "Pipeline completed successfully!"',
        )

        # Task 의존성 정의
        start_task >> hello_task >> end_task

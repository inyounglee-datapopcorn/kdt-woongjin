import pendulum, random
from airflow import DAG
from airflow.decorators import task
"""
🔸 TaskFlow API
    - 파이썬 함수를 데코레이터로 감싸서 태스크로 변환하는 방식
    - 함수의 반환값이 다음 태스크 함수의 인자로 자동 전달되어 데이터 흐름을 간단히 표현 가능
    - AIRFLOW의 의존성 설정(task1 >> task2) 방식 대신, 함수 호출만으로 태스크 간 종속성 정의 가능
    - Pythonic한 방식으로 코드 작성이 가능하기 때문에 PythonOperator 대신 사용하는 것을 권장!
"""

default_args = dict(
    owner = 'popcorn',
    email = ['datapopcorn@gmail.com'],
    email_on_failure = False,
    retries = 3
    )

with DAG(
    dag_id="03_python_taskflow_dag",
    start_date=pendulum.datetime(2025, 8, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *",
    tags = ['20250824','BASIC'],
    default_args = default_args,
    catchup=False
):
    @task(task_id='select_lang')
    def random_language():
        lang_list = ["PYTHON", 'JAVA', 'RUST']
        lang = random.sample(lang_list, 1)[0]
        print("SELECTED LANGUAGE : ", lang)
        return lang
    
    # @task(task_id='one')
    # def list_one():
    #     return [1,2,3]

    # @task(task_id='two')
    # def list_two(lst):
    #     return lst + [4,5,6]

    # @task(task_id='three')
    # def list_three(lst):
    #     return lst + [7,8,9]
    
    select_lang = random_language()
    # one = list_one()
    # two = list_two(one)
    # three = list_three(two)
    
    select_lang
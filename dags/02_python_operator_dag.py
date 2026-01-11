import pendulum, random
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# 프로그래밍 언어 3개 중 랜덤하게 출력해주는 함수!
def random_language():
    lang_list = ["PYTHON", 'JAVA', 'RUST']
    lang = random.sample(lang_list, 1)[0]
    print("SELECTED LANGUAGE : ", lang)
    return lang

# def random_language_args(*args):
#     lang = random.sample(args, 1)[0]
#     print("SELECTED LANGUAGE : ", lang)
#     return lang

# def random_language_kwargs(**kwargs):
#     lang = random.sample(kwargs['lang_list'], 1)[0]
#     print("SELECTED LANGUAGE : ", lang)
#     return lang

default_args = dict(
    owner = 'popcorn',
    email = ['datapopcorn@gmail.com'],
    email_on_failure = False,
    retries = 3
    )

with DAG(
    dag_id="02_python_operator_dag",
    start_date=pendulum.datetime(2025, 8, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *",
    tags = ['20250824', 'BASIC'],
    default_args = default_args,
    catchup=False
):
    select_lang = PythonOperator(
        task_id = 'select_lang',
        python_callable=random_language
    )
    
    # select_lang_args = PythonOperator(
    #     task_id = 'select_lang_args',
    #     python_callable=random_language_args,
    #     op_args=['PYTHON', 'JAVA', 'RUST']
    # )
    
    # select_lang_kwargs = PythonOperator(
    #     task_id = 'select_lang_kwargs',
    #     python_callable=random_language_kwargs,
    #     op_kwargs=dict(
    #         lang_list=['PYTHON', 'JAVA', 'RUST']
    #         )
    # )
    
select_lang
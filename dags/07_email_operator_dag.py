import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.providers.smtp.operators.smtp import EmailOperator
"""
🔸 EmailOperator
    - 지정한 수신자에게 제목과 본문, 첨부파일 등을 담은 이메일 발송 작업을 수행하는 오퍼레이터
    - 'Google 계정 관리 > 앱 비밀번호' 에서 비밀번호 생성 필요!
    - Airflow UI에서 'SMTP' connection 설정 필요!

🔸 Connection 설정 정보
    - Conn Id : gmail_connection
    - Host : smtp.gmail.com
    - Login : 개인 Gmail 주소
    - Port : 465
    - Password : 앱 비밀번호(16자리)
    - Extra Fields :
        - From email : 이메일 발송자 주소
        - (Port 587 사용 시 'Disable SSL' 체크 필수)

https://airflow.apache.org/docs/apache-airflow/2.2.0/_api/airflow/operators/email/index.html
"""

default_args = dict(
    owner = 'datapopcorn',
    email = ['datapopcorn@gmail.com'],
    email_on_failure = True,
    retries = 3,
    retry_delay = timedelta(minutes=1)
    )

with DAG(
    dag_id="07_email_operator_dag",
    start_date=pendulum.datetime(2025, 8, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *",
    default_args = default_args,
    catchup=False
):
    
    today = pendulum.now('Asia/Seoul').format('YYYYMMDD')
    
    send_email = EmailOperator(
        task_id='send_email',
        conn_id='gmail_connection',

        to='datapopcorn@gmail.com',
        subject=f'DAILY AIRFLOW - {today}',
        html_content="""
        <html>
        <body>
            <h2>안녕하세요!</h2>
            <p>이것은 Airflow EmailOperator를 이용한 샘플 이메일입니다.</p>
            <ul>
                <li>오늘 날짜: {{ ds }}</li>
                <li>이메일 발송 테스트</li>
            </ul>
            <p style="color:blue;">감사합니다.<br>Airflow 드림</p>
        </body>
        </html>
        """,
    )
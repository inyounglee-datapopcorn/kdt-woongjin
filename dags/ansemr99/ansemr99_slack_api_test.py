from airflow import DAG
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
import pendulum

with DAG(
    dag_id='ansemr99_slack_api_test_dag',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule='0 0 * * *',
    catchup=False,
    tags=['ansemr99', 'slack', 'api'],
) as dag:

    # 주의: 슬랙 앱(Bot)을 해당 채널에 먼저 초대해야 메시지 전송이 가능합니다.
    # 예: 채널에서 '/invite @App_Name' 입력
    send_slack = SlackAPIPostOperator(
        task_id='send_slack_message_api',
        slack_conn_id='ansemr99_slack_conn',
        channel='#bot-playground',  # 보낼 채널명을 입력하세요 (예: #general)
        text=':wave: 안녕하세요!',
        username='승우봇',
    )

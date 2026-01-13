from airflow import DAG
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
import pendulum

with DAG(
    dag_id='xoosl033110_slack_api_test_dag',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule='0 0 * * *',
    catchup=False,
    tags=['xoosl033110', 'slack', 'api'],
) as dag:

    send_slack = SlackAPIPostOperator(
        task_id='send_slack_message_api',
        slack_conn_id='xoosl033110_slack_conn',
        channel='#bot-playground',  # 보낼 채널명을 입력하세요 (예: #general)
        text=':rocket: Airflow -> 안녕하세요?.'
    )
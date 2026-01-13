from airflow import DAG
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
import pendulum

with DAG(
    dag_id='purple45663_12_2_slack_api_test_dag',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule='0 0 * * *',
    catchup=False,
    tags=['purple45663', 'slack', 'api'],
) as dag:

    send_slack = SlackAPIPostOperator(
        task_id='send_slack_message_api',
        slack_conn_id='purple45663_slack_conn',
        channel='#bot-playground',  # 보낼 채널명을 입력하세요 (예: #general)
        text=':rocket: Airflow -> Slack API (Token) 연결 성공! purple45663 테스트 DAG입니다.',
    )

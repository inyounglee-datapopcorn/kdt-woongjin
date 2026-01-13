from airflow import DAG
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
import pendulum

with DAG(
    dag_id='kate29397_slack_api_test_dag',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule='0 0 * * *',
    catchup=False,
    tags=['popcorn', 'slack', 'api'],
) as dag:

    send_slack = SlackAPIPostOperator(
        task_id='send_slack_message_api',
        slack_conn_id='kate29397_slack_conn',
        channel='#bot-playground', 
        text=':rocket: Airflow -> Slack API (Token) 연결 성공! 12_2 테스트 DAG입니다.'
    )

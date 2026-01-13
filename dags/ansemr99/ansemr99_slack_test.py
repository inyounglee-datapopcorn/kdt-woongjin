from airflow import DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import pendulum

with DAG(
    dag_id='ansemr99_slack_test_dag',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule='0 0 * * *',
    catchup=False,
) as dag:

    send_slack = SlackWebhookOperator(
        task_id='send_slack_message',
        slack_webhook_conn_id='ansemr99_slack_conn',

        message=':tada: Airflow -> Slack 연결 성공! 깃허브 액션을 통해 배포된 DAG에서 보낸 메시지입니다.',
        username='Airflow Bot',
    )

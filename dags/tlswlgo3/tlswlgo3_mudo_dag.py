from datetime import datetime, timedelta
import requests
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'tlswlgo3',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Functions
def create_schema_and_table(**kwargs):
    """
    Creates the schema and table in Supabase (Postgres) if they don't exist.
    """
    try:
        hook = PostgresHook(postgres_conn_id='tlswlgo3_supabase_conn')
        
        # Schema creation
        create_schema_query = "CREATE SCHEMA IF NOT EXISTS tlswlgo3;"
        hook.run(create_schema_query, autocommit=True)
        logging.info("Schema 'tlswlgo3' checked/created.")

        # Table creation
        create_table_query = """
        CREATE TABLE IF NOT EXISTS tlswlgo3.youtube_videos (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT,
            channel_title TEXT,
            description TEXT,
            thumbnail_url TEXT,
            view_count BIGINT,
            like_count BIGINT,
            comment_count BIGINT,
            published_at TIMESTAMP,
            collected_at TIMESTAMP
        ); 
        """
        hook.run(create_table_query, autocommit=True)
        logging.info("Table 'tlswlgo3.youtube_videos' checked/created.")
        
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        raise

def collect_and_save_data(**kwargs):
    """
    Collects data from YouTube API and saves strictly defined fields to Supabase.
    searches for '무한도전', combines results by viewCount and date to cover popular and recent.
    """
    api_key = Variable.get("tlswlgo3_youtube_apikey", default_var=None)
    if not api_key:
        # Fallback to hardcoded key if Variable is missing, though previous turn used hardcoded
        api_key
    logging.info("Starting data collection...")

    # We will fetch a mix of popular and recent videos to build the archive
    # 1. By View Count (Legends)
    # 2. By Date (Recent/New uploads)
    
    video_ids = set()
    base_url = "https://www.googleapis.com/youtube/v3/search"
    common_params = {
        'part': 'id',
        'q': '무한도전',
        'type': 'video',
        'maxResults': 25, # 25 + 25 = 50 items per run roughly
        'key': api_key
    }

    # Fetch 1: Popular
    try:
        logging.info("Fetching popular videos...")
        params_pop = common_params.copy()
        params_pop['order'] = 'viewCount'
        resp_pop = requests.get(base_url, params=params_pop, timeout=10).json()
        for item in resp_pop.get('items', []):
            video_ids.add(item['id']['videoId'])
    except Exception as e:
        logging.error(f"Error fetching popular videos: {e}")

    # Fetch 2: Recent
    try:
        logging.info("Fetching recent videos...")
        params_date = common_params.copy()
        params_date['order'] = 'date'
        resp_date = requests.get(base_url, params=params_date, timeout=10).json()
        for item in resp_date.get('items', []):
            video_ids.add(item['id']['videoId'])
    except Exception as e:
        logging.error(f"Error fetching recent videos: {e}")

    if not video_ids:
        logging.info("No video IDs found.")
        return

    # Fetch Details (Statistics)
    ids_list = list(video_ids)
    logging.info(f"Fetching details for {len(ids_list)} videos...")
    
    # The API allows max 50 ids per call
    stats_url = "https://www.googleapis.com/youtube/v3/videos"
    stats_params = {
        'part': 'snippet,statistics',
        'id': ','.join(ids_list),
        'key': api_key
    }
    
    try:
        stats_resp = requests.get(stats_url, params=stats_params, timeout=10).json()
    except Exception as e:
        logging.error(f"Error fetching video details: {e}")
        raise

    processed_rows = []
    
    for item in stats_resp.get('items', []):
        try:
            row = (
                item['id'],                                         # video_id
                item['snippet']['channelId'],                       # channel_id
                item['snippet'].get('channelTitle', ''),            # channel_title
                item['snippet']['description'],                     # description
                item['snippet']['thumbnails']['high']['url'],       # thumbnail_url
                int(item['statistics'].get('viewCount', 0)),        # view_count
                int(item['statistics'].get('likeCount', 0)),        # like_count
                int(item['statistics'].get('commentCount', 0)),     # comment_count
                item['snippet']['publishedAt'],                     # published_at
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')        # collected_at
            )
            processed_rows.append(row)
        except KeyError as e:
            logging.warning(f"Skipping item {item.get('id')} due to missing key: {e}")

    if not processed_rows:
        logging.info("No rows to insert.")
        return

    # Upsert to Supabase
    try:
        logging.info(f"Upserting {len(processed_rows)} rows into Supabase...")
        hook = PostgresHook(postgres_conn_id='tlswlgo3_supabase_conn')
        hook.insert_rows(
            table='tlswlgo3.youtube_videos',
            rows=processed_rows,
            target_fields=[
                'video_id', 'channel_id', 'channel_title', 'description', 
                'thumbnail_url', 'view_count', 'like_count', 'comment_count', 
                'published_at', 'collected_at'
            ],
            replace=True, # Handles de-duplication (Upsert)
            replace_index=['video_id']
        )
        logging.info(f"Upsert successful.")
    except Exception as e:
        logging.error(f"Error inserting rows: {e}")
        raise

with DAG(
    dag_id='tlswlgo3_mudo_archive_dag',
    default_args=default_args,
    description='Collect Infinite Challenge videos every 30 mins',
    schedule='*/30 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['infinite_challenge', 'woongjin'],
) as dag:

    # Task 1: Create Table
    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_schema_and_table
    )

    # Task 2: Collect Data
    collect_data = PythonOperator(
        task_id='collect_and_save_data',
        python_callable=collect_and_save_data
    )

    create_table >> collect_data

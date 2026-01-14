from airflow.providers.postgres.hooks.postgres import PostgresHook  # Airflow에서 DB 연결을 도와주는 도구
##
class SupabaseManager:
    """Supabase(데이터베이스)에 직접 명령(SQL)을 내려서 데이터를 저장하는 담당자"""
    
    def __init__(self, conn_id='qoxjf135_supabase_conn'):
        # Airflow 관리자 화면에서 만든 'supabase_conn' 연결 정보를 사용하여 DB와 통신할 준비를 합니다.
        # Supabase는 PostgreSQL이라는 종류의 DB를 사용하므로 PostgresHook을 사용합니다.
        self.hook = PostgresHook(postgres_conn_id=conn_id)

    def insert_daily_trend(self, summary_data):
        """
        수집된 일별 데이터를 'daily_trends' 테이블에 저장하거나 업데이트하는 함수
        """
        # SQL 문법 설명:
        # INSERT INTO ... : 데이터를 삽입합니다.
        # ON CONFLICT (date, keyword) : 만약 날짜와 키워드가 같은 데이터가 이미 있다면,
        # DO UPDATE SET ... : 새로운 정보로 덮어씌웁니다 (업데이트).
        sql = """
        INSERT INTO daily_trends (date, keyword, video_count, total_views, total_likes, total_comments)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, keyword)
        DO UPDATE SET
            video_count = EXCLUDED.video_count,
            total_views = EXCLUDED.total_views,
            total_likes = EXCLUDED.total_likes,
            total_comments = EXCLUDED.total_comments;
        """
        
        # 실제 데이터 값을 SQL의 %s 자리에 순서대로 넣습니다.
        params = (
            summary_data['date'],
            summary_data['keyword'],
            summary_data['video_count'],
            summary_data['total_views'],
            summary_data['total_likes'],
            summary_data['total_comments']
        )
        
        try:
            # 준비한 SQL 문을 실행합니다.
            self.hook.run(sql, parameters=params)
            print(f"[+] DB 저장 완료 (SQL 직접 실행): {summary_data['date']} ({summary_data['keyword']})")
        except Exception as e:
            print(f"[!] DB 저장 중 문제가 생겼습니다: {e}")
            raise e

from airflow.models import Variable  # Airflow에 저장된 변수를 가져오는 도구
from googleapiclient.discovery import build  # 구글 서비스(유튜브)를 사용하기 위한 도구
import pandas as pd  # 데이터를 표 형태로 처리하는 도구
from datetime import datetime, timedelta, timezone  # 날짜와 시간을 계산하는 도구
##
class YouTubeTrendCrawler:
    """유튜브에서 키워드별 트렌드 데이터를 수집하는 로봇 클래스"""
    
    def __init__(self):
        # 1. Airflow 관리자 화면에서 설정한 'YOUTUBE_API_KEY' 변수를 가져옵니다.
        #    이 키는 유튜브 데이터를 빌려 쓰기 위한 '출입증' 같은 것입니다.
        self.api_key = Variable.get("QOXJF135_YOUTUBE_API_KEY")
        if not self.api_key:
            raise ValueError("[!] Airflow Variable에 'QOXJF135_YOUTUBE_API_KEY'가 없습니다. 설정을 확인해 주세요.")
        
        # 2. 유튜브 API 서비스와 연결합니다. (버전 3 사용)
        self.youtube = build("youtube", "v3", developerKey=self.api_key)

    def get_metrics_for_period(self, keyword, start_date, end_date):
        """특정 날짜 범위(시작일~종료일) 동안의 영상 정보(조회수 등)를 가져오는 함수"""
        
        # 유튜브 API가 알아듣는 시간 형식(예: 2024-01-01T00:00:00Z)으로 날짜를 바꿉니다.
        start_time = start_date.strftime('%Y-%m-%dT00:00:00Z')
        end_time = end_date.strftime('%Y-%m-%dT23:59:59Z')
        
        video_list = []  # 수집한 데이터를 담을 바구니
        next_page_token = None  # 검색 결과가 많을 때 다음 페이지를 가리키는 포인터

        while True:
            try:
                # [A] 검색 API 호출: 키워드에 맞는 영상 목록을 찾습니다.
                search_response = self.youtube.search().list(
                    q=keyword,
                    part="id,snippet",
                    publishedAfter=start_time,
                    publishedBefore=end_time,
                    maxResults=50,  # 테스트를 위해 조회 개수를 1개로 제한합니다 (기존 50)
                    type="video",
                    order="date",  # 최신순
                    regionCode="KR",  # 한국 지역
                    relevanceLanguage="ko",  # 한국어 우선
                    pageToken=next_page_token
                ).execute()

                items = search_response.get("items", [])
                if not items:
                    break

                # 검색된 영상들의 고유 ID만 뽑아냅니다.
                current_batch_ids = [item["id"]["videoId"] for item in items]
                
                # [B] 상세 정보 API 호출: 검색 결과에는 조회수가 없어서 따로 물어봐야 합니다.
                stats_response = self.youtube.videos().list(
                    part="statistics,snippet",
                    id=",".join(current_batch_ids)  # 여러 ID를 쉼표로 연결해서 한꺼번에 요청
                ).execute()

                # 조회수, 좋아요, 댓글 수를 하나씩 꺼내서 바구니에 담습니다.
                for item in stats_response.get("items", []):
                    stats = item["statistics"]
                    pub_date = item["snippet"]["publishedAt"][:10]  # 날짜만 추출 (YYYY-MM-DD)
                    video_list.append({
                        "date": pub_date,
                        "view_count": int(stats.get("viewCount", 0)),
                        "like_count": int(stats.get("likeCount", 0)),
                        "comment_count": int(stats.get("commentCount", 0))
                    })

                # 다음 페이지가 있는지 확인하고, 있으면 계속 진행합니다.
                next_page_token = search_response.get("nextPageToken")
                if not next_page_token:
                    break
            except Exception as e:
                # API 사용 한도(할당량)를 다 썼을 때의 처리
                if "quotaExceeded" in str(e):
                    print("[알림] 오늘 사용할 수 있는 유튜브 API 할당량을 모두 사용했습니다.")
                    return video_list
                print(f"[오류] 데이터 수집 중 문제가 발생했습니다: {e}")
                break

        return video_list

    def get_historical_data(self, keyword, start_date=None, end_date=None):
        """전체 기간을 분석하고 일별로 합산된 통계 데이터를 만드는 함수"""
        
        # 1. 종료일 설정: 날짜 설정이 없으면 '어제'를 기준으로 합니다.
        if end_date is None:
            end_date = datetime.now(timezone.utc) - timedelta(days=1)
            
        # 2. 시작일 설정: 날짜 설정이 없으면 '2025-07-17' 고정 날짜를 사용합니다.
        if start_date is None:
            start_date = datetime(2025, 7, 17, tzinfo=timezone.utc)
        
        print(f"[*] 분석 시작 범위: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
        
        all_videos = []
        current_end = end_date
        
        # 유튜브 검색 제한을 피하기 위해 30일 단위로 끊어서 수집합니다.
        while current_end >= start_date:
            current_start = max(start_date, current_end - timedelta(days=30))
            print(f"[*] 기간 수집 중: {current_start.strftime('%Y-%m-%d')} ~ {current_end.strftime('%Y-%m-%d')}...")
            
            period_data = self.get_metrics_for_period(keyword, current_start, current_end)
            all_videos.extend(period_data)
            
            current_end = current_start - timedelta(days=1)
            
        if not all_videos:
            return None

        # [C] 수집한 낱개 영상 데이터를 표(DataFrame)로 만들어 분석합니다.
        df = pd.DataFrame(all_videos)
        
        # 같은 날짜끼리 묶어서(groupby) 총 영상 수, 총 조회수 등을 계산합니다.
        summary_df = df.groupby("date").agg({
            "view_count": ["count", "sum"],
            "like_count": "sum",
            "comment_count": "sum"
        }).reset_index()

        # 표의 칼럼 이름을 보기 좋게 바꿉니다.
        summary_df.columns = ["date", "video_count", "total_views", "total_likes", "total_comments"]
        summary_df["keyword"] = keyword
        
        return summary_df

import os
import requests
import json
from datetime import datetime

# Airflow 환경에서 파일 위치 보장을 위해 절대 경로 사용
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LAST_ID_FILE = os.path.join(BASE_DIR, 'last_video_id.txt')
PLAYLIST_ID = 'PL1FPDVeoyuPfSTmRCEjr2GGOTnNqE_mnn'

def run_my_crawler(api_key):
    # 1. 마지막으로 수집했던 비디오 ID 읽기 (중복 방지용)
    last_collected_id = ""
    if os.path.exists(LAST_ID_FILE):
        with open(LAST_ID_FILE, 'r', encoding='utf-8') as f:
            last_collected_id = f.read().strip()

    # 2. 플레이리스트 아이템 가져오기
    url = f"https://www.googleapis.com/youtube/v3/playlistItems?part=snippet,contentDetails&maxResults=50&playlistId={PLAYLIST_ID}&key={api_key}"
    response = requests.get(url).json()
    
    # Error handling for API response
    if 'error' in response:
        print(f"Error from YouTube API: {response['error'].get('message')}")
        return

    new_video_ids = []
    items = response.get('items', [])
    for item in items:
        v_id = item['contentDetails']['videoId']
        if v_id == last_collected_id: # 이전에 수집한 ID를 만나면 중단
            break
        new_video_ids.append(v_id)

    if not new_video_ids:
        print("새로운 영상이 없습니다.")
        return

    # 3. 새로운 영상들의 상세 정보(조회수, 좋아요) 가져오기
    ids_str = ','.join(new_video_ids)
    stats_url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={ids_str}&key={api_key}"
    stats_response = requests.get(stats_url).json()

    results = []
    for item in stats_response.get('items', []):
        results.append({
            'channel_id': item['snippet']['channelId'],
            'video_id': item['id'],
            'title': item['snippet']['title'],
            'description': item['snippet']['description'],
            'thumbnail_url': item['snippet']['thumbnails']['high']['url'],
            'view_count': item['statistics'].get('viewCount', '0'),
            'like_count': item['statistics'].get('likeCount', '0'),
            'comment_count': item['statistics'].get('commentCount', '0'),
            'published_at': item['snippet']['publishedAt'],
            'collected_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

    # 4. 결과 출력 및 저장 (이 부분에 DB 저장 로직을 넣으면 됩니다)
    for res in results:
        print(f"수집완료: {res['title']} (조회수: {res['view_count']})")

    # 5. [중요] 가장 최신 영상 ID를 책갈피로 저장
    with open(LAST_ID_FILE, 'w', encoding='utf-8') as f:
        f.write(new_video_ids[0])

    return results

if __name__ == "__main__":
    # 서버 환경용이므로 단독 실행 시에는 환경 변수 등을 활용하도록 구성 가능
    # 여기서는 Airflow를 통한 호출이 주 목적임
    pass
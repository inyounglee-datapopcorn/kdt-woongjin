import requests
import json
import pandas as pd
import time
from datetime import datetime, timedelta
from urllib.parse import quote
import os
import sys
import io
from supabase import create_client, Client
from airflow.hooks.base import BaseHook

# 한글 출력 깨짐 방지
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

class SupabaseManager:
    """데이터베이스(Supabase)와 소통하는 담당자 클래스"""
    def __init__(self, conn_id='supabase_conn'):
        # Airflow Connection에서 Supabase 접속 정보 가져오기
        try:
            conn = BaseHook.get_connection(conn_id)
            url = conn.host
            key = conn.password
            
            # 필수 정보가 없으면 에러 발생
            if not url or not key:
                raise ValueError(f"[!] Airflow Connection '{conn_id}'에 URL 또는 Key가 설정되어 있는지 확인해주세요.")

            # Supabase 클라이언트 생성   
            self.supabase: Client = create_client(url, key)
        except Exception as e:
            raise ValueError(f"[!] Airflow Connection '{conn_id}'를 불러오는 중 오류 발생: {e}")

    def insert_blog_trend(self, data):
        """네이버 블로그 트렌드 데이터를 naver_blog_trends 테이블에 삽입/업데이트"""
        try:
            # upsert : 데이터가 있으면 업데이트, 없으면 삽입
            # on_conflict : date와 keyword가 같으면 중복으로 판단
            response = self.supabase.table("naver_blog_trends").upsert(
                data,
                on_conflict="date,keyword"
            ).execute()
            return response
        except Exception as e:
            print(f"[!] DB 저장 중 오류 발생 ({data.get('date', 'unknown')}): {e}")
            return None

class NaverBlogCrawler:
    """네이버 블로그 데이터를 수집하는 클래스"""
    def __init__(self, conn_id='supabase_conn'):
        # 네이버 블로그 검색 API 엔드포인트
        self.url = "https://section.blog.naver.com/ajax/SearchList.naver"
        # HTTP 요청 시 사용할 헤더 (브라우저인 척 하기 위한 설정)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*"
        }

        # DB 연결 시도 (실패해도 크롤링은 계속 진행)
        try:
            self.db = SupabaseManager(conn_id=conn_id)
        except Exception as e:
            self.db = None
            print(f"[*] DB 연동 생략: {e}")

    def get_blog_count(self, keyword, start_date="", end_date=""):
        params = {
            "countPerPage": "7", # 페이지당 게시글 수
            "currentPage": "1", # 현재 페이지
            "startDate": start_date, # 검색 시작일
            "endDate": end_date, # 검색 종료일
            "keyword": keyword, # 검색 키워드
            "orderBy": "sim" # 정렬 기준
        }
        
        # Referer 헤더 추가 (네이버가 요청 출처를 확인하도록)
        headers = self.headers.copy()
        headers["Referer"] = f"https://section.blog.naver.com/Search/Post.naver?pageNo=1&rangeType=ALL&orderBy=sim&keyword={quote(keyword)}"
        
        try:
            # API 요청 보내기
            response = requests.get(self.url, params=params, headers=headers)
            if response.status_code != 200:
                return None
            
            # 응답에서 JSON 부분만 추출 (불필요한 앞부분 제거)
            content = response.text
            start_idx = content.find('{')
            if start_idx == -1:
                return None
            
            clean_content = content[start_idx:].strip()
            data = json.loads(clean_content)
            
            # 전체 게시글 개수 추출
            total_count = data.get("result", {}).get("totalCount", 0)
            return total_count
        except Exception as e:
            print(f"[-] Error fetching count for '{keyword}': {e}")
            return None

    def run_aggregation(self, keywords, start_date=None, end_date=None, days=180):
        results = []

        # 날짜 설정 로직
        if start_date and end_date:
            # 직접 날짜가 지정된 경우 (datetime 객체 또는 문자열)
            start_date_dt = pd.to_datetime(start_date)
            end_date_dt = pd.to_datetime(end_date)
        else:
            # 기본값: 종료일은 어제, 시작일은 days일 전
            end_date_dt = datetime.now() - timedelta(days=1)
            start_date_dt = end_date_dt - timedelta(days=days)
        
        print(f"[+] Starting daily aggregation for {len(keywords)} keywords")
        
        # 시작일부터 종료일까지 하루씩 반복
        current_dt = start_date_dt
        while current_dt <= end_date_dt:
            date_str = current_dt.strftime("%Y-%m-%d")
            
            # 각 키워드별로 데이터 수집
            for keyword in keywords:
                # 해당 날짜의 블로그 게시글 수 가져오기
                count = self.get_blog_count(keyword, start_date=date_str, end_date=date_str)
                
                if count is not None:
                    data_row = {
                        "date": date_str,
                        "keyword": keyword,
                        "total_count": count
                    }
                    results.append(data_row)
                    
                    # DB에 저장
                    if self.db:
                        self.db.insert_blog_trend(data_row)
                
                time.sleep(0.5)
            current_dt += timedelta(days=1)
        
        return results

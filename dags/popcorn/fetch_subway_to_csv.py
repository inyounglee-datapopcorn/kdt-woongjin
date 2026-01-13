import logging
import json
import csv
import urllib.request
from datetime import datetime
import os

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
SEOUL_API_KEY = "5a4c61745532696e393557726c696f"
TARGET_LINES = [
    "1호선", "2호선", "3호선", "4호선", "5호선", 
    "6호선", "7호선", "8호선", "9호선",
    "경의중앙선", "공항철도", "수인분당선", "신분당선"
]

def collect_subway_data():
    all_records = []
    
    for line in TARGET_LINES:
        try:
            # API 호출 (urllib 사용)
            encoded_line = urllib.parse.quote(line)
            url = f"http://swopenapi.seoul.go.kr/api/subway/{SEOUL_API_KEY}/json/realtimePosition/1/100/{encoded_line}"
            logging.info(f"Fetching data for {line}...")
            
            with urllib.request.urlopen(url) as response:
                if response.status != 200:
                    logging.error(f"Failed to fetch {line}: Status {response.status}")
                    continue
                data = json.loads(response.read().decode('utf-8'))
            
            # 데이터 파싱
            if 'realtimePositionList' in data:
                items = data['realtimePositionList']
                logging.info(f"{line}: Found {len(items)} trains")
                
                for item in items:
                    # 매핑
                    recptn_dt = item.get("recptnDt")
                    parsed_time = None
                    if recptn_dt:
                        try:
                            parsed_time = datetime.strptime(recptn_dt, "%Y-%m-%d %H:%M:%S").isoformat()
                        except:
                            parsed_time = recptn_dt

                    record = {
                        "line_id": item.get("subwayId"),
                        "line_name": item.get("subwayNm"),
                        "station_id": item.get("statnId"),
                        "station_name": item.get("statnNm"),
                        "train_number": item.get("trainNo"),
                        "last_rec_date": item.get("lastRecptnDt"),
                        "last_rec_time": parsed_time,
                        "direction_type": item.get("updnLine"),
                        "dest_station_id": item.get("statnTid"),
                        "dest_station_name": item.get("statnTnm"),
                        "train_status": item.get("trainSttus"),
                        "is_express": item.get("directAt"),
                        "is_last_train": item.get("lstcarAt") == "1"
                    }
                    all_records.append(record)
            else:
                logging.info(f"{line}: No data found")
                
        except Exception as e:
            logging.error(f"Error fetching data for {line}: {e}")
            continue
            
    # CSV 저장 (csv 모듈 사용)
    if all_records:
        output_file = "realtime_subway_data.csv"
        keys = all_records[0].keys()
        
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(all_records)
            
        logging.info(f"Successfully saved {len(all_records)} records to {output_file}")
        print(f"\n[Success] Data saved to {os.path.abspath(output_file)}")
    else:
        logging.warning("No records collected.")

if __name__ == "__main__":
    collect_subway_data()

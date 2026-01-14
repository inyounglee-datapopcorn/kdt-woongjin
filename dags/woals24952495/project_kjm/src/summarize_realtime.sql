-- [확정 SQL] 수집한 실시간 데이터 가공 코드1.
-- 목적: 1분 단위 스냅샷 데이터를 열차/역별 '도착/출발' 이벤트로 압축

DROP VIEW IF EXISTS test1_realtime_summary;

CREATE VIEW test1_realtime_summary AS
SELECT 
    -- 1. 날짜별로 묶기 (last_rec_time 사용)
    DATE(last_rec_time) as created_date, 
    line_name,
    station_name,
    up_down, 
    -- 2. 열차 번호에서 숫자만 추출
    regexp_replace(train_code, '[^0-9]', '', 'g') as train_code_num,
    
    -- 3. 급행 여부 및 종착역 정보 추가
    is_express,
    dest_station_name,
    
    -- 4. 도착 시각 추출: 진입(0) 또는 도착(1) 중 가장 빠른 시간
    MIN(last_rec_time) FILTER (WHERE train_status IN (0, 1)) as actual_arrival,
    
    -- 5. 출발 시각 추출: 출발(2) 중 가장 빠른 시간
    MIN(last_rec_time) FILTER (WHERE train_status = 2) as actual_departure

FROM final_realtime_subway
WHERE train_status IN (0, 1, 2) -- 전역출발(3) 제외
GROUP BY 
    1, 2, 3, 4, 5, 6, 7
ORDER BY 
    created_date DESC, actual_arrival DESC;

# 🏆 서울 지하철 지연 분석: 대시보드 마스터 가이드

이 문서는 지하철 지연 데이터를 통해 실시간 운영 상태를 모니터링하고, 지연의 원인을 심층 분석하기 위한 **전략 기획**과 **실행용 SQL 코드**를 하나로 통합한 마스터 가이드입니다.

---

## 🏛️ 대시보드 설계 철학: 3-Layer 분석
멋진 대시보드를 위해 데이터를 세 가지 계층으로 나누어 배치합니다.

1.  **현상 파악 (Macro)**: 어느 호선이 가장 많이 밀리는가? (전체 순위)
2.  **흐름 분석 (Trend)**: 어느 시간대에 지연이 집중되는가? (시간대 분포)
3.  **원인 진단 (Micro)**: 유독 이 역은 왜 늦는가? (개별 열차 분석)

---

## 🚀 레이어별 상세 가이드

### [Layer 1] 호선별 지연 랭킹 (Bar Chart)
> **"오늘의 지연 꼴찌 노선은?"**
*   **SQL 쿼리**:
    ```sql
    SELECT 
        line_name,
        ROUND(AVG(EXTRACT(EPOCH FROM delay_duration) / 60)::numeric, 1) AS avg_delay_min,
        COUNT(*) AS delay_count
    FROM table_redash_history
    WHERE created_date = CURRENT_DATE 
      AND delay_duration > interval '0 seconds'
    GROUP BY line_name
    ORDER BY avg_delay_min DESC;
    ```
*   **Redash 설정**: `Chart Type: Bar`, `X: line_name`, `Y: avg_delay_min`

### [Layer 2] 시간대별 지연 히트맵 (Heatmap)
> **"지연의 출근 시간대 집중도 확인"**
*   **SQL 쿼리**:
    ```sql
    SELECT 
        line_name,
        EXTRACT(HOUR FROM kst_arrival) AS arrival_hour,
        ROUND(AVG(EXTRACT(EPOCH FROM delay_duration) / 60)::numeric, 1) AS avg_delay_min
    FROM table_redash_history
    WHERE created_date = CURRENT_DATE
    GROUP BY 1, 2;
    ```
*   **Redash 설정**: `Chart Type: Heatmap`, `X: arrival_hour`, `Y: line_name`, `Color: avg_delay_min`

### [Layer 3] 역별 정밀 산점도 (Deep-Dive)
> **"이 역은 왜 늦을까? 개별 열차 낱낱이 파헤치기"**
*   **SQL 쿼리**:
    ```sql
    SELECT 
        scheduled_arrival_text, 
        ROUND((EXTRACT(EPOCH FROM delay_duration) / 60)::numeric, 1) AS delay_minutes,
        train_code_num,
        dest_station_name,
        CASE WHEN up_down::text = '0' THEN '상행/내선' ELSE '하행/외선' END AS direction
    FROM table_redash_history
    WHERE station_name = '{{ station_name }}'
      AND line_name = '{{ line_name }}'
      AND up_down::text = '{{ direction_0_or_1 }}'
      AND created_date = CURRENT_DATE
    ORDER BY scheduled_arrival_text;
    ```
*   **Redash 설정**: `Chart Type: Scatter`, `X: scheduled_arrival_text`, `Y: delay_minutes`, `Group by: direction`

### [Layer 4] 지연 분포 분석 (New!)
> **"일찍 오는 차 vs 늦는 차의 통계적 쏠림 확인"**
*   **SQL 쿼리**:
    ```sql
    SELECT 
        ROUND((EXTRACT(EPOCH FROM delay_duration) / 60)::numeric, 0) AS delay_group,
        COUNT(*) AS train_count
    FROM table_redash_history
    WHERE station_name = '{{ station_name }}'
      AND line_name = '{{ line_name }}'
      AND up_down::text = '{{ direction_0_or_1 }}'
      AND created_date = CURRENT_DATE
    GROUP BY 1 ORDER BY 1;
    ```
*   **Redash 설정**: `Chart Type: Bar`, `X: delay_group`, `Y: train_count`

---

## 📈 대시보드 최종 레이아웃 추천
| 영역 | 콘텐츠 | 시각화 타입 |
| :--- | :--- | :--- |
| **상단 (Top)** | 오늘 총 지연 발생 건수 / 평균 지연 시간 | Counter (숫자 카드) |
| **중단 (Middle)** | 호선별 랭킹 & 시간대 히트맵 | Bar Chart & Heatmap |
| **하단 (Bottom)** | **상세 분석: 산점도 & 통계 분포** | **Scatter & Bar (나란히 배치)** |

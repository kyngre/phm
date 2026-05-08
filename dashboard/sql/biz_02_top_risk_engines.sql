-- 비즈니스 탭 차트 2: 위험 엔진 Top 10
-- 차트 타입: Table (또는 Big Number with Trendline)
-- 정렬: rul_pred ASC, risk_tier=CRITICAL 우선
-- 각 (dataset, unit) 의 가장 최신 예측만 보여준다.
WITH latest AS (
    SELECT
        dataset_id, unit_id, cycle, rul_pred, risk_tier, predict_ts,
        ROW_NUMBER() OVER (PARTITION BY dataset_id, unit_id ORDER BY cycle DESC) AS rn
    FROM iceberg.gold.rul_prediction
    WHERE model_version = 'gbt-v0'
)
SELECT dataset_id, unit_id, cycle, rul_pred, risk_tier, predict_ts
FROM latest
WHERE rn = 1
ORDER BY rul_pred ASC
LIMIT 10;

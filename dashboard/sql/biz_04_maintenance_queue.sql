-- 비즈니스 탭 차트 4: 정비 권고 큐
-- 차트 타입: Table
-- risk_tier IN ('CRITICAL','HIGH'), CI 하한이 0 가까운 엔진 우선.
WITH latest AS (
    SELECT
        dataset_id, unit_id, cycle,
        rul_pred, rul_pred_lower, rul_pred_upper, risk_tier, predict_ts,
        ROW_NUMBER() OVER (PARTITION BY dataset_id, unit_id ORDER BY cycle DESC) AS rn
    FROM iceberg.gold.rul_prediction
    WHERE model_version = 'gbt-v0'
)
SELECT
    dataset_id, unit_id, cycle, predict_ts,
    rul_pred, rul_pred_lower, rul_pred_upper, risk_tier,
    CASE
        WHEN risk_tier = 'CRITICAL' THEN '즉시 점검'
        WHEN risk_tier = 'HIGH'     THEN '7일 내 점검'
        ELSE '관찰'
    END AS action
FROM latest
WHERE rn = 1
  AND risk_tier IN ('CRITICAL', 'HIGH')
ORDER BY rul_pred ASC;

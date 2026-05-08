-- 비즈니스 탭 차트 1: Fleet RUL 분포 히스토그램
-- 차트 타입: Histogram
-- X: rul_pred (bin=10), Y: count
-- 필터: predict_date (Time-range), dataset_id, model_version
SELECT
    predict_ts,
    DATE(predict_ts)   AS predict_date,
    dataset_id,
    model_version,
    rul_pred,
    risk_tier
FROM iceberg.gold.rul_prediction
WHERE rul_pred IS NOT NULL;

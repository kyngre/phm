-- 운영 탭 차트 6: 모델 MAE drift
-- 차트 타입: Line, X = predict_date, Y = mae, series = model_version
-- 7일 이동평균은 Superset Time-series chart 의 'rolling mean' 옵션으로 계산.
SELECT
    DATE(predict_ts)                            AS predict_date,
    model_version,
    dataset_id,
    COUNT(*)                                    AS n,
    ROUND(AVG(abs_error), 2)                    AS mae,
    ROUND(SQRT(AVG(POWER(abs_error, 2))), 2)    AS rmse,
    ROUND(SUM(phm08_score), 2)                  AS phm08_total
FROM iceberg.gold.rul_prediction
WHERE rul_actual IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

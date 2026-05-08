-- 5) RUL 예측 MAE drift — 일자 × 모델 별 정확도 추이.
--    abs_error 가 NULL 이 아닌 행만 (rul_actual 확인된 행) 사용.
SELECT
    DATE(predict_ts)    AS predict_date,
    model_version,
    dataset_id,
    COUNT(*)            AS n,
    ROUND(AVG(abs_error), 2)               AS mae,
    ROUND(SQRT(AVG(POWER(abs_error, 2))), 2) AS rmse,
    ROUND(SUM(phm08_score), 2)             AS phm08_total
FROM iceberg.gold.rul_prediction
WHERE rul_actual IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;

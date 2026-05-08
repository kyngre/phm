-- 8) 모델 버전 간 예측 일관성 — 동일 (dataset, unit, cycle) 에 대해 버전별 차이.
--    Iceberg time-travel 이 아닌 같은 테이블 내 model_version 다중 적재 비교.
WITH paired AS (
    SELECT
        a.dataset_id,
        a.unit_id,
        a.cycle,
        a.model_version       AS v_a,
        a.rul_pred            AS pred_a,
        b.model_version       AS v_b,
        b.rul_pred            AS pred_b
    FROM iceberg.gold.rul_prediction a
    JOIN iceberg.gold.rul_prediction b
      ON  a.dataset_id = b.dataset_id
      AND a.unit_id    = b.unit_id
      AND a.cycle      = b.cycle
      AND a.model_version < b.model_version       -- 중복 (a,b)/(b,a) 제거
)
SELECT
    v_a, v_b,
    COUNT(*)                               AS n,
    ROUND(AVG(ABS(pred_a - pred_b)), 2)    AS mean_abs_diff,
    ROUND(MAX(ABS(pred_a - pred_b)), 2)    AS max_abs_diff,
    ROUND(CORR(pred_a, pred_b), 4)         AS correlation
FROM paired
GROUP BY v_a, v_b
ORDER BY v_a, v_b;

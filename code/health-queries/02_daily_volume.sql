-- 2) dataset × condition 별 일자 행 수 추이 (수집 정상성 + 운영조건 분포 변화 감지).
SELECT
    DATE(s.event_ts)         AS event_date,
    s.dataset_id,
    s.op_condition_cluster,
    COUNT(*)                 AS rows,
    COUNT(DISTINCT s.unit_id) AS units
FROM iceberg.silver.engine_health s
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;

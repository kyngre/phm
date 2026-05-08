-- 7) 운영조건 클러스터 분포 변화 — data drift 감지.
--    "최신 simulation day" vs 7일 전 분포 비교. 비율 차이 큰 cluster 가 drift 의심.
--    시뮬레이션 데이터(event_ts) 가 과거에 분산되어 있을 수 있어 CURRENT_DATE 가 아니라
--    데이터 자체의 MAX(event_ts) 기준으로 today/week_ago 를 정의.
WITH t_anchor AS (
    SELECT DATE(MAX(event_ts)) AS today_d
      FROM iceberg.silver.engine_health
),
daily AS (
    SELECT
        DATE(s.event_ts) AS d,
        s.dataset_id,
        s.op_condition_cluster,
        COUNT(*)         AS rows
    FROM iceberg.silver.engine_health s, t_anchor a
    WHERE s.event_ts >= CAST(a.today_d AS TIMESTAMP) - INTERVAL '8' DAY
    GROUP BY 1, 2, 3
),
ratios AS (
    SELECT
        d, dataset_id, op_condition_cluster,
        rows,
        SUM(rows) OVER (PARTITION BY d, dataset_id) AS day_total,
        rows * 1.0 / SUM(rows) OVER (PARTITION BY d, dataset_id) AS pct
    FROM daily
)
SELECT
    today.dataset_id,
    today.op_condition_cluster,
    ROUND(today.pct, 4)                              AS today_pct,
    ROUND(week_ago.pct, 4)                           AS week_ago_pct,
    ROUND(today.pct - week_ago.pct, 4)               AS delta
FROM ratios today
CROSS JOIN t_anchor a
LEFT JOIN ratios week_ago
       ON week_ago.dataset_id           = today.dataset_id
      AND week_ago.op_condition_cluster = today.op_condition_cluster
      AND week_ago.d = a.today_d - INTERVAL '7' DAY
WHERE today.d = a.today_d
ORDER BY ABS(COALESCE(today.pct, 0) - COALESCE(week_ago.pct, 0)) DESC;

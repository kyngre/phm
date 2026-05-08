-- 1) 센서 dropout 감지: 엔진별 마지막 cycle 도착 시각.
--    임계: 마지막 ingest 가 30분 이상 지나면 dropout 의심 (Trino/Spark 공통).
SELECT
    dataset_id,
    unit_id,
    MAX(cycle)                       AS last_cycle,
    MAX(ingest_ts)                   AS last_ingest_ts,
    CURRENT_TIMESTAMP                AS now_ts,
    DATE_DIFF('minute', MAX(ingest_ts), CURRENT_TIMESTAMP) AS gap_minutes
FROM iceberg.bronze.engine_sensor_raw
GROUP BY dataset_id, unit_id
HAVING DATE_DIFF('minute', MAX(ingest_ts), CURRENT_TIMESTAMP) > 30
ORDER BY gap_minutes DESC;

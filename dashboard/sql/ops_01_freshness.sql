-- 운영 탭 차트 1: 데이터 신선도
-- 차트 타입: Big Number (per dataset_id) — gap_minutes
SELECT
    dataset_id,
    MAX(ingest_ts)                                              AS last_ingest_ts,
    DATE_DIFF('minute', MAX(ingest_ts), CURRENT_TIMESTAMP)      AS gap_minutes
FROM iceberg.bronze.engine_sensor_raw
GROUP BY dataset_id;

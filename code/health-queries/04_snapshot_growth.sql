-- 4) snapshot 증가율 — expire 정책 점검 (90일 보관 + 최소 20개 유지).
SELECT
    'bronze.engine_sensor_raw' AS table_name,
    COUNT(*)              AS snapshot_count,
    MIN(committed_at)     AS oldest_snapshot,
    MAX(committed_at)     AS latest_snapshot,
    DATE_DIFF('day', MIN(committed_at), MAX(committed_at)) AS span_days
FROM iceberg.bronze."engine_sensor_raw$snapshots"
UNION ALL
SELECT 'silver.engine_health',
    COUNT(*), MIN(committed_at), MAX(committed_at),
    DATE_DIFF('day', MIN(committed_at), MAX(committed_at))
FROM iceberg.silver."engine_health$snapshots"
UNION ALL
SELECT 'gold.rul_prediction',
    COUNT(*), MIN(committed_at), MAX(committed_at),
    DATE_DIFF('day', MIN(committed_at), MAX(committed_at))
FROM iceberg.gold."rul_prediction$snapshots";

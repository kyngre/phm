-- 운영 탭 차트 4: Iceberg snapshot 추이 (테이블별)
-- 차트 타입: Time-series Line, X = committed_at, Y = cumulative count
SELECT 'bronze.engine_sensor_raw' AS tbl, committed_at, snapshot_id, operation
FROM iceberg.bronze."engine_sensor_raw$snapshots"
UNION ALL
SELECT 'silver.engine_health', committed_at, snapshot_id, operation
FROM iceberg.silver."engine_health$snapshots"
UNION ALL
SELECT 'gold.rul_prediction', committed_at, snapshot_id, operation
FROM iceberg.gold."rul_prediction$snapshots"
ORDER BY committed_at;

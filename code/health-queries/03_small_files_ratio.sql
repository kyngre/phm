-- 3) 작은 파일 비율 (< 128MB) — 컴팩션 트리거 신호.
--    Iceberg `files` 메타테이블 사용. 임계는 target file size 와 동일하게 잡음.
SELECT
    'bronze.engine_sensor_raw' AS table_name,
    COUNT(*) FILTER (WHERE file_size_in_bytes < 134217728) AS small_files,
    COUNT(*)                                                AS total_files,
    ROUND(100.0 * COUNT(*) FILTER (WHERE file_size_in_bytes < 134217728) / COUNT(*), 2) AS small_pct,
    ROUND(AVG(file_size_in_bytes) / 1048576.0, 2)           AS avg_mb,
    ROUND(SUM(file_size_in_bytes) / 1048576.0, 2)           AS total_mb
FROM iceberg.bronze."engine_sensor_raw$files"
UNION ALL
SELECT
    'silver.engine_health',
    COUNT(*) FILTER (WHERE file_size_in_bytes < 134217728),
    COUNT(*),
    ROUND(100.0 * COUNT(*) FILTER (WHERE file_size_in_bytes < 134217728) / COUNT(*), 2),
    ROUND(AVG(file_size_in_bytes) / 1048576.0, 2),
    ROUND(SUM(file_size_in_bytes) / 1048576.0, 2)
FROM iceberg.silver."engine_health$files"
UNION ALL
SELECT
    'gold.rul_prediction',
    COUNT(*) FILTER (WHERE file_size_in_bytes < 134217728),
    COUNT(*),
    ROUND(100.0 * COUNT(*) FILTER (WHERE file_size_in_bytes < 134217728) / COUNT(*), 2),
    ROUND(AVG(file_size_in_bytes) / 1048576.0, 2),
    ROUND(SUM(file_size_in_bytes) / 1048576.0, 2)
FROM iceberg.gold."rul_prediction$files";

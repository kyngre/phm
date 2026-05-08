-- 운영 탭 차트 3: 작은 파일 비율 + 평균 크기 (테이블별)
-- 차트 타입: Table 또는 Bar
SELECT 'bronze.engine_sensor_raw' AS tbl,
       COUNT(*)                                                       AS files,
       ROUND(AVG(file_size_in_bytes) / 1048576.0, 1)                  AS avg_mb,
       ROUND(SUM(IF(file_size_in_bytes < 134217728, 1, 0)) * 1.0 / COUNT(*), 4) AS small_ratio
FROM iceberg.bronze."engine_sensor_raw$files"
UNION ALL
SELECT 'silver.engine_health',
       COUNT(*),
       ROUND(AVG(file_size_in_bytes) / 1048576.0, 1),
       ROUND(SUM(IF(file_size_in_bytes < 134217728, 1, 0)) * 1.0 / COUNT(*), 4)
FROM iceberg.silver."engine_health$files"
UNION ALL
SELECT 'gold.rul_prediction',
       COUNT(*),
       ROUND(AVG(file_size_in_bytes) / 1048576.0, 1),
       ROUND(SUM(IF(file_size_in_bytes < 134217728, 1, 0)) * 1.0 / COUNT(*), 4)
FROM iceberg.gold."rul_prediction$files";

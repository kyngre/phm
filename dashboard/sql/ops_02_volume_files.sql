-- 운영 탭 차트 2: 일자별 행 수 (Bronze 기준)
-- 차트 타입: Bar (stacked by dataset_id)
-- 파일 수/평균 크기는 ops_03 (`$files` 메타테이블) 에서 별도로 본다.
SELECT
    DATE(ingest_ts)  AS d,
    dataset_id,
    COUNT(*)         AS rows
FROM iceberg.bronze.engine_sensor_raw
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

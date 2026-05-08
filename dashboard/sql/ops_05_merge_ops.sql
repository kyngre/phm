-- 운영 탭 차트 5: Silver MERGE 작업 패턴
-- 차트 타입: Time-series Bar (X = committed_at, series = operation, Y = added_rows)
-- 특징: operation('append', 'replace' 등)에 따라 summary 맵에 특정 키가 없을 수 있음.
-- 주의: Trino 에러(Key not present in map) 방지를 위해 대괄호([]) 대신 
--      element_at() 함수를 사용하여 안전하게 NULL을 반환하도록 처리함.
SELECT
    committed_at,
    operation,
    CAST(element_at(summary, 'added-records') AS BIGINT) AS added_rows,
    CAST(element_at(summary, 'deleted-records') AS BIGINT) AS deleted_rows,
    CAST(element_at(summary, 'total-records') AS BIGINT) AS total_rows,
    CAST(element_at(summary, 'added-data-files') AS BIGINT) AS added_files,
    CAST(element_at(summary, 'removed-data-files') AS BIGINT) AS removed_files
FROM iceberg.silver."engine_health$snapshots"
ORDER BY committed_at DESC
LIMIT 200
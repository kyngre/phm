-- 6) Silver MERGE 작업 통계 — replace/append 빈도 + 추가/삭제 행수.
--    OCC 충돌은 Spark application log 에만 남으므로, 여기서는 commit 패턴을 본다.
--    summary map 의 'operation', 'added-records', 'deleted-records' 키 추출.
SELECT
    snapshot_id,
    committed_at,
    operation,
    summary['added-records']     AS added_rows,
    summary['deleted-records']   AS deleted_rows,
    summary['total-records']     AS total_rows,
    summary['added-data-files']  AS added_files,
    summary['removed-data-files'] AS removed_files
FROM iceberg.silver."engine_health$snapshots"
ORDER BY committed_at DESC
LIMIT 50;

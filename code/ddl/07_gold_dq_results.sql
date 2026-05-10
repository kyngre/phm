-- ─────────────────────────────────────────────────────────────
-- Gold: dq_results — 데이터 품질 검증 결과
--
-- 목적: code/health-queries/ 가 *운영 메트릭* (dropout/drift/snapshot 추이) 만
--       다루는 것에 비해, 이 테이블은 *데이터 자체의 품질* (NULL/range/uniqueness/
--       freshness/consistency) 을 행 단위로 측정·기록.
-- 입력: dq_check.py 가 phm.bronze + phm.silver + phm.gold 를 스캔하여 rule 평가.
-- 키:   (rule_name, layer, dataset_id, run_date) — 같은 날 재실행 시 UPDATE.
-- 임계치 평가: status = PASS / WARN / FAIL (operator 와 threshold 로 결정).
-- 참조: code/pipelines/dq_check.py, orchestration/dags/dq_check_dag.py
-- ─────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS phm.gold.dq_results;

CREATE TABLE phm.gold.dq_results (
    rule_name        STRING      COMMENT '예: bronze_null_key_columns',
    layer            STRING      COMMENT 'bronze | silver | gold',
    dataset_id       STRING      COMMENT 'FD001~FD004 또는 __ALL__ (cross-dataset 집계)',

    measured_value   DOUBLE      COMMENT '실제 측정값 (대개 bad row 수 또는 비율)',
    threshold        DOUBLE      COMMENT '허용 한계',
    operator         STRING      COMMENT '<= | < | == — measured 와 threshold 비교',
    status           STRING      COMMENT 'PASS | WARN | FAIL',

    sample_size      BIGINT      COMMENT '검사한 전체 행 수 (분모 또는 모집단)',
    description      STRING      COMMENT '한국어 한 줄 설명 (대시보드 표시용)',

    run_ts           TIMESTAMP   COMMENT '평가 실행 시각',
    run_date         DATE        COMMENT '멱등키 일부 (같은 날 재실행 = UPDATE)'
)
USING iceberg
PARTITIONED BY (run_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.merge.mode' = 'copy-on-write',
    -- 운영 메트릭 — 90일 정도면 trend 보기 충분
    'history.expire.max-snapshot-age-ms' = '7776000000'
);

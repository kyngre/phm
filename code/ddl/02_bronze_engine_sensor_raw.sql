-- ─────────────────────────────────────────────────────────────
-- Bronze: 원본 센서 스트림 (C-MAPSS 형식 그대로 + 적재 메타)
--
-- 입력: Kafka 토픽 phm.engine.sensor (1 cycle = 1 메시지)
-- 보존: 무기한 (감사·재처리 가능성)
-- 쓰기 정책: append-only (writeTo(...).append()).
--   ─ MERGE INTO 미사용 → batch 마다 read 비용 0, 트래픽 무관 일정 비용.
--   ─ Spark Streaming foreachBatch at-least-once 로 발생 가능한 dup 은
--     Silver 진입의 dedup_bronze() 가 (source_file, line_no) 단위 row_number=1
--     로 흡수 (first-write-wins).
--   ─ 결과: Bronze 행수 ≥ Silver 행수 가 정상 (dup 발생 시 strict >).
--   ─ 작은 파일/snapshot 누적은 maintenance/ 가 흡수.
-- ─────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS phm.bronze.engine_sensor_raw;

CREATE TABLE phm.bronze.engine_sensor_raw (
    -- 식별자
    dataset_id      STRING      COMMENT 'FD001 | FD002 | FD003 | FD004',
    unit_id         INT         COMMENT '엔진 ID',
    cycle           INT         COMMENT '운영 cycle 번호 (1부터 단조증가)',

    -- 운영 세팅 (3개)
    op_setting_1    DOUBLE,
    op_setting_2    DOUBLE,
    op_setting_3    DOUBLE,

    -- 센서 측정값 (21개; C-MAPSS는 26컬럼이지만 실제 사용 21개)
    sensor_1  DOUBLE, sensor_2  DOUBLE, sensor_3  DOUBLE, sensor_4  DOUBLE,
    sensor_5  DOUBLE, sensor_6  DOUBLE, sensor_7  DOUBLE, sensor_8  DOUBLE,
    sensor_9  DOUBLE, sensor_10 DOUBLE, sensor_11 DOUBLE, sensor_12 DOUBLE,
    sensor_13 DOUBLE, sensor_14 DOUBLE, sensor_15 DOUBLE, sensor_16 DOUBLE,
    sensor_17 DOUBLE, sensor_18 DOUBLE, sensor_19 DOUBLE, sensor_20 DOUBLE,
    sensor_21 DOUBLE,

    -- 적재 메타 (재처리·감사용)
    event_ts        TIMESTAMP   COMMENT '시뮬레이션 발생 시각 (Kafka 발행 시각)',
    ingest_ts       TIMESTAMP   COMMENT 'Bronze 적재 시각',
    source_file     STRING      COMMENT '예: train_FD001.txt',
    line_no         BIGINT      COMMENT '소스 파일 행 번호 (멱등키 일부)'
)
USING iceberg
PARTITIONED BY (dataset_id, days(ingest_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728',         -- 128MB
    'write.parquet.compression-codec' = 'zstd',
    'write.distribution-mode' = 'hash',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20',
    'history.expire.min-snapshots-to-keep' = '20',
    'history.expire.max-snapshot-age-ms' = '8640000000'    -- 100일 (학습 윈도우 90d + 마진 10d). orchestration/dags/iceberg_expire_dag.py 와 동기화.
);

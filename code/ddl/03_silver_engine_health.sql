-- ─────────────────────────────────────────────────────────────
-- Silver: 정제 + Health Index + 운영조건 클러스터 + rolling 피처
--
-- 입력: phm.bronze.engine_sensor_raw
-- 변환:
--   1) 결측·이상치 제거, 분산 0인 센서(1,5,6,10,16,18,19) 제외
--   2) op_setting 1~3 기반 K-means 6 cluster (FD002/004 6 condition)
--   3) Health Index = 1 - min(|s_avg_w5|, 3) / 3  (cluster 평균 대비 편차 기반; 0=열화, 1=정상)
--   4) rolling window (5 cycle) 평균/표준편차/추세
-- 멱등 MERGE 키: (dataset_id, unit_id, cycle)
-- ─────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS phm.silver.engine_health;

CREATE TABLE phm.silver.engine_health (
    dataset_id              STRING,
    unit_id                 INT,
    cycle                   INT,

    -- 운영 세팅 + 클러스터
    op_setting_1            DOUBLE,
    op_setting_2            DOUBLE,
    op_setting_3            DOUBLE,
    op_condition_cluster    INT     COMMENT '0~5 (6 condition)',

    -- 정제된 센서값 (분산 0 제외, 정규화된 값)
    s2_norm  DOUBLE, s3_norm  DOUBLE, s4_norm  DOUBLE,
    s7_norm  DOUBLE, s8_norm  DOUBLE, s9_norm  DOUBLE,
    s11_norm DOUBLE, s12_norm DOUBLE, s13_norm DOUBLE, s14_norm DOUBLE,
    s15_norm DOUBLE, s17_norm DOUBLE, s20_norm DOUBLE, s21_norm DOUBLE,

    -- Rolling window 피처 (window=5)
    s_avg_w5     DOUBLE COMMENT '주요 센서 평균',
    s_std_w5     DOUBLE,
    s_trend_w5   DOUBLE COMMENT '선형 회귀 기울기',

    -- Health Index (낮을수록 열화)
    health_index DOUBLE,

    -- 라벨 (학습용; 테스트셋은 RUL_FDxxx.txt에서 채움)
    rul_label    INT     COMMENT 'NULL이면 정답 미정',

    -- 메타
    event_ts        TIMESTAMP,
    ingest_ts       TIMESTAMP,
    silver_ts       TIMESTAMP   COMMENT 'Silver 처리 시각',
    silver_version  STRING      COMMENT '정제 로직 버전 (백필 추적)'
)
USING iceberg
-- 시계열 쿼리 (drift, KPI 추이) 가속 위해 days(event_ts) 추가.
-- op_condition_cluster 는 데이터 컬럼으로 두고 partition 에서 제외 (cardinality 6, days 와 곱하면 partition 수 폭증).
PARTITIONED BY (dataset_id, days(event_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728',
    'write.parquet.compression-codec' = 'zstd',
    'write.distribution-mode' = 'hash',
    'write.merge.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write',
    'history.expire.max-snapshot-age-ms' = '7776000000'
);

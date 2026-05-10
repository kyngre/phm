-- ─────────────────────────────────────────────────────────────
-- Silver: feat_stats — KMeans centroids + per (dataset, cluster) 센서 mean/std
--
-- 목적: incremental Silver 변환의 입력. fit-stats 모드가 주기적으로 갱신,
--       incremental 모드는 이 테이블만 broadcast join 하여 새 bronze 행을 정규화.
-- 키:   (stats_version, dataset_id, cluster_id) — MERGE INTO
-- 행수: stats_version 당 4 datasets × 6 clusters = 24 행
-- 참조: code/pipelines/silver_transform.py (--mode fit-stats / incremental)
-- ─────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS phm.silver.feat_stats;

CREATE TABLE phm.silver.feat_stats (
    stats_version        STRING      COMMENT 'fit 식별자 (예: fs-2026-05-10-1715332800)',
    fit_snapshot_id      BIGINT      COMMENT 'fit 시 사용된 bronze 스냅샷 ID (재현용)',

    -- KMeans 결과 (cluster_id 는 stable_cluster_ids 적용 후 0~5)
    cluster_id           INT,
    center_op_setting_1  DOUBLE,
    center_op_setting_2  DOUBLE,
    center_op_setting_3  DOUBLE,

    -- z-score 통계 (dataset 별로 분리; FD002/004 의 6 condition 분리 정규화)
    dataset_id           STRING      COMMENT 'FD001|FD002|FD003|FD004',
    n_samples            BIGINT      COMMENT '(dataset_id, cluster_id) 표본 수',

    sensor_2_mean  DOUBLE, sensor_2_std  DOUBLE,
    sensor_3_mean  DOUBLE, sensor_3_std  DOUBLE,
    sensor_4_mean  DOUBLE, sensor_4_std  DOUBLE,
    sensor_7_mean  DOUBLE, sensor_7_std  DOUBLE,
    sensor_8_mean  DOUBLE, sensor_8_std  DOUBLE,
    sensor_9_mean  DOUBLE, sensor_9_std  DOUBLE,
    sensor_11_mean DOUBLE, sensor_11_std DOUBLE,
    sensor_12_mean DOUBLE, sensor_12_std DOUBLE,
    sensor_13_mean DOUBLE, sensor_13_std DOUBLE,
    sensor_14_mean DOUBLE, sensor_14_std DOUBLE,
    sensor_15_mean DOUBLE, sensor_15_std DOUBLE,
    sensor_17_mean DOUBLE, sensor_17_std DOUBLE,
    sensor_20_mean DOUBLE, sensor_20_std DOUBLE,
    sensor_21_mean DOUBLE, sensor_21_std DOUBLE,

    fit_ts               TIMESTAMP
)
USING iceberg
PARTITIONED BY (stats_version)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.merge.mode' = 'copy-on-write',
    -- 24행 짜리 메타 테이블 — snapshot 짧게 유지해도 학습 재현엔 충분
    'history.expire.max-snapshot-age-ms' = '8640000000'  -- 100일
);

-- ─────────────────────────────────────────────────────────────
-- Gold: 모델 버전별 성능 지표 (drift 모니터링·논문 결과 표)
--
-- 산출 시점: 새 model_version 추론 완료 시 + 일배치 재계산
-- ─────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS phm.gold.model_metrics;

CREATE TABLE phm.gold.model_metrics (
    model_version       STRING,
    dataset_id          STRING,
    eval_window_start   DATE,
    eval_window_end     DATE,

    -- 표준 지표
    sample_count        BIGINT,
    mae                 DOUBLE,
    rmse                DOUBLE,
    phm08_score         DOUBLE  COMMENT 'PHM08 challenge score (낮을수록 좋음)',

    -- 분포 비교 (모델 drift)
    pred_mean           DOUBLE,
    pred_std            DOUBLE,
    pred_p50            DOUBLE,

    -- 데이터 drift 참고
    silver_snapshot_id  BIGINT,
    silver_row_count    BIGINT,

    -- 메타
    computed_ts         TIMESTAMP
)
USING iceberg
PARTITIONED BY (model_version)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

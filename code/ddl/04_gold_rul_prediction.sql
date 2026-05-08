-- ─────────────────────────────────────────────────────────────
-- Gold: 엔진별 RUL 예측 (모델 버전별 누적)
--
-- 입력: phm.silver.engine_health + 모델 추론
-- 멱등 MERGE 키: (model_version, dataset_id, unit_id, cycle)
-- 활용: 대시보드 직접 쿼리, 모델 비교, drift 분석
-- ─────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS phm.gold.rul_prediction;

CREATE TABLE phm.gold.rul_prediction (
    model_version    STRING       COMMENT 'lstm-v1 | cnn-v1 | ...',
    dataset_id       STRING,
    unit_id          INT,
    cycle            INT,

    -- 예측
    rul_pred         DOUBLE       COMMENT '예측 RUL (cycle)',
    rul_pred_lower   DOUBLE       COMMENT '신뢰구간 하단 (5%)',
    rul_pred_upper   DOUBLE       COMMENT '신뢰구간 상단 (95%)',

    -- 정답 (사후 평가용; 실시간 시점에는 NULL)
    rul_actual       INT,
    abs_error        DOUBLE,
    phm08_score      DOUBLE       COMMENT 'asymmetric scoring fn',

    -- 위험 등급
    risk_tier        STRING       COMMENT 'CRITICAL(<=10) | HIGH(<=30) | MED(<=80) | LOW',

    -- 메타
    predict_ts       TIMESTAMP,
    silver_snapshot_id BIGINT     COMMENT '사용된 Silver Iceberg snapshot (재현성)'
)
USING iceberg
PARTITIONED BY (model_version, dataset_id, days(predict_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728',
    'write.parquet.compression-codec' = 'zstd',
    'write.merge.mode' = 'copy-on-write'
);

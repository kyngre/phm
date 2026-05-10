-- ─────────────────────────────────────────────────────────────
-- Gold: pipeline_state — 추론 watermark + active 모델 추적
--
-- 목적: gold_rul_predict --mode predict 가 어떤 silver 스냅샷까지 처리했는지,
--       어떤 모델 (저장된 파일 경로) 을 로드해야 하는지 기록.
-- 키:   pipeline_name (예: 'gold_rul_predict')
-- 갱신: 매 train run 시 (active_model_version/path 갱신) +
--       매 predict run 시 (last_snapshot_id 전진).
-- 참조: code/pipelines/gold_rul_predict.py
-- ─────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS phm.gold.pipeline_state;

CREATE TABLE phm.gold.pipeline_state (
    pipeline_name           STRING      COMMENT '예: gold_rul_predict',
    last_snapshot_id        BIGINT      COMMENT '마지막으로 처리한 silver 스냅샷 ID',
    last_run_ts             TIMESTAMP,
    rows_processed          BIGINT      COMMENT '직전 predict run 처리 행 수',

    active_model_version    STRING      COMMENT '예: gbt-v0',
    active_model_path       STRING      COMMENT '저장된 PipelineModel 경로 (s3a:// 또는 file://)',
    active_model_trained_ts TIMESTAMP   COMMENT '활성 모델 학습 완료 시각'
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.merge.mode' = 'copy-on-write',
    'history.expire.max-snapshot-age-ms' = '604800000'   -- 7일
);

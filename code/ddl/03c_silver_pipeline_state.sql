-- ─────────────────────────────────────────────────────────────
-- Silver: pipeline_state — 증분 처리 watermark
--
-- 목적: --mode incremental 이 어디까지 처리했는지 기록. 다음 실행 시
--       이 watermark 이후의 bronze 스냅샷만 읽어 비용 O(Δ) 유지.
-- 키:   pipeline_name (예: 'silver_incremental')
-- 갱신: 매 incremental run 마다 MERGE
-- 참조: code/pipelines/silver_transform.py
-- ─────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS phm.silver.pipeline_state;

CREATE TABLE phm.silver.pipeline_state (
    pipeline_name         STRING      COMMENT '예: silver_incremental',
    last_snapshot_id      BIGINT      COMMENT '마지막으로 처리한 bronze 스냅샷 ID',
    last_run_ts           TIMESTAMP,
    rows_processed        BIGINT      COMMENT '직전 run 에서 처리한 silver 행 수 (관측용)',
    active_stats_version  STRING      COMMENT 'incremental 이 사용 중인 feat_stats 버전'
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.merge.mode' = 'copy-on-write',
    -- 1~수 행 짜리 운영 테이블 — snapshot 짧게 유지
    'history.expire.max-snapshot-age-ms' = '604800000'   -- 7일
);

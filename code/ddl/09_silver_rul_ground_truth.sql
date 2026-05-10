-- ─────────────────────────────────────────────────────────────
-- Silver: rul_ground_truth — NASA C-MAPSS RUL_FDxxx.txt 정답
--
-- 목적: test_FDxxx 의 *마지막 cycle 시점 RUL* (학회/논문 표준 평가용).
--       train trajectory 는 run-to-failure 라 silver 에서 rul_label = max(cycle) - cycle 로
--       자체 계산 가능하지만, test 는 partial trajectory 라 외부 ground truth 필수.
-- 입력: data/raw/RUL_FDxxx.txt — 행 번호 N = unit N 의 정답 RUL.
-- 로더: code/pipelines/load_rul_ground_truth.py (Spark, 일회성/재실행 안전)
-- 키:   (dataset_id, unit_id) — MERGE
-- 사용: gold_rul_predict --mode train 의 NASA 표준 평가
--       (`eval_split='nasa_test'` 행을 model_metrics 에 기록).
-- ─────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS phm.silver.rul_ground_truth;

CREATE TABLE phm.silver.rul_ground_truth (
    dataset_id  STRING      COMMENT 'FD001|FD002|FD003|FD004',
    unit_id     INT         COMMENT 'test_FDxxx.txt 의 unit_id',
    true_rul    INT         COMMENT 'test 의 마지막 cycle 시점 정답 RUL (NASA 제공)',
    loaded_ts   TIMESTAMP
)
USING iceberg
PARTITIONED BY (dataset_id)
TBLPROPERTIES (
    'format-version' = '2',
    'write.merge.mode' = 'copy-on-write'
);

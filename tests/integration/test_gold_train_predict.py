"""Iceberg 통합 테스트 — gold_rul_predict 의 학습/추론 분리.

검증:
  A. train → 모델 저장 + train/holdout 분리된 metrics 기록 + pipeline_state 갱신
  B. predict → 저장 모델 로드 → silver 변경분 추론 → rul_prediction MERGE
  C. predict 가 active 모델 없을 때 SystemExit (fail fast)
  D. predict 멱등 — 같은 silver snapshot 재실행 시 no-op
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark", reason="integration tests require pyspark")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

import gold_rul_predict as gp  # noqa: E402

pytestmark = pytest.mark.integration


# ─── 보조: 테이블 생성 ───

NORM_COLS = ", ".join(f"s{s}_norm DOUBLE" for s in gp.KEEP_SENSORS)


def _create_tables(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS test.gold")
    spark.sql("DROP TABLE IF EXISTS test.silver.engine_health")
    spark.sql("DROP TABLE IF EXISTS test.silver.rul_ground_truth")
    spark.sql("DROP TABLE IF EXISTS test.gold.rul_prediction")
    spark.sql("DROP TABLE IF EXISTS test.gold.model_metrics")
    spark.sql("DROP TABLE IF EXISTS test.gold.pipeline_state")

    spark.sql(f"""
        CREATE TABLE test.silver.engine_health (
            dataset_id STRING, unit_id INT, cycle INT,
            op_setting_1 DOUBLE, op_setting_2 DOUBLE, op_setting_3 DOUBLE,
            op_condition_cluster INT,
            {NORM_COLS},
            s_avg_w5 DOUBLE, s_std_w5 DOUBLE, s_trend_w5 DOUBLE,
            health_index DOUBLE, rul_label INT, is_test BOOLEAN,
            event_ts TIMESTAMP, ingest_ts TIMESTAMP, silver_ts TIMESTAMP,
            silver_version STRING
        )
        USING iceberg
        PARTITIONED BY (dataset_id, days(event_ts))
        TBLPROPERTIES ('format-version' = '2')
    """)

    spark.sql("""
        CREATE TABLE test.gold.rul_prediction (
            model_version STRING, dataset_id STRING, unit_id INT, cycle INT,
            rul_pred DOUBLE, rul_pred_lower DOUBLE, rul_pred_upper DOUBLE,
            rul_actual INT, abs_error DOUBLE, phm08_score DOUBLE,
            risk_tier STRING, predict_ts TIMESTAMP, silver_snapshot_id BIGINT
        )
        USING iceberg
        PARTITIONED BY (model_version, dataset_id)
        TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'copy-on-write')
    """)

    spark.sql("""
        CREATE TABLE test.gold.model_metrics (
            model_version STRING, dataset_id STRING,
            eval_window_start DATE, eval_window_end DATE,
            eval_split STRING,
            sample_count BIGINT, mae DOUBLE, rmse DOUBLE, phm08_score DOUBLE,
            pred_mean DOUBLE, pred_std DOUBLE, pred_p50 DOUBLE,
            silver_snapshot_id BIGINT, silver_row_count BIGINT,
            computed_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (model_version)
        TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'copy-on-write')
    """)

    spark.sql("""
        CREATE TABLE test.gold.pipeline_state (
            pipeline_name STRING,
            last_snapshot_id BIGINT, last_run_ts TIMESTAMP, rows_processed BIGINT,
            active_model_version STRING, active_model_path STRING,
            active_model_trained_ts TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'copy-on-write')
    """)

    spark.sql("""
        CREATE TABLE test.silver.rul_ground_truth (
            dataset_id STRING, unit_id INT, true_rul INT, loaded_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (dataset_id)
        TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'copy-on-write')
    """)


def _silver_schema():
    """PySpark 가 None-only 컬럼 (rul_label when is_test) 의 타입을 추론 못 함 →
    명시적 StructType."""
    fields = [
        StructField("dataset_id", StringType()),
        StructField("unit_id", IntegerType()),
        StructField("cycle", IntegerType()),
        StructField("op_setting_1", DoubleType()),
        StructField("op_setting_2", DoubleType()),
        StructField("op_setting_3", DoubleType()),
        StructField("op_condition_cluster", IntegerType()),
    ]
    fields += [StructField(f"s{s}_norm", DoubleType()) for s in gp.KEEP_SENSORS]
    fields += [
        StructField("s_avg_w5", DoubleType()),
        StructField("s_std_w5", DoubleType()),
        StructField("s_trend_w5", DoubleType()),
        StructField("health_index", DoubleType()),
        StructField("rul_label", IntegerType()),
        StructField("is_test", BooleanType()),
        StructField("event_ts", TimestampType()),
        StructField("ingest_ts", TimestampType()),
        StructField("silver_ts", TimestampType()),
        StructField("silver_version", StringType()),
    ]
    return StructType(fields)


def _seed_silver(spark, n_units=10, n_cycles=30, *, is_test=False, unit_offset=0,
                 rul_label_override=None):
    """unit_level_split 가 80/20 분할되도록 충분한 unit 수 + 학습 가능한 신호.

    is_test=True 면 rul_label=NULL (NASA test 시맨틱). unit_offset 으로 train/test 의
    unit_id 를 분리해 같은 silver 테이블에 mix 할 수 있음.
    """
    rows = []
    base_ts = datetime(2025, 8, 1, tzinfo=timezone.utc)
    now = datetime.now(tz=timezone.utc)
    for u in range(1 + unit_offset, n_units + 1 + unit_offset):
        max_c = n_cycles
        for c in range(1, n_cycles + 1):
            health = 1.0 - (max_c - c) / max_c
            rl = (None if is_test
                  else (rul_label_override if rul_label_override is not None
                        else max_c - c))
            tup = (
                "FD001", u, c,
                0.0, 0.0, 100.0,
                0,
                *(health * 0.5 + (s % 3) * 0.01 for s in gp.KEEP_SENSORS),
                health, 0.1, -0.05,
                health, rl, is_test,
                base_ts, base_ts, now, "v1",
            )
            rows.append(tup)
    df = spark.createDataFrame(rows, schema=_silver_schema())
    df.createOrReplaceTempView("_s")
    spark.sql("INSERT INTO test.silver.engine_health SELECT * FROM _s")


def _seed_rul_ground_truth(spark, units_with_rul: dict[int, int]):
    """test_FD001.txt 의 unit_id → 정답 RUL dict 를 silver.rul_ground_truth 에 적재."""
    rows = [("FD001", u, r) for u, r in units_with_rul.items()]
    df = (
        spark.createDataFrame(rows, schema="dataset_id STRING, unit_id INT, true_rul INT")
             .withColumn("loaded_ts", F.current_timestamp())
    )
    df.createOrReplaceTempView("_gt")
    spark.sql("INSERT INTO test.silver.rul_ground_truth SELECT * FROM _gt")


@pytest.fixture
def gold_env(iceberg_spark, monkeypatch, tmp_path):
    iceberg_spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints-it")
    _create_tables(iceberg_spark)
    monkeypatch.setattr(gp, "SILVER_TABLE", "test.silver.engine_health")
    monkeypatch.setattr(gp, "RUL_TABLE", "test.gold.rul_prediction")
    monkeypatch.setattr(gp, "METRICS_TABLE", "test.gold.model_metrics")
    monkeypatch.setattr(gp, "PIPELINE_STATE_TABLE", "test.gold.pipeline_state")
    monkeypatch.setattr(gp, "RUL_GROUND_TRUTH_TABLE", "test.silver.rul_ground_truth")
    # 모델 저장 경로 — Iceberg 와 별개의 로컬 파일 시스템
    monkeypatch.setattr(gp, "MODEL_BASE_PATH", f"file://{tmp_path}/models")
    yield iceberg_spark
    for t in ("test.silver.engine_health", "test.silver.rul_ground_truth",
              "test.gold.rul_prediction", "test.gold.model_metrics",
              "test.gold.pipeline_state"):
        iceberg_spark.sql(f"DROP TABLE IF EXISTS {t}")


def _args(mode, model_version="gbt-v0", rul_cap=130):
    class A:
        pass
    a = A()
    a.mode = mode
    a.model_version = model_version
    a.rul_cap = rul_cap
    a.max_iter = 5      # 빠른 테스트
    a.max_depth = 3
    return a


class TestGoldTrain:
    def test_train_writes_train_and_holdout_metrics(self, gold_env):
        spark = gold_env
        _seed_silver(spark, n_units=10, n_cycles=20)

        gp.run_train(spark, _args("train"))

        # train + holdout 행이 model_metrics 에 들어가야 함
        spark.sql("REFRESH TABLE test.gold.model_metrics")
        splits = (
            spark.sql("""
                SELECT eval_split FROM test.gold.model_metrics
                 WHERE model_version='gbt-v0'
            """).collect()
        )
        seen = {r["eval_split"] for r in splits}
        assert seen == {"train", "holdout"}, f"split 종류 {seen} 불완전"

    def test_train_saves_model_to_disk(self, gold_env, tmp_path):
        spark = gold_env
        _seed_silver(spark, n_units=10, n_cycles=20)

        model_path, sigma = gp.run_train(spark, _args("train"))

        # 파일 시스템에 PipelineModel 디렉토리가 생성됐는지
        local = model_path.replace("file://", "")
        import os
        assert os.path.isdir(local), f"모델 저장 디렉토리 없음: {local}"
        # PipelineModel 의 metadata 디렉토리 존재 확인
        assert os.path.isdir(os.path.join(local, "metadata")), \
            "PipelineModel metadata 디렉토리 없음"
        assert sigma >= 0.0

    def test_train_updates_pipeline_state(self, gold_env):
        spark = gold_env
        _seed_silver(spark, n_units=10, n_cycles=20)

        gp.run_train(spark, _args("train"))

        state = gp.read_pipeline_state(spark)
        assert state is not None
        assert state["active_model_version"] == "gbt-v0"
        assert state["active_model_path"] is not None
        assert state["active_model_trained_ts"] is not None


class TestNasaTestEval:
    """NASA C-MAPSS 표준 평가 — eval_split='nasa_test' 행이 model_metrics 에 들어가야 함."""

    def test_nasa_eval_emits_metrics_row_when_data_available(self, gold_env):
        """is_test silver + rul_ground_truth 가 모두 있으면 nasa_test 행 생성."""
        spark = gold_env
        # train trajectory: unit 1~10
        _seed_silver(spark, n_units=10, n_cycles=20, is_test=False)
        # test trajectory: unit 11~13 (rul_label NULL)
        _seed_silver(spark, n_units=3, n_cycles=15, is_test=True, unit_offset=10)
        # ground truth — test units 의 정답 RUL
        _seed_rul_ground_truth(spark, {11: 30, 12: 50, 13: 70})

        gp.run_train(spark, _args("train"))

        spark.sql("REFRESH TABLE test.gold.model_metrics")
        rows = spark.sql("""
            SELECT eval_split, sample_count
              FROM test.gold.model_metrics
             WHERE model_version='gbt-v0'
        """).collect()
        splits = {r["eval_split"] for r in rows}
        assert "nasa_test" in splits, f"nasa_test split 누락: {splits}"
        # 3 test unit × 1 dataset → sample_count=3 인 nasa_test 행 한 개
        nasa = next(r for r in rows if r["eval_split"] == "nasa_test")
        assert nasa["sample_count"] == 3

    def test_nasa_eval_skipped_without_test_rows(self, gold_env):
        """is_test silver 행이 없으면 nasa_test 행 미생성 (skip)."""
        spark = gold_env
        _seed_silver(spark, n_units=10, n_cycles=20, is_test=False)
        # ground truth 없음 + test 행도 없음

        gp.run_train(spark, _args("train"))

        spark.sql("REFRESH TABLE test.gold.model_metrics")
        rows = spark.sql("""
            SELECT eval_split FROM test.gold.model_metrics
             WHERE model_version='gbt-v0'
        """).collect()
        splits = {r["eval_split"] for r in rows}
        assert "nasa_test" not in splits, "test 데이터 없는데 nasa_test 행 생성됨"
        assert "train" in splits and "holdout" in splits

    def test_nasa_eval_skipped_without_ground_truth(self, gold_env):
        """is_test 행은 있지만 rul_ground_truth 가 비어 있으면 skip."""
        spark = gold_env
        _seed_silver(spark, n_units=10, n_cycles=20, is_test=False)
        _seed_silver(spark, n_units=3, n_cycles=15, is_test=True, unit_offset=10)
        # ground truth 의도적으로 비워둠

        gp.run_train(spark, _args("train"))

        spark.sql("REFRESH TABLE test.gold.model_metrics")
        rows = spark.sql("""
            SELECT eval_split FROM test.gold.model_metrics
             WHERE model_version='gbt-v0'
        """).collect()
        splits = {r["eval_split"] for r in rows}
        assert "nasa_test" not in splits


class TestGoldPredict:
    def test_predict_without_model_fails(self, gold_env):
        spark = gold_env
        _seed_silver(spark, n_units=5, n_cycles=10)
        # train 없이 predict — pipeline_state 비어 있음
        with pytest.raises(SystemExit):
            gp.run_predict(spark, _args("predict"))

    def test_full_then_predict_idempotent(self, gold_env):
        spark = gold_env
        _seed_silver(spark, n_units=10, n_cycles=20)

        gp.run_full(spark, _args("full"))
        spark.sql("REFRESH TABLE test.gold.rul_prediction")
        n1 = spark.table("test.gold.rul_prediction").count()
        assert n1 > 0, "full mode 후 rul_prediction 비어 있음"

        # 두 번째 predict — silver 변경 없으므로 no-op (rows_processed=0)
        gp.run_predict(spark, _args("predict"))
        spark.sql("REFRESH TABLE test.gold.rul_prediction")
        spark.sql("REFRESH TABLE test.gold.pipeline_state")
        n2 = spark.table("test.gold.rul_prediction").count()
        assert n2 == n1, f"멱등성 깨짐 — n1={n1}, n2={n2}"
        rows_proc = spark.sql(
            "SELECT rows_processed FROM test.gold.pipeline_state"
        ).collect()[0]["rows_processed"]
        assert rows_proc == 0, f"silver 변경 없는데 rows_processed={rows_proc}"

    def test_predict_uses_saved_model_not_retrain(self, gold_env):
        """predict 가 학습을 다시 하지 않음 — pipeline_state 의 모델 경로를 그대로 사용."""
        spark = gold_env
        _seed_silver(spark, n_units=10, n_cycles=20)

        gp.run_train(spark, _args("train"))
        path1 = gp.read_pipeline_state(spark)["active_model_path"]

        gp.run_predict(spark, _args("predict"))
        path2 = gp.read_pipeline_state(spark)["active_model_path"]
        # predict 는 같은 모델 경로를 유지 (재학습 X)
        assert path1 == path2, \
            f"predict 가 active_model_path 를 변경: {path1} → {path2}"

"""Spark 유닛 테스트 — silver_transform.py.

Iceberg 없이 로컬 Spark 만으로 핵심 변환을 검증:
  1) stable_cluster_ids: cluster id 가 op_setting_1 오름차순으로 재할당
  2) cluster_op_conditions: seed=42 + stable id → 같은 데이터 N회 → 동일 분포
  3) add_rolling_features: window=3 손계산값과 일치 (avg / stddev_samp / trend)
  4) add_health_index: 경계값 (z=0, |z|=3, |z|>3) 검증
  5) assign_cluster_from_centroids: fit 없이 centroids 만으로 cluster 부여 (incremental 정확성)
  6) apply_zscore_from_stats: feat_stats join 정규화 == inline z-score (full↔incremental 동치)
  7) add_rul_label: max(cycle) - cycle per (dataset_id, unit_id)
"""
from __future__ import annotations

import math

import pytest

pytest.importorskip("pyspark", reason="silver_transform tests require pyspark")

from pyspark.ml.linalg import Vectors
from pyspark.sql import functions as F

from silver_transform import (
    KEEP_SENSORS,
    add_health_index,
    add_is_test_flag,
    add_rolling_features,
    add_rul_label,
    apply_zscore_from_stats,
    apply_zscore_inline,
    assign_cluster_from_centroids,
    cluster_op_conditions,
    stable_cluster_ids,
)


# ──────────────── 1) stable_cluster_ids ────────────────


def _remap_collect(spark, centers, original_ids):
    """원본 cluster id 리스트를 stable_cluster_ids 통과시켜 {원본: 새} dict 반환."""
    df = spark.createDataFrame(
        [(i, oid) for i, oid in enumerate(original_ids)],
        ["row_id", "op_condition_cluster"],
    )
    out = stable_cluster_ids(centers, df, "op_condition_cluster") \
        .orderBy("row_id").collect()
    return {original_ids[r["row_id"]]: r["op_condition_cluster"] for r in out}


class TestStableClusterIds:
    def test_remaps_to_op_setting_1_ascending(self, spark):
        # 3 cluster: center op_setting_1 = [10, -5, 3]
        # 정렬: center[1](-5) < center[2](3) < center[0](10)
        # 새 ID: 1→0, 2→1, 0→2
        centers = [
            Vectors.dense(10.0, 0.0, 0.0),
            Vectors.dense(-5.0, 0.0, 0.0),
            Vectors.dense(3.0, 0.0, 0.0),
        ]
        remap = _remap_collect(spark, centers, [0, 1, 2])
        assert remap == {1: 0, 2: 1, 0: 2}

    def test_break_ties_by_op_setting_2_then_3(self, spark):
        # tie on op1=0 → op2: 1 < 5 → centers[1],[2] 앞
        # tie on (op1=0, op2=1) → op3: 2 < 9 → centers[2] 가 가장 앞
        # 정렬: 2, 1, 0
        centers = [
            Vectors.dense(0.0, 5.0, 0.0),
            Vectors.dense(0.0, 1.0, 9.0),
            Vectors.dense(0.0, 1.0, 2.0),
        ]
        remap = _remap_collect(spark, centers, [0, 1, 2])
        assert remap == {2: 0, 1: 1, 0: 2}

    def test_already_sorted_centers_identity(self, spark):
        centers = [
            Vectors.dense(0.0, 0.0, 0.0),
            Vectors.dense(1.0, 0.0, 0.0),
            Vectors.dense(2.0, 0.0, 0.0),
        ]
        remap = _remap_collect(spark, centers, [0, 1, 2])
        assert remap == {0: 0, 1: 1, 2: 2}


# ──────────────── 2) cluster_op_conditions: seed 안정성 ────────────────


def _make_bronze_df(spark, n_units: int = 4, n_cycles: int = 30):
    """FD002 풍 6 운영조건을 흉내내는 op_setting + 21 sensor 더미 데이터."""
    rows = []
    # 6 cluster center 후보 (op_setting_1, 2, 3)
    centers = [
        (0.0, 0.0, 100.0), (10.0, 0.25, 100.0), (20.0, 0.5, 100.0),
        (25.0, 0.62, 60.0), (35.0, 0.84, 60.0), (42.0, 0.84, 40.0),
    ]
    for u in range(1, n_units + 1):
        for c in range(1, n_cycles + 1):
            cluster = (u + c) % 6
            op1, op2, op3 = centers[cluster]
            # 약간의 노이즈
            noise = (c % 3 - 1) * 0.01
            row = {
                "dataset_id": "FD002",
                "unit_id": u,
                "cycle": c,
                "op_setting_1": op1 + noise,
                "op_setting_2": op2 + noise,
                "op_setting_3": op3,
            }
            for s in range(1, 22):
                row[f"sensor_{s}"] = 500.0 + s * 10 + (c * 0.1) + (cluster * 5)
            rows.append(row)
    return spark.createDataFrame(rows)


class TestClusterOpConditions:
    def test_seed_fixed_run_twice_same_assignment(self, spark):
        """README §3-2: seed=42 + stable_cluster_ids 조건 하 동일 입력 → 동일 cluster 분포."""
        df = _make_bronze_df(spark)
        out1, _ = cluster_op_conditions(df, k=6, seed=42)
        out2, _ = cluster_op_conditions(df, k=6, seed=42)

        a = sorted([(r["unit_id"], r["cycle"], r["op_condition_cluster"])
                    for r in out1.collect()])
        b = sorted([(r["unit_id"], r["cycle"], r["op_condition_cluster"])
                    for r in out2.collect()])
        assert a == b, "seed=42 + stable_cluster_ids 인데 두 실행 결과가 다름"

    def test_cluster_ids_in_range(self, spark):
        df = _make_bronze_df(spark)
        out, _ = cluster_op_conditions(df, k=6, seed=42)
        ids = {r["op_condition_cluster"] for r in out.collect()}
        assert ids.issubset(set(range(6)))
        # 6 cluster 가 다 잡혀야 함 (제대로 분리됐다면)
        assert len(ids) == 6

    def test_cluster_means_are_sorted_by_op1(self, spark):
        """stable_cluster_ids 의 약속: cluster id 순서로 op_setting_1 평균이 단조증가."""
        df = _make_bronze_df(spark)
        out, _ = cluster_op_conditions(df, k=6, seed=42)
        means = (
            out.groupBy("op_condition_cluster")
               .agg(F.avg("op_setting_1").alias("m"))
               .orderBy("op_condition_cluster")
               .collect()
        )
        op1_means = [r["m"] for r in means]
        assert op1_means == sorted(op1_means), \
            f"cluster id 가 op_setting_1 오름차순이 아님: {op1_means}"


# ──────────────── 3) add_rolling_features ────────────────


class TestAddRollingFeatures:
    def test_window_3_against_handcalc(self, spark):
        # 단일 unit, _s_row_mean = norm 컬럼 평균. 1개 norm 컬럼으로 단순화.
        rows = [
            ("FD001", 1, 1, 1.0),
            ("FD001", 1, 2, 3.0),
            ("FD001", 1, 3, 5.0),
            ("FD001", 1, 4, 7.0),
        ]
        df = spark.createDataFrame(rows, ["dataset_id", "unit_id", "cycle", "x_norm"])
        out = add_rolling_features(df, ["x_norm"], window=3)
        out_rows = {r["cycle"]: r for r in out.collect()}

        # cycle 1: window=[1.0]            → avg=1, std=NaN(샘플 1개), trend=(1-1)/2=0
        # cycle 2: window=[1,3]            → avg=2, std=stddev_samp([1,3])=sqrt(2)≈1.414, trend=(3-1)/2=1
        # cycle 3: window=[1,3,5]          → avg=3, std=2, trend=(5-1)/2=2
        # cycle 4: window=[3,5,7]          → avg=5, std=2, trend=(7-3)/2=2
        assert out_rows[1]["s_avg_w5"] == pytest.approx(1.0)
        assert out_rows[2]["s_avg_w5"] == pytest.approx(2.0)
        assert out_rows[3]["s_avg_w5"] == pytest.approx(3.0)
        assert out_rows[4]["s_avg_w5"] == pytest.approx(5.0)

        assert out_rows[2]["s_std_w5"] == pytest.approx(math.sqrt(2.0))
        assert out_rows[3]["s_std_w5"] == pytest.approx(2.0)
        assert out_rows[4]["s_std_w5"] == pytest.approx(2.0)

        assert out_rows[1]["s_trend_w5"] == pytest.approx(0.0)
        assert out_rows[2]["s_trend_w5"] == pytest.approx(1.0)
        assert out_rows[3]["s_trend_w5"] == pytest.approx(2.0)
        assert out_rows[4]["s_trend_w5"] == pytest.approx(2.0)

    def test_window_isolated_per_engine(self, spark):
        """engine timeline 분할: 다른 unit_id 의 값이 rolling 에 섞이면 안 됨."""
        rows = [
            ("FD001", 1, 1, 100.0),
            ("FD001", 1, 2, 100.0),
            ("FD001", 2, 1, 1.0),  # unit 2 — unit 1 의 100 이 끼면 안 됨
            ("FD001", 2, 2, 1.0),
        ]
        df = spark.createDataFrame(rows, ["dataset_id", "unit_id", "cycle", "x_norm"])
        out = add_rolling_features(df, ["x_norm"], window=3).collect()
        for r in out:
            if r["unit_id"] == 2:
                assert r["s_avg_w5"] == pytest.approx(1.0)


# ──────────────── 4) add_health_index ────────────────


class TestAddHealthIndex:
    @pytest.mark.parametrize(
        "z,expected_hi",
        [
            (0.0, 1.0),     # cluster 평균 → 정상
            (1.5, 0.5),     # |z|/3 = 0.5
            (-1.5, 0.5),    # 절댓값
            (3.0, 0.0),     # 경계 → 완전 열화
            (5.0, 0.0),     # clip: |z|>3 도 0 으로
            (-100.0, 0.0),
        ],
    )
    def test_boundary_values(self, spark, z, expected_hi):
        df = spark.createDataFrame([(z,)], ["s_avg_w5"])
        out = add_health_index(df).collect()
        assert out[0]["health_index"] == pytest.approx(expected_hi)

    def test_in_range_0_to_1(self, spark):
        rows = [(float(z) / 10,) for z in range(-100, 101)]  # -10..10 step 0.1
        df = spark.createDataFrame(rows, ["s_avg_w5"])
        out = add_health_index(df).collect()
        for r in out:
            assert 0.0 <= r["health_index"] <= 1.0


# ──────────────── 5) assign_cluster_from_centroids (incremental 모드용) ────────────────


class TestAssignClusterFromCentroids:
    """fit 없이 기존 centroids 만으로 cluster 부여. 증분 모드의 핵심 정확성 보장."""

    def test_matches_kmeans_transform(self, spark):
        """cluster_op_conditions(fit) 와 동일한 입력 + 동일 centroids → 동일 cluster id.

        이게 깨지면 incremental 모드의 결과가 full 모드와 달라진다.
        """
        df = _make_bronze_df(spark, n_units=4, n_cycles=30)
        fitted, sorted_centers = cluster_op_conditions(df, k=6, seed=42)
        assigned = assign_cluster_from_centroids(df, sorted_centers)

        a = sorted([(r["unit_id"], r["cycle"], r["op_condition_cluster"])
                    for r in fitted.collect()])
        b = sorted([(r["unit_id"], r["cycle"], r["op_condition_cluster"])
                    for r in assigned.collect()])
        assert a == b, "centroids 재사용 cluster 부여가 KMeans transform 과 다름"

    def test_argmin_picks_nearest(self, spark):
        """수동으로 만든 점이 가장 가까운 cluster 를 받는지."""
        # cluster 0 center=(0,0,0), cluster 1=(10,0,0), cluster 2=(0,10,0)
        sorted_centers = [(0.0, 0.0, 0.0), (10.0, 0.0, 0.0), (0.0, 10.0, 0.0)]
        # (1,0,0) → cluster 0;  (9,0,0) → cluster 1;  (0,9,0) → cluster 2
        rows = [
            ("FD001", 1, 1, 1.0, 0.0, 0.0),
            ("FD001", 1, 2, 9.0, 0.0, 0.0),
            ("FD001", 1, 3, 0.0, 9.0, 0.0),
        ]
        df = spark.createDataFrame(
            rows,
            ["dataset_id", "unit_id", "cycle",
             "op_setting_1", "op_setting_2", "op_setting_3"],
        )
        out = {r["cycle"]: r["op_condition_cluster"]
               for r in assign_cluster_from_centroids(df, sorted_centers).collect()}
        assert out == {1: 0, 2: 1, 3: 2}

    def test_tie_picks_lowest_id(self, spark):
        """동률 시 cluster_id 작은 쪽이 우선 — incremental 결정성 보장."""
        # 두 centroid 가 (5,0,0) 에서 등거리: (0,0,0) 과 (10,0,0)
        sorted_centers = [(0.0, 0.0, 0.0), (10.0, 0.0, 0.0)]
        df = spark.createDataFrame(
            [("FD001", 1, 1, 5.0, 0.0, 0.0)],
            ["dataset_id", "unit_id", "cycle",
             "op_setting_1", "op_setting_2", "op_setting_3"],
        )
        out = assign_cluster_from_centroids(df, sorted_centers).collect()
        assert out[0]["op_condition_cluster"] == 0


# ──────────────── 6) apply_zscore_from_stats (broadcast join 정규화) ────────────────


class TestApplyZscoreFromStats:
    """incremental 모드는 feat_stats 를 join 으로 적용. 결과가 inline z-score 와 일치해야 함."""

    def _build_stats_df(self, spark, df_with_cluster):
        """현재 배치에서 inline 통계를 계산해 feat_stats 같은 wide 형태로 만든다."""
        from pyspark.sql import functions as F
        agg = [F.count(F.lit(1)).alias("n_samples")]
        for s in KEEP_SENSORS:
            agg += [F.avg(f"sensor_{s}").alias(f"sensor_{s}_mean"),
                    F.stddev_pop(f"sensor_{s}").alias(f"sensor_{s}_std")]
        return (
            df_with_cluster.groupBy("dataset_id", "op_condition_cluster")
                           .agg(*agg)
                           .withColumnRenamed("op_condition_cluster", "cluster_id")
        )

    def test_matches_inline_zscore(self, spark):
        """feat_stats 적용 결과 == 현재 배치 inline 통계 적용 결과 (full ↔ incremental 동치성)."""
        df = _make_bronze_df(spark, n_units=4, n_cycles=20)
        clustered, _centers = cluster_op_conditions(df, k=6, seed=42)

        # full 경로: 자기 자신의 통계로
        inline = apply_zscore_inline(clustered)

        # incremental 경로: 통계를 별도 df 로 만들어 broadcast join
        stats = self._build_stats_df(spark, clustered)
        from_stats = apply_zscore_from_stats(clustered, stats)

        # 두 결과의 norm 컬럼들이 동일해야 함
        norm_cols = [f"s{s}_norm" for s in KEEP_SENSORS]
        a = inline.select("unit_id", "cycle", *norm_cols).orderBy("unit_id", "cycle").collect()
        b = from_stats.select("unit_id", "cycle", *norm_cols).orderBy("unit_id", "cycle").collect()
        assert len(a) == len(b)
        for ra, rb in zip(a, b):
            assert ra["unit_id"] == rb["unit_id"]
            assert ra["cycle"] == rb["cycle"]
            for col in norm_cols:
                assert ra[col] == pytest.approx(rb[col], rel=1e-9, abs=1e-9), \
                    f"({ra['unit_id']},{ra['cycle']}) {col}: inline={ra[col]} stats={rb[col]}"

    def test_zero_std_falls_back_to_zero(self, spark):
        """sensor_X_std=0 인 (cluster, dataset) 은 norm=0 — 분모 0 회피."""
        from pyspark.sql import Row
        # bronze 1행 (n_samples=1 → stddev_pop=0)
        rows = [{
            "dataset_id": "FD001", "unit_id": 1, "cycle": 1,
            "op_setting_1": 0.0, "op_setting_2": 0.0, "op_setting_3": 0.0,
            "op_condition_cluster": 0,
            **{f"sensor_{s}": 100.0 for s in KEEP_SENSORS},
        }]
        df = spark.createDataFrame([Row(**r) for r in rows])
        stats = self._build_stats_df(spark, df)
        out = apply_zscore_from_stats(df, stats).collect()
        for s in KEEP_SENSORS:
            assert out[0][f"s{s}_norm"] == 0.0


# ──────────────── 7) add_is_test_flag (NASA test 분류) ────────────────


class TestAddIsTestFlag:
    def test_train_source_file_is_false(self, spark):
        df = spark.createDataFrame(
            [("train_FD001.txt",), ("train_FD002.txt",)],
            ["source_file"],
        )
        out = {r["source_file"]: r["is_test"] for r in add_is_test_flag(df).collect()}
        assert out == {"train_FD001.txt": False, "train_FD002.txt": False}

    def test_test_source_file_is_true(self, spark):
        df = spark.createDataFrame(
            [("test_FD001.txt",), ("test_FD004.txt",)],
            ["source_file"],
        )
        out = {r["source_file"]: r["is_test"] for r in add_is_test_flag(df).collect()}
        assert out == {"test_FD001.txt": True, "test_FD004.txt": True}

    def test_missing_source_file_column_falls_back_to_false(self, spark):
        """source_file 컬럼이 없는 df 는 안전망으로 is_test=False (모두 train 으로 가정)."""
        df = spark.createDataFrame([(1,)], ["unit_id"])
        out = add_is_test_flag(df).collect()
        assert out[0]["is_test"] is False


# ──────────────── 8) add_rul_label ────────────────


class TestAddRulLabel:
    def test_max_cycle_minus_cycle_per_engine(self, spark):
        """train trajectory (is_test=False): rul_label = max(cycle) - cycle."""
        from pyspark.sql import functions as F
        rows = [
            ("FD001", 1, 1), ("FD001", 1, 2), ("FD001", 1, 3),
            ("FD001", 2, 1), ("FD001", 2, 2),  # unit 2 의 max 는 2
            ("FD002", 1, 1), ("FD002", 1, 5),  # 다른 dataset → 분리
        ]
        df = (
            spark.createDataFrame(rows, ["dataset_id", "unit_id", "cycle"])
                 .withColumn("is_test", F.lit(False))
        )
        out = {(r["dataset_id"], r["unit_id"], r["cycle"]): r["rul_label"]
               for r in add_rul_label(df).collect()}
        # FD001 unit 1: max=3 → rul = 2,1,0
        assert out[("FD001", 1, 1)] == 2
        assert out[("FD001", 1, 2)] == 1
        assert out[("FD001", 1, 3)] == 0
        # FD001 unit 2: max=2 → 1, 0
        assert out[("FD001", 2, 1)] == 1
        assert out[("FD001", 2, 2)] == 0
        # FD002 unit 1: max=5 → 4, 0
        assert out[("FD002", 1, 1)] == 4
        assert out[("FD002", 1, 5)] == 0

    def test_test_trajectory_rul_label_is_null(self, spark):
        """test trajectory (is_test=True): rul_label = NULL (정답은 ground truth 별도)."""
        from pyspark.sql import functions as F
        rows = [("FD001", 1, 1, True), ("FD001", 1, 2, True)]
        df = spark.createDataFrame(rows, ["dataset_id", "unit_id", "cycle", "is_test"])
        out = add_rul_label(df).collect()
        for r in out:
            assert r["rul_label"] is None, \
                f"is_test=True 인데 rul_label={r['rul_label']}"

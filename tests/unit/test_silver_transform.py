"""Spark 유닛 테스트 — silver_transform.py.

Iceberg 없이 로컬 Spark 만으로 핵심 변환 4종을 검증:
  1) stable_cluster_ids: cluster id 가 op_setting_1 오름차순으로 재할당
  2) cluster_op_conditions: seed=42 + stable id → 같은 데이터 N회 → 동일 분포
  3) add_rolling_features: window=3 손계산값과 일치 (avg / stddev_samp / trend)
  4) add_health_index: 경계값 (z=0, |z|=3, |z|>3) 검증
  5) add_rul_label: max(cycle) - cycle per (dataset_id, unit_id)
"""
from __future__ import annotations

import math

import pytest

pytest.importorskip("pyspark", reason="silver_transform tests require pyspark")

from pyspark.ml.linalg import Vectors
from pyspark.sql import functions as F

from silver_transform import (
    add_health_index,
    add_rolling_features,
    add_rul_label,
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


# ──────────────── 5) add_rul_label ────────────────


class TestAddRulLabel:
    def test_max_cycle_minus_cycle_per_engine(self, spark):
        rows = [
            ("FD001", 1, 1), ("FD001", 1, 2), ("FD001", 1, 3),
            ("FD001", 2, 1), ("FD001", 2, 2),  # unit 2 의 max 는 2
            ("FD002", 1, 1), ("FD002", 1, 5),  # 다른 dataset → 분리
        ]
        df = spark.createDataFrame(rows, ["dataset_id", "unit_id", "cycle"])
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

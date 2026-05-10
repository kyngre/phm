"""Unit tests — gold_rul_predict.unit_level_split.

핵심 회귀: split 이 row-level 이 아닌 *unit-level* 이어야 같은 엔진의 cycle 들이
같은 split 으로 묶여 데이터 누수 없음.
"""
from __future__ import annotations

import pytest

pytest.importorskip("pyspark", reason="gold tests require pyspark")

from pyspark.sql import functions as F  # noqa: E402

from gold_rul_predict import unit_level_split  # noqa: E402


def _silver_like_df(spark, n_units: int = 20, n_cycles: int = 30, datasets=("FD001",)):
    """unit_level_split 호출에 필요한 최소 컬럼 — dataset_id, unit_id (+ 1개 더미)."""
    rows = []
    for ds in datasets:
        for u in range(1, n_units + 1):
            for c in range(1, n_cycles + 1):
                rows.append((ds, u, c))
    return spark.createDataFrame(rows, ["dataset_id", "unit_id", "cycle"])


class TestUnitLevelSplit:
    def test_unit_appears_in_only_one_split(self, spark):
        """같은 (dataset, unit) 의 모든 cycle 은 *한 split* 에만."""
        df = _silver_like_df(spark, n_units=50, n_cycles=20)
        train, holdout, _, _ = unit_level_split(df, train_ratio=0.8, seed=42)

        train_units = {(r["dataset_id"], r["unit_id"])
                       for r in train.select("dataset_id", "unit_id").distinct().collect()}
        holdout_units = {(r["dataset_id"], r["unit_id"])
                         for r in holdout.select("dataset_id", "unit_id").distinct().collect()}

        overlap = train_units & holdout_units
        assert overlap == set(), f"unit 누수: {overlap}"

    def test_split_ratio_close_to_target(self, spark):
        """80/20 비율이 ±10% 이내 (작은 표본 hash 분포 분산)."""
        df = _silver_like_df(spark, n_units=100, n_cycles=10)
        _, _, n_train, n_holdout = unit_level_split(df, train_ratio=0.8, seed=42)
        ratio = n_train / (n_train + n_holdout)
        assert 0.7 <= ratio <= 0.9, f"split ratio {ratio:.2f} 가 0.7~0.9 범위 밖"

    def test_seed_deterministic(self, spark):
        """같은 seed → 같은 split (재현성)."""
        df = _silver_like_df(spark, n_units=30)
        t1, h1, _, _ = unit_level_split(df, train_ratio=0.8, seed=42)
        t2, h2, _, _ = unit_level_split(df, train_ratio=0.8, seed=42)
        u1 = sorted([(r["dataset_id"], r["unit_id"])
                     for r in t1.select("dataset_id", "unit_id").distinct().collect()])
        u2 = sorted([(r["dataset_id"], r["unit_id"])
                     for r in t2.select("dataset_id", "unit_id").distinct().collect()])
        assert u1 == u2

    def test_no_rows_lost(self, spark):
        """train + holdout 행 수 = 원본 행 수."""
        df = _silver_like_df(spark, n_units=20, n_cycles=15)
        original = df.count()
        train, holdout, _, _ = unit_level_split(df, train_ratio=0.8, seed=42)
        assert train.count() + holdout.count() == original

    def test_all_datasets_represented_in_both_splits(self, spark):
        """4 dataset 각각이 train/holdout 양쪽에 등장 — stratify 보장."""
        df = _silver_like_df(spark, n_units=30, n_cycles=5,
                             datasets=("FD001", "FD002", "FD003", "FD004"))
        train, holdout, _, _ = unit_level_split(df, train_ratio=0.8, seed=42)
        ds_in_train = {r["dataset_id"] for r in train.select("dataset_id").distinct().collect()}
        ds_in_holdout = {r["dataset_id"] for r in holdout.select("dataset_id").distinct().collect()}
        assert ds_in_train == {"FD001", "FD002", "FD003", "FD004"}
        assert ds_in_holdout == {"FD001", "FD002", "FD003", "FD004"}

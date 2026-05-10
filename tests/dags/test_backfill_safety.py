"""백필 안전성 — README §8-2 의 90일 백필 시나리오와 maintenance 정책 충돌 방지.

정책 (2026-05-10 갱신):
  iceberg_expire_dag.OLDER_THAN_DAYS = 100  ← 학습 윈도우 90d + 마진 10d
  DDL `history.expire.max-snapshot-age-ms` = 8_640_000_000 ms = 100d
  README §8-2: `--base-date $(date -v-90d +%F)` 로 90일 전 백필
  → 마진 10일 — 백필 도중 expire 가 돌아도 학습 데이터 snapshot 보호.

이 테스트는 "현재 정책이 무엇인지" 를 코드로 박아두는 가드레일이다. 정책을 바꾸려면
테스트도 같이 갱신해야 하므로 README/실제 동작/테스트 셋이 동기화된 상태로 유지된다.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

# README §8-2 에 박힌 백필 윈도우 (cmaps_to_kafka --base-date $(date -v-90d +%F))
BACKFILL_WINDOW_DAYS = 90

# Iceberg DDL (02_bronze_engine_sensor_raw.sql, 03_silver_engine_health.sql) 의
# 'history.expire.max-snapshot-age-ms' = 8_640_000_000 = 100일.
# 이 두 값은 같은 의도를 표현하므로 테스트로 동기화 강제.
TABLE_MAX_SNAPSHOT_AGE_DAYS = 100

# 백필 시작 후 expire 가 돌기 전까지 최소 보장해야 할 마진. 운영 SLA — 줄이려면 합의 필요.
MIN_SAFETY_MARGIN_DAYS = 7


class TestExpirePolicy:
    """iceberg_expire_dag 의 build_cmd / OLDER_THAN_DAYS / 테이블 커버리지."""

    def test_older_than_at_least_backfill_window(self):
        from iceberg_expire_dag import OLDER_THAN_DAYS  # type: ignore

        assert OLDER_THAN_DAYS >= BACKFILL_WINDOW_DAYS, (
            f"expire OLDER_THAN_DAYS={OLDER_THAN_DAYS} < backfill {BACKFILL_WINDOW_DAYS}d. "
            f"README §8-2 백필 도중 학습 윈도우 데이터의 snapshot 이 expire 될 수 있음."
        )

    def test_safety_margin_at_least_one_week(self):
        """expire 윈도우 - 백필 윈도우 ≥ 7d. 마진 0 이었던 과거 회귀 방지."""
        from iceberg_expire_dag import OLDER_THAN_DAYS  # type: ignore

        margin = OLDER_THAN_DAYS - BACKFILL_WINDOW_DAYS
        assert margin >= MIN_SAFETY_MARGIN_DAYS, (
            f"expire margin = {margin}d < {MIN_SAFETY_MARGIN_DAYS}d. "
            f"백필 90d 직후 expire 가 돌면 학습 윈도우 snapshot 이 날아갈 수 있음."
        )

    def test_table_ddl_max_snapshot_age_matches(self):
        """DDL 의 max-snapshot-age 가 expire DAG 정책과 일치해야 한다.
        이 둘이 어긋나면 어떤 정책이 진짜인지 모호해짐."""
        from iceberg_expire_dag import OLDER_THAN_DAYS  # type: ignore

        assert OLDER_THAN_DAYS == TABLE_MAX_SNAPSHOT_AGE_DAYS, (
            f"expire DAG OLDER_THAN_DAYS={OLDER_THAN_DAYS} 와 DDL max-snapshot-age "
            f"{TABLE_MAX_SNAPSHOT_AGE_DAYS}d 가 다름 — 두 곳 모두 갱신 필요."
        )

    def test_all_5_tables_covered(self):
        from iceberg_expire_dag import TABLES  # type: ignore

        names = {t for t, _ in TABLES}
        expected = {
            "bronze.engine_sensor_raw",
            "silver.engine_health",
            "gold.rul_prediction",
            "gold.fleet_kpi_daily",
            "gold.model_metrics",
        }
        assert names == expected, f"expire 누락: {expected - names}, 추가: {names - expected}"

    def test_retain_last_differentiated(self):
        """Bronze/Silver/Gold-rul: retain 20, fleet_kpi/metrics: retain 10.
        무조건 최소 한 자릿수가 아닌 두 자릿수 — 롤백 여유 보장."""
        from iceberg_expire_dag import TABLES  # type: ignore

        retain = dict(TABLES)
        assert retain["bronze.engine_sensor_raw"] == 20
        assert retain["silver.engine_health"] == 20
        assert retain["gold.rul_prediction"] == 20
        assert retain["gold.fleet_kpi_daily"] == 10
        assert retain["gold.model_metrics"] == 10
        for tbl, n in retain.items():
            assert n >= 10, f"{tbl} retain_last={n} 가 10 미만 — 롤백 여유 부족"

    def test_build_cmd_emits_5_calls_with_dynamic_cutoff(self):
        from iceberg_expire_dag import OLDER_THAN_DAYS, build_cmd  # type: ignore

        cmd = build_cmd()
        # 5개 테이블 → 5개 expire_snapshots CALL
        assert cmd.count("expire_snapshots") == 5
        # 동적 cutoff: NOW − OLDER_THAN_DAYS 가 들어가야 함 (DAG 상수와 동기화)
        expected_date = (datetime.utcnow() - timedelta(days=OLDER_THAN_DAYS)).strftime("%Y-%m-%d")
        assert expected_date in cmd, (
            f"build_cmd 에 동적 cutoff {expected_date} 미포함 — 정적으로 박혔을 가능성"
        )
        # phm-spark 컨테이너에서 spark-sql 로 실행되는지
        assert "phm-spark" in cmd
        assert "spark-sql" in cmd


class TestOrphanCleanupPolicy:
    """iceberg_orphan_cleanup_dag — older_than 7d, 진행 중 write 보호."""

    def test_older_than_at_least_one_day(self):
        from iceberg_orphan_cleanup_dag import OLDER_THAN_DAYS  # type: ignore

        assert OLDER_THAN_DAYS >= 1, (
            "orphan cleanup older_than < 1d — 진행 중인 streaming/MERGE write 가 "
            "orphan 으로 잡혀 데이터 손상 가능"
        )

    def test_orphan_safety_buffer(self):
        """7일 마진 — README §5 #4 snapshot 증가율 모니터링 + Spark job 최장 실행시간."""
        from iceberg_orphan_cleanup_dag import OLDER_THAN_DAYS  # type: ignore

        assert OLDER_THAN_DAYS >= 7, (
            f"orphan older_than={OLDER_THAN_DAYS}d < 7d — 백필 도중 cleanup 충돌 위험"
        )

    def test_all_5_tables_covered(self):
        from iceberg_orphan_cleanup_dag import TABLES  # type: ignore

        assert set(TABLES) == {
            "bronze.engine_sensor_raw",
            "silver.engine_health",
            "gold.rul_prediction",
            "gold.fleet_kpi_daily",
            "gold.model_metrics",
        }

    def test_dry_run_disabled_in_production(self):
        """orphan_cleanup 의 SQL 템플릿이 dry_run=false 인지 확인.

        dry_run=true 였다가 그대로 운영에 반영되면 GC 가 실제로 안 돈다 — 침묵 실패."""
        from iceberg_orphan_cleanup_dag import ORPHAN_SQL  # type: ignore

        assert "dry_run => false" in ORPHAN_SQL, (
            "remove_orphan_files 가 dry_run=true 로 묶여 있음 — 운영에서 실제로 GC 안 돌음"
        )

    def test_build_cmd_emits_5_calls_with_dynamic_cutoff(self):
        from iceberg_orphan_cleanup_dag import build_cmd  # type: ignore

        cmd = build_cmd()
        assert cmd.count("remove_orphan_files") == 5
        expected_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
        assert expected_date in cmd

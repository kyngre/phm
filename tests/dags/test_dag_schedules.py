"""스케줄 정합성 — README §2/§5 와 cron 표현식 일치 + DAG 간 시간순서.

운영 합의:
  silver_merge(:00) → gold_rul_predict(:15)            : 매시간 15분 갭
  iceberg_compaction(03:00) → iceberg_expire(04:00)    : 컴팩션 후 1시간 뒤 expire
  ingest_streaming(매 5분): bronze streaming 헬스체크
  health_check(매시 :30): KPI/drift 감시
  gold_kpi(00:30 일배치)
  iceberg_orphan_cleanup(일요일 05:00)
"""
from __future__ import annotations

import pytest

from conftest import EXPECTED_DAGS  # type: ignore[import-not-found]

# parametrize 는 collection 시점에 평가되므로 모듈 상수 사용 (fixture 불가)
SCHEDULE_PARAMS = [(d, m["schedule"]) for d, m in EXPECTED_DAGS.items()]
TAG_PARAMS = [(d, m["tags"]) for d, m in EXPECTED_DAGS.items()]


class TestSchedules:
    @pytest.mark.parametrize("dag_id,expected_cron", SCHEDULE_PARAMS)
    def test_schedule_matches_readme(self, dag_bag, dag_id, expected_cron):
        dag = dag_bag.get_dag(dag_id)
        # Airflow 2.x: schedule_interval (deprecated) / schedule_attr
        actual = getattr(dag, "schedule_interval", None) or dag.timetable.summary
        assert actual == expected_cron, (
            f"{dag_id} schedule={actual} (expected {expected_cron}). "
            f"README §2/§5 와 동기화 필요."
        )

    @pytest.mark.parametrize("dag_id,expected_tags", TAG_PARAMS)
    def test_tags_match_readme(self, dag_bag, dag_id, expected_tags):
        dag = dag_bag.get_dag(dag_id)
        assert sorted(dag.tags) == sorted(expected_tags), (
            f"{dag_id} tags={dag.tags} (expected {expected_tags})"
        )


class TestPipelineOrdering:
    """DAG 간 시간 의존성 — 같은 시각이 아니라 분 단위 갭으로 직렬화."""

    def test_silver_runs_before_gold_rul(self, dag_bag):
        """silver(:00) → gold_rul(:15) — 시그널 단계로 묶지 않고 cron 갭으로 분리."""
        silver = EXPECTED_DAGS["silver_merge_dag"]["schedule"]
        gold = EXPECTED_DAGS["gold_rul_predict_dag"]["schedule"]
        # 둘 다 매시간; gold 의 minute 가 silver 보다 커야 함
        silver_min = int(silver.split()[0]) if silver.split()[0].isdigit() else 0
        gold_min = int(gold.split()[0])
        assert silver_min < gold_min, (
            f"silver_merge(min={silver_min}) >= gold_rul(min={gold_min}) — 순서 역전"
        )
        # 최소 5분 갭 (Spark job 평균 실행시간 여유)
        assert gold_min - silver_min >= 5

    def test_compaction_runs_before_expire(self, dag_bag):
        """compaction(03:00) → expire(04:00) — expire 가 막 컴팩션된 snapshot 까지 잡지 않게.

        README §5 #4 의 "snapshot 증가율" 모니터링이 의미 있으려면 compaction 직후
        expire 가 너무 가까이 붙으면 안 됨.
        """
        comp = EXPECTED_DAGS["iceberg_compaction_dag"]["schedule"]
        expire = EXPECTED_DAGS["iceberg_expire_dag"]["schedule"]
        # 0 3 * * *  vs  0 4 * * *
        comp_hour = int(comp.split()[1])
        expire_hour = int(expire.split()[1])
        assert expire_hour > comp_hour, "expire 가 compaction 보다 먼저 실행됨"
        assert expire_hour - comp_hour >= 1, "compaction → expire 갭이 1시간 미만"

    def test_streaming_check_short_interval(self, dag_bag):
        """ingest_streaming 은 5분 간격 — bronze streaming 죽으면 빨리 감지."""
        cron = EXPECTED_DAGS["ingest_streaming_dag"]["schedule"]
        assert cron.startswith("*/"), f"ingest_streaming cron={cron} 분-간격 표현 아님"
        interval = int(cron.split()[0].lstrip("*/"))
        assert interval <= 5, f"streaming 헬스체크 간격 {interval}분 — SLA 초과"

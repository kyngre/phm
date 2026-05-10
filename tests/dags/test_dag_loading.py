"""Airflow 가장 흔한 회귀 — DAG 파싱 무결성 + 구조 약속."""
from __future__ import annotations

import pytest

# parametrize 는 collection 시점에 평가되므로 모듈 상수로 EXPECTED_DAGS 를 가져온다.
# (이것만 fixture 로 못 바꿈 — pytest.mark.parametrize 의 한계)
from conftest import EXPECTED_DAGS  # type: ignore[import-not-found]

DAG_IDS = sorted(EXPECTED_DAGS.keys())


class TestDagParsing:
    def test_no_import_errors(self, dag_bag):
        """import_errors 가 비어있어야 함. 하나라도 깨지면 그 파일 상세를 출력."""
        if dag_bag.import_errors:
            details = "\n".join(f"  {f}: {e}" for f, e in dag_bag.import_errors.items())
            pytest.fail(f"DAG import errors:\n{details}")

    def test_all_expected_dags_present(self, dag_bag, expected_dags):
        loaded = set(dag_bag.dag_ids)
        expected = set(expected_dags.keys())
        missing = expected - loaded
        extra = loaded - expected
        assert not missing, f"누락된 DAG: {missing}"
        assert not extra, (
            f"등록 안 된 신규 DAG: {extra}. EXPECTED_DAGS 와 README 를 갱신했는지 확인."
        )

    def test_dag_id_matches_filename(self, dag_bag):
        """dag_id 는 파일명(.py 제외)과 같아야 한다 — 추적성."""
        for dag_id, dag in dag_bag.dags.items():
            fname = dag.fileloc.rsplit("/", 1)[-1].removesuffix(".py")
            assert dag_id == fname, (
                f"dag_id={dag_id} vs filename={fname} 불일치 ({dag.fileloc})"
            )


class TestDagStructure:
    @pytest.mark.parametrize("dag_id", DAG_IDS)
    def test_default_args_retries(self, dag_bag, dag_id):
        """모든 DAG: retries=2, depends_on_past=False (재실행 안전)."""
        dag = dag_bag.get_dag(dag_id)
        assert dag.default_args["retries"] == 2, f"{dag_id} retries 가 2 가 아님"
        assert dag.default_args["depends_on_past"] is False, (
            f"{dag_id} depends_on_past=True — backfill 시 데드락 위험"
        )

    @pytest.mark.parametrize("dag_id", DAG_IDS)
    def test_catchup_disabled(self, dag_bag, dag_id):
        """catchup=False — 실행 안 된 과거 schedule 은 자동 백필하지 않음 (의도)."""
        dag = dag_bag.get_dag(dag_id)
        assert dag.catchup is False, (
            f"{dag_id} catchup=True — 정지 후 재기동 시 폭주 위험"
        )

    @pytest.mark.parametrize("dag_id", DAG_IDS)
    def test_has_at_least_one_task(self, dag_bag, dag_id):
        dag = dag_bag.get_dag(dag_id)
        assert len(dag.tasks) >= 1

    def test_no_task_id_collisions(self, dag_bag):
        for dag_id, dag in dag_bag.dags.items():
            ids = [t.task_id for t in dag.tasks]
            assert len(ids) == len(set(ids)), f"{dag_id} task_id 중복: {ids}"


class TestDagOwnership:
    @pytest.mark.parametrize("dag_id", DAG_IDS)
    def test_owner_set_to_phm(self, dag_bag, dag_id):
        """오너 'phm' 일관 — Slack alert routing/감사 추적용 약속."""
        dag = dag_bag.get_dag(dag_id)
        assert dag.default_args["owner"] == "phm", f"{dag_id} owner != 'phm'"

    @pytest.mark.parametrize("dag_id", DAG_IDS)
    def test_has_tags(self, dag_bag, dag_id):
        dag = dag_bag.get_dag(dag_id)
        assert dag.tags, f"{dag_id} tags 비어있음 — UI 필터링 불가"

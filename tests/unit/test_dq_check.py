"""Unit tests — dq_check.DqResult.status 임계 로직.

DataFrame 가 필요한 rule 함수는 통합 테스트 (test_dq_check_e2e.py) 에서 다루고,
여기서는 임계 비교 로직만 빠르게 검증.
"""
from __future__ import annotations

import pytest

from dq_check import DqResult


def _r(measured, threshold, op):
    return DqResult(
        rule_name="t", layer="bronze", dataset_id="FD001",
        measured_value=measured, threshold=threshold, operator=op,
        sample_size=100, description="-",
    )


class TestDqResultStatus:
    @pytest.mark.parametrize(
        "measured,threshold,op,expected",
        [
            (0.0,   0.0, "<=", "PASS"),
            (0.0,   0.0, "<",  "WARN"),    # 0 < 0 거짓 — warn 영역이 [0,0] 이라 경계 WARN
            (0.5,   1.0, "<=", "PASS"),
            (1.0,   1.0, "<=", "PASS"),
            (1.4,   1.0, "<=", "WARN"),    # 1.4 ≤ 1.5 (warn_factor=1.5)
            (1.5,   1.0, "<=", "WARN"),    # 경계 (1.5 == 1.5)
            (1.6,   1.0, "<=", "FAIL"),
            (5.0,   5.0, "==", "PASS"),
            (5.0001, 5.0, "==", "FAIL"),
        ],
    )
    def test_status_matrix(self, measured, threshold, op, expected):
        assert _r(measured, threshold, op).status() == expected

    def test_zero_threshold_eq(self):
        """count 형 rule (threshold=0, op='<=') — 1 건만 있어도 WARN/FAIL."""
        assert _r(0.0, 0.0, "<=").status() == "PASS"
        # measured=1 vs threshold=0 → 1 <= 0*1.5 = 0 거짓 → FAIL (WARN 영역 없음)
        assert _r(1.0, 0.0, "<=").status() == "FAIL"

    def test_unknown_operator_raises(self):
        with pytest.raises(ValueError):
            _r(1.0, 2.0, "?").status()

    def test_warn_factor_override(self):
        r = _r(1.4, 1.0, "<=")
        assert r.status(warn_factor=1.0) == "FAIL"   # warn 구간 없음
        assert r.status(warn_factor=2.0) == "WARN"   # 더 너그러움

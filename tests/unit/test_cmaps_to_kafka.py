"""순수 Python 유닛 테스트 — cmaps_to_kafka.py.

검증 포인트:
  - parse_interval: 시뮬레이션 시간 단위 파싱 (s/m/h/d) 및 잘못된 입력 거부
  - parse_line: C-MAPSS 26 컬럼 텍스트 → dict 변환 (불완전 라인은 None)
  - synth_event_ts: README §0-2 의 합성 공식 정확성
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

import pytest

from cmaps_to_kafka import parse_interval, parse_line, synth_event_ts


class TestParseInterval:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("30s", timedelta(seconds=30)),
            ("5m", timedelta(minutes=5)),
            ("1h", timedelta(hours=1)),
            ("1d", timedelta(days=1)),
            ("12h", timedelta(hours=12)),
            ("  2d  ", timedelta(days=2)),  # whitespace trim
        ],
    )
    def test_valid(self, raw, expected):
        assert parse_interval(raw) == expected

    @pytest.mark.parametrize("raw", ["", "1", "h", "1x", "abc", "-1h", "1.5h"])
    def test_invalid_raises(self, raw):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_interval(raw)


class TestParseLine:
    def _make_line(self, n_cols: int = 26) -> str:
        # unit_id=1, cycle=1, op_setting_1~3=0.5, sensor_1~21=10.0...
        vals = ["1", "1", "0.5", "0.5", "0.5"] + ["10.0"] * 21
        return " ".join(vals[:n_cols])

    def test_full_line_parsed(self):
        rec = parse_line(self._make_line())
        assert rec is not None
        assert rec["unit_id"] == 1
        assert rec["cycle"] == 1
        assert rec["op_setting_1"] == 0.5
        assert rec["sensor_1"] == 10.0
        assert rec["sensor_21"] == 10.0
        # 26 컬럼: unit, cycle, 3 op, 21 sensor
        assert len(rec) == 1 + 1 + 3 + 21

    def test_short_line_returns_none(self):
        assert parse_line(self._make_line(n_cols=20)) is None
        assert parse_line("") is None
        assert parse_line("\n") is None

    def test_extra_whitespace_ok(self):
        line = "  1  2  0.1  0.2  0.3 " + " ".join(["1.0"] * 21)
        rec = parse_line(line)
        assert rec["unit_id"] == 1
        assert rec["cycle"] == 2

    def test_types(self):
        rec = parse_line(self._make_line())
        assert isinstance(rec["unit_id"], int)
        assert isinstance(rec["cycle"], int)
        assert isinstance(rec["op_setting_1"], float)
        assert isinstance(rec["sensor_1"], float)


class TestSynthEventTs:
    """README §0-2 공식: event_ts = base + unit_jitter*unit_id + interval*(cycle-1)"""

    BASE = datetime(2025, 8, 1, tzinfo=timezone.utc)

    def test_first_unit_first_cycle(self):
        # README 예시: FD001 unit=1, cycle=1, jitter=1h → 2025-08-01T01:00
        ts = synth_event_ts(
            self.BASE, unit_id=1, cycle=1,
            interval=timedelta(hours=1), unit_jitter=timedelta(hours=1),
        )
        assert ts == "2025-08-01T01:00:00.000Z"

    def test_readme_example_unit5_cycle10(self):
        # README §0-2 예시 그대로: 2025-08-01 + 1h*5 + 1h*9 = 2025-08-01T14:00
        ts = synth_event_ts(
            self.BASE, unit_id=5, cycle=10,
            interval=timedelta(hours=1), unit_jitter=timedelta(hours=1),
        )
        assert ts == "2025-08-01T14:00:00.000Z"

    def test_zero_jitter_synchronizes_units(self):
        # README §0-2 대안: unit-jitter=0s → 모든 unit 의 cycle 1 이 동시각
        ts1 = synth_event_ts(self.BASE, 1, 1, timedelta(hours=1), timedelta(0))
        ts99 = synth_event_ts(self.BASE, 99, 1, timedelta(hours=1), timedelta(0))
        assert ts1 == ts99 == "2025-08-01T00:00:00.000Z"

    def test_iso_format_with_z(self):
        # Spark from_json TimestampType 가 안정 파싱하는 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\''
        ts = synth_event_ts(self.BASE, 1, 1, timedelta(minutes=5), timedelta(0))
        assert ts.endswith("Z")
        assert "." in ts  # millisecond
        # 핵심: dateutil 등 외부 의존성 없이 strict 비교
        assert ts == "2025-08-01T00:00:00.000Z"

    def test_dense_interval_5m(self):
        # README 시나리오: interval=5m (100x 고밀도)
        ts = synth_event_ts(
            self.BASE, unit_id=0, cycle=13,
            interval=timedelta(minutes=5), unit_jitter=timedelta(0),
        )
        assert ts == "2025-08-01T01:00:00.000Z"  # 12 cycle × 5m = 60m

"""Unit tests — load_rul_ground_truth.parse_rul_file.

NASA C-MAPSS RUL_FDxxx.txt 형식: 행 단위, 행 N (1-base) = unit N 의 정답 RUL.
"""
from __future__ import annotations

import pytest

from load_rul_ground_truth import parse_rul_file


class TestParseRulFile:
    def test_simple_int_per_line(self, tmp_path):
        f = tmp_path / "RUL_FD001.txt"
        f.write_text("100\n50\n200\n")
        rows = parse_rul_file(f, "FD001")
        # line N → unit_id N
        assert rows == [
            ("FD001", 1, 100),
            ("FD001", 2, 50),
            ("FD001", 3, 200),
        ]

    def test_float_values_truncated_to_int(self, tmp_path):
        """RUL_FDxxx.txt 의 일부 dataset 은 부동소수 형식 (예: '112.0')."""
        f = tmp_path / "RUL_FD002.txt"
        f.write_text("112.0\n98.0\n")
        rows = parse_rul_file(f, "FD002")
        assert rows == [("FD002", 1, 112), ("FD002", 2, 98)]

    def test_blank_lines_ignored(self, tmp_path):
        """빈 줄/whitespace 행은 line_no 카운트에서 제외 (skip)."""
        f = tmp_path / "RUL_FD001.txt"
        f.write_text("100\n\n200\n   \n300\n")
        rows = parse_rul_file(f, "FD001")
        # 빈 줄은 unit_id 카운트에 포함되지 않음 — 100, 200, 300 → unit 1, 3, 5
        assert [r[1] for r in rows] == [1, 3, 5]
        assert [r[2] for r in rows] == [100, 200, 300]

    def test_invalid_value_raises(self, tmp_path):
        f = tmp_path / "RUL_FD001.txt"
        f.write_text("100\nabc\n")
        with pytest.raises(ValueError, match="비정수"):
            parse_rul_file(f, "FD001")

    def test_dataset_id_threaded_through(self, tmp_path):
        f = tmp_path / "RUL_FD003.txt"
        f.write_text("42\n")
        rows = parse_rul_file(f, "FD003")
        assert rows[0][0] == "FD003"

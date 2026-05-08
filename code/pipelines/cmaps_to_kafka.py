"""
C-MAPSS txt → Kafka 시뮬레이터 (Time-Travel Simulation).

각 행(= 1 cycle = 1 비행)을 JSON 메시지로 Kafka 토픽 `phm.engine.sensor` 에 발행.
unit/cycle 순서를 유지하면서 발행 페이싱은 --cycle-interval/--speedup 으로 조절.

▶ event_ts (event-time) 합성 — Iceberg 의 시계열 가치(time-travel, partition pruning)를
   실제로 입증하기 위한 핵심 변경.

   event_ts = base_date  +  unit_id × unit_jitter  +  (cycle − 1) × interval

   - base-date  : 시뮬 기준 시각 (default 2025-08-01, UTC)
   - interval   : 1 cycle 의 시간 폭 (default 1h — C-MAPSS cycle ≈ 1 비행)
   - unit-jitter: 엔진별 출발 시점 분산 (default 1h — fleet 동시성 흉내)

실행 예:
  python /workspace/code/pipelines/cmaps_to_kafka.py \
      --datasets FD001,FD002,FD003,FD004 \
      --base-date 2025-08-01 --interval 1h --unit-jitter 1h \
      --speedup 1000
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA_DIR = Path(os.environ.get("CMAPS_DATA_DIR", "/workspace/data/raw"))
TOPIC = os.environ.get("KAFKA_TOPIC", "phm.engine.sensor")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

COLS = (
    ["unit_id", "cycle", "op_setting_1", "op_setting_2", "op_setting_3"]
    + [f"sensor_{i}" for i in range(1, 22)]
)

_INTERVAL_RE = re.compile(r"^(\d+)([smhd])$")
_UNIT_KW = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}


def parse_interval(s: str) -> timedelta:
    m = _INTERVAL_RE.match(s.strip())
    if not m:
        raise argparse.ArgumentTypeError(
            f"interval '{s}' 형식 오류. 예: 30s, 5m, 1h, 1d"
        )
    n, u = int(m.group(1)), m.group(2)
    return timedelta(**{_UNIT_KW[u]: n})


def parse_line(line: str) -> dict | None:
    parts = line.strip().split()
    if len(parts) < 26:
        return None
    vals = parts[:26]
    rec = {}
    rec["unit_id"] = int(vals[0])
    rec["cycle"] = int(vals[1])
    for k, v in zip(COLS[2:], vals[2:]):
        rec[k] = float(v)
    return rec


def iter_dataset(dataset_id: str, max_units: int | None):
    src = DATA_DIR / f"train_{dataset_id}.txt"
    if not src.exists():
        raise FileNotFoundError(src)
    with src.open() as f:
        for line_no, line in enumerate(f, start=1):
            rec = parse_line(line)
            if rec is None:
                continue
            if max_units and rec["unit_id"] > max_units:
                continue
            rec["dataset_id"] = dataset_id
            rec["source_file"] = src.name
            rec["line_no"] = line_no
            yield rec


def synth_event_ts(base: datetime, unit_id: int, cycle: int,
                   interval: timedelta, unit_jitter: timedelta) -> str:
    ts = base + unit_jitter * unit_id + interval * (cycle - 1)
    # ISO with millisecond Z — Spark from_json 의 TimestampType 가 안정적으로 파싱.
    return ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datasets", default="FD001,FD002,FD003,FD004")
    ap.add_argument("--max-units", type=int, default=None,
                    help="dataset 당 최대 unit 수 (시연용)")

    # ── time-travel 시뮬 인자 ──
    ap.add_argument("--base-date", default="2025-08-01",
                    help="event_ts 기준 시각 (ISO date, UTC). 동일 base-date 면 데이터 재현 가능.")
    ap.add_argument("--interval", type=parse_interval, default=parse_interval("1h"),
                    help="1 cycle 의 시간 폭 (예: 30s, 5m, 1h, 1d). default 1h")
    ap.add_argument("--unit-jitter", type=parse_interval, default=parse_interval("1h"),
                    help="unit_id 별 출발 시점 분산 (default 1h → fleet 흉내)")

    # ── 발행 페이싱 (event_ts 와 무관, sleep 만 담당) ──
    ap.add_argument("--cycle-interval", type=float, default=60.0,
                    help="발행 페이싱: cycle 간 실시간 간격(초). event_ts 와 무관.")
    ap.add_argument("--speedup", type=float, default=600.0,
                    help="발행 가속 배수. 기본 600 → 1 cycle = 0.1s")

    ap.add_argument("--bootstrap", default=BOOTSTRAP)
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    base = datetime.fromisoformat(args.base_date).replace(tzinfo=timezone.utc)
    sleep_per_cycle = args.cycle_interval / args.speedup
    print(f"[sim] base={base.isoformat()} interval={args.interval} "
          f"unit_jitter={args.unit_jitter} sleep_per_cycle={sleep_per_cycle:.4f}s",
          flush=True)

    if args.dry_run:
        producer = None
    else:
        try:
            from kafka import KafkaProducer
        except ImportError:
            print("kafka-python 미설치. 다음 실행 후 재시도:")
            print("  docker exec -u root phm-spark pip install kafka-python")
            sys.exit(2)
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            linger_ms=50,
            acks=1,
        )

    datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
    total = 0
    t0 = time.time()
    iters = [iter(iter_dataset(d, args.max_units)) for d in datasets]
    active = list(range(len(iters)))
    min_ts, max_ts = None, None
    while active:
        for idx in list(active):
            try:
                rec = next(iters[idx])
            except StopIteration:
                active.remove(idx)
                continue
            rec["event_ts"] = synth_event_ts(
                base, rec["unit_id"], rec["cycle"],
                args.interval, args.unit_jitter,
            )
            min_ts = rec["event_ts"] if (min_ts is None or rec["event_ts"] < min_ts) else min_ts
            max_ts = rec["event_ts"] if (max_ts is None or rec["event_ts"] > max_ts) else max_ts
            key = f"{rec['dataset_id']}:{rec['unit_id']}"
            if producer:
                producer.send(args.topic, key=key, value=rec)
            total += 1
            if total % 5000 == 0:
                elapsed = time.time() - t0
                print(f"[{elapsed:6.1f}s] published {total} records "
                      f"(active datasets: {len(active)}, last event_ts={rec['event_ts']})",
                      flush=True)
        if sleep_per_cycle > 0:
            time.sleep(sleep_per_cycle)

    if producer:
        producer.flush()
        producer.close()
    print(f"DONE. total={total} elapsed={time.time()-t0:.1f}s "
          f"event_ts span: [{min_ts}, {max_ts}]")


if __name__ == "__main__":
    main()

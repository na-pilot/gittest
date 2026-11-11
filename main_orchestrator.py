"""10분 간격으로 채널을 수집하고 소싱 전략을 저장한다."""

from __future__ import annotations

import argparse
import errno
import math
import os
import signal
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

from database import KST, RadarDatabase, utc_now
from sub_agents import (
    CHANNEL_WEIGHTS,
    DEFAULT_KRW_PER_RMB,
    DEFAULT_PLATFORM_FEE_PCT,
    ChannelReport,
    MultiChannelTrendScout,
    SourcingStrategistAgent,
)


DEFAULT_INTERVAL_SECONDS = 600


class SchedulerAlreadyRunning(RuntimeError):
    pass


@contextmanager
def scheduler_lock(db_path: Path):
    """OS releases the lock when the scheduler process exits, including on a crash."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = db_path.with_name(db_path.name + ".lock")
    with lock_path.open("a+b") as handle:
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                raise SchedulerAlreadyRunning(f"Another scheduler is using {db_path}") from exc
            raise
        yield


class RadarOrchestrator:
    def __init__(
        self,
        database: RadarDatabase,
        interval: float = DEFAULT_INTERVAL_SECONDS,
        scout: MultiChannelTrendScout | None = None,
        strategist: SourcingStrategistAgent | None = None,
        *,
        origin: str = "simulation",
    ):
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("Collection interval must be positive and finite.")
        self.database = database
        self.interval = interval
        self.scout = scout if scout is not None else MultiChannelTrendScout()
        self.strategist = strategist if strategist is not None else SourcingStrategistAgent()
        self.origin = origin
        self.stop_event = threading.Event()
        self.cycle = 0
        self.started_at: datetime | None = None
        self.last_saved_at: datetime | None = None
        self.last_error: str | None = None

    def _publish_schedule(self, status: str, next_run_at: datetime | None = None) -> None:
        try:
            self.database.set_schedule(
                status, self.cycle, self.interval,
                started_at=self.started_at, last_saved_at=self.last_saved_at,
                next_run_at=next_run_at, last_error=self.last_error,
            )
        except Exception as exc:
            print(f"  [WARN] Schedule write failed: {exc}", file=sys.stderr, flush=True)

    @staticmethod
    def _print_channel(report: ChannelReport) -> None:
        channel_number = tuple(CHANNEL_WEIGHTS).index(report.channel) + 1
        average = f"{report.avg_score:.1f}/100" if report.avg_score is not None else "-"
        print(
            f"  [{channel_number}/4] {report.channel:<10} {report.status.upper():<7} "
            f"{report.item_count:>2}/{report.expected_count:<2} SKUs    avg {average:<8} "
            f"{report.duration_ms:>4} ms",
            flush=True,
        )

    def _run_cycle(self, started_at: datetime) -> None:
        print(f"\n[{started_at.astimezone(KST):%Y-%m-%d %H:%M:%S} KST] Cycle {self.cycle:04d}", flush=True)
        signals, reports = self.scout.collect(started_at, on_channel=self._print_channel)
        plans = [self.strategist.plan(signal) for signal in signals]
        print(f"  [PLAN] {len(plans)} sourcing strategies prepared", flush=True)
        batch_id = self.database.save_batch(started_at, signals, plans, reports, origin=self.origin)
        self.last_saved_at = utc_now()
        print(f"  [SAVE] Batch {batch_id}  |  {len(signals)} trend scores  |  {len(reports)} channel logs", flush=True)
        if not signals:
            self.last_error = "종합 지수를 계산할 수 있는 SKU가 없습니다. 채널 수집 로그를 확인하세요."

    def run(self, *, once: bool = False) -> int:
        with scheduler_lock(self.database.path):
            self.database.initialize(seed=self.origin == "simulation")
            print(
                f"SOURCING RADAR  |  {self.origin.upper()}  |  {self.interval:g}s interval\n"
                f"SQLite: {self.database.path}",
                flush=True,
            )
            try:
                while not self.stop_event.is_set():
                    started_monotonic = time.monotonic()
                    started_at = utc_now()
                    self.started_at = started_at
                    self.cycle += 1
                    self.last_error = None
                    self._publish_schedule("collecting")
                    try:
                        self._run_cycle(started_at)
                    except Exception as exc:
                        self.last_error = f"{type(exc).__name__}: {exc}"[:400]
                        print(f"  [ERROR] {self.last_error}", file=sys.stderr, flush=True)

                    if once:
                        return 1 if self.last_error else 0
                    if self.stop_event.is_set():
                        break

                    # Keep the start-to-start cadence and skip slots missed by a slow provider.
                    elapsed = time.monotonic() - started_monotonic
                    slots = int(elapsed // self.interval) + 1
                    deadline = started_monotonic + slots * self.interval
                    next_run = utc_now() + timedelta(seconds=max(0, deadline - time.monotonic()))
                    self._publish_schedule("error" if self.last_error else "waiting", next_run)
                    print(f"  [NEXT] {next_run.astimezone(KST):%H:%M:%S} KST", flush=True)
                    # Short waits let Ctrl+C interrupt the idle period on Windows.
                    while not self.stop_event.is_set():
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        self.stop_event.wait(min(remaining, 1.0))
            finally:
                self._publish_schedule("offline")
                print("\nSourcing Radar stopped.", flush=True)
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Sourcing Radar scheduler")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SECONDS, help="batch interval in seconds (default: 600)")
    parser.add_argument("--once", action="store_true", help="run one batch and exit")
    parser.add_argument("--db", type=Path, help="use a different SQLite file")
    parser.add_argument("--krw-per-rmb", type=float, default=DEFAULT_KRW_PER_RMB, help="assumed KRW per RMB (default: 200)")
    parser.add_argument("--platform-fee-pct", type=float, default=DEFAULT_PLATFORM_FEE_PCT, help="assumed fee as percent of sale (default: 12)")
    args = parser.parse_args()
    try:
        strategist = SourcingStrategistAgent(args.krw_per_rmb, args.platform_fee_pct)
        radar = RadarOrchestrator(RadarDatabase(args.db), args.interval, strategist=strategist)
    except ValueError as exc:
        parser.error(str(exc))

    def stop(_signum, _frame):
        radar.stop_event.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        return radar.run(once=args.once)
    except (SchedulerAlreadyRunning, OSError, sqlite3.Error) as exc:
        print(f"[STARTUP ERROR] {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""SQLite 저장소와 한국시간 기준 조회."""

from __future__ import annotations

import os
import sqlite3
import time
from collections.abc import Sequence
from contextlib import closing
from dataclasses import asdict, fields
from datetime import datetime, time as wall_time, timedelta, timezone
from pathlib import Path

from sub_agents import (
    CHANNEL_WEIGHTS,
    ChannelReport,
    MultiChannelTrendScout,
    SourcingPlan,
    SourcingStrategistAgent,
    TrendSignal,
)


UTC = timezone.utc
KST = timezone(timedelta(hours=9), "KST")
DEFAULT_DB_PATH = Path(__file__).resolve().with_name("sourcing_radar.db")
SCORE_COLUMNS = ", ".join(f"{channel}_score" for channel in CHANNEL_WEIGHTS)
SCORE_AVERAGES = ", ".join(f"AVG(t.{channel}_score) AS {channel}_score" for channel in CHANNEL_WEIGHTS)

SCHEMA = """
CREATE TABLE IF NOT EXISTS radar_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sourcing_items (
    sku TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    kind TEXT NOT NULL,
    pain_point TEXT NOT NULL,
    selling_point TEXT NOT NULL,
    search_zh TEXT NOT NULL,
    search_en TEXT NOT NULL,
    spec TEXT NOT NULL,
    sample_check TEXT NOT NULL,
    unit_min_rmb REAL NOT NULL,
    unit_max_rmb REAL NOT NULL,
    unit_min_krw INTEGER NOT NULL,
    unit_max_krw INTEGER NOT NULL,
    logistics_krw INTEGER NOT NULL,
    platform_fee_pct REAL NOT NULL,
    krw_per_rmb REAL NOT NULL,
    target_price_krw INTEGER NOT NULL,
    margin_pct REAL NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS collection_batches (
    id INTEGER PRIMARY KEY,
    observed_at TEXT NOT NULL,
    origin TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_time ON collection_batches(observed_at);

CREATE TABLE IF NOT EXISTS trend_scores (
    batch_id INTEGER NOT NULL REFERENCES collection_batches(id),
    sku TEXT NOT NULL REFERENCES sourcing_items(sku),
    trend_index REAL NOT NULL CHECK (trend_index BETWEEN 0 AND 100),
    naver_score REAL CHECK (naver_score BETWEEN 0 AND 100),
    youtube_score REAL CHECK (youtube_score BETWEEN 0 AND 100),
    instagram_score REAL CHECK (instagram_score BETWEEN 0 AND 100),
    google_score REAL CHECK (google_score BETWEEN 0 AND 100),
    PRIMARY KEY (batch_id, sku)
);
CREATE INDEX IF NOT EXISTS idx_scores_sku ON trend_scores(sku, batch_id);

CREATE TABLE IF NOT EXISTS channel_logs (
    batch_id INTEGER NOT NULL REFERENCES collection_batches(id),
    channel TEXT NOT NULL,
    status TEXT NOT NULL,
    item_count INTEGER NOT NULL,
    expected_count INTEGER NOT NULL,
    avg_score REAL,
    duration_ms INTEGER NOT NULL,
    detail TEXT NOT NULL,
    PRIMARY KEY (batch_id, channel)
);

CREATE TABLE IF NOT EXISTS scheduler_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    status TEXT NOT NULL,
    cycle INTEGER NOT NULL,
    interval_seconds REAL NOT NULL,
    started_at TEXT,
    last_saved_at TEXT,
    next_run_at TEXT,
    last_error TEXT
);
"""

PLAN_COLUMNS = tuple(field.name for field in fields(SourcingPlan)) + ("updated_at",)
UPSERT_PLAN = f"""
INSERT INTO sourcing_items ({", ".join(PLAN_COLUMNS)})
VALUES ({", ".join(":" + column for column in PLAN_COLUMNS)})
ON CONFLICT(sku) DO UPDATE SET
    {", ".join(f"{column} = excluded.{column}" for column in PLAN_COLUMNS if column != "sku")}
WHERE excluded.updated_at >= sourcing_items.updated_at
"""


def resolve_db_path(path: str | Path | None = None) -> Path:
    return Path(path or os.environ.get("RADAR_DB_PATH") or DEFAULT_DB_PATH).expanduser().resolve()


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_stamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Radar timestamps must be timezone-aware.")
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def period_bounds(days: int, now: datetime) -> tuple[str, str]:
    """Include today and the previous days-1 KST dates up to the query time."""
    if days not in (1, 7, 30):
        raise ValueError("Select a 1, 7, or 30 day window.")
    until = utc_stamp(now)
    first_date = now.astimezone(KST).date() - timedelta(days=days - 1)
    first = datetime.combine(first_date, wall_time.min, tzinfo=KST)
    return utc_stamp(first), until


class RadarDatabase:
    def __init__(self, path: str | Path | None = None):
        self.path = resolve_db_path(path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def initialize(self, *, seed: bool = True, now: datetime | None = None) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as conn, conn:
            # PRAGMA journal_mode may report BUSY immediately when both processes open a new DB.
            for attempt in range(4):
                try:
                    conn.execute("PRAGMA journal_mode = WAL").fetchone()
                    break
                except sqlite3.OperationalError as exc:
                    if "locked" not in str(exc).lower() or attempt == 3:
                        raise
                    time.sleep(0.15 * (attempt + 1))
            conn.executescript("BEGIN IMMEDIATE;\n" + SCHEMA)
            seeded = conn.execute("SELECT 1 FROM radar_metadata WHERE key = 'initial_seed'").fetchone()
            if seed and not seeded:
                empty = conn.execute("SELECT 1 FROM collection_batches LIMIT 1").fetchone() is None
                if empty:
                    self._seed(conn, now or utc_now())
                conn.execute(
                    "INSERT INTO radar_metadata(key, value) VALUES ('initial_seed', ?)",
                    ("created" if empty else "existing_data",),
                )

    @staticmethod
    def _seed(conn: sqlite3.Connection, now: datetime) -> None:
        start, _ = period_bounds(30, now)
        first = datetime.fromisoformat(start)
        moments = [first + timedelta(hours=hour) for hour in range(0, 30 * 24, 4)]
        moments = [moment for moment in moments if moment <= now]
        current = now.astimezone(UTC)
        latest = current.replace(minute=current.minute // 10 * 10, second=0, microsecond=0)
        if latest > moments[-1]:
            moments.append(latest)

        scout = MultiChannelTrendScout()
        strategist = SourcingStrategistAgent()
        initial_signals, _ = scout.collect(moments[0])
        plans = [strategist.plan(signal) for signal in initial_signals]
        RadarDatabase._upsert_plans(conn, utc_stamp(moments[-1]), plans)
        for moment in moments:
            signals, reports = scout.collect(moment)
            RadarDatabase._insert_batch(conn, utc_stamp(moment), "seed", signals, reports)

    @staticmethod
    def _upsert_plans(conn: sqlite3.Connection, stamp: str, plans: Sequence[SourcingPlan]) -> None:
        conn.executemany(UPSERT_PLAN, ({**asdict(plan), "updated_at": stamp} for plan in plans))

    @staticmethod
    def _insert_batch(
        conn: sqlite3.Connection,
        stamp: str,
        origin: str,
        signals: Sequence[TrendSignal],
        reports: Sequence[ChannelReport],
    ) -> int:
        batch_id = conn.execute(
            "INSERT INTO collection_batches(observed_at, origin) VALUES (?, ?)", (stamp, origin),
        ).lastrowid
        conn.executemany(
            f"INSERT INTO trend_scores(batch_id, sku, trend_index, {SCORE_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                (batch_id, signal.candidate.sku, signal.trend_index,
                 *(signal.channel_scores.get(channel) for channel in CHANNEL_WEIGHTS))
                for signal in signals
            ),
        )
        conn.executemany(
            """INSERT INTO channel_logs(
                   batch_id, channel, status, item_count, expected_count, avg_score, duration_ms, detail
               ) VALUES (:batch_id, :channel, :status, :item_count, :expected_count, :avg_score, :duration_ms, :detail)""",
            ({"batch_id": batch_id, **asdict(report)} for report in reports),
        )
        return batch_id

    def save_batch(
        self,
        observed_at: datetime,
        signals: Sequence[TrendSignal],
        plans: Sequence[SourcingPlan],
        reports: Sequence[ChannelReport],
        *,
        origin: str = "simulation",
    ) -> int:
        stamp = utc_stamp(observed_at)
        if {plan.sku for plan in plans} != {signal.candidate.sku for signal in signals}:
            raise ValueError("Every observed SKU needs a matching sourcing plan.")
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            self._upsert_plans(conn, stamp, plans)
            return self._insert_batch(conn, stamp, origin, signals, reports)

    def set_schedule(
        self,
        status: str,
        cycle: int,
        interval_seconds: float,
        *,
        started_at: datetime | None,
        last_saved_at: datetime | None,
        next_run_at: datetime | None,
        last_error: str | None,
    ) -> None:
        with closing(self._connect()) as conn, conn:
            conn.execute(
                """INSERT INTO scheduler_state (
                       id, status, cycle, interval_seconds, started_at, last_saved_at, next_run_at, last_error
                   ) VALUES (1, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                       status = excluded.status, cycle = excluded.cycle, interval_seconds = excluded.interval_seconds,
                       started_at = excluded.started_at, last_saved_at = excluded.last_saved_at,
                       next_run_at = excluded.next_run_at, last_error = excluded.last_error""",
                (status, cycle, interval_seconds, utc_stamp(started_at) if started_at else None,
                 utc_stamp(last_saved_at) if last_saved_at else None, utc_stamp(next_run_at) if next_run_at else None,
                 last_error),
            )

    def get_schedule(self) -> dict | None:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT * FROM scheduler_state WHERE id = 1").fetchone()
            return dict(row) if row is not None else None

    def summary(self, days: int, category: str | None, *, now: datetime | None = None) -> dict:
        now = now or utc_now()
        start, end = period_bounds(days, now)
        today, _ = period_bounds(1, now)
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN")
            totals = conn.execute(
                """SELECT COUNT(*) AS sku_count, AVG(i.margin_pct) AS avg_margin
                   FROM sourcing_items i
                   WHERE (? IS NULL OR i.category = ?) AND EXISTS (
                       SELECT 1 FROM trend_scores t JOIN collection_batches b ON b.id = t.batch_id
                       WHERE t.sku = i.sku AND b.observed_at BETWEEN ? AND ?
                   )""",
                (category, category, start, end),
            ).fetchone()
            channels = conn.execute(
                f"""SELECT {SCORE_AVERAGES}
                    FROM collection_batches b
                    JOIN trend_scores t ON t.batch_id = b.id
                    JOIN sourcing_items i ON i.sku = t.sku
                    WHERE b.observed_at BETWEEN ? AND ? AND (? IS NULL OR i.category = ?)""",
                (today, end, category, category),
            ).fetchone()
        available = {
            channel: channels[f"{channel}_score"] for channel in CHANNEL_WEIGHTS
            if channels[f"{channel}_score"] is not None
        }
        leader = max(available, key=available.get) if available else None
        return {**dict(totals), "leader": leader, "leader_score": available.get(leader)}

    @staticmethod
    def _series(conn: sqlite3.Connection, days: int, category: str | None, now: datetime) -> list[dict]:
        start, end = period_bounds(days, now)
        local = "substr(b.observed_at, 1, 19), '+9 hours'"
        # 오늘은 10분 구간으로, 7/30일은 KST 달력 날짜로 그룹화한다.
        bucket = (
            f"strftime('%Y-%m-%dT%H:', {local}) || "
            f"printf('%02d:00', CAST(strftime('%M', {local}) AS INTEGER) / 10 * 10)"
            if days == 1 else f"date({local})"
        )
        rows = conn.execute(
            f"""SELECT {bucket} AS bucket, i.category, AVG(t.trend_index) AS trend_index,
                       COUNT(*) AS observations, {SCORE_AVERAGES}
                FROM collection_batches b
                JOIN trend_scores t ON t.batch_id = b.id
                JOIN sourcing_items i ON i.sku = t.sku
                WHERE b.observed_at BETWEEN ? AND ? AND (? IS NULL OR i.category = ?)
                GROUP BY bucket, i.category
                ORDER BY bucket, i.category""",
            (start, end, category, category),
        )
        return [dict(row) for row in rows]

    @staticmethod
    def _recommendations(conn: sqlite3.Connection, start: str, end: str, category: str | None) -> list[dict]:
        rows = conn.execute(
            """WITH recent AS (
                   SELECT t.*, b.observed_at, b.origin,
                          ROW_NUMBER() OVER (PARTITION BY t.sku ORDER BY b.observed_at DESC, b.id DESC) AS position
                   FROM collection_batches b
                   JOIN trend_scores t ON t.batch_id = b.id
                   JOIN sourcing_items i ON i.sku = t.sku
                   WHERE b.observed_at BETWEEN ? AND ? AND (? IS NULL OR i.category = ?)
               )
               SELECT i.*, r.trend_index, r.naver_score, r.youtube_score, r.instagram_score, r.google_score,
                      r.observed_at, r.origin
               FROM recent r JOIN sourcing_items i ON i.sku = r.sku
               WHERE r.position = 1
               ORDER BY r.trend_index DESC, i.margin_pct DESC, i.sku""",
            (start, end, category, category),
        )
        return [dict(row) for row in rows]

    def dashboard_view(
        self,
        days: int,
        category: str | None,
        *,
        compare: bool = False,
        now: datetime | None = None,
    ) -> dict:
        now = now or utc_now()
        start, end = period_bounds(days, now)
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN")
            series = {span: self._series(conn, span, category, now) for span in ((1, 30) if compare else (days,))}
            recommendations = self._recommendations(conn, start, end, category)
            logs = conn.execute(
                """SELECT b.observed_at, b.origin, l.channel, l.status, l.item_count, l.expected_count,
                          l.avg_score, l.duration_ms, l.detail
                   FROM collection_batches b JOIN channel_logs l ON l.batch_id = b.id
                   WHERE b.observed_at BETWEEN ? AND ?
                   ORDER BY b.observed_at DESC, b.id DESC, l.channel
                   LIMIT 80""",
                (start, end),
            )
            return {"series": series, "recommendations": recommendations, "logs": [dict(row) for row in logs]}

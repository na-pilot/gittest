"""SQLite 저장소와 한국시간 기준 조회."""

from __future__ import annotations

import os
import sqlite3
import time
from collections.abc import Sequence
from contextlib import closing
from dataclasses import asdict, fields
from datetime import date, datetime, time as wall_time, timedelta, timezone
from itertools import groupby
from pathlib import Path
from uuid import uuid4

from coupang_catalog import TOP_KEYWORDS
from coupang_collector import DailyKeyword, competition_grade, demo_snapshot, market_grade
from sub_agents import (
    CATEGORY_LABELS,
    CHANNEL_WEIGHTS,
    KEYWORDS_PER_SECTOR,
    ChannelReport,
    MultiChannelTrendScout,
    SourcingPlan,
    SourcingStrategistAgent,
    TrendSignal,
)


UTC = timezone.utc
KST = timezone(timedelta(hours=9), "KST")
DEFAULT_DB_PATH = Path(__file__).resolve().with_name("sourcing_radar.db")
DEFAULT_INTERVAL_SECONDS = 600
RANKING_WINDOW = timedelta(days=30)
RANKING_COMPARISON_LAG = timedelta(days=1)
RANKING_SEED_KEY = "keyword_ranking_seed_v1"
COUPANG_SEED_DAYS = 7
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

CREATE TABLE IF NOT EXISTS coupang_daily_keywords (
    record_date TEXT NOT NULL,
    tier1_cat TEXT NOT NULL,
    tier2_cat TEXT NOT NULL,
    rank INTEGER NOT NULL CHECK (rank BETWEEN 1 AND 30),
    keyword TEXT NOT NULL,
    monthly_search_vol INTEGER NOT NULL CHECK (monthly_search_vol >= 0),
    avg_reviews REAL NOT NULL CHECK (avg_reviews >= 0),
    wing_ratio REAL NOT NULL CHECK (wing_ratio BETWEEN 0 AND 100),
    market_size_grade TEXT NOT NULL,
    competition_grade TEXT NOT NULL,
    is_brand INTEGER NOT NULL CHECK (is_brand IN (0, 1)),
    avg_price_krw INTEGER NOT NULL,
    product_count INTEGER NOT NULL,
    sample_count INTEGER NOT NULL,
    review_sample_count INTEGER NOT NULL,
    price_sample_count INTEGER NOT NULL,
    sample_source TEXT NOT NULL,
    volume_source TEXT NOT NULL,
    product_count_source TEXT NOT NULL,
    collected_at TEXT NOT NULL,
    PRIMARY KEY (record_date, tier1_cat, tier2_cat, keyword)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS coupang_daily_runs (
    record_date TEXT PRIMARY KEY,
    claim_token TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'finished', 'error')),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    observed_count INTEGER NOT NULL DEFAULT 0,
    row_count INTEGER NOT NULL DEFAULT 0,
    note TEXT NOT NULL DEFAULT ''
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
COUPANG_COLUMNS = tuple(field.name for field in fields(DailyKeyword))
COUPANG_KEY = ("record_date", "tier1_cat", "tier2_cat", "keyword")
COUPANG_SOURCE_ORDER = {"seed": 0, "simulation": 1, "ego_lite": 2}
COUPANG_INSERT = f"""
INSERT INTO coupang_daily_keywords ({", ".join(COUPANG_COLUMNS)})
VALUES ({", ".join(":" + column for column in COUPANG_COLUMNS)})
ON CONFLICT ({", ".join(COUPANG_KEY)}) DO NOTHING
"""
COUPANG_UPDATE = f"""
UPDATE coupang_daily_keywords SET
    {", ".join(f"{column} = :{column}" for column in COUPANG_COLUMNS if column not in (*COUPANG_KEY, "rank"))}
WHERE record_date = :record_date AND tier1_cat = :tier1_cat AND tier2_cat = :tier2_cat AND keyword = :keyword
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
            if not seed:
                return
            current_time = now or utc_now()
            if conn.execute("SELECT 1 FROM coupang_daily_keywords LIMIT 1").fetchone() is None:
                today = current_time.astimezone(KST).date()
                for offset in range(COUPANG_SEED_DAYS - 1, -1, -1):
                    self._merge_coupang_rows(conn, demo_snapshot(today - timedelta(days=offset)))
            seeded = conn.execute("SELECT 1 FROM radar_metadata WHERE key = ?", (RANKING_SEED_KEY,)).fetchone()
            if seeded:
                return
            non_demo = conn.execute(
                "SELECT 1 FROM collection_batches WHERE origin NOT IN ('seed', 'simulation') LIMIT 1"
            ).fetchone()
            if not non_demo:
                self._seed(conn, current_time)
            conn.execute(
                "INSERT INTO radar_metadata(key, value) VALUES (?, ?)",
                (RANKING_SEED_KEY, "skipped_non_demo" if non_demo else "created"),
            )

    @staticmethod
    def _seed(conn: sqlite3.Connection, now: datetime) -> None:
        interval = DEFAULT_INTERVAL_SECONDS
        last_slot = int(now.timestamp() // interval) * interval
        # Include the preceding day so that the prior ranking also has a full 30-day window.
        first_slot = last_slot - int((RANKING_WINDOW + RANKING_COMPARISON_LAG).total_seconds())
        first = datetime.fromtimestamp(first_slot, UTC)
        existing_by_slot: dict[int, set[str]] = {}
        for row in conn.execute(
            """SELECT b.observed_at, t.sku
               FROM collection_batches b JOIN trend_scores t ON t.batch_id = b.id
               WHERE b.observed_at BETWEEN ? AND ?""",
            (utc_stamp(first), utc_stamp(now)),
        ):
            observed_at = datetime.fromisoformat(row["observed_at"])
            slot = int(observed_at.timestamp() // interval) * interval
            existing_by_slot.setdefault(slot, set()).add(row["sku"])

        # Preserve saved observations and sourcing assumptions in older demo databases.
        scout = MultiChannelTrendScout()
        strategist = SourcingStrategistAgent()
        existing_items = {row["sku"] for row in conn.execute("SELECT sku FROM sourcing_items")}
        initial_signals, _ = scout.collect(first)
        plans = [strategist.plan(signal) for signal in initial_signals if signal.candidate.sku not in existing_items]
        RadarDatabase._upsert_plans(conn, utc_stamp(first), plans)
        for slot in range(first_slot, last_slot + 1, interval):
            collector = scout
            if slot in existing_by_slot:
                missing = tuple(item for item in scout.candidates if item.sku not in existing_by_slot[slot])
                if not missing:
                    continue
                collector = MultiChannelTrendScout(catalog=missing)
            moment = datetime.fromtimestamp(slot, UTC)
            signals, reports = collector.collect(moment)
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

    @staticmethod
    def _merge_coupang_rows(conn: sqlite3.Connection, rows: Sequence[DailyKeyword]) -> None:
        scope_columns = ("record_date", "tier1_cat", "tier2_cat")

        def scope(row: DailyKeyword) -> tuple[str, str, str]:
            return row.record_date, row.tier1_cat, row.tier2_cat

        for identity, group in groupby(sorted(rows, key=scope), key=scope):
            scope_params = dict(zip(scope_columns, identity))
            existing = {row["keyword"]: dict(row) for row in conn.execute(
                """SELECT * FROM coupang_daily_keywords
                   WHERE record_date = :record_date AND tier1_cat = :tier1_cat AND tier2_cat = :tier2_cat""",
                scope_params,
            )}
            inserts, updates = [], []
            for row in group:
                incoming = asdict(row)
                old = existing.get(row.keyword)
                if old is None:
                    inserts.append(incoming)
                    continue
                new_priority = COUPANG_SOURCE_ORDER[row.sample_source]
                old_priority = COUPANG_SOURCE_ORDER[old["sample_source"]]
                newer_sample = (new_priority, row.collected_at) >= (old_priority, old["collected_at"])
                data = (incoming if newer_sample else old).copy()

                # The best source is tracked independently for volume, page sample and product count.
                volume_data = incoming if row.volume_source == "csv" or old["volume_source"] != "csv" else old
                if data["product_count_source"] != "page":
                    seen_count = incoming if row.product_count_source == "page" else old
                    if seen_count["product_count_source"] == "page":
                        data["product_count"] = seen_count["product_count"]
                        data["product_count_source"] = "page"
                volume = data["monthly_search_vol"] = volume_data["monthly_search_vol"]
                data["volume_source"] = volume_data["volume_source"]
                data["market_size_grade"] = market_grade(volume, data["avg_price_krw"])
                data["competition_grade"] = competition_grade(volume, data["product_count"], data["avg_reviews"])
                updates.append(data)
            conn.executemany(COUPANG_INSERT, inserts)
            conn.executemany(COUPANG_UPDATE, updates)
            conn.execute(
                """WITH ranked AS (
                       SELECT keyword, ROW_NUMBER() OVER (ORDER BY monthly_search_vol DESC, keyword) AS position
                       FROM coupang_daily_keywords
                       WHERE record_date = :record_date AND tier1_cat = :tier1_cat AND tier2_cat = :tier2_cat
                   )
                   UPDATE coupang_daily_keywords
                   SET rank = (SELECT position FROM ranked WHERE ranked.keyword = coupang_daily_keywords.keyword)
                   WHERE record_date = :record_date AND tier1_cat = :tier1_cat AND tier2_cat = :tier2_cat""",
                scope_params,
            )

    def save_coupang_snapshot(self, rows: Sequence[DailyKeyword]) -> None:
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            self._merge_coupang_rows(conn, rows)

    def coupang_date_range(self) -> tuple[date, date] | None:
        with closing(self._connect()) as conn:
            first, last = conn.execute("SELECT MIN(record_date), MAX(record_date) FROM coupang_daily_keywords").fetchone()
        return (date.fromisoformat(first), date.fromisoformat(last)) if first is not None else None

    def get_coupang_keywords(
        self,
        day: date,
        tier1: str | None = None,
        tier2: str | None = None,
        *,
        hide_brands: bool = False,
        min_volume: int = 0,
        max_reviews: int | None = None,
        market_grades: Sequence[str] = (),
        competition_grades: Sequence[str] = (),
    ) -> list[dict]:
        filters = ["record_date = ?", "monthly_search_vol >= ?"]
        args: list = [day.isoformat(), min_volume]
        for column, value in (("tier1_cat", tier1), ("tier2_cat", tier2)):
            if value is not None:
                filters.append(f"{column} = ?")
                args.append(value)
        if hide_brands:
            filters.append("is_brand = 0")
        if max_reviews is not None:
            filters.append("avg_reviews <= ?")
            args.append(max_reviews)
        for column, grades in (("market_size_grade", market_grades), ("competition_grade", competition_grades)):
            if grades:
                filters.append(f"{column} IN ({', '.join('?' for _ in grades)})")
                args.extend(grades)
        args.append(TOP_KEYWORDS)
        with closing(self._connect()) as conn:
            rows = conn.execute(
                f"""SELECT * FROM coupang_daily_keywords WHERE {" AND ".join(filters)}
                    ORDER BY monthly_search_vol DESC, keyword, tier1_cat, tier2_cat LIMIT ?""",
                args,
            ).fetchall()
        return [dict(row) for row in rows]

    def claim_coupang_daily_run(self, day: date, *, now: datetime | None = None) -> str | None:
        """The scheduler calls this under its process lock; a prior incomplete run can resume."""
        now = now or utc_now()
        token = uuid4().hex
        with closing(self._connect()) as conn, conn:
            result = conn.execute(
                """INSERT INTO coupang_daily_runs(record_date, claim_token, status, started_at)
                   VALUES (?, ?, 'running', ?)
                   ON CONFLICT (record_date) DO UPDATE SET
                       claim_token = excluded.claim_token, status = 'running', started_at = excluded.started_at
                   WHERE coupang_daily_runs.status = 'running'""",
                (day.isoformat(), token, utc_stamp(now)),
            )
            return token if result.rowcount == 1 else None

    def finish_coupang_daily_run(
        self, day: date, token: str, *, rows: Sequence[DailyKeyword] = (), observed_count: int = 0,
        note: str = "", failed: bool = False,
    ) -> None:
        with closing(self._connect()) as conn, conn:
            result = conn.execute(
                """UPDATE coupang_daily_runs SET
                   status = ?, finished_at = ?, observed_count = ?, row_count = ?, note = ?
                   WHERE record_date = ? AND claim_token = ? AND status = 'running'""",
                ("error" if failed else "finished", utc_stamp(utc_now()), observed_count, len(rows), note,
                 day.isoformat(), token),
            )
            if result.rowcount == 1:
                self._merge_coupang_rows(conn, rows)

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

    def get_top20_keywords_by_sector(
        self, category: str | None = None, *, now: datetime | None = None,
    ) -> dict[str, list[dict]]:
        """Rank the latest 30 days and compare against the same window 24 hours ago."""
        now = now or utc_now()
        end = utc_stamp(now)
        now = now.astimezone(UTC)
        start = utc_stamp(now - RANKING_WINDOW)
        previous_end = utc_stamp(now - RANKING_COMPARISON_LAG)
        previous_start = utc_stamp(now - RANKING_WINDOW - RANKING_COMPARISON_LAG)
        sectors = CATEGORY_LABELS if category is None else (category,)
        result = {sector: [] for sector in sectors}
        # Within each batch, normalize over available channels as the scout does.
        # The stored trend_index is rounded to one decimal; contributions keep full precision.
        available_weight = " + ".join(
            f"(CASE WHEN t.{channel}_score IS NULL THEN 0.0 ELSE {weight} END)"
            for channel, weight in CHANNEL_WEIGHTS.items()
        )
        contributions = ",\n".join(
            f"SUM(CASE WHEN b.observed_at > :start THEN "
            f"t.{channel}_score * {weight} / NULLIF({available_weight}, 0) END) AS {channel}_total"
            for channel, weight in CHANNEL_WEIGHTS.items()
        )
        category_filter = "AND i.category = :category" if category is not None else ""
        sql = f"""
            WITH totals AS (
                SELECT t.sku, i.name AS keyword, i.category,
                       ROUND(SUM(CASE WHEN b.observed_at > :start THEN t.trend_index END), 1)
                           AS cumulative_score,
                       ROUND(SUM(CASE WHEN b.observed_at <= :previous_end THEN t.trend_index END), 1)
                           AS previous_score,
                       SUM(CASE WHEN b.observed_at > :start THEN 1 ELSE 0 END) AS observation_count,
                       {contributions}
                FROM collection_batches b
                JOIN trend_scores t ON t.batch_id = b.id
                JOIN sourcing_items i ON i.sku = t.sku
                WHERE b.observed_at > :previous_start AND b.observed_at <= :end
                      {category_filter}
                GROUP BY t.sku
            ), current_ranks AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cumulative_score DESC, sku) AS rank
                FROM totals WHERE cumulative_score IS NOT NULL
            ), previous_ranks AS (
                SELECT sku, ROW_NUMBER() OVER (PARTITION BY category ORDER BY previous_score DESC, sku) AS previous_rank
                FROM totals WHERE previous_score IS NOT NULL
            )
            SELECT c.*, p.previous_rank
            FROM current_ranks c LEFT JOIN previous_ranks p ON p.sku = c.sku
            WHERE c.rank <= :limit
            ORDER BY c.category, c.rank
        """
        with closing(self._connect()) as conn:
            rows = conn.execute(sql, {
                "start": start, "end": end, "previous_start": previous_start, "previous_end": previous_end,
                "category": category, "limit": KEYWORDS_PER_SECTOR,
            }).fetchall()
        for row in rows:
            if row["category"] not in result:
                continue
            channel_totals = {channel: row[f"{channel}_total"] for channel in CHANNEL_WEIGHTS}
            observed = {channel: total for channel, total in channel_totals.items() if total is not None}
            previous_rank = row["previous_rank"]
            result[row["category"]].append({
                "sku": row["sku"],
                "keyword": row["keyword"],
                "rank": row["rank"],
                "previous_rank": previous_rank,
                "rank_delta": previous_rank - row["rank"] if previous_rank is not None else None,
                "cumulative_score": row["cumulative_score"],
                "observation_count": row["observation_count"],
                "channel_totals": channel_totals,
                "leading_channel": max(observed, key=observed.get) if observed else None,
            })
        return result

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
        now: datetime | None = None,
    ) -> dict:
        now = now or utc_now()
        start, end = period_bounds(days, now)
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN")
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
            return {"recommendations": recommendations, "logs": [dict(row) for row in logs]}

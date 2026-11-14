"""Streamlit 소싱 관제판. 수집은 스케줄러가 담당하고 화면은 SQLite를 조회한다."""

from __future__ import annotations

import html
import math
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from threading import Event

import pandas as pd
import streamlit as st

from coupang_catalog import COUPANG_CATEGORIES
from coupang_collector import (
    COUPANG_MANUAL_LIVE_LIMIT,
    GRADES,
    SAMPLE_SOURCE_LABELS,
    VOLUME_SOURCE_LABELS,
    CollectionResult,
    CoupangCollector,
    normalize_keyword,
)
from database import KST, RadarDatabase, resolve_db_path, utc_now
from sub_agents import CATEGORY_LABELS, CHANNEL_LABELS, CHANNEL_WEIGHTS, DEMO_CATALOG, KEYWORDS_PER_SECTOR


PERIOD_LABELS = {1: "오늘 하루", 7: "최근 7일", 30: "최근 30일"}
CATEGORY_SHORT = {"industrial": "공산품", "beauty": "뷰티", "food": "일반식품"}
CATEGORY_COLORS = {"공산품": "#4FE2B4", "뷰티": "#B09AFF", "일반식품": "#F9BF6B"}
CHANNEL_SHORT = {"naver": "네이버", "youtube": "유튜브", "instagram": "인스타그램", "google": "구글"}
RANK_CHANNEL_NAMES = {**CHANNEL_SHORT, "instagram": "인스타"}
LOG_STATUS = {"ok": "완료", "partial": "일부 수집", "empty": "신호 없음", "error": "오류"}
ORIGIN_LABELS = {"seed": "초기 데모", "simulation": "시뮬레이션 배치", "live": "실수집"}
COUPANG_GRADE_COLORS = {
    "매우 좋음": ("#83efc5", "#16352f"),
    "좋음": ("#afe4b1", "#243933"),
    "보통": ("#c4d0e3", "#253449"),
    "나쁨": ("#ffd195", "#49351f"),
    "매우 나쁨": ("#ffb2bc", "#422731"),
}

STYLES = """
<style>
  :root {
    --radar-text: #eaf1fa;
    --radar-muted: #a4b4cc;
    --radar-accent: #4fe2b4;
    --radar-border: #26344b;
  }
  .stApp {
    background: radial-gradient(ellipse at 72% -22%, #1a2b42 0%, transparent 48%), #0b101a;
    color: var(--radar-text);
  }
  header[data-testid="stHeader"] { background: transparent; }
  .block-container { max-width: 1520px; padding: 2.2rem 2.6rem 3.5rem; }
  .radar-hero {
    position: relative; display: flex; align-items: center; justify-content: space-between;
    gap: 1.2rem; overflow: hidden; min-height: 110px; padding: 1.25rem 1.5rem;
    border: 1px solid #30465d; border-radius: 16px;
    background:
      repeating-linear-gradient(90deg, transparent 0 35px, #648db00d 36px 37px),
      linear-gradient(105deg, #111c2c 38%, #173331);
  }
  .radar-hero h1 {
    margin: 0; padding: 0; font-size: clamp(2rem, 3vw, 2.65rem);
    letter-spacing: -.045em; line-height: 1.13; font-weight: 700;
  }
  .radar-hero p { margin: .55rem 0 0; color: #b3c5dc; font-size: .92rem; }
  .radar-mode {
    display: inline-flex; align-items: center; gap: .55rem; border: 1px solid #3a796b;
    color: #b1f5df; background: #102824; border-radius: 999px; padding: .45rem .75rem;
    font-weight: 650; font-size: .8rem; white-space: nowrap;
  }
  .radar-mode::before {
    content: ""; width: 7px; height: 7px; border-radius: 50%; background: var(--radar-accent);
  }
  [data-testid="stMetric"] {
    min-height: 101px; padding: .85rem 1.05rem; border: 1px solid var(--radar-border);
    background: linear-gradient(160deg, #172335, #111a28); border-radius: 13px;
  }
  [data-testid="stMetricLabel"] p { color: var(--radar-muted); font-size: .82rem; }
  [data-testid="stMetricValue"] > div { font-size: clamp(1.35rem, 2.25vw, 1.95rem); letter-spacing: -.035em; }
  [data-testid="stCaptionContainer"] p { color: var(--radar-muted); }
  [data-testid="stVerticalBlockBorderWrapper"]:has(> [data-testid="stVerticalBlock"] .radar-card-top) {
    background: #111b2a;
  }
  .radar-card-meta {
    display: flex; justify-content: space-between; align-items: center; gap: .5rem;
    font-size: .75rem; color: var(--radar-muted); font-variant-numeric: tabular-nums;
  }
  .radar-category {
    color: var(--category-color); font-weight: 650; border: 1px solid var(--radar-border);
    padding: .18rem .55rem; border-radius: 6px; background: #101929;
  }
  .radar-card-top {
    display: flex; justify-content: space-between; align-items: center; gap: .65rem; margin: .48rem 0 .38rem;
  }
  .radar-card-top h3 { margin: 0; padding: 0; font-size: 1.22rem; letter-spacing: -.02em; }
  .radar-signal { text-align: right; white-space: nowrap; }
  .radar-signal b { display: block; color: var(--radar-accent); font-size: 1.45rem; line-height: 1.2; }
  .radar-signal span, .radar-spec { color: var(--radar-muted); font-size: .75rem; }
  .radar-spec { margin: .2rem 0 .9rem; line-height: 1.55; }
  .radar-channels, .radar-prices { display: grid; gap: .55rem; }
  .radar-channels { grid-template-columns: repeat(4, minmax(0, 1fr)); margin-bottom: .85rem; }
  .radar-channel { min-width: 0; }
  .radar-channel p {
    display: flex; flex-wrap: wrap; gap: .25rem; align-items: baseline; justify-content: space-between;
    color: #afbdd2; font-size: .73rem; line-height: 1.4; margin: 0 0 .25rem;
  }
  .radar-channel b { color: var(--radar-text); font-weight: 600; font-variant-numeric: tabular-nums; }
  .radar-bar { height: 4px; background: #2b394f; border-radius: 99px; }
  .radar-bar i { display: block; height: 100%; background: #6bc6bd; border-radius: 99px; }
  .radar-prices { grid-template-columns: 1.22fr 1fr .88fr; margin-bottom: .85rem; }
  .radar-price {
    min-width: 0; padding: .5rem .65rem; background: #0d1523; border: 1px solid #25354d;
    border-radius: 9px; font-variant-numeric: tabular-nums;
  }
  .radar-price span { display: block; color: var(--radar-muted); font-size: .72rem; }
  .radar-price b { display: block; margin-top: .15rem; color: #edf5ff; font-size: 1rem; }
  .radar-price small { color: #b3c5dc; font-size: .76rem; }
  .radar-price.radar-margin b { color: var(--radar-accent); }
  .radar-brief { display: grid; gap: .45rem; }
  .radar-brief p { margin: 0; font-size: .88rem; line-height: 1.5; color: #d1ddec; }
  .radar-brief strong { color: #edf5ff; margin-right: .35rem; }
  .radar-ranking-grid {
    display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: .85rem; align-items: start;
  }
  .radar-ranking-grid.radar-ranking-detail { grid-template-columns: minmax(0, 1fr); }
  .radar-ranking-panel {
    --sector-border: color-mix(in srgb, var(--sector-color) 37%, var(--radar-border));
    min-width: 0; border: 1px solid var(--radar-border); border-top-color: var(--sector-border);
    border-radius: 13px; overflow: hidden; background: #111a28;
  }
  .radar-ranking-panel h3 {
    margin: 0; padding: .82rem .85rem; font-size: .98rem; color: var(--radar-text);
    background: linear-gradient(110deg, color-mix(in srgb, var(--sector-color) 9%, #162234), #141e2f);
    border-bottom: 1px solid var(--radar-border); letter-spacing: -.02em;
  }
  .radar-ranking-panel h3::before {
    content: ""; display: inline-block; width: 7px; height: 7px; margin-right: .55rem;
    border-radius: 50%; background: var(--sector-color); vertical-align: .12em;
  }
  .radar-ranking-scroll { max-height: 32rem; overflow-y: auto; scrollbar-color: #425571 #111a28; }
  .radar-ranking-detail .radar-ranking-scroll { max-height: none; }
  .radar-ranking-scroll:focus-visible { outline: 2px solid var(--sector-color); outline-offset: -2px; }
  .radar-ranking-table { width: 100%; border-collapse: collapse; table-layout: fixed; }
  .radar-ranking-table th, .radar-ranking-table td { padding: .5rem .26rem; border: 0; }
  .radar-ranking-table th:first-child, .radar-ranking-table td:first-child { padding-left: .7rem; }
  .radar-ranking-table th:last-child, .radar-ranking-table td:last-child { padding-right: .7rem; }
  .radar-ranking-table th {
    color: var(--radar-muted); font-size: .72rem; font-weight: 600; text-align: left;
    background: #0e1725; border-bottom: 1px solid var(--radar-border);
    position: sticky; top: 0; z-index: 1;
  }
  .radar-ranking-table th:nth-child(1) { width: 2.5rem; }
  .radar-ranking-table th:nth-child(2) { width: 2.75rem; }
  .radar-ranking-table th:nth-child(4) { width: 5.45rem; }
  .radar-ranking-table th:nth-child(5) { width: 4.2rem; text-align: center; }
  .radar-ranking-table td {
    font-size: .84rem; border-bottom: 1px solid #202d41; vertical-align: middle;
  }
  .radar-ranking-table tr:last-child td { border-bottom: 0; }
  .radar-ranking-table tbody tr:nth-child(even) { background: #141f30; }
  .radar-ranking-table tbody tr:hover { background: #1b2a40; }
  .radar-ranking-table .ranking-number, .radar-ranking-table .ranking-score {
    font-variant-numeric: tabular-nums; font-weight: 650;
  }
  .radar-ranking-table .ranking-podium { color: var(--sector-color); }
  .radar-ranking-table .ranking-keyword { line-height: 1.35; overflow-wrap: anywhere; word-break: keep-all; }
  .radar-ranking-table .ranking-score { text-align: right; white-space: nowrap; }
  .radar-ranking-table .ranking-channel { text-align: center; }
  .radar-ranking-table .ranking-meter {
    height: 3px; width: 100%; margin-top: .22rem; border-radius: 99px; background: #2b394f;
  }
  .radar-ranking-table .ranking-meter i {
    display: block; height: 100%; background: var(--sector-color); border-radius: 99px; opacity: .85;
  }
  .ranking-change, .ranking-channel-badge {
    display: inline-block; padding: .1rem .27rem; border-radius: 5px; white-space: nowrap;
    font-size: .72rem; line-height: 1.35; font-weight: 650; font-variant-numeric: tabular-nums;
  }
  .ranking-up { color: #83efc5; background: #16352f; }
  .ranking-down { color: #ffb2bc; background: #422731; }
  .ranking-steady { color: #b3c5dc; background: #223046; }
  .ranking-new { color: #ffe0a3; background: #473820; }
  .ranking-channel-badge { color: #b3c5dc; background: #223046; }
  .ranking-channel-naver { color: #83efb4; background: #17352d; }
  .ranking-channel-youtube { color: #ffb2bc; background: #412631; }
  .ranking-channel-instagram { color: #dcc8ff; background: #37294d; }
  .ranking-channel-google { color: #accfff; background: #203655; }
  .radar-ranking-empty { color: var(--radar-muted); padding: .85rem; margin: 0; }
  .st-key-coupang-keywords [data-testid="stVerticalBlockBorderWrapper"] { background: #111a28; }
  .coupang-table-wrap {
    max-height: 27rem; overflow: auto; scrollbar-color: #425571 #111a28;
    border: 1px solid #26344b; border-radius: 10px; background: #111a28;
  }
  .coupang-table-wrap:focus-visible { outline: 2px solid var(--radar-accent); outline-offset: -2px; }
  .coupang-table { width: 100%; min-width: 830px; border-collapse: collapse; font-variant-numeric: tabular-nums; }
  .coupang-table :is(th, td) { padding: .58rem .75rem; border: 0; border-bottom: 1px solid #26344b; }
  .coupang-table th {
    position: sticky; top: 0; z-index: 1; background: #152237;
    color: #bfcce0; text-align: left; font-size: .77rem; white-space: nowrap;
  }
  .coupang-table td { font-size: .85rem; text-align: left; }
  .coupang-table .coupang-number { text-align: right; white-space: nowrap; }
  .coupang-table td:first-child { color: var(--radar-accent); font-weight: 650; }
  .coupang-table tr:nth-child(even) { background: #151f30; }
  .coupang-table tbody tr:hover { background: #1b2a40; }
  .coupang-term { font-weight: 650; }
  .coupang-category { display: block; margin-top: .14rem; color: var(--radar-muted); font-size: .72rem; }
  .coupang-badge {
    display: inline-block; border-radius: 6px; padding: .12rem .46rem;
    color: var(--badge-color); background: var(--badge-bg); font-size: .74rem; font-weight: 650; white-space: nowrap;
  }
  @media (min-width: 721px) {
    .radar-ranking-detail .radar-ranking-table th, .radar-ranking-detail .radar-ranking-table td {
      padding: .62rem 1rem;
    }
    .radar-ranking-detail .radar-ranking-table th:nth-child(1) { width: 5.5rem; }
    .radar-ranking-detail .radar-ranking-table th:nth-child(2) { width: 7rem; }
    .radar-ranking-detail .radar-ranking-table th:nth-child(4) { width: 26%; }
    .radar-ranking-detail .radar-ranking-table th:nth-child(5) { width: 14%; }
    .radar-ranking-detail .radar-ranking-table th:nth-child(6) { width: 8.5rem; text-align: right; }
    .radar-ranking-detail .ranking-observations { color: var(--radar-muted); text-align: right; }
  }
  @media (min-width: 721px) and (max-width: 1110px) {
    .radar-ranking-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  }
  @media (max-width: 720px) {
    .radar-ranking-grid { grid-template-columns: minmax(0, 1fr); }
    .radar-ranking-detail .ranking-observations { display: none; }
  }
  @media (max-width: 700px) {
    .block-container { padding: 2.2rem 1rem 2rem; }
    .radar-hero { align-items: flex-start; flex-direction: column; padding: 1rem; gap: .7rem; }
    .radar-hero p { font-size: .85rem; }
    .radar-prices { gap: .35rem; }
    .radar-price { padding: .45rem; }
    .radar-price b { font-size: .87rem; }
  }
</style>
"""


def escape(value: object) -> str:
    return html.escape(str(value))


def local_stamp(value: str, format_string: str = "%m-%d %H:%M:%S") -> str:
    return datetime.fromisoformat(value).astimezone(KST).strftime(format_string)


@st.cache_resource(show_spinner="소싱 이력을 준비하는 중입니다.")
def open_database(path: str) -> RadarDatabase:
    db = RadarDatabase(path)
    db.initialize()
    return db


@st.cache_data(ttl=15, show_spinner=False)
def cached_summary(path: str, days: int, category: str | None) -> dict:
    return RadarDatabase(path).summary(days, category)


def schedule_display(state: dict | None) -> tuple[str, str]:
    if state is None:
        return "대기", "스케줄러 시작 전"
    status = state["status"]
    cycle = state["cycle"]
    interval = state["interval_seconds"]
    now = utc_now()
    if status == "offline":
        return "중지", f"마지막 사이클 #{cycle:04d}"
    if status == "collecting":
        started = datetime.fromisoformat(state["started_at"])
        if (now - started).total_seconds() > max(30, interval):
            return "지연", f"사이클 #{cycle:04d} 응답 지연"
        return "수집 중", f"사이클 #{cycle:04d}"
    next_run = datetime.fromisoformat(state["next_run_at"])
    seconds = math.ceil((next_run - now).total_seconds())
    if seconds < -max(10, min(60, interval)):
        return "지연", "스케줄러 상태 확인 필요"
    minutes, seconds = divmod(max(0, seconds), 60)
    prefix = "오류 후 재시도" if status == "error" else f"{interval:g}초 주기"
    return f"{minutes:02d}:{seconds:02d}", f"{prefix} · {next_run.astimezone(KST):%H:%M:%S} KST"


@st.fragment(run_every="5s")
def render_kpis(db: RadarDatabase, days: int, category: str | None) -> None:
    try:
        summary = cached_summary(str(db.path), days, category)
        schedule = db.get_schedule()
    except sqlite3.Error as exc:
        st.error(f"요약 데이터를 읽을 수 없습니다. {exc}")
        return
    countdown, schedule_caption = schedule_display(schedule)
    leader = summary["leader"]
    margin = summary["avg_margin"]
    kpis = st.columns(4)
    with kpis[0]:
        st.metric("모니터링 SKU", f'{summary["sku_count"]:,}개')
        st.caption(f"{PERIOD_LABELS[days]} 고유 SKU")
    with kpis[1]:
        st.metric("평균 잠재 마진율", f"{margin:.1f}%" if margin is not None else "—")
        st.caption("SKU별 예시 마진율 평균")
    with kpis[2]:
        st.metric("오늘의 최다 바이럴 채널", CHANNEL_SHORT[leader] if leader else "—")
        st.caption(f'오늘 평균 {summary["leader_score"]:.1f} / 100' if leader else "오늘 수집된 신호 없음")
    with kpis[3]:
        st.metric("다음 수집까지", countdown)
        st.caption(schedule_caption)
    if schedule and schedule["last_error"]:
        st.warning(f'최근 배치 오류 · {schedule["last_error"]}')


def movement_badge(row: dict) -> str:
    change = row["rank_delta"]
    if change is None:
        label, style, tooltip = "NEW", "new", "24시간 전 비교 구간에 관측 없음"
    else:
        tooltip = f'24시간 전 {row["previous_rank"]}위'
        if change > 0:
            label, style = f"▲{change}", "up"
        elif change < 0:
            label, style = f"▼{-change}", "down"
        else:
            label, style = "-", "steady"
    return f'<span class="ranking-change ranking-{style}" title="{tooltip}">{label}</span>'


def ranking_panel(category: str, rows: list[dict], *, detailed: bool) -> str:
    title = escape(CATEGORY_LABELS[category])
    color = CATEGORY_COLORS[CATEGORY_SHORT[category]]
    header = f'<section class="radar-ranking-panel" style="--sector-color:{color}"><h3>{title}</h3>'
    if not rows:
        return header + '<p class="radar-ranking-empty">최근 30일 동안 수집된 검색어가 없습니다.</p></section>'

    top_score = rows[0]["cumulative_score"] or 1
    entries = []
    for row in rows:
        score = row["cumulative_score"]
        percent = score / top_score * 100
        rank = row["rank"]
        rank_style = "ranking-number ranking-podium" if rank <= 3 else "ranking-number"
        channel = row["leading_channel"]
        channel_label = RANK_CHANNEL_NAMES.get(channel, "—")
        channel_style = f"ranking-channel-{channel}" if channel in CHANNEL_WEIGHTS else ""
        channel_details = []
        for key, total in row["channel_totals"].items():
            value = f"{total:,.1f}" if total is not None else "—"
            channel_details.append(f"{RANK_CHANNEL_NAMES[key]} {value}")
        channel_tooltip = escape(" · ".join(channel_details))
        observations = f'<td class="ranking-observations">{row["observation_count"]:,}회</td>' if detailed else ""
        entries.append(f"""
          <tr>
            <td class="{rank_style}">{rank}</td>
            <td>{movement_badge(row)}</td>
            <td class="ranking-keyword">{escape(row["keyword"])}</td>
            <td class="ranking-score">{score:,.1f}
              <div class="ranking-meter" aria-hidden="true"><i style="width:{percent:.2f}%"></i></div>
            </td>
            <td class="ranking-channel">
              <span class="ranking-channel-badge {channel_style}" title="{channel_tooltip}">{channel_label}</span>
            </td>
            {observations}
          </tr>
        """)

    score_heading = "30일 누적 지수" if detailed else "누적 지수"
    channel_heading = "주력 반응 채널" if detailed else "채널"
    observation_heading = '<th scope="col" class="ranking-observations">수집 횟수</th>' if detailed else ""
    scroll_access = "" if detailed else f'role="region" tabindex="0" aria-label="{title} 순위 스크롤 영역"'
    return header + f"""
      <div class="radar-ranking-scroll" {scroll_access}>
        <table class="radar-ranking-table" aria-label="{title} 최근 30일 인기 검색어">
          <thead><tr>
            <th scope="col">순위</th><th scope="col">등락</th><th scope="col">인기 검색어</th>
            <th scope="col" style="text-align:right">{score_heading}</th>
            <th scope="col">{channel_heading}</th>
            {observation_heading}
          </tr></thead>
          <tbody>{"".join(entries)}</tbody>
        </table>
      </div>
      </section>
    """


def render_rankings(rankings: dict[str, list[dict]], category: str | None) -> None:
    st.subheader(f"최근 30일 누적 인기 검색어 TOP {KEYWORDS_PER_SECTOR}")
    st.caption("30일 배치 점수를 합산합니다. 등락은 24시간 전 순위, 막대는 섹터 1위 점수와 비교합니다.")
    detailed = category is not None
    layout = "radar-ranking-grid radar-ranking-detail" if detailed else "radar-ranking-grid"
    panels = "".join(ranking_panel(sector, rows, detailed=detailed) for sector, rows in rankings.items())
    st.html(f'<div class="{layout}">{panels}</div>')


@st.cache_resource
def coupang_executor() -> ThreadPoolExecutor:
    return ThreadPoolExecutor(max_workers=1, thread_name_prefix="coupang-refresh")


def refresh_coupang_snapshot(
    db: RadarDatabase, day: date, tier1: str | None, tier2: str | None, cancel: Event,
) -> CollectionResult:
    result = CoupangCollector().collect(
        day, tier1, tier2, max_live_keywords=COUPANG_MANUAL_LIVE_LIMIT, stop_event=cancel,
    )
    db.save_coupang_snapshot(result.rows)
    return result


def coupang_badge(grade: str) -> str:
    foreground, background = COUPANG_GRADE_COLORS[grade]
    return f'<span class="coupang-badge" style="--badge-color:{foreground};--badge-bg:{background}">{grade}</span>'


def render_coupang_table(rows: list[dict]) -> None:
    entries = []
    for rank, row in enumerate(rows, start=1):
        keyword = escape(row["keyword"])
        category = f'{escape(row["tier1_cat"])} · {escape(row["tier2_cat"])}'
        volume_source = VOLUME_SOURCE_LABELS[row["volume_source"]]
        sample_source = SAMPLE_SOURCE_LABELS[row["sample_source"]]
        entries.append(f"""
          <tr>
            <td>{rank}</td>
            <td><span class="coupang-term">{keyword}</span><span class="coupang-category">{category}</span></td>
            <td class="coupang-number" title="{volume_source}">{row["monthly_search_vol"]:,}</td>
            <td class="coupang-number" title="{sample_source}">{row["avg_reviews"]:,.1f}</td>
            <td class="coupang-number" title="{sample_source}">{row["wing_ratio"]:.1f}%</td>
            <td>{coupang_badge(row["market_size_grade"])}</td>
            <td>{coupang_badge(row["competition_grade"])}</td>
          </tr>
        """)
    st.html(f"""
      <div class="coupang-table-wrap" role="region" tabindex="0" aria-label="쿠팡 키워드 순위">
        <table class="coupang-table" aria-label="쿠팡 키워드 TOP 30">
          <thead><tr>
            <th scope="col">순위</th><th scope="col">키워드</th>
            <th scope="col" class="coupang-number">월 검색량</th>
            <th scope="col" class="coupang-number">1P 평균 리뷰수</th>
            <th scope="col" class="coupang-number">일반판매자(윙) 비율(%)</th>
            <th scope="col">시장 규모</th><th scope="col">경쟁 강도</th>
          </tr></thead>
          <tbody>{"".join(entries)}</tbody>
        </table>
      </div>
    """)


def render_coupang_selection(rows: list[dict]) -> None:
    index = st.selectbox(
        "소싱에 참고할 키워드", range(len(rows)), index=None, placeholder="키워드를 선택하세요",
        format_func=lambda i: f'{i + 1}위 · {rows[i]["keyword"]}',
    )
    if index is None:
        return
    row = rows[index]
    keyword = row["keyword"]
    st.caption("한국어 키워드 · 코드 블록 오른쪽 아이콘으로 복사")
    st.code(keyword, language=None, wrap_lines=True)
    key = normalize_keyword(keyword)
    candidate = next((item for item in DEMO_CATALOG if normalize_keyword(item.name) == key), None)
    if candidate is not None and not row["is_brand"]:
        st.caption("1688 소싱 추천과 일치하는 중문 검색어")
        st.code(candidate.search_zh, language=None, wrap_lines=True)
    else:
        st.caption("1688 검색에 활용하려면 상품 종류와 소재·규격을 중문 검색어로 정리해 보세요.")
    product_source = "검색 화면 상품 수" if row["product_count_source"] == "page" else "상품 수 예시"
    sources = f'{SAMPLE_SOURCE_LABELS[row["sample_source"]]} · {VOLUME_SOURCE_LABELS[row["volume_source"]]} · {product_source}'
    if row["sample_count"]:
        sources += f' · 상품 {row["sample_count"]}개 (리뷰 {row["review_sample_count"]}개, 가격 {row["price_sample_count"]}개)'
    st.caption(sources)


def render_coupang(db: RadarDatabase) -> None:
    st.subheader("🛒 쿠팡 카테고리별 키워드 발굴 (1·2차 분류 TOP 30)")
    with st.container(border=True, key="coupang-keywords"):
        today = utc_now().astimezone(KST).date()
        date_range = db.coupang_date_range()
        selection = st.columns([1.15, 1.3, 1, 0.8], vertical_alignment="bottom")
        with selection[0]:
            tier1 = st.selectbox("1차 카테고리 선택", (None, *COUPANG_CATEGORIES), format_func=lambda value: value or "전체")
        with selection[1]:
            children = COUPANG_CATEGORIES[tier1] if tier1 else ()
            tier2 = st.selectbox(
                "2차 카테고리 선택", (None, *children), format_func=lambda value: value or "전체",
                disabled=tier1 is None, key=f"coupang-tier2-{tier1}",
            )
        with selection[2]:
            day = st.date_input("기준 날짜", value=today, min_value=date_range[0] if date_range else today, max_value=today)
        with selection[3]:
            hide_brands = st.checkbox("브랜드 제거")

        future = st.session_state.get("coupang_refresh_job")
        if future is not None and future.done():
            canceled = st.session_state.pop("coupang_refresh_cancel").is_set()
            try:
                result = future.result()
                status = "조회 중지 후 결과 저장" if canceled else "조회 범위 저장 완료"
                message = f"{status} · {len(result.rows):,}개 키워드 · Ego Lite 관측 {result.observed_count}건"
                st.session_state["coupang_refresh_message"] = (message, result.note)
            except Exception as exc:
                st.session_state["coupang_refresh_message"] = ("갱신 결과를 저장하지 못했습니다.", type(exc).__name__)
            del st.session_state["coupang_refresh_job"]
            future = None
        if st.button(
            "🔄 에고라이트(Ego Lite)로 현재 카테고리 즉시 갱신",
            disabled=future is not None or day != today,
            help=f"오늘 스냅샷을 갱신하고 월 검색량 상위 {COUPANG_MANUAL_LIVE_LIMIT}개를 브라우저로 조회합니다.",
        ):
            cancel = Event()
            st.session_state["coupang_refresh_cancel"] = cancel
            st.session_state["coupang_refresh_job"] = coupang_executor().submit(
                refresh_coupang_snapshot, db, day, tier1, tier2, cancel,
            )
            st.session_state.pop("coupang_refresh_message", None)
            st.rerun()
        if future is not None:
            st.info("Ego Lite 연결과 쿠팡 첫 페이지 확인 중입니다.")
            cancel = st.session_state["coupang_refresh_cancel"]
            if st.button("수집 중지", disabled=cancel.is_set()):
                cancel.set()
                st.info("진행 중인 페이지 조회가 끝나면 수집을 중지합니다.")
        elif "coupang_refresh_message" in st.session_state:
            message, note = st.session_state["coupang_refresh_message"]
            st.info(f"{message}. {note}" if note else message)

        with st.expander("세부 수치 필터"):
            filters = st.columns(4, vertical_alignment="top")
            with filters[0]:
                min_volume = st.number_input("최소 월 검색량", min_value=0, max_value=100_000, value=0, step=1000)
            with filters[1]:
                max_reviews = st.number_input(
                    "최대 평균 리뷰수", min_value=0, value=None, step=100,
                    placeholder="제한 없음", help="예를 들어 2,000을 입력하면 1페이지 평균 리뷰수가 2,000개 이하인 행만 표시합니다.",
                )
            with filters[2]:
                market = st.multiselect("시장 규모", GRADES, placeholder="모든 등급")
            with filters[3]:
                competition = st.multiselect("경쟁 강도", GRADES, placeholder="모든 등급")
        rows = db.get_coupang_keywords(
            day, tier1, tier2, hide_brands=hide_brands, min_volume=min_volume, max_reviews=max_reviews,
            market_grades=market, competition_grades=competition,
        )
        if not rows:
            st.info("선택한 날짜와 필터에 맞는 스냅샷이 없습니다.")
            return
        cdp_count = sum(row["sample_source"] == "ego_lite" for row in rows)
        csv_count = sum(row["volume_source"] == "csv" for row in rows)
        st.caption(
            f"{day:%Y.%m.%d} KST · {len(rows)}개 · 월 검색량순 · "
            f"1P 관측 {cdp_count} / 예시 {len(rows) - cdp_count} · 월 검색량 CSV {csv_count} / 예시 {len(rows) - csv_count}"
        )
        render_coupang_table(rows)
        render_coupang_selection(rows)
        with st.popover("지표 산정 기준"):
            st.write(
                "시장 규모는 월 검색량과 첫 페이지 평균 가격의 곱으로 비교합니다. "
                "경쟁 강도는 등록 상품 수 대비 검색 수요와 평균 리뷰수를 반영하며, ‘매우 좋음’은 경쟁 부담이 낮음을 뜻합니다."
            )
            st.caption(
                "윙 비율은 첫 페이지 최대 36개 상품의 배송 표시 기준입니다. "
                "판매자로켓과 판매자 배송을 포함하며, 쿠팡 직매입 로켓배송을 제외합니다. 배송 표시가 없는 상품은 일반배송으로 추정합니다."
            )


def channel_bars(row: dict) -> str:
    bars = []
    for channel, label in CHANNEL_SHORT.items():
        score = row[f"{channel}_score"]
        number = f"{score:.1f}" if score is not None else "—"
        width = score if score is not None else 0
        bars.append(
            f'<div class="radar-channel" title="{CHANNEL_LABELS[channel]} · '
            f'가중치 {CHANNEL_WEIGHTS[channel]:.0%}"><p>{label}<b>{number}</b></p>'
            f'<div class="radar-bar"><i style="width:{width:.1f}%"></i></div></div>'
        )
    return "".join(bars)


def render_card(row: dict, language: str) -> None:
    category = CATEGORY_SHORT[row["category"]]
    color = CATEGORY_COLORS[category]
    with st.container(border=True, key=f'sku-{row["sku"]}'):
        st.html(f"""
        <div class="radar-card-meta">
          <span class="radar-category" style="--category-color:{color}">{category} · {escape(row["kind"])}</span>
          <span>{escape(row["sku"])} · {local_stamp(row["observed_at"], "%m.%d %H:%M")}</span>
        </div>
        <div class="radar-card-top">
          <h3>{escape(row["name"])}</h3>
          <div class="radar-signal"><b>{row["trend_index"]:.1f}</b><span>종합 지수</span></div>
        </div>
        <p class="radar-spec">{escape(row["spec"])}</p>
        <div class="radar-channels">{channel_bars(row)}</div>
        <div class="radar-prices">
          <div class="radar-price"><span>1688 예상 단가</span>
            <b>CN¥{row["unit_min_rmb"]:g}–{row["unit_max_rmb"]:g}</b>
            <small>₩{row["unit_min_krw"]:,}–{row["unit_max_krw"]:,}</small></div>
          <div class="radar-price"><span>국내 목표 판매가</span><b>₩{row["target_price_krw"]:,}</b></div>
          <div class="radar-price radar-margin"><span>예상 마진율</span><b>{row["margin_pct"]:.1f}%</b></div>
        </div>
        <div class="radar-brief">
          <p><strong>4050 구매 고민</strong> {escape(row["pain_point"])}</p>
          <p><strong>마케팅 소구점</strong> {escape(row["selling_point"])}</p>
        </div>
        """)
        search_label = "중문" if language == "zh" else "영문"
        st.caption(f"1688 {search_label} 검색어")
        st.code(row[f"search_{language}"], language=None, wrap_lines=True)
        with st.expander("샘플 발주 및 원가 가정"):
            st.markdown(f'**샘플 검수**  \n{row["sample_check"]}')
            st.caption("동일 사양으로 100·300·500개 구간 단가, 최소 주문 수량과 포장 사양을 문의하세요.")
            st.caption(
                f'가정 환율 1 RMB = ₩{row["krw_per_rmb"]:g} · 물류·포장비 ₩{row["logistics_krw"]:,}/판매 단위'
                f' · 판매 수수료 {row["platform_fee_pct"]:g}%'
            )


def render_board(rows: list[dict]) -> None:
    st.subheader("1688 소싱 추천 보드")
    if not rows:
        st.info("선택한 카테고리와 기간의 소싱 후보가 없습니다.")
        return
    st.caption(f"최신 종합 지수 순 · {len(rows)}개 SKU · 검색어 오른쪽 복사 아이콘으로 1688 검색창에 붙여넣으세요.")
    with st.popover("마진율 계산 기준"):
        st.markdown(
            "예상 마진율 = (목표 판매가 − 소싱가 범위의 중간값 − 물류·포장비 − 판매 수수료) ÷ 목표 판매가 × 100"
        )
        st.caption("공급가·환율·물류비는 예시입니다. 관세, 세금, 광고 및 반품 비용은 별도입니다.")
    controls = st.columns([3, 2], vertical_alignment="bottom")
    with controls[0]:
        language = st.segmented_control(
            "검색어 언어", ("zh", "en"), default="zh", format_func={"zh": "중문", "en": "영문"}.get,
            key="search-language",
        ) or "zh"
    with controls[1]:
        show_all = len(rows) > 6 and st.toggle(f"{len(rows)}개 모두 표시", key="all-cards")
    visible = rows if show_all else rows[:6]
    for start in range(0, len(visible), 2):
        for column, row in zip(st.columns(2), visible[start:start + 2]):
            with column:
                render_card(row, language)


def render_logs(rows: list[dict]) -> None:
    st.subheader("4대 채널 수집 로그")
    st.caption("스케줄러 전체 수집 로그 · 조회 기간의 최신 80건 · 한국시간(KST)")
    if not rows:
        st.info("이 기간에 기록된 채널 로그가 없습니다.")
        return
    logs = pd.DataFrame([
        {
            "수집 시각": local_stamp(row["observed_at"]),
            "채널": CHANNEL_LABELS[row["channel"]],
            "상태": LOG_STATUS.get(row["status"], row["status"]),
            "유효 / 대상": f'{row["item_count"]} / {row["expected_count"]}',
            "평균 반응": row["avg_score"],
            "응답(ms)": row["duration_ms"],
            "출처": ORIGIN_LABELS.get(row["origin"], row["origin"]),
            "세부 내용": row["detail"],
        }
        for row in rows
    ])
    st.dataframe(
        logs, width="stretch", hide_index=True, height=310,
        column_config={"평균 반응": st.column_config.NumberColumn(format="%.1f")},
    )


@st.fragment(run_every="30s")
def render_workspace(db: RadarDatabase, days: int, category: str | None) -> None:
    try:
        now = utc_now()
        rankings = db.get_top20_keywords_by_sector(category, now=now)
        view = db.dashboard_view(days, category, now=now)
    except sqlite3.Error as exc:
        st.error(f"소싱 이력을 조회할 수 없습니다. {exc}")
        return
    render_rankings(rankings, category)
    try:
        render_coupang(db)
    except sqlite3.Error as exc:
        st.error(f"쿠팡 스냅샷을 읽을 수 없습니다. {exc}")
    render_board(view["recommendations"])
    render_logs(view["logs"])


def main() -> None:
    st.set_page_config(page_title="Sourcing Radar", layout="wide")
    st.html(STYLES)
    st.html("""
      <div class="radar-hero">
        <div><h1>Sourcing Radar</h1><p>4050 상품 트렌드와 1688 소싱 전략</p></div>
        <span class="radar-mode">시뮬레이션 · 예시 원가</span>
      </div>
    """)
    try:
        db = open_database(str(resolve_db_path()))
    except (sqlite3.Error, OSError) as exc:
        st.error(f"소싱 데이터베이스를 열 수 없습니다. {exc}")
        st.stop()

    with st.container(border=True, key="global-scope"):
        filters = st.columns([2.8, 1.4], vertical_alignment="bottom")
        with filters[0]:
            categories = {"all": "전체", **CATEGORY_LABELS}
            selected = st.pills("카테고리", tuple(categories), default="all", format_func=categories.get, key="category")
        with filters[1]:
            days = st.segmented_control(
                "조회 기간", tuple(PERIOD_LABELS), default=30, format_func=PERIOD_LABELS.get, key="period",
                help="KPI, 추천 보드와 수집 로그의 기간입니다. 검색어 랭킹은 항상 최근 30일을 집계합니다.",
            ) or 30
    category = selected if selected and selected != "all" else None
    render_kpis(db, days, category)
    render_workspace(db, days, category)


if __name__ == "__main__":
    main()

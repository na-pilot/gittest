"""Streamlit 소싱 관제판. 수집은 스케줄러가 담당하고 화면은 SQLite를 조회한다."""

from __future__ import annotations

import html
import math
import sqlite3
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from database import KST, RadarDatabase, resolve_db_path, utc_now
from sub_agents import CATEGORY_LABELS, CHANNEL_LABELS, CHANNEL_WEIGHTS


PERIOD_LABELS = {1: "오늘 하루", 7: "최근 7일", 30: "최근 30일"}
CATEGORY_SHORT = {"industrial": "공산품", "beauty": "뷰티", "food": "일반식품"}
CATEGORY_COLORS = {"공산품": "#4FE2B4", "뷰티": "#B09AFF", "일반식품": "#F9BF6B"}
CHANNEL_SHORT = {"naver": "네이버", "youtube": "유튜브", "instagram": "인스타그램", "google": "구글"}
LOG_STATUS = {"ok": "완료", "partial": "일부 수집", "empty": "신호 없음", "error": "오류"}
ORIGIN_LABELS = {"seed": "초기 데모", "simulation": "시뮬레이션 배치", "live": "실수집"}

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


def make_chart(rows: list[dict], days: int, category: str | None) -> go.Figure:
    frame = pd.DataFrame(rows)
    frame["time"] = pd.to_datetime(frame["bucket"])
    frame["label"] = frame["category"].map(CATEGORY_SHORT)
    hover_columns = []
    for channel in CHANNEL_WEIGHTS:
        column = f"{channel}_hover"
        frame[column] = frame[f"{channel}_score"].map(lambda value: f"{value:.1f}" if pd.notna(value) else "—")
        hover_columns.append(column)
    fig = px.line(
        frame, x="time", y="trend_index", color="label", markers=True,
        category_orders={"label": list(CATEGORY_COLORS)},
        color_discrete_map=CATEGORY_COLORS, custom_data=["observations", *hover_columns],
    )
    date_format = "%m.%d %H:%M" if days == 1 else "%m.%d"
    fig.update_traces(
        line={"width": 2.7}, marker={"size": 5 if days == 1 else 4},
        hovertemplate=(
            f"<b>%{{fullData.name}} · %{{x|{date_format}}} KST</b>"
            "<br>종합 지수 %{y:.1f} · 관측 %{customdata[0]}건"
            "<br>네이버 %{customdata[1]} · 유튜브 %{customdata[2]}"
            "<br>인스타그램 %{customdata[3]} · 구글 %{customdata[4]}<extra></extra>"
        ),
    )
    fig.update_layout(
        template="plotly_dark", height=330, margin={"l": 12, "r": 16, "t": 10, "b": 12},
        paper_bgcolor="#111a28", plot_bgcolor="#111a28", font={"color": "#d1ddec", "size": 12},
        legend=go.layout.Legend(orientation="h", y=1.04, yanchor="bottom", x=0, title_text=""),
        hoverlabel=go.layout.Hoverlabel(bgcolor="#1c2a3f", font_color="#eef5ff", bordercolor="#435574"),
        hovermode="closest", uirevision=f"{days}:{category}",
    )
    fig.update_xaxes(
        title=None, tickformat="%H:%M" if days == 1 else "%m.%d",
        gridcolor="#233147", linecolor="#233147", showspikes=True, spikemode="across",
        spikesnap="cursor", spikedash="dot", spikecolor="#5a708d", nticks=9,
    )
    fig.update_yaxes(title="종합 지수", range=[0, 100], dtick=20, gridcolor="#233147", zeroline=False)
    return fig


def render_chart(rows: list[dict], days: int, category: str | None) -> None:
    with st.container(border=True, key=f"trend-panel-{days}"):
        st.markdown(f"**{PERIOD_LABELS[days]}**")
        if not rows:
            st.info("이 기간에 수집된 트렌드가 없습니다.")
            return
        aggregation = "10분 구간 평균" if days == 1 else "일별 평균"
        st.caption(f"{aggregation} · 한국시간(KST) · 0–100점")
        st.plotly_chart(
            make_chart(rows, days, category), width="stretch", theme=None,
            config={"displaylogo": False, "scrollZoom": False}, key=f"trend-chart-{days}",
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
def render_workspace(db: RadarDatabase, days: int, category: str | None, compare: bool) -> None:
    try:
        view = db.dashboard_view(days, category, compare=compare)
    except sqlite3.Error as exc:
        st.error(f"소싱 이력을 조회할 수 없습니다. {exc}")
        return
    st.subheader("트렌드 추이")
    if compare:
        for column, span in zip(st.columns(2), (1, 30)):
            with column:
                render_chart(view["series"][span], span, category)
    else:
        render_chart(view["series"][days], days, category)
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
        filters = st.columns([2.8, 1.4, 1], vertical_alignment="bottom")
        with filters[0]:
            categories = {"all": "전체", **CATEGORY_LABELS}
            selected = st.pills("카테고리", tuple(categories), default="all", format_func=categories.get, key="category")
        with filters[1]:
            days = st.segmented_control(
                "조회 기간", tuple(PERIOD_LABELS), default=30, format_func=PERIOD_LABELS.get, key="period",
            ) or 30
        with filters[2]:
            compare = st.toggle("오늘·30일 함께 보기", key="compare")
    category = selected if selected and selected != "all" else None
    render_kpis(db, days, category)
    render_workspace(db, days, category, compare)


if __name__ == "__main__":
    main()

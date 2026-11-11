"""상품 분류, 채널 신호 수집, 소싱 전략을 담당하는 에이전트."""

from __future__ import annotations

import math
import random
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from statistics import mean
from typing import Protocol


CATEGORY_LABELS = {
    "industrial": "4050 프리미엄 공산품",
    "beauty": "K-뷰티 기초/디바이스",
    "food": "4050 스틱/분말 일반식품",
}
CHANNEL_LABELS = {
    "naver": "네이버 쇼핑/데이터랩",
    "youtube": "유튜브 인기 급상승",
    "instagram": "인스타그램 릴스 해시태그",
    "google": "구글 트렌드",
}
CHANNEL_WEIGHTS = {"naver": 0.35, "youtube": 0.25, "instagram": 0.20, "google": 0.20}
MIN_CHANNELS = 2
DEFAULT_KRW_PER_RMB = 200.0
DEFAULT_PLATFORM_FEE_PCT = 12.0

ALLOWED_KINDS = {
    "industrial": {"생활", "차량", "비의료 웰니스"},
    "beauty": {"기초 스킨케어", "홈케어 기기", "안티에이징", "두피 기기", "바디 기기"},
    "food": {"단백질 분말", "효소 스틱", "콜라겐 스틱", "곡물 분말", "곡물 스틱"},
}


@dataclass(frozen=True)
class Candidate:
    sku: str
    name: str
    category: str
    kind: str
    target_price_krw: int
    unit_cost_rmb: tuple[float, float]
    logistics_krw: int
    baseline: float
    search_zh: str
    search_en: str
    spec: str
    pain_point: str
    selling_point: str
    sample_check: str
    audience: str = "4050"
    bulk_suitable: bool = False
    food_class: str | None = None


# 공급사 견적과 실제 반응 데이터가 없는 예시 카탈로그. 원가는 아래 판매 단위 기준.
DEMO_CATALOG = (
    Candidate(
        sku="IND-01", name="충전식 보풀 제거기", category="industrial", kind="생활",
        target_price_krw=49_900, unit_cost_rmb=(32, 48), logistics_krw=6_500, baseline=73,
        search_zh="充电式 毛球修剪器 大刀头 可更换刀网",
        search_en="rechargeable fabric shaver replaceable large blade guard",
        spec="본체 1개 + 여분 칼망 1개 · USB-C 충전 · 분리형 먼지통",
        pain_point="아끼는 니트와 코트가 보풀 때문에 낡아 보여 교체 비용이 든다.",
        selling_point="새 옷 구매 전에 옷장 속 니트를 손질하는 모습을 반쪽 비교로 보여준다.",
        sample_check="니트 세 종류로 원단 걸림과 칼망 발열을 확인하고 여분 칼망 공급가를 따로 받는다.",
        bulk_suitable=True,
    ),
    Candidate(
        sku="IND-02", name="차량용 통풍 허리 쿠션", category="industrial", kind="차량",
        target_price_krw=69_000, unit_cost_rmb=(42, 63), logistics_krw=9_500, baseline=67,
        search_zh="汽车 腰靠 记忆棉 透气 网布 可拆洗",
        search_en="car lumbar cushion memory foam breathable washable cover",
        spec="쿠션 1개 · 메모리폼 · 분리 세탁 커버 · 조절 스트랩",
        pain_point="오래 운전하면 등과 좌석 사이가 뜨고 쿠션이 자꾸 밀린다.",
        selling_point="차종별 장착 사진과 스트랩 고정 방법을 보여주고 실제 두께를 수치로 표기한다.",
        sample_check="운전석 두 종류에서 밀림과 시트 간섭을 확인하고 압축 포장 후 복원 시간을 잰다.",
        bulk_suitable=True,
    ),
    Candidate(
        sku="IND-03", name="타이머형 온열 무릎 랩", category="industrial", kind="비의료 웰니스",
        target_price_krw=89_000, unit_cost_rmb=(58, 82), logistics_krw=10_500, baseline=76,
        search_zh="USB 加热护膝 定时 三档温度 可调绑带",
        search_en="USB heated knee wrap timer three heat levels adjustable strap",
        spec="무릎 랩 1개 · 3단 온도 · 자동 종료 · 둘레 조절 밴드",
        pain_point="저녁에 앉아서 쉴 때 무릎 주변이 쌀쌀하고 담요는 쉽게 흘러내린다.",
        selling_point="소파에서 밴드를 감고 타이머를 설정하는 저녁 루틴을 보여준다.",
        sample_check="표면 온도 편차와 자동 종료를 시험하고, 어댑터 포함 여부와 전원 사양을 견적서에 명시한다.",
        bulk_suitable=True,
    ),
    Candidate(
        sku="IND-04", name="길이 조절 전동 청소 브러시", category="industrial", kind="생활",
        target_price_krw=79_000, unit_cost_rmb=(47, 72), logistics_krw=11_500, baseline=65,
        search_zh="无线 电动清洁刷 伸缩长柄 浴室 多刷头",
        search_en="cordless bathroom scrubber telescopic handle interchangeable heads",
        spec="본체 1개 · 길이 조절 손잡이 · 욕실용 브러시 헤드 3종",
        pain_point="욕실 바닥과 줄눈을 닦을 때 오래 숙여야 하고 솔이 금방 닳는다.",
        selling_point="서 있는 자세에서 벽과 바닥을 닦는 장면으로 손잡이 길이와 헤드 교체를 설명한다.",
        sample_check="젖은 타일에서 토크와 손잡이 유격을 보고 헤드별 교체 MOQ와 포장 부피를 확인한다.",
        bulk_suitable=True,
    ),
    Candidate(
        sku="BEA-01", name="무향 펩타이드 보습 크림", category="beauty", kind="기초 스킨케어",
        target_price_krw=49_000, unit_cost_rmb=(18, 30), logistics_krw=6_800, baseline=74,
        search_zh="无香 肽 保湿 面霜 50g 化妆品 ODM",
        search_en="fragrance free peptide moisturizing cream 50g private label ODM",
        spec="크림 50g 1개 · 무향 제형 의뢰 · 공기 유입을 줄이는 펌프 용기",
        pain_point="세안 후 당김이 신경 쓰이지만 향과 끈적임이 강한 크림은 손이 가지 않는다.",
        selling_point="손등 발림과 흡수 후 마무리를 보여주고, 기초 루틴의 마지막 단계로 제안한다.",
        sample_check="제형별 전성분과 배치 문서를 받고 펌프 토출량, 향, 고온 보관 후 분리 여부를 비교한다.",
    ),
    Candidate(
        sku="BEA-02", name="듀얼 헤드 홈케어 마사지기", category="beauty", kind="홈케어 기기",
        target_price_krw=89_000, unit_cost_rmb=(68, 102), logistics_krw=10_000, baseline=79,
        search_zh="家用 美容 按摩仪 双滚轮 三档 USB 充电",
        search_en="home facial massager dual roller three levels USB rechargeable",
        spec="본체 1개 · 듀얼 롤러 · 강도 3단 · 전용 파우치",
        pain_point="여러 단계의 홈케어는 번거롭고 사용법이 복잡한 기기는 방치하게 된다.",
        selling_point="세안 후 사용 순서를 15초 영상으로 시연하고 한 손 조작과 보관을 강조한다.",
        sample_check="롤러 표면 마감과 머리카락 끼임을 확인하고 세척 범위와 교체 부품 단가를 받는다.",
    ),
    Candidate(
        sku="BEA-03", name="분리형 두피 세정 브러시", category="beauty", kind="두피 기기",
        target_price_krw=45_000, unit_cost_rmb=(24, 36), logistics_krw=6_500, baseline=66,
        search_zh="头皮 清洁 按摩器 硅胶 可拆卸 刷头",
        search_en="scalp cleansing massager detachable silicone brush head",
        spec="브러시 1개 + 헤드 2종 · 부드러운 실리콘 · 물 빠짐 받침",
        pain_point="손톱으로 두피를 문지르기 부담스럽고 브러시에 물과 샴푸가 남는다.",
        selling_point="헤드 분리부터 헹굼과 건조까지 보여주고, 손에 쥔 크기를 영상에 담는다.",
        sample_check="젖은 손에서의 미끄러짐, 모발 엉킴, 분해 후 건조 시간을 체크한다.",
    ),
    Candidate(
        sku="BEA-04", name="세라마이드 보습 앰플", category="beauty", kind="기초 스킨케어",
        target_price_krw=48_000, unit_cost_rmb=(20, 32), logistics_krw=7_300, baseline=71,
        search_zh="神经酰胺 保湿 精华液 30ml 无香 ODM",
        search_en="ceramide hydrating serum fragrance free 30ml cosmetic ODM",
        spec="앰플 30mL 1개 · 무향 의뢰 · 잠금형 펌프",
        pain_point="기초 제품을 여러 겹 쓰면 밀리고 낮에는 메이크업 전에 사용하기 부담스럽다.",
        selling_point="한 펌프 양과 크림을 덧바를 때의 마무리를 보여주는 짧은 제형 리뷰를 만든다.",
        sample_check="샘플의 점도와 밀림을 비교하고 펌프 누액, 용기 호환성, 배치 표기 위치를 확인한다.",
    ),
    Candidate(
        sku="FOD-01", name="식물성 단백질 분말", category="food", kind="단백질 분말",
        target_price_krw=36_000, unit_cost_rmb=(28, 42), logistics_krw=7_500, baseline=71,
        search_zh="植物蛋白粉 豌豆蛋白 原味 无添加糖 400克 OEM",
        search_en="plain pea plant protein powder 400g no added sugar OEM",
        spec="분말 400g 1통 · 완두 단백 베이스 · 무향 또는 곡물 맛 샘플",
        pain_point="아침 준비 시간이 짧고 달거나 향이 강한 쉐이크는 매일 먹기 부담스럽다.",
        selling_point="우유나 두유에 한 스푼 섞는 과정을 보여주고, 원재료와 1회 분량을 안내한다.",
        sample_check="원재료·알레르기 자료와 일반식품 분류 근거를 받고, 찬물 용해도와 끝맛을 비교한다.",
        food_class="일반식품",
    ),
    Candidate(
        sku="FOD-02", name="볶은 곡물 미숫가루 스틱", category="food", kind="곡물 스틱",
        target_price_krw=29_900, unit_cost_rmb=(23, 34), logistics_krw=6_000, baseline=75,
        search_zh="烘焙 杂粮粉 独立条包 20条 冲调谷物 OEM",
        search_en="roasted multigrain instant powder 20 single serve stick packs OEM",
        spec="20g × 20포 1박스 · 볶은 곡물 베이스 · 낱개 밀봉",
        pain_point="익숙한 미숫가루를 마시고 싶지만 계량과 보관이 번거롭다.",
        selling_point="출근 가방에서 한 포를 꺼내 물에 타는 장면과 1포당 가격을 보여준다.",
        sample_check="곡물 배합과 알레르기 자료, 낱포 실링 강도, 찬물에서 뭉침 여부를 확인한다.",
        food_class="일반식품",
    ),
    Candidate(
        sku="FOD-03", name="곡물 발효 분말 스틱", category="food", kind="효소 스틱",
        target_price_krw=34_900, unit_cost_rmb=(29, 43), logistics_krw=6_300, baseline=63,
        search_zh="谷物发酵粉 谷物酵素粉 条包 普通食品 30条 OEM",
        search_en="fermented grain enzyme powder ordinary food 30 stick packs OEM",
        spec="3g × 30포 1박스 · 곡물 발효분말 배합 · 일반식품 사양",
        pain_point="큰 통의 분말은 휴대가 어렵고 발효 맛이 강하면 꾸준히 먹기 어렵다.",
        selling_point="요거트에 한 포를 섞는 레시피를 보여주고, 맛과 포장 편의성에 집중한다.",
        sample_check="발효 원료와 배합표, 일반식품 분류 근거를 요청하고 맛과 포장 방습성을 확인한다.",
        food_class="일반식품",
    ),
    Candidate(
        sku="FOD-04", name="콜라겐 분말 스틱", category="food", kind="콜라겐 스틱",
        target_price_krw=39_900, unit_cost_rmb=(32, 50), logistics_krw=7_200, baseline=72,
        search_zh="胶原蛋白肽 固体饮料 粉末 条包 普通食品 20条",
        search_en="collagen peptide drink powder ordinary food 20 single serve sticks",
        spec="5g × 20포 1박스 · 음료에 섞는 분말 · 일반식품 사양",
        pain_point="정량을 따로 잴 필요 없이 익숙한 음료에 섞을 수 있는 소포장을 원한다.",
        selling_point="커피나 두유에 섞는 장면과 1포 분량을 보여주고, 향과 용해감을 비교한다.",
        sample_check="콜라겐 원료의 출처와 알레르기 자료, 일반식품 분류 근거, 낱포 표시 정보를 확인한다.",
        food_class="일반식품",
    ),
)


def eligible(candidate: Candidate) -> bool:
    if candidate.audience != "4050" or candidate.kind not in ALLOWED_KINDS.get(candidate.category, ()):
        return False
    if candidate.category == "industrial":
        return candidate.bulk_suitable and 40_000 <= candidate.target_price_krw <= 100_000
    if candidate.category == "food":
        return candidate.food_class == "일반식품"
    return True


class ChannelProvider(Protocol):
    def fetch(self, candidates: Sequence[Candidate], observed_at: datetime) -> Mapping[str, float]:
        """Return normalized 0–100 reaction scores keyed by SKU; omit unavailable scores."""
        ...


class SimulationChannelProvider:
    # 각 채널에서 구매 의도, 시연 영상, 릴스 노출 등이 더 두드러지는 카테고리.
    CHANNEL_BIAS = {
        "naver": (6, 2, 3),
        "youtube": (2, -2, 6),
        "instagram": (-4, 8, -2),
        "google": (0, 1, 0),
    }

    def __init__(self, channel: str):
        self.channel = channel

    def fetch(self, candidates: Sequence[Candidate], observed_at: datetime) -> dict[str, float]:
        day = observed_at.timestamp() / 86_400
        sample = int(observed_at.timestamp() // 60)
        scores = {}
        for item in candidates:
            group = tuple(CATEGORY_LABELS).index(item.category)
            category_wave = 9 * math.sin(day * math.tau / 17 + group * 2)
            weekly_wave = 5 * math.cos(day * math.tau / 7 + group)
            item_phase = random.Random(item.sku).uniform(0, math.tau)
            item_wave = 4 * math.sin(day * math.tau / 9 + item_phase)
            noise = random.Random(f"{self.channel}:{item.sku}:{sample}").uniform(-5, 5)
            score = (item.baseline + category_wave + weekly_wave + item_wave
                     + self.CHANNEL_BIAS[self.channel][group] + noise)
            scores[item.sku] = round(max(0, min(100, score)), 1)
        return scores


@dataclass(frozen=True)
class ChannelReport:
    channel: str
    status: str
    item_count: int
    expected_count: int
    avg_score: float | None
    duration_ms: int
    detail: str


@dataclass(frozen=True)
class TrendSignal:
    candidate: Candidate
    channel_scores: dict[str, float | None]
    trend_index: float


class MultiChannelTrendScout:
    def __init__(
        self,
        providers: Mapping[str, ChannelProvider] | None = None,
        catalog: Sequence[Candidate] = DEMO_CATALOG,
    ):
        self.candidates = tuple(item for item in catalog if eligible(item))
        if len({item.sku for item in self.candidates}) != len(self.candidates):
            raise ValueError("The eligible catalog contains duplicate SKUs.")
        self.providers = (
            {channel: SimulationChannelProvider(channel) for channel in CHANNEL_WEIGHTS}
            if providers is None else dict(providers)
        )
        if set(self.providers) != set(CHANNEL_WEIGHTS):
            raise ValueError("Configure exactly naver, youtube, instagram, and google providers.")

    def collect(
        self,
        observed_at: datetime,
        on_channel: Callable[[ChannelReport], None] | None = None,
    ) -> tuple[list[TrendSignal], list[ChannelReport]]:
        by_channel = {}
        reports = []
        expected = len(self.candidates)
        for channel in CHANNEL_WEIGHTS:
            started = time.monotonic()
            valid = {}
            try:
                raw = self.providers[channel].fetch(self.candidates, observed_at)
                invalid = 0
                for item in self.candidates:
                    value = raw.get(item.sku)
                    if value is None:
                        continue
                    try:
                        score = float(value)
                    except (TypeError, ValueError, OverflowError):
                        invalid += 1
                        continue
                    if isinstance(value, bool) or not 0 <= score <= 100:
                        invalid += 1
                        continue
                    valid[item.sku] = score
                missing = expected - len(valid) - invalid
                status = "empty"
                if valid:
                    status = "ok" if len(valid) == expected else "partial"
                detail = f"누락 {missing}건 · 범위/형식 오류 {invalid}건" if missing or invalid else "수집 완료"
                if not expected:
                    detail = "적격 SKU 없음"
            except Exception as exc:
                valid = {}
                status = "error"
                # 공급자 예외 메시지에 인증값이 포함될 수 있어 오류 유형만 기록한다.
                detail = f"공급자 요청 실패 ({type(exc).__name__})"
            by_channel[channel] = valid
            report = ChannelReport(
                channel, status, len(valid), expected, round(mean(valid.values()), 1) if valid else None,
                round((time.monotonic() - started) * 1_000), detail,
            )
            reports.append(report)
            if on_channel is not None:
                on_channel(report)

        signals = []
        for item in self.candidates:
            scores = {channel: values.get(item.sku) for channel, values in by_channel.items()}
            available = {channel: score for channel, score in scores.items() if score is not None}
            if len(available) < MIN_CHANNELS:
                continue
            weight = sum(CHANNEL_WEIGHTS[channel] for channel in available)
            weighted_score = sum(score * CHANNEL_WEIGHTS[channel] for channel, score in available.items())
            signals.append(TrendSignal(item, scores, round(weighted_score / weight, 1)))
        return signals, reports


@dataclass(frozen=True)
class SourcingPlan:
    sku: str
    name: str
    category: str
    kind: str
    pain_point: str
    selling_point: str
    search_zh: str
    search_en: str
    spec: str
    sample_check: str
    unit_min_rmb: float
    unit_max_rmb: float
    unit_min_krw: int
    unit_max_krw: int
    logistics_krw: int
    platform_fee_pct: float
    krw_per_rmb: float
    target_price_krw: int
    margin_pct: float


class SourcingStrategistAgent:
    def __init__(
        self,
        krw_per_rmb: float = DEFAULT_KRW_PER_RMB,
        platform_fee_pct: float = DEFAULT_PLATFORM_FEE_PCT,
    ):
        if not math.isfinite(krw_per_rmb) or krw_per_rmb <= 0:
            raise ValueError("KRW per RMB must be a positive finite number.")
        if not math.isfinite(platform_fee_pct) or not 0 <= platform_fee_pct < 100:
            raise ValueError("Platform fee must be between 0 and 100 percent, exclusive of 100.")
        self.krw_per_rmb = krw_per_rmb
        self.platform_fee_pct = platform_fee_pct

    def plan(self, signal: TrendSignal) -> SourcingPlan:
        item = signal.candidate
        min_rmb, max_rmb = item.unit_cost_rmb
        if not 0 < min_rmb <= max_rmb or item.target_price_krw <= 0 or item.logistics_krw < 0:
            raise ValueError(f"Invalid pricing inputs for {item.sku}.")
        min_krw = round(min_rmb * self.krw_per_rmb)
        max_krw = round(max_rmb * self.krw_per_rmb)
        purchase_krw = (min_krw + max_krw) / 2
        platform_fee_krw = item.target_price_krw * self.platform_fee_pct / 100
        margin = (item.target_price_krw - purchase_krw - item.logistics_krw - platform_fee_krw)
        return SourcingPlan(
            sku=item.sku, name=item.name, category=item.category, kind=item.kind,
            pain_point=item.pain_point, selling_point=item.selling_point,
            search_zh=item.search_zh, search_en=item.search_en, spec=item.spec, sample_check=item.sample_check,
            unit_min_rmb=min_rmb, unit_max_rmb=max_rmb, unit_min_krw=min_krw, unit_max_krw=max_krw,
            logistics_krw=item.logistics_krw, platform_fee_pct=self.platform_fee_pct,
            krw_per_rmb=self.krw_per_rmb, target_price_krw=item.target_price_krw,
            margin_pct=round(margin / item.target_price_krw * 100, 1),
        )

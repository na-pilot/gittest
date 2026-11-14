"""쿠팡 일일 키워드의 데모 생성과 Ego Lite의 첫 검색 결과 페이지 수집."""

from __future__ import annotations

import csv
import math
import os
import random
import re
import unicodedata
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean
from threading import Event
from urllib.request import ProxyHandler, build_opener
from urllib.parse import urlencode, urlsplit

from coupang_catalog import COUPANG_CATEGORIES, TOP_KEYWORDS


CDP_ENDPOINT = "http://127.0.0.1:9222"
CONNECT_TIMEOUT_MS = 2_500
PAGE_TIMEOUT_MS = 7_000
REQUEST_PAUSE_SECONDS = 1.3
PAGE_SIZE = 36
COUPANG_DAILY_LIVE_LIMIT = TOP_KEYWORDS
COUPANG_MANUAL_LIVE_LIMIT = TOP_KEYWORDS
GRADES = ("매우 좋음", "좋음", "보통", "나쁨", "매우 나쁨")
SAMPLE_SOURCE_LABELS = {"seed": "초기 데모", "simulation": "시뮬레이션", "ego_lite": "Ego Lite 1P 관측"}
VOLUME_SOURCE_LABELS = {"estimate": "월 검색량 예시", "csv": "월 검색량 CSV"}
BRANDS = (
    "삼성", "갤럭시", "엘지전자", "LG", "애플", "Apple", "아이폰", "아이패드", "맥세이프",
    "샤오미", "Xiaomi", "다이슨", "Dyson", "필립스", "Philips", "테팔", "Tefal", "로지텍", "Logitech",
    "앤커", "Anker", "코멧", "락앤락", "브리타", "쿠쿠", "쿠첸", "한샘", "이케아", "IKEA",
    "아토젯", "다우니", "피죤", "3M", "아모레퍼시픽", "설화수", "이니스프리", "라운드랩", "닥터지",
    "라보에이치", "미쟝센", "일리윤", "뉴트로지나", "라로슈포제", "피지오겔", "세타필", "AHC",
    "마이프로틴", "Myprotein", "옵티멈뉴트리션", "비비고", "오뚜기", "청정원", "샘표", "담터", "오설록",
    "산과들에", "맥심", "동서식품", "켈로그", "농심", "풀무원",
    "나이키", "Nike", "아디다스", "Adidas", "뉴발란스", "유니클로", "Uniqlo", "키플링", "Kipling",
    "스케쳐스", "Skechers", "크록스", "Crocs", "노스페이스", "코베아", "네이처하이크", "멜킨",
    "타이틀리스트", "캘러웨이", "불스원", "소낙스", "아이나비", "파인뷰", "미쉐린", "Michelin",
    "현대모비스", "보쉬", "Bosch", "스타벅스", "Starbucks",
)


def normalize_keyword(keyword: str) -> str:
    return "".join(char for char in unicodedata.normalize("NFKC", keyword).casefold() if char.isalnum())


_KOREAN_BRANDS = tuple(normalize_keyword(brand) for brand in BRANDS if not brand.isascii())
_LATIN_BRANDS = re.compile(
    r"(?<![a-z0-9])(?:" + "|".join(re.escape(brand.casefold()) for brand in BRANDS if brand.isascii()) + r")(?![a-z0-9])"
)


def is_brand_keyword(keyword: str) -> bool:
    text = unicodedata.normalize("NFKC", keyword).casefold()
    tokens = re.findall(r"[가-힣]+", text)
    return (
        any("".join(tokens[index:]).startswith(brand) for index in range(len(tokens)) for brand in _KOREAN_BRANDS)
        or _LATIN_BRANDS.search(text) is not None
    )


def market_grade(monthly_search_vol: int, average_price_krw: int) -> str:
    proxy = monthly_search_vol * average_price_krw
    boundaries = (1_800_000_000, 750_000_000, 280_000_000, 90_000_000)
    return next((grade for grade, lower in zip(GRADES, boundaries) if proxy >= lower), GRADES[-1])


def competition_grade(monthly_search_vol: int, product_count: int, avg_reviews: float) -> str:
    if monthly_search_vol == 0:
        return GRADES[-1]
    pressure = 0.6 * math.log10(1 + 20 * product_count / monthly_search_vol) + 0.4 * math.log10(1 + avg_reviews / 80)
    boundaries = (0.45, 0.75, 1.05, 1.35)
    return next((grade for grade, upper in zip(GRADES, boundaries) if pressure < upper), GRADES[-1])


@dataclass(frozen=True)
class DailyKeyword:
    record_date: str
    tier1_cat: str
    tier2_cat: str
    rank: int
    keyword: str
    monthly_search_vol: int
    avg_reviews: float
    wing_ratio: float
    market_size_grade: str
    competition_grade: str
    is_brand: bool
    avg_price_krw: int
    product_count: int
    sample_count: int
    review_sample_count: int
    price_sample_count: int
    sample_source: str
    volume_source: str
    product_count_source: str
    collected_at: str


@dataclass(frozen=True)
class SearchStats:
    avg_reviews: float
    wing_ratio: float
    avg_price_krw: int
    product_count: int | None
    sample_count: int
    review_sample_count: int
    price_sample_count: int


@dataclass(frozen=True)
class CollectionResult:
    rows: list[DailyKeyword]
    observed_count: int
    requested_count: int
    note: str


def _stamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def demo_snapshot(
    day: date,
    tier1: str | None = None,
    tier2: str | None = None,
    *,
    source: str = "seed",
    volumes: Mapping[str, int] | None = None,
) -> list[DailyKeyword]:
    if tier1 is not None and tier1 not in COUPANG_CATEGORIES:
        raise ValueError("Unknown Coupang category.")
    if tier2 is not None and (tier1 is None or tier2 not in COUPANG_CATEGORIES[tier1]):
        raise ValueError("The subcategory must belong to the selected Coupang category.")
    rows = []
    stamp = _stamp()
    volumes = volumes or {}
    for category, subcategories in COUPANG_CATEGORIES.items():
        if tier1 is not None and category != tier1:
            continue
        for subcategory, seed in subcategories.items():
            if tier2 is not None and subcategory != tier2:
                continue
            candidates = []
            for position, keyword in enumerate(seed.keywords):
                key = normalize_keyword(keyword)
                base = random.Random(f"{category}:{subcategory}:{key}")
                drift = random.Random(f"{day}:{category}:{subcategory}:{key}")
                estimated_volume = round(seed.peak_volume / (1 + position / 4.8) ** 1.18 * drift.uniform(0.84, 1.16))
                volume = volumes.get(key, estimated_volume)
                exposure = (estimated_volume / 20_000) ** 0.35
                reviews = round(base.lognormvariate(6.55, 1.12) * exposure * drift.uniform(0.91, 1.09), 1)
                products = round(max(estimated_volume * base.uniform(0.018, 1.8) * drift.uniform(0.9, 1.1), PAGE_SIZE))
                price = round(seed.typical_price_krw * base.uniform(0.47, 1.7) * drift.uniform(0.95, 1.05) / 100) * 100
                ratio = round(base.randint(4, 33) / PAGE_SIZE * 100, 1)
                candidates.append(DailyKeyword(
                    record_date=day.isoformat(), tier1_cat=category, tier2_cat=subcategory,
                    rank=0, keyword=keyword, monthly_search_vol=volume, avg_reviews=reviews, wing_ratio=ratio,
                    market_size_grade=market_grade(volume, price), competition_grade=competition_grade(volume, products, reviews),
                    is_brand=is_brand_keyword(keyword), avg_price_krw=price, product_count=products,
                    sample_count=0, review_sample_count=0, price_sample_count=0,
                    sample_source=source, volume_source="csv" if key in volumes else "estimate",
                    product_count_source="estimate", collected_at=stamp,
                ))
            candidates.sort(key=lambda row: (-row.monthly_search_vol, row.keyword))
            rows.extend(replace(row, rank=rank) for rank, row in enumerate(candidates, start=1))
    return rows


def read_volumes(path: Path) -> dict[str, int]:
    volumes = {}
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not {"keyword", "monthly_search_vol"}.issubset(reader.fieldnames or ()):
            raise ValueError("CSV columns needed: keyword, monthly_search_vol.")
        for row in reader:
            key = normalize_keyword(row["keyword"] or "")
            count = (row["monthly_search_vol"] or "").strip().replace(",", "")
            if key and count.isascii() and count.isdecimal() and len(count) <= 12:
                volumes[key] = int(count)
    return volumes


class CollectionUnavailable(RuntimeError):
    """The browser or its search page could not provide a usable observation."""


PRODUCT_SELECTOR = (
    "#productList > li.search-product, li[class*='ProductUnit_productUnit'], "
    "li[class*='productUnit'], li:has(a[href*='/vp/products/'])"
)
EXTRACT_SEARCH_PAGE = r"""
({ selector, limit }) => {
  const text = (element, selectors) => {
    for (const selector of selectors) {
      const node = element.querySelector(selector);
      if (node?.textContent.trim()) return node.textContent.trim();
    }
    return "";
  };
  const results = document.querySelector(
    '#productList, [class*="SearchProductList_productList"], [class*="productList_productList"], .search-content'
  ) ?? document;
  const cards = [];
  const seen = new Set();
  for (const node of results.querySelectorAll(selector)) {
    const id = node.querySelector('a[href*="/vp/products/"]')?.getAttribute("href")?.match(/\/vp\/products\/(\d+)/)?.[1];
    if (!id || seen.has(id)) continue;
    seen.add(id);
    const badges = [...node.querySelectorAll(
      'img[alt*="로켓"], img[src*="rocket" i], [aria-label*="배송"], [class*="deliveryBadge"], ' +
      '[class*="DeliveryBadge"], [class*="badgeRocket"], .badge, .badges'
    )].map((badge) => [
      badge.getAttribute("alt"), badge.getAttribute("aria-label"), badge.textContent,
      badge.getAttribute("src")?.split("/").pop()?.split("?")[0], badge.getAttribute("class"),
    ].filter(Boolean).join(" ")).join(" ");
    cards.push({
      reviews: text(node, [".rating-total-count", '[class*="ratingTotalCount"]', '[class*="ratingCount"]',
        '[class*="reviewCount"]', '[class*="ReviewCount"]']),
      price: text(node, [".price-value", '[class*="Price_priceValue"]', '[class*="priceValue"]',
        '[class*="SalePrice_number"]', '[class*="salePrice"]']),
      badges,
    });
    if (cards.length === limit) break;
  }
  const countNode = document.querySelector("#productList[data-total-count], [data-search-result-count]");
  const countText = text(document, [
    ".search-result-count", '[class*="searchResultCount"]', '[class*="SearchResultCount"]', ".search-query-result"
  ]);
  return { cards, count: countNode?.getAttribute("data-total-count") ??
      countNode?.getAttribute("data-search-result-count") ?? countText };
}
"""
SELLER_DELIVERY = ("판매자로켓", "로켓그로스", "sellerrocket", "rocketseller", "rocketgrowth", "판매자배송", "일반배송")
TOTAL_COUNT_PATTERN = re.compile(r"(?:검색\s*결과|전체)\s*[:(]?\s*([\d,]+)\s*(?:개|건)")


def _count(text: str) -> int | None:
    cleaned = text.strip().strip("()").replace(",", "").strip()
    if cleaned in ("리뷰 없음", "상품평 없음"):
        return 0
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(만|천)?\s*(?:개|건|원)?", cleaned, flags=re.ASCII)
    if not match:
        return None
    number, unit = match.groups()
    if len(number) > 12:
        return None
    return round(float(number) * {"만": 10_000, "천": 1_000, None: 1}[unit])


def _seller_delivery(badges: str) -> bool:
    compact = normalize_keyword(badges)
    if any(token in compact for token in SELLER_DELIVERY):
        return True
    return not any(token in compact for token in ("로켓", "rocket"))


def _page_stats(payload: dict) -> SearchStats:
    cards = payload["cards"][:PAGE_SIZE]
    if not cards:
        raise CollectionUnavailable("첫 페이지에서 상품을 확인할 수 없습니다.")
    reviews, prices = [], []
    for card in cards:
        review_count = _count(card["reviews"])
        price = _count(card["price"])
        if review_count is not None:
            reviews.append(review_count)
        if price:
            prices.append(price)
    if not reviews or not prices:
        raise CollectionUnavailable("첫 페이지의 리뷰수 또는 가격을 확인할 수 없습니다.")
    count_text = payload["count"]
    count = _count(count_text)
    if count is None:
        match = TOTAL_COUNT_PATTERN.search(count_text)
        count = _count(match[1]) if match else None
    return SearchStats(
        avg_reviews=round(mean(reviews), 1),
        wing_ratio=round(100 * sum(_seller_delivery(card["badges"]) for card in cards) / len(cards), 1),
        avg_price_krw=round(mean(prices)),
        product_count=count if count is not None and count >= len(cards) else None,
        sample_count=len(cards), review_sample_count=len(reviews), price_sample_count=len(prices),
    )


class EgoLiteCollector:
    @contextmanager
    def search_page(self) -> Iterator:
        # Avoid spawning Playwright's driver when the local browser endpoint is disabled.
        try:
            opener = build_opener(ProxyHandler({}))
            with opener.open(f"{CDP_ENDPOINT}/json/version", timeout=CONNECT_TIMEOUT_MS / 1000):
                pass
        except OSError as exc:
            raise CollectionUnavailable("Ego Lite의 로컬 CDP 포트(9222)에 연결할 수 없습니다.") from exc
        from playwright.sync_api import sync_playwright

        # Reuse the default context. Open and close only our own tab.
        with sync_playwright() as playwright:
            browser = playwright.chromium.connect_over_cdp(CDP_ENDPOINT, timeout=CONNECT_TIMEOUT_MS, no_defaults=True)
            if not browser.contexts:
                raise CollectionUnavailable("Ego Lite에서 기존 브라우저 세션을 찾지 못했습니다.")
            context = next((
                candidate for candidate in browser.contexts
                if any(urlsplit(tab.url).hostname in {"coupang.com", "www.coupang.com", "wing.coupang.com"}
                       for tab in candidate.pages)
            ), browser.contexts[0])
            page = context.new_page()
            try:
                yield page
            finally:
                with suppress(Exception):
                    page.close()

    @staticmethod
    def read_search(page, keyword: str) -> SearchStats:
        query = urlencode({"q": keyword, "page": 1, "listSize": PAGE_SIZE})
        response = page.goto(
            f"https://www.coupang.com/np/search?{query}", wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS,
        )
        destination = urlsplit(page.url)
        if (
            response is None or not response.ok
            or destination.hostname not in {"coupang.com", "www.coupang.com"}
            or destination.path != "/np/search"
        ):
            raise CollectionUnavailable("쿠팡 검색 응답 또는 로그인 상태를 확인해 주세요.")
        page.locator(PRODUCT_SELECTOR).first.wait_for(state="attached", timeout=PAGE_TIMEOUT_MS)
        payload = page.evaluate(EXTRACT_SEARCH_PAGE, {"selector": PRODUCT_SELECTOR, "limit": PAGE_SIZE})
        return _page_stats(payload)


class CoupangCollector:
    def __init__(self, volume_csv: Path | None = None):
        configured = os.environ.get("COUPANG_SEARCH_VOLUME_CSV")
        self.volume_csv = volume_csv or (Path(configured) if configured else None)

    def collect(
        self,
        day: date,
        tier1: str | None = None,
        tier2: str | None = None,
        *,
        max_live_keywords: int | None = None,
        per_category_limit: int | None = None,
        stop_event: Event | None = None,
    ) -> CollectionResult:
        notes = []
        volumes = {}
        if self.volume_csv is not None:
            try:
                volumes = read_volumes(self.volume_csv)
                if not volumes:
                    notes.append("CSV에 유효한 월 검색량이 없습니다.")
            except (OSError, ValueError, csv.Error):
                notes.append("월 검색량 CSV를 읽지 못해 예시 값을 사용했습니다.")
        rows = demo_snapshot(day, tier1, tier2, source="simulation", volumes=volumes)
        candidates = (index for index, row in enumerate(rows) if per_category_limit is None or row.rank <= per_category_limit)
        candidates = sorted(candidates, key=lambda index: -rows[index].monthly_search_vol)
        if max_live_keywords is not None:
            candidates = candidates[:max(0, max_live_keywords)]
        requested = len(candidates)
        stop = stop_event if stop_event is not None else Event()
        observed_count = 0
        if requested and not stop.is_set():
            browser = EgoLiteCollector()
            try:
                with browser.search_page() as page:
                    for position, index in enumerate(candidates):
                        if stop.is_set() or (position and stop.wait(REQUEST_PAUSE_SECONDS)):
                            break
                        row = rows[index]
                        stats = browser.read_search(page, row.keyword)
                        product_count = stats.product_count if stats.product_count is not None else row.product_count
                        rows[index] = replace(
                            row, avg_reviews=stats.avg_reviews, wing_ratio=stats.wing_ratio, avg_price_krw=stats.avg_price_krw,
                            market_size_grade=market_grade(row.monthly_search_vol, stats.avg_price_krw),
                            competition_grade=competition_grade(row.monthly_search_vol, product_count, stats.avg_reviews),
                            sample_count=stats.sample_count, review_sample_count=stats.review_sample_count,
                            price_sample_count=stats.price_sample_count, product_count=product_count,
                            sample_source="ego_lite", product_count_source="page" if stats.product_count is not None else "estimate",
                            collected_at=_stamp(),
                        )
                        observed_count += 1
            except ImportError:
                notes.append("Playwright를 설치하지 않아 시뮬레이션을 사용했습니다.")
            except CollectionUnavailable as exc:
                notes.append(f"{exc} 미확인 키워드는 시뮬레이션을 사용했습니다.")
            except Exception:
                if observed_count:
                    notes.append("Ego Lite 수집이 중단되어 남은 키워드는 시뮬레이션을 사용했습니다.")
                else:
                    notes.append("Ego Lite CDP 또는 쿠팡 검색 화면에 연결할 수 없어 시뮬레이션을 사용했습니다.")
        if max_live_keywords is not None and requested < len(rows):
            notes.append(f"즉시 갱신은 검색량 상위 {requested}개 키워드를 조회합니다.")
        elif per_category_limit == 0:
            notes.append("일일 배치를 시뮬레이션 모드로 실행했습니다.")
        elif per_category_limit is not None and requested < len(rows):
            notes.append(f"일일 브라우저 조회는 중분류별 상위 {per_category_limit}개를 대상으로 합니다.")
        return CollectionResult(rows, observed_count, requested, " ".join(notes))

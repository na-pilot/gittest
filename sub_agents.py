"""상품 분류, 채널 신호 수집, 소싱 전략을 담당하는 에이전트."""

from __future__ import annotations

import math
import random
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from statistics import mean
from typing import Protocol
from zlib import crc32


CATEGORY_LABELS = {
    "industrial": "4050 프리미엄 공산품",
    "beauty": "K-뷰티 기초/디바이스",
    "food": "4050 스틱/분말 일반식품",
}
KEYWORDS_PER_SECTOR = 20
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
    focus_channel: str | None = None


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
        sku="IND-05", name="차량용 무선 진공청소기", category="industrial", kind="차량",
        target_price_krw=79_000, unit_cost_rmb=(48, 72), logistics_krw=9_000, baseline=77,
        search_zh="车载 无线 吸尘器 可拆滤芯 缝隙吸嘴", search_en="cordless car vacuum washable filter crevice nozzle",
        spec="본체 1개 · 틈새 노즐 · 세척형 필터 · USB-C 충전",
        pain_point="시트 레일과 컵홀더에 과자 부스러기가 들어가면 큰 청소기로 꺼내기 어렵다.",
        selling_point="시트 틈의 부스러기를 노즐로 빨아들이고 먼지통을 비우는 장면을 이어 보여준다.",
        sample_check="필터 장착 유격, 완충 사용 시간과 모래 흡입 후 먼지통 누출을 점검한다.",
        bulk_suitable=True, focus_channel="youtube",
    ),
    Candidate(
        sku="IND-06", name="접이식 차량 트렁크 정리함", category="industrial", kind="차량",
        target_price_krw=59_000, unit_cost_rmb=(38, 61), logistics_krw=11_000, baseline=68,
        search_zh="汽车 后备箱 收纳箱 折叠 带盖 防滑", search_en="foldable car trunk organizer lid anti slip",
        spec="정리함 1개 · 45L 예시 · 분리 칸막이 · 덮개 · 바닥 고정 패드",
        pain_point="장보기 봉투와 세차 용품이 섞이고 주행 중 트렁크에서 굴러다닌다.",
        selling_point="생수와 장바구니를 분리 수납하고 빈 정리함을 접는 실제 부피를 보여준다.",
        sample_check="손잡이 하중과 칸막이 고정력을 확인하고 접힌 포장 치수를 요청한다.",
        bulk_suitable=True, focus_channel="naver",
    ),
    Candidate(
        sku="IND-07", name="디지털 타이어 공기주입기", category="industrial", kind="차량",
        target_price_krw=89_000, unit_cost_rmb=(65, 94), logistics_krw=10_000, baseline=80,
        search_zh="车载 充气泵 数显 自动停止 预设胎压", search_en="digital tire inflator pressure preset auto stop",
        spec="본체 1개 · 수치 표시 · 압력 설정 후 자동 종료 · 밸브 어댑터",
        pain_point="공기압 경고가 뜨면 어디서 수치를 확인하고 얼마나 넣을지 헷갈린다.",
        selling_point="밸브를 연결하고 표시창에서 목표 압력과 현재 압력을 확인하는 과정을 보여준다.",
        sample_check="기준 게이지와의 편차, 연속 사용 중 발열과 호스 체결을 점검한다.",
        bulk_suitable=True, focus_channel="google",
    ),
    Candidate(
        sku="IND-08", name="차량 앞유리 단열 차양", category="industrial", kind="차량",
        target_price_krw=49_900, unit_cost_rmb=(31, 46), logistics_krw=6_900, baseline=72,
        search_zh="汽车 前挡风 遮阳挡 折叠 隔热 多层", search_en="folding car windshield multilayer sun shade",
        spec="차양 1개 · 다층 단열재 · 사이즈 2종 · 보관 밴드",
        pain_point="야외 주차 후 운전대와 앞좌석이 뜨겁고 큰 차양은 보관이 불편하다.",
        selling_point="차종별 유리 실측표와 펼쳤을 때 덮이는 범위를 사진으로 안내한다.",
        sample_check="가장자리 들뜸, 접힘 내구성, 햇볕 노출 후 원단 냄새를 확인한다.",
        bulk_suitable=True, focus_channel="google",
    ),
    Candidate(
        sku="IND-09", name="차량용 타이머 열선 방석", category="industrial", kind="차량",
        target_price_krw=69_000, unit_cost_rmb=(42, 68), logistics_krw=8_500, baseline=76,
        search_zh="汽车 加热坐垫 12V 定时 三档 防滑", search_en="12V heated car seat cushion timer non slip",
        spec="방석 1개 · 12V 전원 · 온도 3단 · 종료 타이머 · 미끄럼 방지 바닥",
        pain_point="겨울 첫 출근 때 좌석이 차갑고 방석이 승하차할 때 따라 움직인다.",
        selling_point="좌석에 고정하는 방법과 전원 플러그, 단계별 표시등을 실제 차량에서 보여준다.",
        sample_check="표면 온도 편차, 타이머와 전원 케이블 피복을 확인한다.",
        bulk_suitable=True, focus_channel="youtube",
    ),
    Candidate(
        sku="IND-10", name="논슬립 접이식 2단 발판", category="industrial", kind="생활",
        target_price_krw=54_900, unit_cost_rmb=(37, 59), logistics_krw=11_500, baseline=66,
        search_zh="家用 两步 折叠梯 防滑 加宽 踏板", search_en="foldable two step stool wide anti slip steps",
        spec="2단 발판 1개 · 논슬립 패드 · 접힘 잠금 · 운반 손잡이",
        pain_point="높은 수납장을 쓸 때 의자를 옮기기 번거롭고 발 디딜 곳이 좁다.",
        selling_point="발판의 실제 폭과 접었을 때 가구 옆에 들어가는 깊이를 함께 보여준다.",
        sample_check="펼침 잠금, 바닥 흔들림, 발판 하중 시험 자료와 모서리 마감을 확인한다.",
        bulk_suitable=True, focus_channel="naver",
    ),
    Candidate(
        sku="IND-11", name="스테인리스 진공 밀폐용기 세트", category="industrial", kind="생활",
        target_price_krw=79_000, unit_cost_rmb=(55, 81), logistics_krw=9_500, baseline=78,
        search_zh="不锈钢 真空 保鲜盒 三件套 手动抽气泵", search_en="stainless vacuum food container three piece hand pump set",
        spec="용기 3개 · 수동 펌프 · 분리 세척 패킹 · 용량 표기",
        pain_point="남은 반찬을 담는 용기의 패킹에 냄새가 배고 크기가 제각각이라 쌓기 어렵다.",
        selling_point="뚜껑 잠금, 펌프 사용, 패킹 분리와 냉장고 적층 모습을 짧게 보여준다.",
        sample_check="식품 접촉 소재 자료, 뚜껑 누설과 여분 패킹 공급 조건을 확인한다.",
        bulk_suitable=True, focus_channel="instagram",
    ),
    Candidate(
        sku="IND-12", name="슬라이딩 싱크대 하부 선반", category="industrial", kind="생활",
        target_price_krw=59_900, unit_cost_rmb=(41, 62), logistics_krw=10_500, baseline=73,
        search_zh="水槽下 双层 抽拉 收纳架 可调 免打孔", search_en="two tier sliding under sink organizer adjustable",
        spec="2단 선반 1개 · 인출 바구니 · 폭 조절 · 좌우 배치 가능",
        pain_point="싱크대 배관 뒤에 둔 세제와 리필 용기가 보이지 않아 쓰던 것을 또 산다.",
        selling_point="배관을 피해 설치하는 예와 아래 칸을 당겨 뒤쪽 물건을 꺼내는 모습을 보여준다.",
        sample_check="최대 인출 시 전도, 레일 걸림과 조립 공차를 확인한다.",
        bulk_suitable=True, focus_channel="instagram",
    ),
    Candidate(
        sku="IND-13", name="접이식 핸디 의류 스티머", category="industrial", kind="생활",
        target_price_krw=89_000, unit_cost_rmb=(64, 95), logistics_krw=10_000, baseline=78,
        search_zh="折叠 手持 挂烫机 可拆水箱 防滴漏", search_en="folding handheld garment steamer detachable water tank",
        spec="스티머 1개 · 분리 물통 · 회전 접이 손잡이 · 전원 사양 명시",
        pain_point="외출 직전 셔츠 주름을 발견해도 다리미판을 꺼내기 번거롭다.",
        selling_point="옷걸이에 걸린 셔츠 한쪽을 손질하고 실제 예열 시간을 함께 제시한다.",
        sample_check="각도별 물방울 튐, 예열 시간과 전압·플러그 옵션을 확인한다.",
        bulk_suitable=True, focus_channel="youtube",
    ),
    Candidate(
        sku="IND-14", name="접이식 소형 빨래 건조대", category="industrial", kind="생활",
        target_price_krw=69_900, unit_cost_rmb=(44, 67), logistics_krw=13_500, baseline=64,
        search_zh="家用 折叠 晾衣架 小户型 翼型 防锈", search_en="compact folding laundry rack rust resistant folding wings",
        spec="건조대 1개 · 접이 날개 · 양말 걸이 · 바닥 보호 캡",
        pain_point="소량 빨래를 널려고 거실 한가운데 큰 건조대를 펼치기 부담스럽다.",
        selling_point="수건 여섯 장을 널었을 때의 폭과 접힌 보관 치수를 비교한다.",
        sample_check="젖은 수건 적재 시 흔들림, 코팅 손상과 경첩 끼임을 확인한다.",
        bulk_suitable=True, focus_channel="naver",
    ),
    Candidate(
        sku="IND-15", name="가정용 무선 진공 포장기", category="industrial", kind="생활",
        target_price_krw=79_000, unit_cost_rmb=(52, 76), logistics_krw=9_300, baseline=76,
        search_zh="无线 真空 封口机 干湿两用 家用 真空袋", search_en="cordless vacuum sealer dry moist food starter bags",
        spec="포장기 1개 · 건식·습식 모드 · 호환 진공백 10장",
        pain_point="대량으로 산 식재료를 나눠 얼려도 봉지 안에 공기가 차고 정리가 어렵다.",
        selling_point="같은 재료를 한 끼 단위로 밀봉하고 냉동실에 세워 수납하는 과정을 보여준다.",
        sample_check="봉합 폭, 밀봉 유지 시간, 습식 사용 시 액체 역류와 백 규격을 확인한다.",
        bulk_suitable=True, focus_channel="youtube",
    ),
    Candidate(
        sku="IND-16", name="높이 조절 흔들 발 받침대", category="industrial", kind="비의료 웰니스",
        target_price_krw=59_000, unit_cost_rmb=(34, 55), logistics_krw=9_500, baseline=69,
        search_zh="办公 脚踏板 高度可调 摇摆 防滑", search_en="adjustable rocking desk footrest anti slip",
        spec="발 받침대 1개 · 높이 3단 · 각도 흔들판 · 분리 매트",
        pain_point="책상 의자를 올리면 발이 바닥에 닿지 않고 발 위치를 바꾸기 어렵다.",
        selling_point="의자 높이를 그대로 두고 받침 높이와 각도를 바꾸는 사용 장면을 보여준다.",
        sample_check="높이 고정 핀과 최저 높이, 발판 움직임 소음과 바닥 자국을 확인한다.",
        bulk_suitable=True, focus_channel="google",
    ),
    Candidate(
        sku="IND-17", name="분리형 온열 눈 찜질 마스크", category="industrial", kind="비의료 웰니스",
        target_price_krw=49_000, unit_cost_rmb=(33, 49), logistics_krw=6_900, baseline=78,
        search_zh="USB 热敷 眼罩 定时 可拆洗 温控", search_en="USB heated eye mask removable cover timer",
        spec="마스크 1개 · 온도 조절 · 타이머 · 분리 커버",
        pain_point="잠자기 전 온수 수건을 준비하면 금방 식고 세탁물이 늘어난다.",
        selling_point="커버를 끼우고 타이머를 맞춘 뒤 쉬는 저녁 사용 루틴을 보여준다.",
        sample_check="눈 주변 열 균일성, 자동 종료, 커버 수축과 밴드 압박을 확인한다.",
        bulk_suitable=True, focus_channel="instagram",
    ),
    Candidate(
        sku="IND-18", name="분리 커버 발 롤러 마사지기", category="industrial", kind="비의료 웰니스",
        target_price_krw=94_000, unit_cost_rmb=(67, 96), logistics_krw=13_000, baseline=74,
        search_zh="家用 足底 滚轮 按摩器 低噪 可拆洗 布套", search_en="quiet foot roller massager removable washable liner",
        spec="본체 1개 · 강도 2단 · 분리 세탁 내피 · 자동 종료",
        pain_point="오래 서 있다 귀가했을 때 편히 발을 올려놓고 싶지만 소리 큰 기기는 부담스럽다.",
        selling_point="거실에서의 작동 소리와 내피를 분리해 세탁하는 방법을 영상에 담는다.",
        sample_check="작동 소음과 양쪽 롤러 강도 편차, 발 크기별 착용 공간을 확인한다.",
        bulk_suitable=True, focus_channel="youtube",
    ),
    Candidate(
        sku="IND-19", name="목·어깨 온열 케이프", category="industrial", kind="비의료 웰니스",
        target_price_krw=84_000, unit_cost_rmb=(55, 83), logistics_krw=9_800, baseline=77,
        search_zh="肩颈 加热 披肩 三档 定时 磁扣", search_en="heated neck shoulder cape magnetic clasp timer",
        spec="케이프 1개 · 목깃 · 앞잠금 · 온도 3단 · 자동 종료",
        pain_point="책을 읽거나 컴퓨터를 쓸 때 어깨에 두른 담요가 흘러내린다.",
        selling_point="앉아서 팔을 움직일 때의 고정감과 목깃이 덮는 범위를 보여준다.",
        sample_check="목 주변 과열, 목깃 마감, 잠금 유지력과 세탁 범위를 확인한다.",
        bulk_suitable=True, focus_channel="instagram",
    ),
    Candidate(
        sku="IND-20", name="충전식 양면 손난로 2개 세트", category="industrial", kind="비의료 웰니스",
        target_price_krw=49_900, unit_cost_rmb=(32, 49), logistics_krw=7_800, baseline=73,
        search_zh="可充电 暖手宝 双面发热 磁吸 分体 两只装", search_en="rechargeable magnetic hand warmers double sided pair",
        spec="손난로 2개 · 자석 결합 · 단계별 온도 · 보관 파우치",
        pain_point="산책 중 양손이 시린데 일회용 핫팩은 보관하기 번거롭다.",
        selling_point="두 개로 나눠 각각 주머니에 넣고 충전하는 방법과 휴대 크기를 보여준다.",
        sample_check="두 본체의 온도 차이, 완충 사용 시간과 충전 단자 접촉을 확인한다.",
        bulk_suitable=True, focus_channel="google",
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
        sku="BEA-05", name="레티놀 나이트 크림", category="beauty", kind="안티에이징",
        target_price_krw=55_000, unit_cost_rmb=(24, 39), logistics_krw=7_300, baseline=80,
        search_zh="视黄醇 晚霜 避光 真空泵 30g ODM", search_en="retinol night cream 30g opaque airless pump ODM",
        spec="30g 1개 · 차광 펌프 · 저녁용 보습 제형",
        pain_point="밤용 크림을 찾지만 진한 향과 끈적임이 있는 제형은 쓰기 어렵다.",
        selling_point="저녁 루틴에 넣는 순서와 한 펌프의 발림을 보여주고 성분 표기를 읽기 쉽게 정리한다.",
        sample_check="성분 함량 자료와 용기 차광 사양을 받고, 고온 보관 후 색과 냄새 변화를 확인한다.",
        focus_channel="google",
    ),
    Candidate(
        sku="BEA-06", name="바쿠치올 보습 세럼", category="beauty", kind="안티에이징",
        target_price_krw=48_000, unit_cost_rmb=(21, 34), logistics_krw=7_000, baseline=74,
        search_zh="补骨脂酚 保湿 精华液 30ml 无香 ODM", search_en="bakuchiol hydrating serum 30ml fragrance free ODM",
        spec="30mL 1개 · 무향 의뢰 · 펌프형 · 가벼운 제형",
        pain_point="저녁 스킨케어 제품을 바꾸고 싶지만 오일처럼 무거운 마무리가 부담스럽다.",
        selling_point="손등에 얇게 펴 바른 뒤 다음 단계에서 밀리는지 실제 사용량으로 보여준다.",
        sample_check="원료 함량 자료와 향료 유무를 확인하고 샘플의 유분감과 펌프 토출량을 비교한다.",
        focus_channel="instagram",
    ),
    Candidate(
        sku="BEA-07", name="판테놀 수분 장벽 크림", category="beauty", kind="기초 스킨케어",
        target_price_krw=42_000, unit_cost_rmb=(18, 30), logistics_krw=6_800, baseline=75,
        search_zh="泛醇 保湿 面霜 无香 50g 软管 ODM", search_en="panthenol fragrance free moisturizing cream 50g tube ODM",
        spec="50g 1개 · 무향 크림 · 한 손으로 여는 튜브",
        pain_point="세안 직후 피부가 당겨 보습제를 찾지만 단지에 손을 넣는 방식이 번거롭다.",
        selling_point="튜브로 덜어 쓰는 사용성과 세안 후 제형이 펴지는 모습을 함께 보여준다.",
        sample_check="튜브 끝 누액, 내용물 배합 문서와 다른 보습제 위에서의 밀림을 확인한다.",
        focus_channel="naver",
    ),
    Candidate(
        sku="BEA-08", name="히알루론산 수분 토너패드", category="beauty", kind="기초 스킨케어",
        target_price_krw=35_000, unit_cost_rmb=(19, 31), logistics_krw=7_800, baseline=70,
        search_zh="透明质酸 保湿 化妆棉 湿敷 棉片 60片 ODM", search_en="hyaluronic hydrating toner pads 60 sheets ODM",
        spec="60매 1통 · 평면 패드 · 내장 집게 · 이중 밀봉",
        pain_point="출근 준비 중 화장솜에 토너를 따르는 과정이 길고 솜이 쉽게 마른다.",
        selling_point="집게로 한 장씩 꺼내는 모습과 패드 크기, 한 통의 사용 횟수를 보여준다.",
        sample_check="첫 장과 마지막 장의 함침량 차이, 집게 고정과 용기 누수를 확인한다.",
        focus_channel="instagram",
    ),
    Candidate(
        sku="BEA-09", name="나이아신아마이드 기초 앰플", category="beauty", kind="기초 스킨케어",
        target_price_krw=45_000, unit_cost_rmb=(20, 33), logistics_krw=6_900, baseline=77,
        search_zh="烟酰胺 补水 精华液 30ml 滴管 无香 ODM", search_en="niacinamide hydrating facial serum dropper 30ml ODM",
        spec="30mL 1개 · 스포이트 용기 · 향료 없는 제형 의뢰",
        pain_point="성분 이름을 보고 제품을 고르지만 여러 앰플을 겹쳐 바르면 제형이 밀린다.",
        selling_point="세안 후 두 방울을 바르고 크림을 올리는 단순한 사용 순서를 제안한다.",
        sample_check="전성분과 배합 자료, 스포이트 반복 토출량과 유리병 완충 포장을 확인한다.",
        focus_channel="google",
    ),
    Candidate(
        sku="BEA-10", name="무향 펌프형 바디 보습 로션", category="beauty", kind="기초 스킨케어",
        target_price_krw=39_000, unit_cost_rmb=(22, 37), logistics_krw=8_600, baseline=69,
        search_zh="无香 保湿 身体乳 300ml 按压泵 ODM", search_en="fragrance free body moisturizer 300ml pump ODM",
        spec="300mL 1개 · 잠금 펌프 · 흡수감이 가벼운 제형",
        pain_point="샤워 후 급히 옷을 입어야 하는데 바디로션이 끈적이거나 향이 오래 남는다.",
        selling_point="한 손으로 눌러 덜어 쓰는 과정과 바른 직후의 마무리를 보여준다.",
        sample_check="펌프가 거의 빈 용기에서도 작동하는지 보고 제형 분리와 배송 중 누액을 확인한다.",
        focus_channel="naver",
    ),
    Candidate(
        sku="BEA-11", name="콜드 롤러 페이셜 마사지기", category="beauty", kind="홈케어 기기",
        target_price_krw=49_000, unit_cost_rmb=(25, 41), logistics_krw=7_400, baseline=72,
        search_zh="脸部 冰感 按摩 滚轮 不锈钢 可拆 冷敷", search_en="stainless facial cooling roller detachable head",
        spec="롤러 1개 · 분리형 냉감 헤드 · 보호 캡",
        pain_point="아침 관리 도구를 냉장고에 넣어도 젖은 헤드를 위생적으로 보관하기 어렵다.",
        selling_point="보호 캡을 열고 볼과 턱 주변을 굴린 뒤 헤드를 닦는 루틴을 보여준다.",
        sample_check="헤드 유격, 냉장 후 결로와 캡 밀착, 금속 접합부 마감을 확인한다.",
        focus_channel="instagram",
    ),
    Candidate(
        sku="BEA-12", name="실리콘 진동 클렌징 브러시", category="beauty", kind="홈케어 기기",
        target_price_krw=59_900, unit_cost_rmb=(36, 58), logistics_krw=7_900, baseline=77,
        search_zh="硅胶 声波 洁面仪 三档 充电 软刷头", search_en="rechargeable silicone sonic face cleansing brush soft head",
        spec="본체 1개 · 실리콘 돌기 · 진동 3단 · 건조 받침",
        pain_point="세안 도구를 쓰고 싶어도 젖은 솔이 잘 마르지 않고 사용 강도를 조절하기 어렵다.",
        selling_point="낮은 진동 단계, 거품 사용과 받침에 세워 말리는 과정까지 보여준다.",
        sample_check="가동 소음, 돌기 끝 거칠기와 세척 범위에 관한 공급자 시험 자료를 확인한다.",
        focus_channel="youtube",
    ),
    Candidate(
        sku="BEA-13", name="온도 조절 페이셜 스티머", category="beauty", kind="홈케어 기기",
        target_price_krw=79_000, unit_cost_rmb=(47, 75), logistics_krw=10_500, baseline=78,
        search_zh="家用 面部 蒸脸仪 可拆水箱 定时 温度调节", search_en="facial steamer detachable tank timer temperature setting",
        spec="스티머 1개 · 분리 물통 · 타이머 · 분사각 조절",
        pain_point="집에서 쓰는 스티머의 물통을 씻기 어렵고 분사 방향이 고정되면 자세가 불편하다.",
        selling_point="탁자 높이에서 분사각을 맞추고 사용 뒤 물통을 분리하는 방법을 보여준다.",
        sample_check="정격 전원, 분사 온도 편차와 물 부족 시 종료 동작을 확인한다.",
        focus_channel="youtube",
    ),
    Candidate(
        sku="BEA-14", name="회전 헤드 전동 두피 마사지기", category="beauty", kind="두피 기기",
        target_price_krw=69_900, unit_cost_rmb=(42, 65), logistics_krw=8_800, baseline=79,
        search_zh="电动 头皮 按摩器 四头 旋转 可拆 硅胶", search_en="electric scalp massager four rotating detachable silicone heads",
        spec="본체 1개 · 분리형 실리콘 헤드 4개 · 속도 2단",
        pain_point="손으로 두피를 오래 문지르기 번거롭고 회전 브러시에 머리카락이 감길까 걱정된다.",
        selling_point="짧은 머리와 긴 머리에서 낮은 단계로 사용하는 각도와 헤드 분리법을 시연한다.",
        sample_check="모발 감김, 헤드 이탈, 충전 베이스 접촉과 작동 소음을 확인한다.",
        focus_channel="youtube",
    ),
    Candidate(
        sku="BEA-15", name="두피 세럼 도포 롤온 브러시", category="beauty", kind="두피 기기",
        target_price_krw=35_000, unit_cost_rmb=(17, 28), logistics_krw=6_300, baseline=68,
        search_zh="头皮 精华 涂抹梳 滚珠 便携 储液 可清洗", search_en="refillable scalp serum roller applicator comb washable",
        spec="도포 브러시 1개 · 소형 저장통 · 보호 캡 · 롤온 헤드",
        pain_point="스포이트로 두피에 바르면 내용물이 모발을 타고 흘러 손에 묻는다.",
        selling_point="가르마를 따라 도포하고 저장통을 비워 세척하는 동작을 보여준다.",
        sample_check="묽은 제형과 점성 제형의 토출량 차이, 거꾸로 보관 시 누액을 확인한다.",
        focus_channel="instagram",
    ),
    Candidate(
        sku="BEA-16", name="온열 괄사 페이셜 디바이스", category="beauty", kind="홈케어 기기",
        target_price_krw=79_000, unit_cost_rmb=(45, 68), logistics_krw=8_600, baseline=76,
        search_zh="加热 面部 刮痧 按摩仪 温控 USB 充电", search_en="heated facial gua sha massager temperature control USB",
        spec="본체 1개 · 온열 2단 · 곡면 헤드 · 보관 파우치",
        pain_point="차가운 도구가 얼굴에 닿는 느낌이 부담스럽고 세면대에 두면 쉽게 미끄러진다.",
        selling_point="크림을 바른 뒤 볼과 턱에 닿는 곡면과 전원 조작을 가까이서 보여준다.",
        sample_check="표면 온도 균일성, 모서리 마감과 자동 종료를 확인한다.",
        focus_channel="instagram",
    ),
    Candidate(
        sku="BEA-17", name="롱핸들 전동 바디 브러시", category="beauty", kind="바디 기기",
        target_price_krw=69_000, unit_cost_rmb=(38, 62), logistics_krw=9_400, baseline=73,
        search_zh="电动 沐浴刷 长柄 可换软刷头 可拆手柄", search_en="electric long handle body brush replaceable soft heads",
        spec="본체 1개 · 손잡이 분리 · 부드러운 헤드 2종 · 걸이 고리",
        pain_point="등을 닦기 위해 팔을 뒤로 뻗기 어렵고 물에 젖은 손잡이는 잘 미끄러진다.",
        selling_point="손잡이를 붙인 상태와 손에 쥔 상태의 사용 범위를 실제 길이와 함께 보여준다.",
        sample_check="젖은 손의 그립, 손잡이 잠금, 헤드 교체 수량과 공급 조건을 확인한다.",
        focus_channel="youtube",
    ),
    Candidate(
        sku="BEA-18", name="저자극 스쿠알란 클렌징 밤", category="beauty", kind="기초 스킨케어",
        target_price_krw=39_900, unit_cost_rmb=(20, 33), logistics_krw=7_100, baseline=71,
        search_zh="角鲨烷 洁面 膏 无香 80g 乳化 ODM", search_en="fragrance free squalane emulsifying cleansing balm 80g ODM",
        spec="밤 80g 1개 · 스패출러 · 향료 없는 제형 의뢰",
        pain_point="세안 후 당김은 줄이고 싶지만 진한 향이나 미끄러운 잔여감이 싫다.",
        selling_point="밤이 물과 만났을 때 유화되는 모습과 헹군 뒤의 사용감을 보여준다.",
        sample_check="향료와 원료 자료를 받고 상온·고온에서의 녹는 정도와 유화 속도를 비교한다.",
        focus_channel="naver",
    ),
    Candidate(
        sku="BEA-19", name="펩타이드 넥 보습 크림", category="beauty", kind="안티에이징",
        target_price_krw=49_900, unit_cost_rmb=(24, 37), logistics_krw=7_300, baseline=74,
        search_zh="多肽 颈霜 保湿 50g 无香 真空泵 ODM", search_en="peptide neck moisturizer fragrance free airless 50g ODM",
        spec="50g 1개 · 펌프 용기 · 목과 쇄골에 펴 바르는 제형",
        pain_point="얼굴에 쓰는 크림을 목에 넓게 바르면 옷깃에 묻고 용량이 빨리 줄어든다.",
        selling_point="한 펌프로 목과 쇄골에 펴 바르는 양과 옷깃에 닿기 전 마무리를 보여준다.",
        sample_check="흡수 후 옷깃 묻어남, 고온 보관 안정성과 펌프 수급 조건을 확인한다.",
        focus_channel="google",
    ),
    Candidate(
        sku="BEA-20", name="곡면 냉온 겸용 아이 롤러", category="beauty", kind="홈케어 기기",
        target_price_krw=64_000, unit_cost_rmb=(37, 59), logistics_krw=7_900, baseline=71,
        search_zh="冷热 眼部 按摩仪 滚珠 定时 便携", search_en="portable warming cooling eye roller with timer",
        spec="본체 1개 · 냉온 전환 · 곡면 롤러 · 보호 캡",
        pain_point="눈가 관리 도구를 고를 때 헤드가 눈 밑 곡선에 잘 닿는지 판단하기 어렵다.",
        selling_point="헤드의 크기와 곡률을 손 위에서 보여주고 두 모드의 조작을 짧게 시연한다.",
        sample_check="모드별 표면 온도, 전환 시간, 롤러 마감과 캡의 보관 편의성을 확인한다.",
        focus_channel="google",
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
    Candidate(
        sku="FOD-05", name="무가당 검은콩 단백질 분말", category="food", kind="단백질 분말",
        target_price_krw=35_000, unit_cost_rmb=(26, 40), logistics_krw=7_300, baseline=77,
        search_zh="黑豆 蛋白粉 无添加糖 400g 普通食品 OEM", search_en="unsweetened black soybean protein powder 400g OEM",
        spec="400g 1통 · 볶은 검은콩 베이스 · 계량 스푼",
        pain_point="달거나 향이 강한 쉐이크 대신 익숙한 콩 맛을 원한다.",
        selling_point="두유에 타는 한 컵 레시피와 1회 분량의 원재료 표시를 함께 보여준다.",
        sample_check="대두 알레르기와 당류 자료를 받고 찬 두유에서의 분산과 콩 비린 맛을 비교한다.",
        food_class="일반식품", focus_channel="naver",
    ),
    Candidate(
        sku="FOD-06", name="플레인 유청 단백질 분말", category="food", kind="단백질 분말",
        target_price_krw=42_000, unit_cost_rmb=(38, 55), logistics_krw=8_400, baseline=80,
        search_zh="乳清 蛋白粉 原味 无香精 500克 食品 OEM", search_en="plain whey protein powder no flavor 500g OEM",
        spec="500g 1통 · 플레인 제형 · 계량 스푼 · 밀봉 뚜껑",
        pain_point="우유나 요거트에 섞을 때 인공적인 단맛이 기존 맛을 덮는 것이 싫다.",
        selling_point="플레인 요거트에 섞는 양과 섞은 뒤의 질감을 실제 컵으로 보여준다.",
        sample_check="유단백 원산지와 우유 알레르기 표시, 실측 단백질 자료와 뭉침 정도를 확인한다.",
        food_class="일반식품", focus_channel="google",
    ),
    Candidate(
        sku="FOD-07", name="현미 단백질 분말", category="food", kind="단백질 분말",
        target_price_krw=36_900, unit_cost_rmb=(30, 45), logistics_krw=7_500, baseline=69,
        search_zh="糙米 蛋白粉 原味 400g 食品 原料 OEM", search_en="plain brown rice protein powder 400g food OEM",
        spec="400g 1통 · 현미 단백 원료 · 가는 입도 의뢰",
        pain_point="곡물 기반 분말을 고를 때 물에 타면 거칠게 가라앉는 것이 걱정된다.",
        selling_point="찬 음료에 저은 뒤 1분과 5분의 침전 정도를 실제 잔으로 보여준다.",
        sample_check="현미 원료와 단백질 분석 자료, 분말 입도와 이취를 확인한다.",
        food_class="일반식품", focus_channel="google",
    ),
    Candidate(
        sku="FOD-08", name="귀리·완두 단백질 분말", category="food", kind="단백질 분말",
        target_price_krw=37_900, unit_cost_rmb=(29, 45), logistics_krw=7_600, baseline=74,
        search_zh="燕麦 豌豆 蛋白 冲调粉 400g 原味 OEM", search_en="oat and pea protein drink powder plain 400g OEM",
        spec="400g 1통 · 귀리·완두 배합 · 스푼 포함",
        pain_point="한 가지 콩 맛이 강한 분말은 질리지만 곡물을 별도로 섞을 시간은 없다.",
        selling_point="귀리와 완두 배합을 공개하고 오트밀이나 두유에 곁들이는 양을 안내한다.",
        sample_check="귀리 배합과 교차 접촉 자료를 받고 따뜻한 물에서 점도와 향을 비교한다.",
        food_class="일반식품", focus_channel="youtube",
    ),
    Candidate(
        sku="FOD-09", name="볶은 검은콩 곡물 스틱", category="food", kind="곡물 스틱",
        target_price_krw=29_900, unit_cost_rmb=(21, 32), logistics_krw=6_100, baseline=73,
        search_zh="烘焙 黑豆粉 谷物 冲调 独立条包 20条", search_en="roasted black soybean grain drink powder 20 stick packs",
        spec="18g × 20포 1박스 · 볶은 검은콩·곡물 배합 · 절취선",
        pain_point="큰 통의 검은콩 가루는 사무실에 가져가기 불편하고 스푼에 남아 흘리기 쉽다.",
        selling_point="작은 텀블러에 한 포를 붓는 과정을 보여주고 물의 권장 양을 표기한다.",
        sample_check="대두 알레르기, 볶은 정도와 분말 입도, 절취선 개봉성을 확인한다.",
        food_class="일반식품", focus_channel="naver",
    ),
    Candidate(
        sku="FOD-10", name="검은깨 곡물 라테 스틱", category="food", kind="곡물 스틱",
        target_price_krw=32_000, unit_cost_rmb=(24, 37), logistics_krw=6_500, baseline=78,
        search_zh="黑芝麻 谷物 拿铁 冲调粉 小条包 20条 OEM", search_en="black sesame grain latte powder 20 stick packs OEM",
        spec="20g × 20포 1박스 · 검은깨 베이스 · 따뜻한 우유용 레시피",
        pain_point="달지 않은 고소한 음료를 마시고 싶지만 검은깨를 따로 갈거나 계량하기 어렵다.",
        selling_point="따뜻한 우유에 한 포를 섞은 뒤 색과 농도를 컵으로 보여준다.",
        sample_check="참깨 알레르기 자료, 볶은 향과 기름 냄새 변화, 상온 보관 안정성을 확인한다.",
        food_class="일반식품", focus_channel="instagram",
    ),
    Candidate(
        sku="FOD-11", name="볶은 현미 미세 분말", category="food", kind="곡물 분말",
        target_price_krw=27_900, unit_cost_rmb=(19, 29), logistics_krw=6_700, baseline=65,
        search_zh="烘焙 糙米粉 细粉 冲调 400克 普通食品", search_en="fine roasted brown rice drink powder 400g",
        spec="400g 1봉 · 지퍼백 · 미세 분말 · 계량 예시 제공",
        pain_point="부피가 큰 통보다 주방 서랍에 보관할 수 있는 곡물 가루를 선호한다.",
        selling_point="지퍼백을 닫아 보관하는 크기와 따뜻한 물에 타는 간단한 계량법을 보여준다.",
        sample_check="현미 원료 배합, 입도, 지퍼 밀착과 개봉 후 냄새 변화를 점검한다.",
        food_class="일반식품", focus_channel="youtube",
    ),
    Candidate(
        sku="FOD-12", name="보리·귀리 아침 곡물 스틱", category="food", kind="곡물 스틱",
        target_price_krw=30_000, unit_cost_rmb=(22, 34), logistics_krw=6_200, baseline=71,
        search_zh="大麦 燕麦 早餐 谷物粉 速溶 20条 OEM", search_en="barley oat instant breakfast grain powder 20 sticks OEM",
        spec="20g × 20포 1박스 · 보리·귀리 배합 · 무향 제형 의뢰",
        pain_point="아침에 죽을 끓일 시간은 없고 찬 음료는 부담스러운 날이 있다.",
        selling_point="따뜻한 물만으로 한 잔을 만드는 순서와 한 포의 농도를 보여준다.",
        sample_check="곡물 원산지와 알레르기 자료, 온수에서 젓는 시간과 침전량을 확인한다.",
        food_class="일반식품", focus_channel="youtube",
    ),
    Candidate(
        sku="FOD-13", name="율무 곡물 라테 스틱", category="food", kind="곡물 스틱",
        target_price_krw=32_900, unit_cost_rmb=(25, 38), logistics_krw=6_300, baseline=75,
        search_zh="薏米 谷物 拿铁 粉 低甜 独立条包 20条", search_en="mildly sweet coix seed grain latte powder 20 sticks",
        spec="20g × 20포 1박스 · 율무와 볶은 곡물 배합 · 밀봉 스틱",
        pain_point="익숙한 율무차의 풍미를 좋아하지만 지나치게 단 믹스는 찾지 않게 된다.",
        selling_point="따뜻한 물과 우유로 각각 만든 컵의 맛과 농도를 비교한다.",
        sample_check="율무 함량, 당류와 알레르기 자료를 받고 샘플별 단맛과 잔여감을 확인한다.",
        food_class="일반식품", focus_channel="naver",
    ),
    Candidate(
        sku="FOD-14", name="단호박 곡물 분말 스틱", category="food", kind="곡물 스틱",
        target_price_krw=34_000, unit_cost_rmb=(25, 39), logistics_krw=6_400, baseline=76,
        search_zh="南瓜 谷物 冲调粉 即冲 独立条包 20条 OEM", search_en="instant pumpkin grain drink powder 20 sticks OEM",
        spec="22g × 20포 1박스 · 단호박 분말과 곡물 · 온수 레시피",
        pain_point="단호박 풍미가 나는 간식을 먹고 싶어도 손질하고 조리하는 시간은 내기 어렵다.",
        selling_point="머그컵에 한 포를 섞어 색과 질감을 보여주고 농도 조절용 물 양을 제안한다.",
        sample_check="단호박 배합표, 점도와 분말의 색 편차, 스틱 방습 포장을 확인한다.",
        food_class="일반식품", focus_channel="instagram",
    ),
    Candidate(
        sku="FOD-15", name="현미 발효 효소 스틱", category="food", kind="효소 스틱",
        target_price_krw=35_900, unit_cost_rmb=(28, 43), logistics_krw=6_300, baseline=72,
        search_zh="糙米 发酵粉 谷物酵素粉 条包 普通食品 30条", search_en="fermented brown rice enzyme powder ordinary food 30 sticks",
        spec="3g × 30포 1박스 · 현미 발효분말 · 개별 방습 포장",
        pain_point="발효 곡물 맛이 지나치게 시큼하면 음료나 요거트에 넣기 어렵다.",
        selling_point="무가당 요거트에 한 포를 넣고 맛과 섞이는 정도를 설명한다.",
        sample_check="발효 원료와 일반식품 분류 자료를 받고 낱포 중량과 방습성을 확인한다.",
        food_class="일반식품", focus_channel="google",
    ),
    Candidate(
        sku="FOD-16", name="볶은 보리 발효 분말 스틱", category="food", kind="효소 스틱",
        target_price_krw=33_900, unit_cost_rmb=(27, 41), logistics_krw=6_400, baseline=68,
        search_zh="烘焙 大麦 发酵 谷物粉 普通食品 30条 代工", search_en="roasted barley fermented grain powder ordinary food 30 sticks",
        spec="3g × 30포 1박스 · 볶은 보리 발효분말 · 소포장",
        pain_point="발효 곡물 제품을 고를 때 원료가 무엇인지와 볶은 풍미를 쉽게 비교하기 어렵다.",
        selling_point="배합표를 보여주고 미지근한 물과 요거트에서 느껴지는 향의 차이를 소개한다.",
        sample_check="보리 알레르기와 발효 공정 자료, 배치별 색과 향의 편차를 확인한다.",
        food_class="일반식품", focus_channel="youtube",
    ),
    Candidate(
        sku="FOD-17", name="파인애플 곡물 발효 스틱", category="food", kind="효소 스틱",
        target_price_krw=37_000, unit_cost_rmb=(30, 46), logistics_krw=6_600, baseline=74,
        search_zh="菠萝味 发酵 谷物粉 固体饮料 普通食品 30条", search_en="pineapple flavored fermented grain powder ordinary food 30 sticks",
        spec="3g × 30포 1박스 · 곡물 발효분말과 파인애플 풍미",
        pain_point="고소한 곡물 맛만으로는 반복해서 먹기 심심하지만 과일향이 과한 제품도 부담스럽다.",
        selling_point="탄산수에 한 포를 섞는 레시피로 가벼운 풍미와 휴대성을 보여준다.",
        sample_check="과즙분말과 향료 구분, 당류 자료와 찬 탄산수에 타는 사용감을 확인한다.",
        food_class="일반식품", focus_channel="instagram",
    ),
    Candidate(
        sku="FOD-18", name="레몬 피쉬콜라겐 분말 스틱", category="food", kind="콜라겐 스틱",
        target_price_krw=39_900, unit_cost_rmb=(31, 48), logistics_krw=7_000, baseline=79,
        search_zh="柠檬味 鱼胶原蛋白肽 固体饮料 普通食品 条包", search_en="lemon fish collagen peptide drink powder ordinary food sticks",
        spec="5g × 20포 1박스 · 어류 유래 원료 · 레몬 풍미",
        pain_point="향이 있는 분말에서 생선 원료 특유의 끝맛이 남는 것이 걱정된다.",
        selling_point="찬물에 타고 마신 뒤의 향을 설명하고 낱포 크기를 보여준다.",
        sample_check="어종·원산지와 어류 알레르기 표시, 일반식품 분류 자료와 끝맛을 확인한다.",
        food_class="일반식품", focus_channel="google",
    ),
    Candidate(
        sku="FOD-19", name="유자 콜라겐 분말 스틱", category="food", kind="콜라겐 스틱",
        target_price_krw=41_000, unit_cost_rmb=(33, 50), logistics_krw=7_300, baseline=76,
        search_zh="柚子味 胶原蛋白肽 冲调粉 条包 普通食品 20条", search_en="yuzu collagen drink powder ordinary food 20 sticks",
        spec="5g × 20포 1박스 · 유자 풍미 · 찬물 혼합용",
        pain_point="물만 마시기 심심할 때 향을 더하고 싶지만 과하게 달면 자주 찾지 않는다.",
        selling_point="찬물과 따뜻한 물에서 각각 나는 유자 향을 비교하고 한 포의 당류 표시를 보여준다.",
        sample_check="유자 원료와 감미료 배합, 콜라겐 출처 및 저온에서의 뭉침을 확인한다.",
        food_class="일반식품", focus_channel="instagram",
    ),
    Candidate(
        sku="FOD-20", name="무가당 코코아 콜라겐 스틱", category="food", kind="콜라겐 스틱",
        target_price_krw=39_000, unit_cost_rmb=(31, 47), logistics_krw=7_200, baseline=71,
        search_zh="无添加糖 可可 胶原蛋白肽 固体饮料 20条 普通食品", search_en="no added sugar cocoa collagen drink powder 20 sticks",
        spec="5g × 20포 1박스 · 코코아 배합 · 우유 또는 두유용",
        pain_point="간편하게 코코아 풍미를 내고 싶지만 설탕이 많은 믹스는 피하고 싶다.",
        selling_point="따뜻한 두유에 섞을 때의 농도와 코코아 풍미를 컵 한 잔 기준으로 보여준다.",
        sample_check="원료별 당류, 콜라겐 원산지와 알레르기 자료, 코코아 분말 침전을 확인한다.",
        food_class="일반식품", focus_channel="naver",
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

    @staticmethod
    @lru_cache(maxsize=512)
    def _item_pattern(sku: str) -> tuple[float, int, float]:
        rng = random.Random(sku)
        return rng.uniform(0, math.tau), rng.randrange(13, 22), rng.uniform(10, 20)

    def fetch(self, candidates: Sequence[Candidate], observed_at: datetime) -> dict[str, float]:
        day = observed_at.timestamp() / 86_400
        sample = int(observed_at.timestamp() // 60)
        scores = {}
        groups = {
            category: 9 * math.sin(day * math.tau / 17 + group * 2)
            + 5 * math.cos(day * math.tau / 7 + group) + self.CHANNEL_BIAS[self.channel][group]
            for group, category in enumerate(CATEGORY_LABELS)
        }
        for item in candidates:
            phase, period, amplitude = self._item_pattern(item.sku)
            item_wave = amplitude * math.sin(day * math.tau / period + phase)
            noise = (crc32(f"{self.channel}:{item.sku}:{sample}".encode()) / 0xFFFFFFFF - 0.5) * 10
            score = item.baseline + groups[item.category] + item_wave + noise
            if item.focus_channel is not None:
                if item.focus_channel == self.channel:
                    score += 22
                else:
                    # A keyword found elsewhere can have much lower shopping intent.
                    score -= 32 if self.channel == "naver" else 14
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

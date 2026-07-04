"""
부동산 가격분석 서비스 - Streamlit 대시보드
"""
import streamlit as st
import pandas as pd
import numpy as np
import os
import json
import math
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
import numexpr as ne

# 릴리스 모드 — True: 게시판 글쓰기·파일업로드 비활성 (보안·안정성)
RELEASE_READ_ONLY = True

from buy_decision.schemas import PURPOSES
from buy_decision.view_model import build_buy_decision_view_model
from data_loader import load_all_data, load_apt_data, load_rent_data, get_sigungu_name
from board import (
    init_db,
    delete_saved_chart,
    get_shared_chart,
    list_saved_charts,
    list_watchlists,
    revoke_shared_chart,
    save_chart_settings,
    save_condition_set,
    share_saved_chart,
    upsert_watchlist,
)
from analysis import (
    correlation_matrix,
    correlation_by_region,
    correlation_by_period,
    scatter_analysis,
    multiple_regression,
    detect_outliers,
    cluster_regions,
    granger_causality_test,
    compute_lead_lag_signal,
    evaluate_condition_rules,
    pct_from_peak,
    peak_drawdown_then_rebound,
    prepare_screener_dataset,
    run_region_backtest,
    rolling_consecutive_change,
    vs_moving_avg,
    compute_value_score,
    compute_market_temperature,
    interpolate_quintile_to_percentile,
    compute_purchasing_power,
    rank_sigungu_grade,
    match_income_to_property,
    forecast_price,
    HAS_PROPHET,
)
from tax_calculator import (
    calc_acquisition_tax,
    calc_capital_gains_tax,
    calc_investment_return,
)

st.set_page_config(
    page_title="부동산 가격분석 대시보드",
    page_icon="🏠",
    layout="wide",
)

# === 다크 프로(TradingView형) 전역 테마 CSS (2026-07-04) ===
st.markdown(
    """
    <style>
    :root{
      --re-bg:#0E1117; --re-panel:#161B22; --re-border:#2A313C;
      --re-text:#E6EDF3; --re-muted:#8B949E; --re-accent:#2962FF;
      --re-up:#26A69A; --re-down:#EF5350;
    }
    html, body, [class*="css"]{ font-family:"Noto Sans KR","Pretendard",-apple-system,sans-serif; }
    /* 상단 툴바(Deploy·메뉴) 숨김, 헤더 투명 */
    [data-testid="stToolbar"], [data-testid="stAppDeployButton"], .stDeployButton{ display:none !important; }
    header[data-testid="stHeader"]{ background:transparent; }
    /* 본문 밀도 */
    [data-testid="stMainBlockContainer"], .block-container{ padding-top:1.4rem; padding-bottom:2.5rem; max-width:1520px; }
    /* 사이드바 */
    section[data-testid="stSidebar"]{ background:var(--re-panel); border-right:1px solid var(--re-border); }
    /* 제목 타이포 */
    h1,h2,h3{ letter-spacing:-0.01em; font-weight:800; }
    h1{ font-size:1.9rem; } h2{ font-size:1.4rem; }
    /* 지표(metric) 카드화 */
    [data-testid="stMetric"]{
      background:var(--re-panel); border:1px solid var(--re-border);
      border-radius:12px; padding:14px 16px;
    }
    [data-testid="stMetricValue"]{ font-weight:800; font-size:1.7rem; }
    [data-testid="stMetricLabel"] p{ color:var(--re-muted); font-weight:600; }
    /* 세그먼티드 컨트롤(탭 네비) */
    [data-testid="stButtonGroup"] button{ border-radius:8px !important; font-weight:700; }
    /* 차트·표 패널화 */
    [data-testid="stPlotlyChart"]{
      background:var(--re-panel); border:1px solid var(--re-border);
      border-radius:12px; padding:8px 10px;
    }
    [data-testid="stDataFrame"]{ border:1px solid var(--re-border); border-radius:12px; }
    /* expander 패널화 */
    [data-testid="stExpander"]{ border:1px solid var(--re-border); border-radius:12px; background:var(--re-panel); }
    /* 구분선 */
    hr{ border-color:var(--re-border); }
    /* 스크롤바 */
    ::-webkit-scrollbar{ width:10px; height:10px; }
    ::-webkit-scrollbar-thumb{ background:#30363d; border-radius:6px; }
    ::-webkit-scrollbar-track{ background:transparent; }
    </style>
    """,
    unsafe_allow_html=True,
)

# Streamlit Cloud (Linux) 한글 폰트 기본값 — packages.txt에 fonts-noto-cjk 설치 필요
PLOTLY_FONT = dict(family="Noto Sans KR, Noto Sans CJK KR, sans-serif", size=12)

# === 다크 프로(TradingView형) Plotly 템플릿 (2026-07-04) ===
# 모든 차트에 전역 적용. 개별 figure가 template/색을 명시하지 않으면 이 값을 사용.
pio.templates["re_dark"] = go.layout.Template(
    layout=dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Noto Sans KR, Noto Sans CJK KR, sans-serif", size=12, color="#E6EDF3"),
        colorway=["#2962FF", "#26A69A", "#EF5350", "#F5B301", "#AB47BC", "#26C6DA", "#66BB6A", "#FF7043"],
        xaxis=dict(gridcolor="#222A35", zerolinecolor="#222A35", linecolor="#2A313C", tickfont=dict(color="#8B949E")),
        yaxis=dict(gridcolor="#222A35", zerolinecolor="#222A35", linecolor="#2A313C", tickfont=dict(color="#8B949E")),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#C9D1D9")),
        hoverlabel=dict(bgcolor="#161B22", font=dict(color="#E6EDF3", size=12), bordercolor="#2A313C"),
        margin=dict(t=48, r=16, b=16, l=16),
    )
)
pio.templates.default = "re_dark"


INDICATOR_CATALOG = [
    # 가격
    {"group": "가격", "column": "평균가격", "label": "실거래 평균가격", "source": "실거래", "unit": "만원", "best_for": "실제 거래금액 흐름", "caution": "거래량이 적은 지역은 월별 변동성이 큼"},
    {"group": "가격", "column": "평균단가_per_m2", "label": "실거래 m²당 가격", "source": "실거래", "unit": "만원/m²", "best_for": "면적 차이를 줄인 지역 비교", "caution": "거래된 평형 구성이 바뀌면 왜곡 가능"},
    {"group": "가격", "column": "아파트매매가격지수", "label": "BOK 아파트매매가격지수", "source": "BOK", "unit": "지수", "best_for": "장기 가격 흐름 비교", "caution": "실거래 금액이 아니라 지수"},
    {"group": "가격", "column": "KB_선도50지수", "label": "KB 선도50지수", "source": "KB부동산", "unit": "지수", "best_for": "시장 선행 분위기 확인", "caution": "대표 단지 중심 지수라 지역별 실거래와 다를 수 있음"},
    # 전세/임대
    {"group": "전세", "column": "전세_보증금평균", "label": "전세 보증금평균", "source": "실거래", "unit": "만원", "best_for": "전세가격 흐름", "caution": "거래 표본 영향 큼"},
    {"group": "전세", "column": "전세_보증금단가", "label": "전세 m²당 보증금", "source": "실거래", "unit": "만원/m²", "best_for": "면적 차이를 줄인 전세 비교", "caution": "평형 구성 변화 영향"},
    {"group": "전세", "column": "아파트전세가격지수", "label": "BOK 아파트전세가격지수", "source": "BOK", "unit": "지수", "best_for": "장기 전세 흐름 비교", "caution": "실거래 보증금이 아니라 지수"},
    {"group": "전세", "column": "KB_J_PIR", "label": "KB J-PIR", "source": "KB부동산", "unit": "년", "best_for": "전세 부담도 확인", "caution": "제공 지역이 제한적일 수 있음"},
    {"group": "전세", "column": "월세_월세평균", "label": "월세 평균", "source": "실거래", "unit": "만원/월", "best_for": "월세 부담 확인", "caution": "보증금 수준과 함께 봐야 함"},
    {"group": "전세", "column": "KB_월세지수", "label": "KB 월세지수", "source": "KB부동산", "unit": "지수", "best_for": "월세 시장 흐름", "caution": "금액이 아니라 지수"},
    # 거래
    {"group": "거래", "column": "거래량", "label": "매매 거래량", "source": "실거래", "unit": "건", "best_for": "시장 활성도", "caution": "월별 계절성 영향"},
    {"group": "거래", "column": "전세_거래량", "label": "전세 거래량", "source": "실거래", "unit": "건", "best_for": "전세 수요 강도", "caution": "계약갱신/신규 구분 한계"},
    {"group": "거래", "column": "KB_매매거래지수", "label": "KB 매매거래지수", "source": "KB부동산", "unit": "지수", "best_for": "거래 체감 강도", "caution": "실거래 건수가 아니라 지수"},
    # 수요
    {"group": "수요", "column": "NPS_가입자수", "label": "NPS 가입자수", "source": "국민연금", "unit": "명", "best_for": "고용 기반 수요", "caution": "직장가입자 중심"},
    {"group": "수요", "column": "NPS_1인당고지금액", "label": "NPS 1인당고지금액", "source": "국민연금", "unit": "원", "best_for": "지역 소득수준 대리변수", "caution": "고지금액 기반 추정"},
    {"group": "수요", "column": "가구_소득평균", "label": "가구 소득평균", "source": "KOSIS", "unit": "만원", "best_for": "구매력/PIR 계산", "caution": "시도 단위 중심"},
    {"group": "수요", "column": "주담대_잔액", "label": "주담대 잔액", "source": "BOK", "unit": "십억원", "best_for": "레버리지 총량", "caution": "가격 상승과 부채 부담 양쪽 의미"},
    {"group": "수요", "column": "KB_HAI", "label": "KB 주택구매력", "source": "KB부동산", "unit": "지수", "best_for": "구매 가능성", "caution": "제공 지역 제한 가능"},
    # 공급
    {"group": "공급", "column": "인허가_호수", "label": "아파트 인허가", "source": "통계청/국토부", "unit": "호", "best_for": "향후 공급 선행", "caution": "입주까지 시차 존재"},
    {"group": "공급", "column": "착공_호수", "label": "착공 호수", "source": "KOSIS/국토부", "unit": "호", "best_for": "실제 공급 진행", "caution": "준공 전 지연 가능"},
    {"group": "공급", "column": "준공_호수", "label": "준공 호수", "source": "KOSIS/국토부", "unit": "호", "best_for": "입주 공급 확인", "caution": "지역별 집계 기준 확인 필요"},
    {"group": "공급", "column": "입주예정_세대수", "label": "입주예정 세대수", "source": "REB/입주예정", "unit": "호", "best_for": "향후 실제 입주 물량 확인", "caution": "예정 물량이라 지연·변경 가능"},
    {"group": "공급", "column": "입주예정_단지수", "label": "입주예정 단지수", "source": "REB/입주예정", "unit": "개", "best_for": "공급 집중도 확인", "caution": "단지 규모 차이는 세대수와 함께 봐야 함"},
    {"group": "공급", "column": "미분양_호수", "label": "미분양 호수", "source": "BOK/국토부", "unit": "호", "best_for": "공급 부담", "caution": "공표 지연 가능"},
    {"group": "공급", "column": "미분양소화기간", "label": "미분양 소화기간", "source": "파생", "unit": "개월", "best_for": "미분양 부담을 거래속도로 환산", "caution": "거래량 급감 시 급등 가능"},
    # 심리/금리
    {"group": "심리/금리", "column": "기준금리", "label": "기준금리", "source": "BOK", "unit": "%", "best_for": "금융환경", "caution": "대출금리와 직접 일치하지 않음"},
    {"group": "심리/금리", "column": "국고채_10년", "label": "국고채 10년", "source": "BOK", "unit": "%", "best_for": "장기금리 환경", "caution": "주담대 금리와 시차 존재"},
    {"group": "심리/금리", "column": "주택가격전망CSI", "label": "주택가격전망CSI", "source": "BOK", "unit": "지수", "best_for": "가격 기대심리", "caution": "심리 지표"},
    {"group": "심리/금리", "column": "KB_매수우위지수", "label": "KB 매수우위지수", "source": "KB부동산", "unit": "지수", "best_for": "매수/매도 힘 비교", "caution": "설문/지수 성격"},
    {"group": "심리/금리", "column": "부동산소비심리지수", "label": "부동산소비심리지수", "source": "국토연구원", "unit": "지수", "best_for": "부동산 소비 심리", "caution": "심리 지표"},
    # 파생
    {"group": "파생", "column": "PIR", "label": "PIR", "source": "파생", "unit": "배", "best_for": "소득 대비 가격 부담", "caution": "소득 데이터 기준에 좌우"},
    {"group": "파생", "column": "PIR_NPS", "label": "PIR(NPS 기반)", "source": "파생", "unit": "배", "best_for": "NPS 기반 지역 비교", "caution": "직장가입자 소득 추정"},
    {"group": "파생", "column": "전세가율", "label": "전세가율", "source": "파생", "unit": "%", "best_for": "매매 대비 전세 지지력", "caution": "매매/전세 표본 차이 영향"},
    {"group": "파생", "column": "갭비용", "label": "갭투자 비용", "source": "파생", "unit": "만원", "best_for": "전세 끼고 매수 시 필요 현금", "caution": "평균값 기반"},
    {"group": "파생", "column": "임대수익률", "label": "실질 임대수익률", "source": "파생", "unit": "%", "best_for": "임대 수익 매력", "caution": "세금/공실 미반영"},
    {"group": "파생", "column": "금리조정PIR", "label": "금리조정 PIR", "source": "파생", "unit": "배", "best_for": "금리 부담 반영 가격 부담", "caution": "단순 조정식"},
]

INDICATOR_META = {item["column"]: item for item in INDICATOR_CATALOG}


def register_fig(name: str, fig, tab_name: str):
    """Plotly figure를 게시판 저장용으로 등록하고 한글 폰트를 일괄 적용"""
    if "_board_figures" not in st.session_state:
        st.session_state["_board_figures"] = {}
    st.session_state["_board_figures"][name] = {"fig": fig, "tab_name": tab_name}
    fig.update_layout(font=PLOTLY_FONT)

# --- 데이터 로딩 (캐싱) ---
@st.cache_data(show_spinner=False)
def get_data():
    return load_all_data()


try:
    data = get_data()
except Exception as e:
    st.error(f"데이터 로딩 실패: {e}")
    data = {}

apt_df = data.get("apt", pd.DataFrame())
jeonse_df = data.get("jeonse", pd.DataFrame())
wolse_df = data.get("wolse", pd.DataFrame())
rent_all_df = data.get("rent_all", pd.DataFrame())
pop_df = data.get("pop", pd.DataFrame())
grdp_df = data.get("grdp", pd.DataFrame())
permit_df = data.get("permit", pd.DataFrame())
nps_df = data.get("nps", pd.DataFrame())
loan_df = data.get("loan", pd.DataFrame())
asset_df = data.get("asset", pd.DataFrame())
yearly_df = data.get("yearly", pd.DataFrame())
monthly_df = data.get("monthly", pd.DataFrame())
policy_events_df = data.get("policy_events", pd.DataFrame())
quintile_df = data.get("quintile", pd.DataFrame())
movein_plan_df = data.get("movein_plan", pd.DataFrame())
movein_sigungu_monthly_df = data.get("movein_sigungu_monthly", pd.DataFrame())
movein_sido_monthly_df = data.get("movein_sido_monthly", pd.DataFrame())

# --- 사이드바 필터 ---
st.sidebar.title("필터 설정")

# 캐시 재빌드 버튼
if st.sidebar.button("Rebuild Cache"):
    with st.sidebar:
        with st.spinner("캐시 재빌드 중..."):
            load_apt_data(force_rebuild=True)
            load_rent_data("jeonse", force_rebuild=True)
            load_rent_data("wolse", force_rebuild=True)
            load_rent_data("all", force_rebuild=True)
        st.success("캐시 재빌드 완료!")
        st.cache_data.clear()
        st.rerun()

# 분석 모드 선택
analysis_mode = st.sidebar.radio(
    "분석 모드",
    ["매매 분석", "전세 분석", "월세 분석", "전체임대 분석"],
    help="매매 분석: 전세/월세 지표가 원인변수로 포함됩니다",
)

# 시도 선택
all_sido = sorted(apt_df["시도"].dropna().unique()) if "시도" in apt_df.columns else []
selected_sido = st.sidebar.multiselect("시도 선택", all_sido, default=["서울"] if "서울" in all_sido else all_sido[:1])

# 시군구(지역코드) 선택
if "지역코드" in apt_df.columns and selected_sido:
    sido_apt = apt_df[apt_df["시도"].isin(selected_sido)]
    all_codes = sorted(sido_apt["지역코드"].dropna().unique())
    code_labels = {code: f"{get_sigungu_name(code)} ({sido_apt[sido_apt['지역코드']==code]['시도'].iloc[0]})" for code in all_codes if len(sido_apt[sido_apt['지역코드']==code]) > 0}
    selected_codes = st.sidebar.multiselect(
        "시군구(지역코드) 선택",
        options=all_codes,
        format_func=lambda x: code_labels.get(x, x),
        default=[],
        help="비워두면 선택한 시도 전체가 적용됩니다",
    )
else:
    selected_codes = []

# 기간 선택
year_range = (int(apt_df["연도"].min()), int(apt_df["연도"].max())) if not apt_df.empty else (2006, 2024)
selected_years = st.sidebar.slider(
    "기간 선택",
    min_value=year_range[0],
    max_value=year_range[1],
    value=year_range,
)

freq = st.sidebar.radio("분석 단위", ["연별", "월별"])

# --- 분석 모드별 변수 설정 ---
# 결과(Y) 변수: 현재 분석 대상의 가격/거래량
# 원인(X) 변수: 외부 요인 + (매매 모드일 때) 임대차 지표

# 임대차 컬럼명 매핑
RENT_MODE_MAP = {
    "전세 분석": {"df_key": "jeonse", "price": "보증금평균", "vol": "임대거래량", "unit": "보증금단가_per_m2",
                  "sido_price": "전세_보증금평균", "sido_vol": "전세_거래량", "sido_unit": "전세_보증금단가"},
    "월세 분석": {"df_key": "wolse", "price": "보증금평균", "vol": "임대거래량", "unit": "보증금단가_per_m2",
                  "wolse": "월세평균", "sido_price": "월세_보증금평균", "sido_vol": "월세_거래량",
                  "sido_unit": "월세_보증금단가", "sido_wolse": "월세_월세평균"},
    "전체임대 분석": {"df_key": "rent_all", "price": "보증금평균", "vol": "임대거래량", "unit": "보증금단가_per_m2",
                      "wolse": "월세평균", "sido_price": "임대전체_보증금평균", "sido_vol": "임대전체_거래량",
                      "sido_unit": "임대전체_보증금단가", "sido_wolse": "임대전체_월세평균"},
}

if analysis_mode == "매매 분석":
    # 결과변수: 매매 지표
    result_vars = ["평균가격", "거래량", "평균단가_per_m2"]
    # 원인변수: 외부요인 + 임대차 지표 + 수요(소득/대출/자산) 지표
    cause_vars = []
    for col in ["총인구", "GRDP", "인허가_호수"]:
        if col in yearly_df.columns:
            cause_vars.append(col)
    for col in ["NPS_가입자수", "NPS_1인당고지금액", "NPS_사업장수", "NPS_고용증감",
                "주담대_잔액", "주담대_증감률", "주담대_비중",
                "가구_자산평균", "가구_부채평균", "가구_순자산", "가구_소득평균", "DSR",
                "미분양_평균", "미분양_호수", "입주예정_세대수", "입주예정_단지수",
                "전월세전환율", "지가변동률",
                "기준금리", "CD_91일", "국고채_3년", "국고채_5년", "국고채_10년",
                "아파트매매가격지수", "아파트전세가격지수"]:
        if col in yearly_df.columns:
            cause_vars.append(col)
    for col in ["전세_보증금평균", "전세_거래량", "전세_보증금단가",
                "월세_보증금평균", "월세_거래량", "월세_보증금단가", "월세_월세평균",
                "임대전체_보증금평균", "임대전체_거래량", "임대전체_보증금단가", "임대전체_월세평균"]:
        if col in yearly_df.columns:
            cause_vars.append(col)
    available_vars = result_vars + cause_vars
    price_col = "평균가격"
    vol_col = "거래량"
    unit_col = "평균단가_per_m2"
    mode_label = "매매"
else:
    # 임대차 분석 모드
    rm = RENT_MODE_MAP[analysis_mode]
    result_vars = [rm["sido_price"], rm["sido_vol"], rm["sido_unit"]]
    if "sido_wolse" in rm and rm["sido_wolse"] in yearly_df.columns:
        result_vars.append(rm["sido_wolse"])
    cause_vars = []
    for col in ["총인구", "GRDP", "인허가_호수",
                "NPS_가입자수", "NPS_1인당고지금액", "NPS_사업장수", "NPS_고용증감",
                "주담대_잔액", "주담대_증감률", "주담대_비중",
                "가구_자산평균", "가구_부채평균", "가구_순자산", "가구_소득평균", "DSR",
                "미분양_평균", "미분양_호수", "입주예정_세대수", "입주예정_단지수",
                "전월세전환율", "지가변동률",
                "기준금리", "CD_91일", "국고채_3년", "국고채_5년", "국고채_10년",
                "아파트매매가격지수", "아파트전세가격지수"]:
        if col in yearly_df.columns:
            cause_vars.append(col)
    available_vars = result_vars + cause_vars
    price_col = rm["sido_price"]
    vol_col = rm["sido_vol"]
    unit_col = rm["sido_unit"]
    mode_label = analysis_mode.replace(" 분석", "")

    # 시군구 레벨 임대차 데이터
    rent_src_df = data[rm["df_key"]]

# --- 데이터 필터링 ---
def filter_apt_data(df):
    """시군구 코드가 선택되었으면 시군구로, 아니면 시도로 필터"""
    if df.empty:
        return df
    mask = df["연도"].between(*selected_years)
    if selected_codes:
        mask = mask & df["지역코드"].isin(selected_codes)
    elif selected_sido and "시도" in df.columns:
        mask = mask & df["시도"].isin(selected_sido)
    return df[mask].copy()


def filter_sido_data(df):
    """시도 레벨 데이터 필터"""
    if df.empty:
        return df
    mask = df["연도"].between(*selected_years)
    if selected_sido:
        mask = mask & df["시도"].isin(selected_sido)
    return df[mask].copy()


filtered_apt = filter_apt_data(apt_df)
filtered_yearly = filter_sido_data(yearly_df)
filtered_monthly = filter_sido_data(monthly_df) if not monthly_df.empty else pd.DataFrame()

# 임대차 시군구 레벨 필터 (임대차 분석 모드용)
if analysis_mode != "매매 분석":
    filtered_rent = filter_apt_data(rent_src_df)
else:
    filtered_rent = pd.DataFrame()

# 분석 단위에 따른 시도 레벨 데이터
analysis_df = filtered_monthly if freq == "월별" and not filtered_monthly.empty else filtered_yearly

# --- 시군구 레벨 집계 ---
def aggregate_by_code(df, time_col, agg_cols=None):
    """지역코드별로 시계열 데이터 집계"""
    if df.empty:
        return df
    group_cols = ["지역코드", time_col] if time_col in df.columns else ["지역코드", "연도"]
    if "시도" in df.columns:
        group_cols = ["시도"] + group_cols

    if agg_cols is None:
        agg_cols = {"평균가격": ("평균가격", "mean"), "거래량": ("거래량", "sum"), "평균단가_per_m2": ("평균단가_per_m2", "mean")}

    return df.groupby(group_cols).agg(**agg_cols).reset_index()


def aggregate_rent_by_code(df, time_col):
    """임대차 데이터를 지역코드별로 시계열 집계"""
    if df.empty:
        return df
    group_cols = ["지역코드", time_col] if time_col in df.columns else ["지역코드", "연도"]
    if "시도" in df.columns:
        group_cols = ["시도"] + group_cols

    agg_dict = {
        "보증금평균": ("보증금평균", "mean"),
        "임대거래량": ("임대거래량", "sum"),
        "보증금단가_per_m2": ("보증금단가_per_m2", "mean"),
    }
    if "월세평균" in df.columns:
        agg_dict["월세평균"] = ("월세평균", "mean")

    return df.groupby(group_cols).agg(**agg_dict).reset_index()


# --- 수식 계산 캐시 함수 ---
@st.cache_data(show_spinner=False)
def _compute_formulas(
    formula_strs: tuple,   # ((label, expr_str), ...)
    var_names: tuple,      # 사용 가능한 수치형 컬럼명 목록
    sido_list: tuple,
    time_col: str,
    cache_key: str,
    _src_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    수식 문자열을 numexpr로 계산하여 캐싱 (eval() 대신 사용 — RCE 취약점 원천 차단).
    numexpr은 사칙연산·log/exp/sqrt/abs 등 수치 연산만 허용하며 속성 접근·함수 호출이 문법적으로 불가.
    _src_df: 언더스코어 접두어로 Streamlit 해싱 제외 (cache_key가 대신 무효화 담당)
    """
    src = _src_df.copy()
    if sido_list:
        src = src[src["시도"].isin(list(sido_list))]

    parts = []
    for sido_name, group in src.groupby("시도"):
        group = group.sort_values(time_col).reset_index(drop=True)
        row = group[[time_col, "시도"]].copy()
        # numexpr local_dict: 변수명 → numpy array (pandas Series 자동 변환)
        namespace_ne = {}
        for col in var_names:
            if col in group.columns:
                namespace_ne[col] = group[col].astype(float).values
        for label, expr in formula_strs:
            if not expr.strip():
                row[label] = np.nan
                continue
            try:
                with np.errstate(divide="ignore", invalid="ignore"):
                    result = ne.evaluate(expr, local_dict=namespace_ne)
                if result.ndim == 0:  # 스칼라 결과
                    row[label] = float(result)
                else:
                    s = pd.Series(result, index=group.index)
                    row[label] = s.replace([np.inf, -np.inf], np.nan).values
            except Exception:
                row[label] = np.nan
        parts.append(row)

    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


# --- 페이지 구성 ---
# 탭을 두 축으로 분리하고, '선택한 페이지 1개'만 렌더한다(lazy rendering).
#   🧭 직관 축: 시장을 10초 안에 직관적으로 이해 (Overview·수요공급·거래·매물)
#   🔬 검증 축: 구매력을 정량화해 가설을 세우고 실거래가에 대입 (매수판단·적정가·자유차트)
# st.tabs는 7개 탭 본문을 매 실행마다 전부 계산해 느리므로, segmented_control로 바꿔
# 비활성 페이지의 무거운 차트·연산을 아예 실행하지 않는다.
_PAGES = [
    "🧭 Overview", "🧭 수요공급분석", "🧭 거래현황", "🧭 매물현황",
    "🔬 매수판단", "🔬 적정가·구매력", "🔬 자유차트",
]
st.caption("🧭 직관 = 시장을 빠르게 이해  ·  🔬 검증 = 구매력 정량화·가설 검증")
_sel = st.segmented_control(
    "분석 화면",
    _PAGES,
    default=_PAGES[0],
    key="active_page",
    label_visibility="collapsed",
)
_active = _sel or _PAGES[0]

# === 전역 KPI 요약 바 — 모든 탭에서 항상 표시 ===
def _render_kpi_bar():
    _items = []

    # 1. 시장 온도계
    try:
        _ms, _md, _ = compute_market_temperature(analysis_df)
        _ml = "과열" if _ms > 60 else ("침체" if _ms < 40 else "중립")
        _mc = "#EF5350" if _ms > 60 else ("#2962FF" if _ms < 40 else "#F5B301")
        _items.append(("🌡️ 시장온도계", f"{_ms:.0f}", f"{_md:+.1f} {_ml}", _mc))
    except Exception:
        _items.append(("🌡️ 시장온도계", "N/A", "", "#8B949E"))

    # 2. 평균가격  3. 거래량
    if analysis_mode == "매매 분석" and not filtered_apt.empty and "연도" in filtered_apt.columns:
        _yl = int(filtered_apt["연도"].max())
        _yp = _yl - 1
        _grp = filtered_apt.groupby("연도")
        _pm = _grp["평균가격"].mean()
        _pnow, _pprev = _pm.get(_yl), _pm.get(_yp)
        if _pnow is not None:
            _pd = f"YoY {(_pnow-_pprev)/_pprev*100:+.1f}%" if (_pprev and _pprev != 0) else ""
            _items.append(("💰 평균가격", f"{_pnow:,.0f}만", _pd,
                           "#26A69A" if (_pprev and _pnow > _pprev) else "#EF5350"))
        else:
            _items.append(("💰 평균가격", "N/A", "", "#8B949E"))
        _vm = _grp["거래량"].sum()
        _vnow, _vprev = _vm.get(_yl), _vm.get(_yp)
        if _vnow is not None:
            _vd = f"YoY {int(_vnow-_vprev):+,}건" if _vprev is not None else ""
            _items.append(("📊 거래량", f"{int(_vnow):,}건", _vd,
                           "#26A69A" if (_vprev is not None and _vnow > _vprev) else "#EF5350"))
        else:
            _items.append(("📊 거래량", "N/A", "", "#8B949E"))
    else:
        _items.append(("💰 평균가격", "N/A", "", "#8B949E"))
        _items.append(("📊 거래량", "N/A", "", "#8B949E"))

    # 4. 전세가율
    try:
        if "전세가율" in analysis_df.columns and "연도" in analysis_df.columns and not analysis_df.empty:
            _jg = analysis_df.groupby("연도")["전세가율"].mean()
            _jy = int(analysis_df["연도"].max())
            _jr, _jp = _jg.get(_jy), _jg.get(_jy - 1)
            if _jr is not None:
                _jd = f"{_jr-_jp:+.1f}%p" if _jp is not None else ""
                _items.append(("🏠 전세가율", f"{_jr:.1f}%", _jd,
                               "#26A69A" if (_jp is not None and _jr > _jp) else "#EF5350"))
            else:
                _items.append(("🏠 전세가율", "N/A", "", "#8B949E"))
        else:
            _items.append(("🏠 전세가율", "N/A", "", "#8B949E"))
    except Exception:
        _items.append(("🏠 전세가율", "N/A", "", "#8B949E"))

    # 5. KB 매수우위 or 주택가격전망CSI
    try:
        _kc = next((c for c in ["KB_매수우위지수", "주택가격전망CSI"]
                    if c in analysis_df.columns and analysis_df[c].notna().any()), None)
        if _kc and not analysis_df.empty:
            _kv = float(analysis_df[_kc].dropna().iloc[-1])
            _kl = "매수우위" if _kv > 100 else "매도우위"
            _kname = "KB 매수우위" if "매수" in _kc else "가격전망CSI"
            _items.append((f"💡 {_kname}", f"{_kv:.1f}",
                           _kl, "#26A69A" if _kv > 100 else "#EF5350"))
        else:
            _items.append(("💡 KB매수우위", "N/A", "", "#8B949E"))
    except Exception:
        _items.append(("💡 KB매수우위", "N/A", "", "#8B949E"))

    # HTML 렌더링
    _cards = ""
    for _lbl, _val, _dlt, _clr in _items:
        _dlt_html = (f'<div style="font-size:11px;color:{_clr};margin-top:2px;">{_dlt}</div>'
                     if _dlt else '<div style="font-size:11px;min-height:16px;"></div>')
        _cards += f"""
        <div style="flex:1;background:#161B22;border:1px solid #2A313C;border-radius:10px;
                    padding:10px 14px;min-width:0;box-sizing:border-box;">
          <div style="font-size:11px;color:#8B949E;font-weight:600;
                      white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{_lbl}</div>
          <div style="font-size:1.3rem;font-weight:800;color:#E6EDF3;
                      line-height:1.2;margin-top:4px;letter-spacing:-0.02em;">{_val}</div>
          {_dlt_html}
        </div>"""
    st.markdown(
        f'<div style="display:flex;gap:10px;margin:10px 0 4px;">{_cards}</div>'
        '<hr style="border:none;border-top:1px solid #2A313C;margin:12px 0 16px;">',
        unsafe_allow_html=True,
    )

_render_kpi_bar()

# 각 탭 변수는 이제 '활성 여부' 불리언이다. 아래 `if <탭>:` 블록은 alias를 그대로
# 따르므로(예: main_tab6 = valuation_tab), 해당 페이지가 선택됐을 때만 실행된다.
overview_tab      = (_active == "🧭 Overview")
demand_supply_tab = (_active == "🧭 수요공급분석")
transaction_tab   = (_active == "🧭 거래현황")
listing_tab       = (_active == "🧭 매물현황")
buy_decision_tab  = (_active == "🔬 매수판단")
affordability_tab = (_active == "🔬 적정가·구매력")
valuation_tab     = (_active == "🔬 자유차트")

# 기존 11개 화면 블록을 7개 상위 페이지로 재배치한다.
main_tab1 = overview_tab
main_tab2 = transaction_tab
main_tab3 = transaction_tab
main_tab4 = demand_supply_tab
main_tab5 = demand_supply_tab
main_tab6 = valuation_tab
main_tab7 = valuation_tab
main_tab8 = valuation_tab
main_tab9 = valuation_tab
main_tab10 = affordability_tab  # 소득-매물 매칭/구매력 → 독립 '적정가·구매력' 페이지로 승격
main_tab11 = listing_tab        # 커뮤니티 게시판은 매물현황 페이지에 유지

TAB_USAGE_GUIDES = {
    "Overview": {
        "purpose": "선택 지역의 가격·거래·전세·수요·공급 상태를 한 화면에서 빠르게 훑어봅니다.",
        "steps": [
            "왼쪽 필터에서 시도/시군구, 기간, 분석 단위를 먼저 정합니다.",
            "핵심 KPI와 저평가·고평가 순위를 보고 관심 지역을 좁힙니다.",
            "데이터 출처 및 최신성 표에서 지표별 기준 시점과 결측률을 확인합니다.",
        ],
        "example": "예: 서울 → 강남구·송파구 선택 후 Overview에서 밸류스코어와 거래량 변화를 먼저 확인합니다.",
    },
    "매수판단": {
        "purpose": "두 지역을 목적별 가중치로 비교해 어느 쪽을 더 깊게 볼지 판단합니다.",
        "steps": [
            "지역 A와 지역 B를 고릅니다.",
            "목적을 실거주/전세끼고 매수/투자 중 하나로 선택합니다.",
            "종합 판단, 축별 비교표, 2x2 차트를 함께 보고 강점·주의축을 확인합니다.",
        ],
        "example": "예: 지역 A=종로구, 지역 B=동작구, 목적=전세끼고 매수로 놓고 전세지지력과 입주물량 부담을 비교합니다.",
    },
    "수요공급분석": {
        "purpose": "수요·공급 균형, 입주물량, 인허가/착공/준공, 미분양 흐름을 확인합니다.",
        "steps": [
            "관심 지역과 기간을 선택합니다.",
            "수요-공급 분석기에서 가격 국면과 수요·공급 지표의 방향을 확인합니다.",
            "입주물량/인허가/미분양 세부 탭에서 공급 부담이 언제 집중되는지 봅니다.",
        ],
        "example": "예: 경기 화성시를 선택하고 2026년 입주예정 세대수와 미분양 흐름을 함께 확인합니다.",
    },
    "거래현황": {
        "purpose": "거래량, 평균가격, 갭, 지역 순위 등 실제 거래 기반 시장 활력을 봅니다.",
        "steps": [
            "매매/전세/월세 모드와 기간을 선택합니다.",
            "거래현황에서 최근 거래량과 가격 흐름을 확인합니다.",
            "시계열 비교, 가격비교, 갭분석, 지역순위로 관심 지역을 서로 비교합니다.",
        ],
        "example": "예: 서울 주요 구를 선택하고 거래량이 회복되는 지역을 지역순위에서 찾은 뒤 가격비교 탭에서 추세를 확인합니다.",
    },
    "매물현황": {
        "purpose": "업로드한 매물 데이터를 기준으로 호가, 매물 수, 단지별 집중도를 확인합니다.",
        "steps": [
            "네이버 등에서 정리한 매물 파일을 업로드하거나 기존 세션 데이터를 사용합니다.",
            "지역/거래유형/가격 범위 필터를 조정합니다.",
            "KPI, 호가 분포, 단지별 매물현황을 보고 원천 표는 펼쳐서 검토합니다.",
        ],
        "example": "예: 성동구 아파트 매물 CSV를 올리고 매매만 필터링해 단지별 최저 호가와 매물 집중도를 확인합니다.",
    },
    "적정가·구매력": {
        "purpose": "소득분위별 구매력(순자산 + 주담대 PMT 역산 대출가능액)을 정량화해, 실제 시군구 거래가와의 갭을 봅니다.",
        "steps": [
            "기준연도·주담대금리·DSR·LTV·대출만기를 설정합니다.",
            "퍼센타일별 자금여력(상위 1%·5%·10% 등)을 확인합니다.",
            "시군구 급지순위와 소득-급지 매칭에서 '어느 분위가 어느 급지를 살 수 있는지'와 갭(음수=살 수 있음)을 봅니다.",
        ],
        "example": "예: 상위 10% 순자산+소득, 20년 주담대 기준 자금여력을 계산해 서울 서초구 실거래가와의 갭을 확인합니다.",
    },
    "자유차트": {
        "purpose": "여러 지역·지표를 자유롭게 겹쳐 보고, 조건식/전략검증/통계검증으로 가설을 점검합니다.",
        "steps": [
            "지역, 지표, 표시 방식(원값/Index=100/변화율 등)을 선택합니다.",
            "슈퍼차트와 전략검증에서 원하는 조건을 만들어 과거 반복성을 봅니다.",
            "통계검증, 회귀분석, 상관관계, 선행 신호로 가설의 신뢰도를 점검합니다.",
        ],
        "example": "예: 서울과 경기의 아파트매매가격지수·금리·입주예정 세대수를 Index=100으로 겹쳐 선행 신호를 확인합니다.",
    },
}


def render_tab_usage_guide(tab_name):
    guide = TAB_USAGE_GUIDES.get(tab_name)
    if not guide:
        return
    with st.expander(f"처음 보는 분을 위한 사용법: {tab_name}", expanded=False):
        st.markdown(f"**무엇을 보는 탭인가요?** {guide['purpose']}")
        st.markdown("**사용 순서**")
        for idx, step in enumerate(guide["steps"], start=1):
            st.write(f"{idx}. {step}")
        st.markdown(f"**예시** {guide['example']}")


# Tab: 매수판단
if buy_decision_tab:
    st.header("어디를 살까?")
    st.caption("과거 및 현재 데이터 기준으로 두 지역을 비교하는 참고 화면입니다.")
    render_tab_usage_guide("매수판단")

    buy_decision_source_df = filtered_monthly if not filtered_monthly.empty else filtered_yearly
    buy_region_col = next((col for col in ["지역코드", "시군구", "지역", "시도"] if col in buy_decision_source_df.columns), None)

    if buy_decision_source_df.empty or buy_region_col is None:
        st.info("매수판단에 사용할 지역 데이터가 부족합니다. 필터 또는 데이터 적재 상태를 확인하세요.")
        with st.expander("데이터 품질/주의사항", expanded=True):
            st.write("필수 데이터가 부족해 비교 화면을 표시하지 못했습니다.")
            st.write("본 화면은 과거 및 현재 데이터 기반의 비교 참고 자료이며, 투자 판단을 대신하지 않습니다.")
    else:
        buy_region_options = sorted(buy_decision_source_df[buy_region_col].dropna().astype(str).unique())

        def _buy_region_label(region_value):
            region_rows = buy_decision_source_df[buy_decision_source_df[buy_region_col].astype(str) == str(region_value)]
            if "시군구" in region_rows.columns and not region_rows["시군구"].dropna().empty:
                return str(region_rows["시군구"].dropna().iloc[0])
            if "지역" in region_rows.columns and not region_rows["지역"].dropna().empty:
                return str(region_rows["지역"].dropna().iloc[0])
            if "시도" in region_rows.columns and not region_rows["시도"].dropna().empty:
                return str(region_rows["시도"].dropna().iloc[0])
            if buy_region_col == "지역코드":
                return f"{get_sigungu_name(region_value)} ({region_value})"
            return str(region_value)

        if len(buy_region_options) < 2:
            st.info("비교하려면 최소 2개 지역 데이터가 필요합니다.")
            with st.expander("데이터 품질/주의사항", expanded=True):
                st.write("선택 가능한 지역이 2개 미만입니다.")
                st.write("본 화면은 과거 및 현재 데이터 기반의 비교 참고 자료이며, 투자 판단을 대신하지 않습니다.")
        else:
            input_cols = st.columns([1, 1, 1])
            with input_cols[0]:
                buy_region_a = st.selectbox("지역 A", buy_region_options, format_func=_buy_region_label, key="buy_decision_region_a")
            with input_cols[1]:
                default_b_index = 1 if len(buy_region_options) > 1 else 0
                buy_region_b = st.selectbox("지역 B", buy_region_options, index=default_b_index, format_func=_buy_region_label, key="buy_decision_region_b")
            with input_cols[2]:
                buy_purpose = st.radio("목적", PURPOSES, horizontal=True, key="buy_decision_purpose")

            try:
                buy_vm = build_buy_decision_view_model(buy_decision_source_df, buy_region_a, buy_region_b, buy_purpose)
            except Exception as exc:
                buy_vm = None
                st.warning(f"매수판단 화면을 구성하는 중 문제가 발생했습니다: {exc}")

            if buy_vm:
                summary_card = buy_vm.get("summary_card", {})
                st.subheader("종합 판단")
                headline_cols = st.columns([2, 1])
                with headline_cols[0]:
                    st.info(summary_card.get("headline") or summary_card.get("summary", "비교 참고 데이터를 확인하세요."))
                with headline_cols[1]:
                    st.metric("데이터 품질", summary_card.get("data_quality_label", "부분 확인 필요"))
                st.write(summary_card.get("summary", "비교 참고 데이터를 확인하세요."))
                st.caption(summary_card.get("caution", "본 화면은 과거 및 현재 데이터 기반의 비교 참고 자료입니다."))

                reason_cols = st.columns(2)
                with reason_cols[0]:
                    st.markdown("**핵심 근거**")
                    for reason in summary_card.get("reasons", []) or ["종합 점수와 축별 점수를 함께 확인하세요."]:
                        st.write(f"- {reason}")
                with reason_cols[1]:
                    st.markdown("**다음 확인사항**")
                    for check in summary_card.get("next_checks", []) or ["단지별 실거래와 호가를 추가 확인하세요."]:
                        st.write(f"- {check}")

                region_card_cols = st.columns(2)
                for card_col, region_card in zip(region_card_cols, buy_vm.get("region_cards", [])):
                    with card_col:
                        score_value = region_card.get("total_score")
                        score_text = "데이터 부족" if pd.isna(score_value) else f"{score_value:.1f}점"
                        st.metric(region_card.get("region_name", "지역"), score_text, region_card.get("grade", "판단 보류"))
                        st.markdown("**강점**")
                        st.write(", ".join(region_card.get("strengths", [])) if region_card.get("strengths") else "뚜렷한 강점 축 없음")
                        st.markdown("**주의축**")
                        st.write(", ".join(region_card.get("weaknesses", [])) if region_card.get("weaknesses") else "뚜렷한 주의축 없음")
                        key_points = region_card.get("key_points", [])
                        if key_points:
                            st.dataframe(pd.DataFrame(key_points), use_container_width=True)

                st.subheader("비교표")
                comparison_rows = buy_vm.get("comparison_rows", [])
                if comparison_rows:
                    st.dataframe(pd.DataFrame(comparison_rows), use_container_width=True)
                else:
                    st.info("표시할 비교표 데이터가 부족합니다.")

                st.subheader("2x2 차트")
                chart_titles = {
                    "price": "매매가격 추이",
                    "jeonse_ratio": "전세가율 추이",
                    "transaction_volume": "거래량 추이",
                    "movein_volume": "입주물량 추이",
                }
                chart_positions = {
                    "price": (1, 1),
                    "jeonse_ratio": (1, 2),
                    "transaction_volume": (2, 1),
                    "movein_volume": (2, 2),
                }
                buy_fig = make_subplots(rows=2, cols=2, subplot_titles=[chart_titles[key] for key in chart_positions])
                has_chart_points = False
                for chart_key, position in chart_positions.items():
                    for series in buy_vm.get("charts", {}).get(chart_key, []):
                        points_df = pd.DataFrame(series.get("points", []))
                        if points_df.empty:
                            continue
                        has_chart_points = True
                        buy_fig.add_trace(
                            go.Scatter(x=points_df["연월"], y=points_df["value"], mode="lines+markers", name=f"{chart_titles[chart_key]} - {series.get('region_name', '지역')}"),
                            row=position[0],
                            col=position[1],
                        )
                buy_fig.update_layout(height=700, font=PLOTLY_FONT, legend=dict(orientation="h"))
                if has_chart_points:
                    st.plotly_chart(buy_fig, use_container_width=True)
                else:
                    st.info("차트로 표시할 시계열 데이터가 부족합니다.")

                with st.expander("데이터 품질/주의사항", expanded=False):
                    grouped_quality = buy_vm.get("data_quality_grouped", {})
                    for group_name in ["필수", "선택", "주의"]:
                        messages = grouped_quality.get(group_name, [])
                        if messages:
                            st.markdown(f"**{group_name}**")
                            for message in messages:
                                st.write(f"- {message}")
                    if not buy_vm.get("data_quality", []):
                        st.write("필수 데이터 품질 이슈가 확인되지 않았습니다.")
                    st.write("본 화면은 과거 및 현재 데이터 기반의 비교 참고 자료이며, 투자 판단을 대신하지 않습니다.")

# ============================
# Tab 1: Overview
# ============================

# 밸류스코어 캐싱 함수
@st.cache_data(show_spinner=False)
def _cached_value_score(_apt_df, _jeonse_df, _nps_df, year):
    """밸류스코어 계산 캐싱 (언더스코어 접두어로 해싱 제외)"""
    return compute_value_score(_apt_df, _jeonse_df, _nps_df, year=year)

@st.cache_data(show_spinner=False)
def _cached_rank_sigungu(_apt_df, _nps_df, year):
    """시군구 급지순위 캐싱"""
    return rank_sigungu_grade(_apt_df, _nps_df, year=year)

# Prophet 예측은 가장 무거운 연산이므로 결과를 캐싱한다.
# _df(apt_df 기반)는 세션 내 고정이라 해싱 제외하고 sido/periods/price_col로만 키를 만든다.
@st.cache_data(show_spinner=False)
def _cached_forecast_price(_df, sido, periods, price_col):
    """Prophet 가격예측 캐싱"""
    return forecast_price(df=_df, sido=sido, periods=periods, price_col=price_col)

# 클러스터링/Granger는 필터된 집계 DF(작음) 기반이므로 DF를 그대로 해싱해 정확히 캐싱한다.
@st.cache_data(show_spinner=False)
def _cached_cluster_regions(df, features, n_clusters):
    """지역 클러스터링 캐싱"""
    return cluster_regions(df, list(features), n_clusters)

@st.cache_data(show_spinner=False)
def _cached_granger(df, y_var, x_var, max_lag):
    """Granger 인과성 캐싱"""
    return granger_causality_test(df, y_var, x_var, max_lag=max_lag)


def _parse_korean_price_to_manwon(value):
    """한국식 호가 문자열을 만원 단위 숫자로 변환."""
    if pd.isna(value):
        return np.nan
    if isinstance(value, (int, float, np.integer, np.floating)):
        return float(value)

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return np.nan
    if any(token in text for token in ["협의", "문의", "확인"]):
        return np.nan

    cleaned = text.replace(",", "").replace(" ", "")
    cleaned = cleaned.replace("만원", "").replace("만", "")

    eok_match = pd.Series([cleaned]).str.extract(r"([\d.]+)억")[0].iloc[0]
    if pd.notna(eok_match):
        eok_value = float(eok_match) * 10000
        tail = cleaned.split("억", 1)[1]
        tail_match = pd.Series([tail]).str.extract(r"([\d.]+)")[0].iloc[0]
        tail_value = float(tail_match) if pd.notna(tail_match) else 0.0
        return eok_value + tail_value

    number_match = pd.Series([cleaned]).str.extract(r"([\d.]+)")[0].iloc[0]
    return float(number_match) if pd.notna(number_match) else np.nan


def _format_listing_price(value):
    """만원 단위 매물가격을 한국식 억/만원 표기로 표시."""
    if pd.isna(value):
        return "-"
    value = float(value)
    if value >= 10000:
        eok = int(value // 10000)
        man = int(round(value % 10000))
        return f"{eok}억" if man == 0 else f"{eok}억 {man:,}만"
    return f"{int(round(value)):,}만"


def _clean_listing_df(df: pd.DataFrame) -> pd.DataFrame:
    """매물현황 화면용 최소 정제."""
    if df is None or df.empty:
        return pd.DataFrame()
    cleaned = df.copy()
    for col in ["시도", "시군구", "단지명", "거래유형", "면적", "층", "매물URL", "동", "향", "중개사", "비고"]:
        if col in cleaned.columns:
            cleaned[col] = cleaned[col].fillna("").astype(str).str.strip()
            cleaned.loc[cleaned[col].str.lower().isin(["nan", "none", "null"]), col] = ""
    if "매물가격" in cleaned.columns:
        cleaned["매물가격"] = cleaned["매물가격"].apply(_parse_korean_price_to_manwon)
    for col in ["수집일", "확인일"]:
        if col in cleaned.columns:
            cleaned[col] = pd.to_datetime(cleaned[col], errors="coerce")
    return cleaned.reset_index(drop=True)


def _filter_listing_df(df: pd.DataFrame, sido=None, sigungu=None, trade_types=None, price_range=None, keyword="") -> pd.DataFrame:
    """매물현황 필터 적용."""
    if df is None or df.empty:
        return pd.DataFrame()
    filtered = df.copy()
    if sido and "시도" in filtered.columns:
        filtered = filtered[filtered["시도"].isin(sido)]
    if sigungu and "시군구" in filtered.columns:
        filtered = filtered[filtered["시군구"].isin(sigungu)]
    if trade_types and "거래유형" in filtered.columns:
        filtered = filtered[filtered["거래유형"].isin(trade_types)]
    if price_range and "매물가격" in filtered.columns:
        low, high = price_range
        filtered = filtered[filtered["매물가격"].between(low, high, inclusive="both")]
    if keyword:
        keyword = str(keyword).strip().lower()
        if keyword:
            cols = [c for c in ["단지명", "시군구", "시도", "동", "비고"] if c in filtered.columns]
            if cols:
                mask = filtered[cols].astype(str).apply(lambda row: keyword in " ".join(row).lower(), axis=1)
                filtered = filtered[mask]
    return filtered.reset_index(drop=True)


def _summarize_listing_kpis(df: pd.DataFrame) -> dict:
    """매물현황 KPI 계산."""
    if df is None or df.empty:
        return {"매물수": 0, "단지수": 0, "지역수": 0, "중위호가": np.nan, "최저호가": np.nan, "평균호가": np.nan, "최근확인일": None}
    price = pd.to_numeric(df.get("매물가격", pd.Series(dtype=float)), errors="coerce")
    latest = None
    for col in ["확인일", "수집일"]:
        if col in df.columns and pd.to_datetime(df[col], errors="coerce").notna().any():
            latest = pd.to_datetime(df[col], errors="coerce").max()
            break
    return {
        "매물수": len(df),
        "단지수": df["단지명"].replace("", np.nan).nunique() if "단지명" in df.columns else 0,
        "지역수": df["시군구"].replace("", np.nan).nunique() if "시군구" in df.columns else 0,
        "중위호가": price.median(),
        "최저호가": price.min(),
        "평균호가": price.mean(),
        "최근확인일": latest,
    }


def _build_complex_listing_summary(df: pd.DataFrame) -> pd.DataFrame:
    """단지별 매물 요약 테이블 생성."""
    if df is None or df.empty or "단지명" not in df.columns:
        return pd.DataFrame()
    group_cols = [c for c in ["시도", "시군구", "단지명"] if c in df.columns]
    if not group_cols:
        return pd.DataFrame()
    temp = df.copy()
    temp["매물가격"] = pd.to_numeric(temp.get("매물가격"), errors="coerce")
    agg_dict = {
        "매물수": ("매물가격", "count"),
        "최저호가": ("매물가격", "min"),
        "중위호가": ("매물가격", "median"),
        "평균호가": ("매물가격", "mean"),
        "최고호가": ("매물가격", "max"),
    }
    if "거래유형" in temp.columns:
        agg_dict["거래유형"] = ("거래유형", lambda s: ", ".join(sorted({str(v) for v in s if str(v).strip()})))
    if "확인일" in temp.columns:
        agg_dict["최근확인일"] = ("확인일", "max")
    summary = temp.groupby(group_cols, dropna=False).agg(**agg_dict).reset_index()
    if "최저호가" in summary.columns:
        summary = summary.sort_values(["최저호가", "매물수"], ascending=[True, False])
    return summary.reset_index(drop=True)


def normalize_naver_listings(raw_df: pd.DataFrame) -> pd.DataFrame:
    """네이버 매물 CSV/JSON 응답을 앱 표준 매물 스키마로 정규화."""
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()

    col_aliases = {
        "수집일": ["수집일", "collected_at", "collectionDate"],
        "시도": ["시도", "sido", "city"],
        "시군구": ["시군구", "sigungu", "district"],
        "단지명": ["단지명", "complexName", "단지", "아파트명", "articleName"],
        "거래유형": ["거래유형", "tradeTypeName", "거래방식", "매매구분"],
        "매물가격": ["매물가격", "price", "dealOrWarrantPrc", "매매가", "호가"],
        "면적": ["면적", "area", "공급면적", "exclusiveAreaName"],
        "층": ["층", "floorInfo", "floor"],
        "매물URL": ["매물URL", "url", "articleUrl", "link"],
        "동": ["동", "buildingName"],
        "향": ["향", "direction"],
        "중개사": ["중개사", "realtorName"],
        "확인일": ["확인일", "articleConfirmYmd", "확인매물일"],
        "비고": ["비고", "note", "description"],
    }

    normalized = pd.DataFrame()
    for target_col, aliases in col_aliases.items():
        source_col = next((c for c in aliases if c in raw_df.columns), None)
        normalized[target_col] = raw_df[source_col] if source_col else np.nan

    normalized["단지명"] = normalized["단지명"].astype(str).str.strip()
    normalized["시군구"] = normalized["시군구"].astype(str).str.strip()
    normalized["거래유형"] = normalized["거래유형"].fillna("매매").astype(str).str.strip()
    normalized["매물가격"] = normalized["매물가격"].apply(_parse_korean_price_to_manwon)
    normalized = normalized.dropna(subset=["단지명", "매물가격"], how="any")
    return normalized.reset_index(drop=True)


def parse_naver_listing_upload(uploaded_file) -> pd.DataFrame:
    """업로드된 CSV/JSON 파일을 표준 매물 테이블로 변환."""
    if uploaded_file is None:
        return pd.DataFrame()
    try:
        name = uploaded_file.name.lower()
        if name.endswith(".json"):
            raw = json.load(uploaded_file)
            if isinstance(raw, dict):
                raw = raw.get("articleList") or raw.get("data") or raw.get("items") or [raw]
            raw_df = pd.DataFrame(raw)
        else:
            raw_df = pd.read_csv(uploaded_file)
        return normalize_naver_listings(raw_df)
    except Exception as e:
        st.warning(f"네이버 매물 파일을 해석할 수 없습니다: {e}")
        return pd.DataFrame()


def _fmt_var_by_col(col):
    """변수 메타데이터가 정의되기 전에도 화면이 실행되도록 하는 기본 포맷터."""
    meta = INDICATOR_META.get(str(col))
    if meta:
        return f"[{meta['source']}] {meta['label']}"
    return str(col)


def _indicator_label(col):
    meta = INDICATOR_META.get(str(col))
    return meta["label"] if meta else str(col)


def _available_indicator_options(df, groups=None, include_text=False):
    if df is None or df.empty:
        return []
    cols = []
    groups = set(groups) if groups else None
    for item in INDICATOR_CATALOG:
        col = item["column"]
        if groups and item["group"] not in groups:
            continue
        if col in df.columns and df[col].notna().any():
            if include_text or pd.api.types.is_numeric_dtype(df[col]):
                cols.append(col)
    extra_numeric = [
        c for c in _numeric_rule_columns(df)
        if c not in cols and c not in {"연도", "월", "연월", "지역코드"}
    ]
    return cols + extra_numeric


def _render_indicator_picker(label, df, key, groups=None, default=None, multi=False, allow_extra_numeric=True):
    options = _available_indicator_options(df, groups=groups)
    if allow_extra_numeric:
        for col in _numeric_rule_columns(df):
            if col not in options:
                options.append(col)
    if not options:
        return [] if multi else None

    def _default_index():
        if default in options:
            return options.index(default)
        return 0

    if multi:
        default_list = [c for c in (default or []) if c in options]
        return st.multiselect(label, options, default=default_list, key=key, format_func=_fmt_var_by_col)

    selected = st.selectbox(label, options, index=_default_index(), key=key, format_func=_fmt_var_by_col)
    meta = INDICATOR_META.get(str(selected))
    if meta:
        st.caption(f"단위: {meta['unit']} | 용도: {meta['best_for']} | 주의: {meta['caution']}")
    return selected


def _render_grouped_indicator_picker(label, df, key, default_group="가격", default=None, multi=False):
    available_groups = []
    for item in INDICATOR_CATALOG:
        if item["group"] not in available_groups and item["column"] in df.columns and df[item["column"]].notna().any():
            available_groups.append(item["group"])
    if not available_groups:
        return [] if multi else None
    group_index = available_groups.index(default_group) if default_group in available_groups else 0
    group = st.selectbox(f"{label} 지표군", available_groups, index=group_index, key=f"{key}_group")
    return _render_indicator_picker(label, df, key, groups=[group], default=default, multi=multi, allow_extra_numeric=False)


def _source_caption(cols):
    items = []
    for col in cols:
        meta = INDICATOR_META.get(str(col))
        if meta:
            items.append(f"{meta['label']}({meta['source']})")
        else:
            items.append(str(col))
    return "출처: " + ", ".join(dict.fromkeys(items))


def _apply_saved_widget_settings(settings):
    for key, value in (settings or {}).items():
        st.session_state[key] = value


def _chart_save_payload(extra=None):
    keys = [
        "super_regions", "super_indicators_group", "super_indicators", "super_mode",
        "super_custom_expr", "super_custom_label", "analysis_mode", "selected_sido",
        "selected_years", "freq",
    ]
    payload = {k: st.session_state[k] for k in keys if k in st.session_state}
    if extra:
        payload.update(extra)
    return payload


def _numeric_rule_columns(df):
    excluded = {"연도", "월", "연월", "지역코드"}
    return [
        c for c in df.columns
        if c not in excluded and pd.api.types.is_numeric_dtype(df[c]) and df[c].notna().any()
    ]


def _dataset_quality_snapshot(datasets):
    rows = []
    cache_dir = "cache"
    cache_files = [
        os.path.join(cache_dir, f) for f in os.listdir(cache_dir)
        if os.path.isfile(os.path.join(cache_dir, f))
    ] if os.path.isdir(cache_dir) else []
    latest_cache = max((os.path.getmtime(p) for p in cache_files), default=None)
    for name, df in datasets.items():
        if df is None or df.empty:
            continue
        region_col = "시도" if "시도" in df.columns else ("시군구" if "시군구" in df.columns else None)
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        missing_region_count = 0
        if region_col and numeric_cols:
            missing_region_count = int(
                df.groupby(region_col)[numeric_cols]
                .apply(lambda g: g.isna().any(axis=1).any())
                .sum()
            )
        rows.append({
            "데이터셋": name,
            "행": len(df),
            "지역수": df[region_col].nunique() if region_col else np.nan,
            "결측있는지역": missing_region_count,
        })
    return pd.DataFrame(rows), latest_cache, len(cache_files)


def _render_condition_builder(prefix, candidate_cols, default_rules=None, max_rules=5):
    """지역검색기와 전략검증에서 공유하는 조건 빌더."""
    default_rules = default_rules or []
    candidate_cols = list(dict.fromkeys([c for c in candidate_cols if c]))
    if not candidate_cols:
        return [], "AND"

    combine_label = st.radio(
        "조건 조합",
        ["모두 만족 (AND)", "하나라도 만족 (OR)"],
        horizontal=True,
        key=f"{prefix}_combine_label",
    )
    combine = "OR" if "OR" in combine_label else "AND"

    rules = []
    ops = [">", ">=", "<", "<=", "between", "==", "contains"]
    for i in range(max_rules):
        default = default_rules[i] if i < len(default_rules) else {}
        with st.expander(f"조건 {i + 1}", expanded=i < max(1, len(default_rules))):
            enabled = st.checkbox("사용", value=i < len(default_rules), key=f"{prefix}_rule_enabled_{i}")
            if not enabled:
                continue
            c_type, c0, c1, c2, c3, c4 = st.columns([1.6, 1.4, 2.2, 1.1, 1.3, 1.3])
            catalog_groups = []
            for item in INDICATOR_CATALOG:
                if item["column"] in candidate_cols and item["group"] not in catalog_groups:
                    catalog_groups.append(item["group"])
            fallback_cols = [c for c in candidate_cols if c not in INDICATOR_META]
            default_col = default.get("column", candidate_cols[0])
            default_group = INDICATOR_META.get(default_col, {}).get("group", catalog_groups[0] if catalog_groups else "(기타)")
            condition_types = {
                "값 비교": "compare",
                "연속 상승": "consecutive_up",
                "연속 하락": "consecutive_down",
                "이동평균 위": "vs_moving_avg_above",
                "이동평균 아래": "vs_moving_avg_below",
                "고점 대비": "pct_from_peak",
                "하락 후 반등": "peak_drawdown_then_rebound",
            }
            reverse_types = {v: k for k, v in condition_types.items()}
            default_type = reverse_types.get(default.get("type", "compare"), "값 비교")
            with c_type:
                selected_type_label = st.selectbox(
                    "조건유형",
                    list(condition_types.keys()),
                    index=list(condition_types.keys()).index(default_type),
                    key=f"{prefix}_rule_type_{i}",
                )
                selected_type = condition_types[selected_type_label]
            with c0:
                group_options = catalog_groups + (["(기타)"] if fallback_cols else [])
                group_index = group_options.index(default_group) if default_group in group_options else 0
                selected_group = st.selectbox("지표군", group_options, index=group_index, key=f"{prefix}_rule_group_{i}")
            with c1:
                group_cols = [
                    item["column"] for item in INDICATOR_CATALOG
                    if item["group"] == selected_group and item["column"] in candidate_cols
                ] if selected_group != "(기타)" else fallback_cols
                group_cols = group_cols or candidate_cols
                col_index = group_cols.index(default_col) if default_col in group_cols else 0
                column = st.selectbox(
                    "지표",
                    group_cols,
                    index=col_index,
                    key=f"{prefix}_rule_col_{i}",
                    format_func=_fmt_var_by_col,
                )
                meta = INDICATOR_META.get(str(column))
                if meta:
                    st.caption(f"{meta['source']} | {meta['unit']} | {meta['best_for']}")
            with c2:
                default_op = default.get("op", ">")
                op_options = ["<=", "<", ">=", ">", "between"] if selected_type == "pct_from_peak" else ops
                op = st.selectbox(
                    "조건",
                    op_options,
                    index=op_options.index(default_op) if default_op in op_options else 0,
                    key=f"{prefix}_rule_op_{i}",
                    disabled=selected_type in [
                        "consecutive_up", "consecutive_down", "vs_moving_avg_above",
                        "vs_moving_avg_below", "peak_drawdown_then_rebound",
                    ],
                )
            with c3:
                value_label = {
                    "compare": "값",
                    "consecutive_up": "연속기간",
                    "consecutive_down": "연속기간",
                    "vs_moving_avg_above": "평균기간",
                    "vs_moving_avg_below": "평균기간",
                    "pct_from_peak": "기준(%)",
                    "peak_drawdown_then_rebound": "하락률(%)",
                }.get(selected_type, "값")
                value = st.text_input(
                    value_label,
                    value=str(default.get("value", "")),
                    key=f"{prefix}_rule_value_{i}",
                    placeholder="예: 15, 3, -20",
                )
            with c4:
                value2_label = "상한" if selected_type == "compare" else (
                    "반등률(%)" if selected_type == "peak_drawdown_then_rebound"
                    else ("여유폭(%)" if selected_type.startswith("vs_moving_avg") else "최소변화(%)")
                )
                value2 = st.text_input(
                    value2_label,
                    value=str(default.get("value2", "")),
                    key=f"{prefix}_rule_value2_{i}",
                    placeholder="선택 입력",
                    disabled=selected_type == "pct_from_peak" or (selected_type == "compare" and op != "between"),
                )
            if value.strip():
                rule = {"column": column, "op": op, "value": value.strip(), "value2": value2.strip()}
                if selected_type == "consecutive_up":
                    rule.update({"type": "consecutive_up", "n": value.strip(), "min_change": value2.strip() or 0, "group_col": "시도"})
                elif selected_type == "consecutive_down":
                    rule.update({"type": "consecutive_down", "n": value.strip(), "min_change": value2.strip() or 0, "group_col": "시도"})
                elif selected_type == "vs_moving_avg_above":
                    rule.update({"type": "vs_moving_avg", "window": value.strip(), "direction": "above", "threshold_pct": value2.strip() or 0, "group_col": "시도"})
                elif selected_type == "vs_moving_avg_below":
                    rule.update({"type": "vs_moving_avg", "window": value.strip(), "direction": "below", "threshold_pct": value2.strip() or 0, "group_col": "시도"})
                elif selected_type == "pct_from_peak":
                    rule.update({"type": "pct_from_peak", "group_col": "시도"})
                elif selected_type == "peak_drawdown_then_rebound":
                    rule.update({
                        "type": "peak_drawdown_then_rebound",
                        "drawdown_pct": value.strip(),
                        "rebound_pct": value2.strip() or 5,
                        "group_col": "시도",
                    })
                rules.append(rule)
    return rules, combine


def _format_signal_summary(row):
    if row is None or pd.isna(row.get("선행방향", np.nan)):
        return "선행 신호 없음"
    direction = row.get("선행방향", "")
    lead = row.get("먼저움직인기간", 0)
    consistency = row.get("반복성", "")
    return f"{direction} {int(lead)}기간, 반복성 {consistency}"

if main_tab1:
    st.header(f"시장 Overview ({mode_label})")
    render_tab_usage_guide("Overview")
    st.caption("역사는 반복된다는 관점에서 과거 국면, 수요·공급의 움직임, 현재와 닮은 신호를 한 화면에서 추적합니다.")
    quality_df, latest_cache_mtime, cache_file_count = _dataset_quality_snapshot({
        "연간통합": yearly_df,
        "월간통합": monthly_df,
        "매매": apt_df,
        "전세": jeonse_df,
        "월세": wolse_df,
    })
    latest_cache_label = (
        pd.to_datetime(latest_cache_mtime, unit="s").strftime("%Y-%m-%d %H:%M")
        if latest_cache_mtime else "확인불가"
    )
    missing_regions = int(quality_df["결측있는지역"].sum()) if not quality_df.empty else 0
    st.caption(
        f"데이터 신뢰도: 캐시 생성 {latest_cache_label} | 데이터셋 {len(quality_df)}개 "
        f"| 캐시파일 {cache_file_count}개 | 결측 있는 지역 {missing_regions}개"
    )
    with st.expander("데이터 신뢰도 상세"):
        if quality_df.empty:
            st.info("표시할 데이터셋이 없습니다.")
        else:
            st.dataframe(
                quality_df.style.format({"행": "{:,.0f}", "지역수": "{:,.0f}", "결측있는지역": "{:,.0f}"}, na_rep="N/A"),
                use_container_width=True,
                height=220,
            )

    # ──────────────────────────────────────────────────
    # Zone A: 시장 온도계 (Hero Section)
    # ──────────────────────────────────────────────────
    zone_a_left, zone_a_mid, zone_a_right = st.columns([1.2, 1.2, 1])

    # [좌] 시장 종합 스코어 게이지
    with zone_a_left:
        try:
            mkt_score, mkt_delta, mkt_breakdown = compute_market_temperature(analysis_df)
            # 0~100 게이지: 50이 중립, 높을수록 과열
            fig_mkt_gauge = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=mkt_score,
                title={"text": "시장 종합 온도계", "font": {"size": 14}},
                delta={"reference": 50, "valueformat": ".1f"},
                gauge={
                    "axis": {"range": [0, 100], "tickwidth": 1},
                    "bar": {"color": "firebrick" if mkt_score > 60 else ("royalblue" if mkt_score < 40 else "orange")},
                    "steps": [
                        {"range": [0, 40],  "color": "#d0e8ff"},
                        {"range": [40, 60], "color": "#fff3cd"},
                        {"range": [60, 100],"color": "#ffd6d6"},
                    ],
                    "threshold": {
                        "line": {"color": "black", "width": 3},
                        "thickness": 0.75,
                        "value": 50,
                    },
                },
            ))
            fig_mkt_gauge.update_layout(height=280, margin=dict(t=50, b=10, l=10, r=10))
            register_fig("시장온도계_게이지", fig_mkt_gauge, "Overview")
            st.plotly_chart(fig_mkt_gauge, use_container_width=True)
            # 세부 breakdown 표시
            if mkt_breakdown:
                with st.expander("온도계 구성 요소"):
                    for k, v in mkt_breakdown.items():
                        st.caption(f"{k}: {v:.1f}" if isinstance(v, (int, float)) else f"{k}: {v}")
        except Exception as e:
            st.info(f"시장 온도계를 계산할 수 없습니다: {e}")

    # [중] KPI 카드 3개 (최신 연도 기준, YoY delta)
    with zone_a_mid:
        if analysis_mode == "매매 분석" and not apt_df.empty and "연도" in apt_df.columns:
            yr_latest = int(apt_df["연도"].max())
            yr_prev   = yr_latest - 1

            # 연도별 시도 평균 (필터 적용)
            _grp = filtered_apt.groupby("연도")

            # 평균가격 KPI
            _price_now  = _grp["평균가격"].mean().get(yr_latest, None)
            _price_prev = _grp["평균가격"].mean().get(yr_prev, None)
            _price_delta = None
            if _price_now is not None and _price_prev is not None and _price_prev != 0:
                _price_delta = f"{(_price_now - _price_prev) / _price_prev * 100:+.1f}%"

            # 거래량 KPI
            _vol_now  = _grp["거래량"].sum().get(yr_latest, None)
            _vol_prev = _grp["거래량"].sum().get(yr_prev, None)
            _vol_delta = None
            if _vol_now is not None and _vol_prev is not None:
                _vol_delta = f"{int(_vol_now - _vol_prev):+,}건"

            # 전세가율 KPI (yearly_df 기반)
            _jeonse_rate_now  = None
            _jeonse_rate_prev = None
            if "전세가율" in analysis_df.columns:
                _yr_grp = analysis_df.groupby("연도")["전세가율"].mean()
                _jeonse_rate_now  = _yr_grp.get(yr_latest, None)
                _jeonse_rate_prev = _yr_grp.get(yr_prev, None)
            _jeonse_delta = None
            if _jeonse_rate_now is not None and _jeonse_rate_prev is not None:
                _jeonse_delta = f"{_jeonse_rate_now - _jeonse_rate_prev:+.1f}%p"

            st.metric(
                f"평균가격 ({yr_latest})",
                f"{_price_now:,.0f}만원" if _price_now is not None else "N/A",
                delta=_price_delta,
            )
            st.metric(
                f"거래량 ({yr_latest})",
                f"{int(_vol_now):,}건" if _vol_now is not None else "N/A",
                delta=_vol_delta,
            )
            st.metric(
                "전세가율",
                f"{_jeonse_rate_now:.1f}%" if _jeonse_rate_now is not None else "N/A",
                delta=_jeonse_delta,
            )
        else:
            st.info("매매 분석 모드에서 KPI가 표시됩니다.")

    # [우] 시장심리 미니 게이지 2개 (KB 매수우위, CSI)
    with zone_a_right:
        _gauge_cols = ["KB_매수우위지수", "주택가격전망CSI"]
        _gauge_avail = [c for c in _gauge_cols if c in analysis_df.columns and analysis_df[c].notna().any()]
        if _gauge_avail:
            for _gc in _gauge_avail:
                _gs = analysis_df[_gc].dropna()
                _gv = float(_gs.iloc[-1]) if not _gs.empty else None
                if _gv is not None:
                    _gfig = go.Figure(go.Indicator(
                        mode="gauge+number+delta",
                        value=_gv,
                        title={"text": _gc, "font": {"size": 11}},
                        delta={"reference": 100, "valueformat": ".1f"},
                        gauge={
                            "axis": {"range": [0, 200]},
                            "bar": {"color": "darkblue" if "매수" in _gc else "darkorange"},
                            "steps": [
                                {"range": [0, 80],   "color": "lightblue"},
                                {"range": [80, 120],  "color": "lightyellow"},
                                {"range": [120, 200], "color": "lightsalmon"},
                            ],
                            "threshold": {
                                "line": {"color": "red", "width": 3},
                                "thickness": 0.75,
                                "value": 100,
                            },
                        },
                    ))
                    _gfig.update_layout(height=250, margin=dict(t=40, b=5, l=5, r=5))
                    register_fig(f"{_gc}_게이지", _gfig, "Overview")
                    st.plotly_chart(_gfig, use_container_width=True)
        else:
            st.info("KB 지수 / CSI 데이터가 없습니다.")

    st.divider()

    # ──────────────────────────────────────────────────
    # Zone B: 저평가 / 고평가 TOP 10  +  Choropleth 지도
    # ──────────────────────────────────────────────────
    st.subheader("저평가·고평가 지역 순위")
    try:
        _vs_year = int(apt_df["연도"].max()) if not apt_df.empty else None
        _value_df = _cached_value_score(apt_df, jeonse_df, nps_df, _vs_year)

        if _value_df.empty:
            st.warning("밸류스코어를 계산할 데이터가 부족합니다. (apt/jeonse/nps 데이터 확인 필요)")
        else:
            # ── Choropleth 지도 (막대 차트 위에 배치) ──────────────
            try:
                _geo_path = os.path.join(os.path.dirname(__file__), "geo_data", "sigungu.geojson")
                if os.path.exists(_geo_path):
                    with open(_geo_path, encoding="utf-8") as _gf:
                        _geojson = json.load(_gf)

                    # 지역코드를 문자열 5자리로 정규화 (NaN 행 제거 후 변환)
                    _map_df = _value_df.dropna(subset=["지역코드"]).copy()
                    _map_df["지역코드"] = _map_df["지역코드"].astype(str).str.zfill(5)

                    # hover에 표시할 컬럼 선택 (없으면 제외)
                    _hover_cols = [c for c in ["시군구명", "시도", "전세가율", "PIR_NPS"] if c in _map_df.columns]

                    fig_map = px.choropleth_mapbox(
                        _map_df,
                        geojson=_geojson,
                        locations="지역코드",
                        featureidkey="properties.SIG_CD",
                        color="밸류스코어",
                        color_continuous_scale=["#e74c3c", "#f1c40f", "#2ecc71"],  # 빨강→노랑→초록
                        range_color=[_map_df["밸류스코어"].quantile(0.05), _map_df["밸류스코어"].quantile(0.95)],
                        mapbox_style="carto-darkmatter",
                        center={"lat": 36.5, "lon": 127.8},
                        zoom=6,
                        hover_data=_hover_cols,
                        labels={"밸류스코어": "밸류스코어"},
                        title=f"시군구 밸류스코어 지도 ({_vs_year}년)  ●초록=저평가  ●빨강=고평가",
                    )
                    fig_map.update_layout(
                        height=500,
                        margin=dict(t=40, b=10, l=10, r=10),
                        coloraxis_colorbar=dict(title="밸류스코어", thickness=14),
                    )
                    register_fig("밸류스코어_지도", fig_map, "Overview")
                    st.plotly_chart(fig_map, use_container_width=True)
                else:
                    st.info("geo_data/sigungu.geojson 파일이 없어 지도를 표시할 수 없습니다.")
            except Exception as _map_e:
                st.warning(f"지도 렌더링 오류: {_map_e}")

            # ── 저평가/고평가 막대 차트 ─────────────────────────────
            _b_left, _b_right = st.columns(2)

            # 저평가 TOP 10 (밸류스코어 상위 = 상대적으로 저평가)
            with _b_left:
                st.markdown("**저평가 TOP 10**")
                _underval = _value_df.sort_values("밸류스코어", ascending=False).head(10).copy()
                _underval["표시명"] = _underval.apply(
                    lambda r: f"{r.get('시군구명', r.get('지역코드',''))}" +
                              (f" ({r['시도']})" if "시도" in r and pd.notna(r["시도"]) else ""),
                    axis=1
                )
                _underval["hover_text"] = _underval.apply(
                    lambda r: f"전세가율: {r.get('전세가율', float('nan')):.1f}%  PIR: {r.get('PIR_NPS', float('nan')):.1f}배",
                    axis=1
                )
                fig_under = px.bar(
                    _underval,
                    x="밸류스코어", y="표시명",
                    orientation="h",
                    color_discrete_sequence=["#2ecc71"],
                    text=_underval.apply(
                        lambda r: f"{r.get('전세가율', float('nan')):.0f}% | {r.get('PIR_NPS', float('nan')):.1f}배",
                        axis=1
                    ),
                    labels={"밸류스코어": "밸류스코어", "표시명": ""},
                )
                fig_under.update_traces(textposition="outside")
                fig_under.update_layout(
                    height=350, margin=dict(t=20, b=20, l=10, r=20),
                    yaxis=dict(autorange="reversed"),
                )
                register_fig("저평가_TOP10", fig_under, "Overview")
                st.plotly_chart(fig_under, use_container_width=True)

            # 고평가 TOP 10 (밸류스코어 하위 = 상대적으로 고평가)
            with _b_right:
                st.markdown("**고평가 TOP 10**")
                _overval = _value_df.sort_values("밸류스코어", ascending=True).head(10).copy()
                _overval["표시명"] = _overval.apply(
                    lambda r: f"{r.get('시군구명', r.get('지역코드',''))}" +
                              (f" ({r['시도']})" if "시도" in r and pd.notna(r["시도"]) else ""),
                    axis=1
                )
                fig_over = px.bar(
                    _overval,
                    x="밸류스코어", y="표시명",
                    orientation="h",
                    color_discrete_sequence=["#e74c3c"],
                    text=_overval.apply(
                        lambda r: f"{r.get('전세가율', float('nan')):.0f}% | {r.get('PIR_NPS', float('nan')):.1f}배",
                        axis=1
                    ),
                    labels={"밸류스코어": "밸류스코어", "표시명": ""},
                )
                fig_over.update_traces(textposition="outside")
                fig_over.update_layout(
                    height=350, margin=dict(t=20, b=20, l=10, r=20),
                    yaxis=dict(autorange="reversed"),
                )
                register_fig("고평가_TOP10", fig_over, "Overview")
                st.plotly_chart(fig_over, use_container_width=True)

            # 상세 데이터 expander + CSV 다운로드
            with st.expander("상세 데이터 (전체 밸류스코어 테이블)"):
                _disp_cols = [c for c in ["시군구명", "시도", "밸류스코어", "전세가율", "PIR_NPS", "거래회전율_proxy", "가격모멘텀"] if c in _value_df.columns]
                st.dataframe(
                    _value_df[_disp_cols].sort_values("밸류스코어", ascending=False),
                    use_container_width=True,
                    height=350,
                )
                # CSV 다운로드 버튼
                _csv_value = _value_df[_disp_cols].sort_values("밸류스코어", ascending=False).to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    label="📥 밸류스코어 CSV 다운로드",
                    data=_csv_value,
                    file_name=f"value_score_{_vs_year}.csv",
                    mime="text/csv",
                    key="dl_value_score",
                )
    except Exception as e:
        st.error(f"밸류스코어 계산 오류: {e}")

    st.divider()

    # ──────────────────────────────────────────────────
    # Zone C: 핵심 트렌드 2x2
    # ──────────────────────────────────────────────────
    st.subheader("핵심 트렌드")
    _ov_tc = "연월" if freq == "월별" and "연월" in analysis_df.columns else "연도"

    def _trend_index_100(df, time_col, value_cols):
        """각 지표를 첫 유효값=100으로 변환해 단위가 다른 지표를 한 차트에서 비교."""
        idx_df = df[[time_col] + value_cols].copy()
        for _col in value_cols:
            _valid = idx_df[_col].replace([np.inf, -np.inf], np.nan).dropna()
            _base = _valid.iloc[0] if not _valid.empty else np.nan
            idx_df[_col] = np.where(
                pd.notna(_base) and _base != 0,
                idx_df[_col] / _base * 100,
                np.nan,
            )
        return idx_df

    _trend_c1, _trend_c2 = st.columns(2)

    # (1,1) 수요 데이터: 인구, 30대 인구, 일자리
    with _trend_c1:
        try:
            _demand_map = {
                "총인구": "총인구",
                "30대 인구": "30대",
                "일자리": next((c for c in ["NPS_가입자수", "NPS_사업장수", "NPS_고용증감"] if c in analysis_df.columns and analysis_df[c].notna().any()), None),
            }
            _demand_aggs = {
                label: (col, "mean")
                for label, col in _demand_map.items()
                if col and col in analysis_df.columns and analysis_df[col].notna().any()
            }
            if _demand_aggs:
                _demand_df = analysis_df.groupby(_ov_tc).agg(**_demand_aggs).reset_index().sort_values(_ov_tc)
                _demand_cols = [c for c in ["총인구", "30대 인구", "일자리"] if c in _demand_df.columns]
                _demand_idx = _trend_index_100(_demand_df, _ov_tc, _demand_cols)
                _demand_melted = _demand_idx.melt(id_vars=[_ov_tc], value_vars=_demand_cols, var_name="지표", value_name="Index")

                fig_demand = px.line(
                    _demand_melted.dropna(subset=["Index"]),
                    x=_ov_tc, y="Index", color="지표",
                    title="수요 데이터: 인구·30대 인구·일자리 (Index=100)",
                    markers=True,
                    color_discrete_sequence=["#2563eb", "#16a34a", "#f59e0b"],
                )
                fig_demand.update_layout(height=350, legend=dict(orientation="h"))
                fig_demand.update_yaxes(title_text="Index (첫 유효 시점=100)")
                register_fig("핵심트렌드_수요", fig_demand, "Overview")
                st.plotly_chart(fig_demand, use_container_width=True)
            else:
                st.info("수요 데이터(인구, 30대 인구, 일자리)가 없습니다.")
        except Exception as e:
            st.error(f"수요 데이터 차트 오류: {e}")

    # (1,2) 공급 데이터: 인허가, 착공, 준공, 미분양
    with _trend_c2:
        try:
            _supply_cols = [c for c in ["인허가_호수", "착공_호수", "준공_호수"] if c in analysis_df.columns and analysis_df[c].notna().any()]
            _unsold_col = next((c for c in ["미분양_호수", "미분양_평균"] if c in analysis_df.columns and analysis_df[c].notna().any()), None)

            if _supply_cols or _unsold_col:
                _supply_fig = make_subplots(specs=[[{"secondary_y": True}]])
                for _col, _color in zip(_supply_cols, ["#2563eb", "#7c3aed", "#16a34a"]):
                    _sdf = analysis_df.groupby(_ov_tc)[_col].sum().reset_index().sort_values(_ov_tc)
                    _supply_fig.add_trace(
                        go.Scatter(x=_sdf[_ov_tc], y=_sdf[_col], name=_col.replace("_호수", ""), line=dict(color=_color), mode="lines+markers"),
                        secondary_y=False,
                    )
                if _unsold_col:
                    _udf = analysis_df.groupby(_ov_tc)[_unsold_col].mean().reset_index().sort_values(_ov_tc)
                    _supply_fig.add_trace(
                        go.Bar(x=_udf[_ov_tc], y=_udf[_unsold_col], name="미분양", marker_color="#ef4444", opacity=0.35),
                        secondary_y=True,
                    )

                _supply_fig.update_layout(title_text="공급 데이터: 인허가·착공·준공·미분양", height=350, legend=dict(orientation="h"))
                _supply_fig.update_yaxes(title_text="공급 물량(호)", secondary_y=False)
                if _unsold_col:
                    _supply_fig.update_yaxes(title_text="미분양(호)", secondary_y=True)
                register_fig("핵심트렌드_공급", _supply_fig, "Overview")
                st.plotly_chart(_supply_fig, use_container_width=True)
            else:
                st.info("공급 데이터(인허가, 착공, 준공, 미분양)가 없습니다.")
        except Exception as e:
            st.error(f"공급 데이터 차트 오류: {e}")

    _trend_c3, _trend_c4 = st.columns(2)

    # (2,1) 2차 가공 데이터: PIR, J-PIR, 전세가율, 갭
    with _trend_c3:
        try:
            _derived_map = {
                "PIR": next((c for c in ["PIR", "PIR_NPS", "KB_PIR"] if c in analysis_df.columns and analysis_df[c].notna().any()), None),
                "J-PIR": "KB_J_PIR" if "KB_J_PIR" in analysis_df.columns and analysis_df["KB_J_PIR"].notna().any() else None,
                "전세가율": "전세가율" if "전세가율" in analysis_df.columns and analysis_df["전세가율"].notna().any() else None,
                "갭비용": "갭비용" if "갭비용" in analysis_df.columns and analysis_df["갭비용"].notna().any() else None,
            }
            _derived_aggs = {
                label: (col, "mean")
                for label, col in _derived_map.items()
                if col and col in analysis_df.columns and analysis_df[col].notna().any()
            }
            if _derived_aggs:
                _derived_df = analysis_df.groupby(_ov_tc).agg(**_derived_aggs).reset_index().sort_values(_ov_tc)
                _derived_cols = [c for c in ["PIR", "J-PIR", "전세가율", "갭비용"] if c in _derived_df.columns]
                _derived_idx = _trend_index_100(_derived_df, _ov_tc, _derived_cols)
                _derived_melted = _derived_idx.melt(id_vars=[_ov_tc], value_vars=_derived_cols, var_name="지표", value_name="Index")

                fig_derived = px.line(
                    _derived_melted.dropna(subset=["Index"]),
                    x=_ov_tc, y="Index", color="지표",
                    title="2차가공 데이터: PIR·J-PIR·전세가율·갭 (Index=100)",
                    markers=True,
                    color_discrete_sequence=["#dc2626", "#9333ea", "#059669", "#475569"],
                )
                fig_derived.update_layout(height=350, legend=dict(orientation="h"))
                fig_derived.update_yaxes(title_text="Index (첫 유효 시점=100)")
                register_fig("핵심트렌드_2차가공", fig_derived, "Overview")
                st.plotly_chart(fig_derived, use_container_width=True)
            else:
                st.info("2차가공 데이터(PIR, J-PIR, 전세가율, 갭)가 없습니다.")
        except Exception as e:
            st.error(f"2차가공 데이터 차트 오류: {e}")

    # (2,2) 최근 실거래 데이터: 매매 평균가격과 거래량
    with _trend_c4:
        try:
            _deal_df = filtered_apt.copy() if analysis_mode == "매매 분석" else filtered_rent.copy()
            _deal_price_col = "평균가격" if analysis_mode == "매매 분석" else "보증금평균"
            _deal_vol_col = "거래량" if analysis_mode == "매매 분석" else "임대거래량"
            _deal_time_col = "연월" if "연월" in _deal_df.columns else "연도"

            if not _deal_df.empty and _deal_price_col in _deal_df.columns and _deal_vol_col in _deal_df.columns:
                _recent_periods = sorted(_deal_df[_deal_time_col].dropna().unique())[-24:]
                _deal_df = _deal_df[_deal_df[_deal_time_col].isin(_recent_periods)]
                _deal_grp = (
                    _deal_df.groupby(_deal_time_col)
                    .agg(평균가격=(_deal_price_col, "mean"), 거래량=(_deal_vol_col, "sum"))
                    .reset_index()
                    .sort_values(_deal_time_col)
                )

                fig_deal = make_subplots(specs=[[{"secondary_y": True}]])
                fig_deal.add_trace(
                    go.Scatter(x=_deal_grp[_deal_time_col], y=_deal_grp["평균가격"], name="평균가격", line=dict(color="#2563eb"), mode="lines+markers"),
                    secondary_y=False,
                )
                fig_deal.add_trace(
                    go.Bar(x=_deal_grp[_deal_time_col], y=_deal_grp["거래량"], name="거래량", marker_color="#94a3b8", opacity=0.55),
                    secondary_y=True,
                )
                fig_deal.update_layout(title_text=f"최근 실거래 데이터: {mode_label} 가격·거래량", height=350, legend=dict(orientation="h"))
                fig_deal.update_yaxes(title_text="평균가격(만원)", secondary_y=False)
                fig_deal.update_yaxes(title_text="거래량(건)", secondary_y=True)
                register_fig("핵심트렌드_최근실거래", fig_deal, "Overview")
                st.plotly_chart(fig_deal, use_container_width=True)

                with st.expander("최근월 시군구 실거래 요약"):
                    _latest_period = _deal_df[_deal_time_col].max()
                    _latest_deals = _deal_df[_deal_df[_deal_time_col] == _latest_period].copy()
                    _deal_summary = (
                        _latest_deals.groupby(["지역코드"], as_index=False)
                        .agg(평균가격=(_deal_price_col, "mean"), 거래량=(_deal_vol_col, "sum"))
                        .sort_values("거래량", ascending=False)
                    )
                    _deal_summary["시군구명"] = _deal_summary["지역코드"].apply(get_sigungu_name)
                    if "시도" in _latest_deals.columns:
                        _deal_summary["시도"] = _deal_summary["지역코드"].map(
                            _latest_deals.drop_duplicates("지역코드").set_index("지역코드")["시도"]
                        )
                    _deal_cols = [c for c in ["시군구명", "시도", "평균가격", "거래량"] if c in _deal_summary.columns]
                    st.caption(f"기준 기간: {_latest_period}")
                    st.dataframe(
                        _deal_summary[_deal_cols].head(20).style.format({"평균가격": "{:,.0f}", "거래량": "{:,.0f}"}, na_rep="N/A"),
                        use_container_width=True,
                        height=260,
                    )
            else:
                st.info("최근 실거래 데이터가 없습니다.")
        except Exception as e:
            st.error(f"최근 실거래 차트 오류: {e}")

    st.divider()

    # ──────────────────────────────────────────────────
    # Zone D: 역사 흐름과 반복 신호
    # ──────────────────────────────────────────────────
    st.subheader("역사 흐름: 가격은 어떤 수요·공급 국면 뒤에 움직였나")
    try:
        _hist_src = yearly_df.copy()
        if selected_sido and "시도" in _hist_src.columns:
            _hist_src = _hist_src[_hist_src["시도"].isin(selected_sido)]

        _hist_time_col = "연도"
        _hist_price_col = "평균가격" if "평균가격" in _hist_src.columns else None
        _hist_demand_cols = [c for c in ["총인구", "30대", "NPS_가입자수", "NPS_사업장수", "NPS_고용증감", "가구_소득평균"] if c in _hist_src.columns and _hist_src[c].notna().any()]
        _hist_supply_cols = [c for c in ["인허가_호수", "착공_호수", "준공_호수", "미분양_평균"] if c in _hist_src.columns and _hist_src[c].notna().any()]

        if not _hist_src.empty and _hist_price_col and (_hist_demand_cols or _hist_supply_cols):
            _hist_aggs = {"가격": (_hist_price_col, "mean")}
            for _col in _hist_demand_cols:
                _hist_aggs[_col] = (_col, "mean")
            for _col in _hist_supply_cols:
                _hist_aggs[_col] = (_col, "sum" if _col != "미분양_평균" else "mean")

            _hist_df = _hist_src.groupby(_hist_time_col).agg(**_hist_aggs).reset_index().sort_values(_hist_time_col)
            _hist_df["가격변화율"] = _hist_df["가격"].pct_change() * 100
            _hist_df["국면"] = np.select(
                [
                    _hist_df["가격변화율"] >= 5,
                    _hist_df["가격변화율"] <= -3,
                ],
                ["상승기", "하락기"],
                default="중립기",
            )

            _hist_idx_cols = ["가격"] + _hist_demand_cols + _hist_supply_cols
            _hist_idx = _trend_index_100(_hist_df, _hist_time_col, _hist_idx_cols)
            _hist_long = _hist_idx.melt(id_vars=[_hist_time_col], value_vars=_hist_idx_cols, var_name="지표", value_name="Index")

            fig_history = px.line(
                _hist_long.dropna(subset=["Index"]),
                x=_hist_time_col, y="Index", color="지표",
                title="연도별 가격·수요·공급 흐름 (Index=100)",
                markers=True,
            )
            for _, _row in _hist_df.dropna(subset=["가격변화율"]).iterrows():
                if _row["국면"] in ("상승기", "하락기"):
                    _color = "rgba(239,68,68,0.10)" if _row["국면"] == "상승기" else "rgba(37,99,235,0.10)"
                    fig_history.add_vrect(
                        x0=_row[_hist_time_col] - 0.45,
                        x1=_row[_hist_time_col] + 0.45,
                        fillcolor=_color,
                        line_width=0,
                        layer="below",
                    )
            fig_history.update_layout(height=430, legend=dict(orientation="h"))
            fig_history.update_yaxes(title_text="Index (첫 유효 연도=100)")
            register_fig("역사흐름_가격수요공급", fig_history, "Overview")
            st.plotly_chart(fig_history, use_container_width=True)

            _latest_hist = _hist_df.dropna(subset=["가격변화율"]).tail(1)
            _rise_years = _hist_df[_hist_df["국면"] == "상승기"][_hist_time_col].astype(str).tolist()
            _fall_years = _hist_df[_hist_df["국면"] == "하락기"][_hist_time_col].astype(str).tolist()
            _flow_c1, _flow_c2, _flow_c3 = st.columns(3)
            with _flow_c1:
                st.metric("최근 가격 국면", _latest_hist["국면"].iloc[0] if not _latest_hist.empty else "N/A")
            with _flow_c2:
                st.metric("상승 반복 연도", f"{len(_rise_years)}회")
                st.caption(", ".join(_rise_years[-6:]) if _rise_years else "해당 없음")
            with _flow_c3:
                st.metric("하락 반복 연도", f"{len(_fall_years)}회")
                st.caption(", ".join(_fall_years[-6:]) if _fall_years else "해당 없음")

            with st.expander("연도별 수요·공급 원천 테이블"):
                _hist_disp_cols = [_hist_time_col, "국면", "가격", "가격변화율"] + _hist_demand_cols + _hist_supply_cols
                st.dataframe(
                    _hist_df[_hist_disp_cols].style.format({c: "{:,.1f}" for c in _hist_disp_cols if c not in [_hist_time_col, "국면"]}, na_rep="N/A"),
                    use_container_width=True,
                    height=320,
                )
        else:
            st.info("역사 흐름을 그릴 가격·수요·공급 연간 데이터가 부족합니다.")
    except Exception as e:
        st.error(f"역사 흐름 분석 오류: {e}")

    st.divider()
    st.subheader("데이터 출처 및 최신성")
    source_rows = []
    for item in INDICATOR_CATALOG:
        col = item["column"]
        if col not in analysis_df.columns and col not in yearly_df.columns and col not in monthly_df.columns:
            continue
        src_df = analysis_df if col in analysis_df.columns else (monthly_df if col in monthly_df.columns else yearly_df)
        if src_df.empty or col not in src_df.columns:
            continue
        time_col_src = "연월" if "연월" in src_df.columns else ("연도" if "연도" in src_df.columns else None)
        latest = src_df[time_col_src].dropna().max() if time_col_src else "N/A"
        latest = "N/A" if pd.isna(latest) else str(latest)
        region_count = src_df["시도"].nunique() if "시도" in src_df.columns else np.nan
        miss = src_df[col].isna().mean() * 100 if len(src_df) else np.nan
        source_rows.append({
            "지표군": item["group"],
            "지표": item["label"],
            "출처": item["source"],
            "단위": item["unit"],
            "최신 기준": latest,
            "지역 수": region_count,
            "행 수": len(src_df),
            "결측률(%)": miss,
            "용도": item["best_for"],
        })
    if source_rows:
        source_df = pd.DataFrame(source_rows).drop_duplicates(["지표", "출처"])
        st.dataframe(
            source_df.style.format({"결측률(%)": "{:.1f}", "지역 수": "{:,.0f}", "행 수": "{:,.0f}"}, na_rep="N/A"),
            use_container_width=True,
            height=320,
        )
    else:
        st.info("표시할 데이터 출처 정보가 없습니다.")


# ============================
# Tab 2: 시장분석 (시계열비교 + 가격비교 + 갭분석)
# ============================
if main_tab2:
    st.header("거래현황")
    render_tab_usage_guide("거래현황")
    st.caption("실거래 흐름과 네이버 매물 호가를 같은 지역/단지 단위로 비교합니다.")

    _naver_upload = st.file_uploader(
        "네이버부동산 매물 CSV/JSON 업로드",
        type=["csv", "json"],
        key="naver_listing_upload",
        help="반자동 수집 결과나 저장한 응답 파일을 업로드하면 표준 매물 테이블로 정규화합니다.",
    )
    if _naver_upload is not None:
        st.session_state["naver_listings_df"] = parse_naver_listing_upload(_naver_upload)

    _naver_df = _clean_listing_df(st.session_state.get("naver_listings_df", pd.DataFrame()))
    if not _naver_df.empty:
        _naver_price = pd.to_numeric(_naver_df.get("매물가격", pd.Series(dtype=float)), errors="coerce")
        _n1, _n2, _n3 = st.columns(3)
        _n1.metric("업로드 매물", f"{len(_naver_df):,}건")
        _n2.metric("단지 수", f"{_naver_df['단지명'].nunique():,}개")
        _n3.metric("평균 호가", _format_listing_price(_naver_price.mean()))
        with st.expander("네이버 매물 표준 테이블"):
            st.dataframe(
                _naver_df.style.format({"매물가격": "{:,.0f}"}, na_rep="N/A"),
                use_container_width=True,
                height=260,
            )
    else:
        st.info("네이버 매물 데이터가 없으면 실거래 중심으로 표시됩니다. 매물 파일을 업로드하면 호가 비교가 활성화됩니다.")

    sub_ts, sub_price_cmp, sub_gap = st.tabs(["시계열 비교", "가격비교", "갭분석"])

# ── 시계열 비교 서브탭 ──────────────────────────────────────────────
    with sub_ts:
        st.header(f"시계열 비교 ({mode_label}, 듀얼 Y축)")
    
        if analysis_df.empty:
            st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
        else:
            valid_vars_ts = [v for v in available_vars if v in analysis_df.columns and analysis_df[v].notna().any()]
    
            col1, col2, col3 = st.columns(3)
            with col1:
                left_var = _render_grouped_indicator_picker(
                    "좌측 Y축",
                    analysis_df,
                    "left",
                    default_group="가격" if analysis_mode == "매매 분석" else "전세",
                    default=price_col,
                )
            with col2:
                right_candidates = [v for v in valid_vars_ts if v != left_var]
                right_var = _render_grouped_indicator_picker(
                    "우측 Y축",
                    analysis_df[[c for c in analysis_df.columns if c in right_candidates or c in ("시도", "연도", "월", "연월")]],
                    "right",
                    default_group="수요",
                    default=right_candidates[0] if right_candidates else None,
                )
            with col3:
                sido_for_ts = st.selectbox("비교 시도", selected_sido if selected_sido else all_sido, key="ts_sido")
    
            ts_df = analysis_df[analysis_df["시도"] == sido_for_ts].sort_values("연도")
            time_col = "연월" if freq == "월별" and "연월" in ts_df.columns else "연도"
    
            if not ts_df.empty and left_var in ts_df.columns and right_var in ts_df.columns:
                fig_dual = make_subplots(specs=[[{"secondary_y": True}]])
    
                fig_dual.add_trace(
                    go.Scatter(x=ts_df[time_col], y=ts_df[left_var], name=left_var, line=dict(color="blue")),
                    secondary_y=False,
                )
                fig_dual.add_trace(
                    go.Scatter(x=ts_df[time_col], y=ts_df[right_var], name=right_var, line=dict(color="red", dash="dash")),
                    secondary_y=True,
                )
    
                fig_dual.update_layout(title=f"{sido_for_ts}: {left_var} vs {right_var}", xaxis_title="기간")
                fig_dual.update_yaxes(title_text=left_var, secondary_y=False)
                fig_dual.update_yaxes(title_text=right_var, secondary_y=True)
    
                # 정책 이벤트 수직선 오버레이
                show_policy = st.checkbox("정책 이벤트 표시", value=False, key="policy_dual")
                if show_policy and not policy_events_df.empty:
                    colors = {"규제강화": "red", "규제완화": "green", "중립": "gray"}
                    for _, ev in policy_events_df.iterrows():
                        if time_col == "연도":
                            x_val = ev["날짜"].year
                        else:
                            x_val = ev["날짜"].strftime("%Y-%m-%d") if hasattr(ev["날짜"], "strftime") else str(ev["날짜"])
                        _ev_color = colors.get(ev.get("방향", ""), "gray")
                        fig_dual.add_shape(
                            type="line",
                            x0=x_val, x1=x_val,
                            y0=0, y1=1, yref="paper",
                            line=dict(dash="dot", color=_ev_color, width=1),
                        )
                        fig_dual.add_annotation(
                            x=x_val, y=1.0, yref="paper",
                            text=ev.get("이벤트명", ""),
                            showarrow=False, xanchor="right",
                            font=dict(size=8, color=_ev_color),
                            textangle=-45,
                        )
    
                register_fig("시계열_듀얼축", fig_dual, "시장분석")
                st.plotly_chart(fig_dual, use_container_width=True)
                st.caption(_source_caption([left_var, right_var]))
    
                # 상관계수 표시
                valid = ts_df[[left_var, right_var]].dropna()
                if len(valid) >= 3:
                    from scipy import stats as sp_stats
                    r, p = sp_stats.pearsonr(valid[left_var], valid[right_var])
                    st.info(f"같이 움직인 정도: **{r:.4f}** (통계 신뢰도: {1 - p:.1%})")
            else:
                st.info("선택한 변수의 데이터가 부족합니다.")
    
            # 정규화 비교 차트
            st.subheader("정규화 시계열 비교")
            norm_vars = st.multiselect(
                "비교할 변수 선택", valid_vars_ts,
                default=[v for v in [price_col, "GRDP"] if v in valid_vars_ts],
                key="norm_vars",
                format_func=_fmt_var_by_col,
            )
            if norm_vars and sido_for_ts:
                norm_df = ts_df[[time_col] + [v for v in norm_vars if v in ts_df.columns]].dropna()
                if not norm_df.empty:
                    for v in norm_vars:
                        if v in norm_df.columns:
                            vmin, vmax = norm_df[v].min(), norm_df[v].max()
                            if vmax > vmin:
                                norm_df[v] = (norm_df[v] - vmin) / (vmax - vmin)
                            else:
                                norm_df[v] = 0
    
                    melted = norm_df.melt(id_vars=[time_col], var_name="변수", value_name="정규화값")
                    fig_norm = px.line(
                        melted, x=time_col, y="정규화값", color="변수",
                        title=f"{sido_for_ts}: 정규화 시계열 비교 (0-1 스케일)",
                    )
                    register_fig("정규화_비교", fig_norm, "시장분석")
                    st.plotly_chart(fig_norm, use_container_width=True)
    
    # ── 가격비교 서브탭 ─────────────────────────────────────────────────
    with sub_price_cmp:
        st.header(f"가격비교 ({mode_label})")
        st.caption("시도별 가격을 같은 차트에서 비교합니다.")
    
        if analysis_df.empty:
            st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
        else:
            # 가격비교 변수 선택
            pc_vars = [v for v in result_vars if v in analysis_df.columns and analysis_df[v].notna().any()]
            if pc_vars:
                pc_var = st.selectbox("비교 변수", pc_vars, key="price_cmp_var", format_func=_fmt_var_by_col)
                pc_time_col = "연월" if freq == "월별" and "연월" in analysis_df.columns else "연도"
    
                # 시도별 가격 추이 라인 차트
                pc_df = analysis_df.groupby(["시도", pc_time_col])[pc_var].mean().reset_index()
                fig_pc = px.line(
                    pc_df.sort_values(pc_time_col),
                    x=pc_time_col, y=pc_var, color="시도",
                    title=f"시도별 {pc_var} 추이 비교",
                    labels={pc_var: "가격(만원)", pc_time_col: "기간"},
                    markers=True,
                )
                register_fig("가격비교_라인", fig_pc, "시장분석")
                st.plotly_chart(fig_pc, use_container_width=True)
    
                # 최근 연도 막대 비교
                latest_yr = int(analysis_df["연도"].max())
                pc_latest = analysis_df[analysis_df["연도"] == latest_yr].groupby("시도")[pc_var].mean().reset_index()
                pc_latest = pc_latest.sort_values(pc_var, ascending=False)
                fig_pc_bar = px.bar(
                    pc_latest, x="시도", y=pc_var,
                    color=pc_var, color_continuous_scale="Blues",
                    title=f"{latest_yr}년 시도별 {pc_var}",
                )
                register_fig("가격비교_바", fig_pc_bar, "시장분석")
                st.plotly_chart(fig_pc_bar, use_container_width=True)
            else:
                st.info("비교할 수 있는 가격 데이터가 없습니다.")
    
    # ── 갭분석 서브탭 ───────────────────────────────────────────────────
    with sub_gap:
        st.header("갭분석 (매매-전세 갭)")
        st.caption("매매가격과 전세보증금의 차이(갭)를 시각화합니다.")
    
        # 매매가격과 전세보증금 컬럼 탐색
        gap_apt_col   = "평균가격"       if "평균가격"        in analysis_df.columns else None
        gap_jeonse_col = "전세_보증금평균" if "전세_보증금평균" in analysis_df.columns else None
    
        if gap_apt_col and gap_jeonse_col and analysis_df[gap_apt_col].notna().any() and analysis_df[gap_jeonse_col].notna().any():
            gap_time_col = "연월" if freq == "월별" and "연월" in analysis_df.columns else "연도"
            gap_sido = st.selectbox(
                "시도 선택", selected_sido if selected_sido else all_sido, key="gap_sido"
            )
            gap_df = analysis_df[analysis_df["시도"] == gap_sido].sort_values(gap_time_col).copy()
            gap_df["매매전세갭"] = gap_df[gap_apt_col] - gap_df[gap_jeonse_col]
            gap_df["전세가율(%)"] = (gap_df[gap_jeonse_col] / gap_df[gap_apt_col] * 100).where(gap_df[gap_apt_col] > 0)
    
            fig_gap = make_subplots(specs=[[{"secondary_y": True}]])
            fig_gap.add_trace(
                go.Bar(x=gap_df[gap_time_col], y=gap_df["매매전세갭"], name="매매-전세 갭(만원)",
                       marker_color="steelblue", opacity=0.7),
                secondary_y=False,
            )
            fig_gap.add_trace(
                go.Scatter(x=gap_df[gap_time_col], y=gap_df["전세가율(%)"], name="전세가율(%)",
                           line=dict(color="tomato", width=2), mode="lines+markers"),
                secondary_y=True,
            )
            fig_gap.update_layout(title=f"{gap_sido}: 매매-전세 갭 & 전세가율", xaxis_title="기간")
            fig_gap.update_yaxes(title_text="갭(만원)", secondary_y=False)
            fig_gap.update_yaxes(title_text="전세가율(%)", secondary_y=True)
            register_fig("갭분석", fig_gap, "시장분석")
            st.plotly_chart(fig_gap, use_container_width=True)
        else:
            st.info("매매가격과 전세보증금 데이터가 모두 있어야 갭분석을 할 수 있습니다.")
    
    
    # ============================
    # Tab 3: 지역별 분석
    # ============================
if main_tab3:
    st.header(f"지역별 분석 ({mode_label})")

    if analysis_mode == "매매 분석":
        tab4_df = filtered_apt
        tab4_price = "평균가격"
        tab4_vol = "거래량"
        tab4_unit = "평균단가_per_m2"
    else:
        tab4_df = filtered_rent
        tab4_price = "보증금평균"
        tab4_vol = "임대거래량"
        tab4_unit = "보증금단가_per_m2"

    region_sub1, region_sub2, region_sub3 = st.tabs(["지역순위", "요약통계", "인구이동"])

# ── 지역순위 서브탭 ─────────────────────────────────────────────────
    with region_sub1:
        if tab4_df.empty:
            st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
        else:
            latest_year = int(tab4_df["연도"].max())
            st.subheader(f"{latest_year}년 지역별 비교")
    
            compare_options = [tab4_price, tab4_vol, tab4_unit]
            if "월세평균" in tab4_df.columns:
                compare_options.append("월세평균")
            compare_var = st.selectbox("비교 변수", compare_options, key="compare", format_func=_fmt_var_by_col)
    
            latest_data = tab4_df[tab4_df["연도"] == latest_year]
            agg_dict = {
                tab4_price: (tab4_price, "mean"),
                tab4_vol: (tab4_vol, "sum"),
                tab4_unit: (tab4_unit, "mean"),
            }
            if "월세평균" in tab4_df.columns:
                agg_dict["월세평균"] = ("월세평균", "mean")
    
            chart_data = (
                latest_data.groupby(["지역코드"]).agg(**agg_dict)
                .reset_index().sort_values(compare_var, ascending=False)
            )
    
            if not chart_data.empty:
                chart_data["시군구명"] = chart_data["지역코드"].apply(get_sigungu_name)
                fig_region = px.bar(
                    chart_data.head(30), x="시군구명", y=compare_var,
                    color=compare_var,
                    title=f"{latest_year}년 시군구별 {compare_var} (상위 30개)",
                )
                register_fig("지역순위", fig_region, "지역별 분석")
                st.plotly_chart(fig_region, use_container_width=True)
    
            # 시군구별 가격 추이 비교
            if selected_codes:
                st.subheader(f"시군구별 {mode_label} 가격 추이")
                time_col = "연월" if freq == "월별" and "연월" in tab4_df.columns else "연도"
                if analysis_mode == "매매 분석":
                    trend_df = aggregate_by_code(tab4_df, time_col)
                    trend_price = "평균가격"
                else:
                    trend_df = aggregate_rent_by_code(tab4_df, time_col)
                    trend_price = "보증금평균"
    
                if not trend_df.empty:
                    trend_df["시군구명"] = trend_df["지역코드"].apply(get_sigungu_name)
                    fig_compare = px.line(
                        trend_df.sort_values(time_col),
                        x=time_col, y=trend_price, color="시군구명",
                        title=f"시군구별 {mode_label} 평균 가격 추이",
                    )
                    register_fig("지역비교_라인", fig_compare, "지역별 분석")
                    st.plotly_chart(fig_compare, use_container_width=True)
            else:
                st.subheader(f"시도별 {mode_label} 가격 추이 비교")
                if price_col in analysis_df.columns:
                    yearly_compare = (
                        analysis_df.groupby(["시도", "연도"])[price_col]
                        .mean().reset_index()
                    )
                    fig_compare = px.line(
                        yearly_compare.sort_values("연도"),
                        x="연도", y=price_col, color="시도",
                        title=f"시도별 {mode_label} 평균 가격 추이",
                    )
                    register_fig("지역비교_바", fig_compare, "지역별 분석")
                    st.plotly_chart(fig_compare, use_container_width=True)
    
    # ── 요약통계 서브탭 ─────────────────────────────────────────────────
    with region_sub2:
        if tab4_df.empty:
            st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
        else:
            agg_dict_sum = {
                tab4_price: (tab4_price, "mean"),
                tab4_vol: (tab4_vol, "sum"),
                tab4_unit: (tab4_unit, "mean"),
            }
            if "월세평균" in tab4_df.columns:
                agg_dict_sum["월세평균"] = ("월세평균", "mean")
    
            # 시군구별 요약 테이블
            st.subheader("시군구별 요약 통계")
            summary = (
                tab4_df.groupby(["지역코드"]).agg(**agg_dict_sum)
                .reset_index().sort_values(tab4_price, ascending=False)
            )
            summary["시군구명"] = summary["지역코드"].apply(get_sigungu_name)
            if "시도" in tab4_df.columns:
                sido_map = tab4_df.drop_duplicates("지역코드").set_index("지역코드")["시도"]
                summary["시도"] = summary["지역코드"].map(sido_map)
    
            display_cols = ["시군구명"]
            if "시도" in summary.columns:
                display_cols.append("시도")
            display_cols += [c for c in [tab4_price, tab4_vol, tab4_unit] if c in summary.columns]
            if "월세평균" in summary.columns:
                display_cols.append("월세평균")
    
            format_dict = {c: "{:,.0f}" for c in [tab4_price, tab4_vol, tab4_unit, "월세평균"] if c in summary.columns}
            st.dataframe(
                summary[display_cols].style.format(format_dict, na_rep="N/A"),
                use_container_width=True,
            )
    
    # ── 인구이동 서브탭 ─────────────────────────────────────────────────
    with region_sub3:
        st.subheader("인구이동 현황")
        # 인구이동 관련 컬럼 탐색 (전입, 전출, 순이동 포함)
        migration_cols = [c for c in analysis_df.columns if "전입" in c or "전출" in c or "순이동" in c]
        if migration_cols:
            mig_sido = st.selectbox(
                "시도 선택", selected_sido if selected_sido else all_sido, key="mig_sido"
            )
            mig_time_col = "연월" if freq == "월별" and "연월" in analysis_df.columns else "연도"
            mig_df = analysis_df[analysis_df["시도"] == mig_sido].sort_values(mig_time_col)
    
            # 전입/전출/순이동 막대차트
            avail_mig = [c for c in migration_cols if c in mig_df.columns and mig_df[c].notna().any()]
            if avail_mig:
                mig_melted = mig_df[[mig_time_col] + avail_mig].melt(
                    id_vars=[mig_time_col], var_name="구분", value_name="인원수"
                )
                fig_mig = px.bar(
                    mig_melted.sort_values(mig_time_col),
                    x=mig_time_col, y="인원수", color="구분", barmode="group",
                    title=f"{mig_sido}: 인구이동 현황",
                    labels={mig_time_col: "기간", "인원수": "인원(명)"},
                )
                register_fig("인구이동", fig_mig, "지역별 분석")
                st.plotly_chart(fig_mig, use_container_width=True)
            else:
                st.info("선택한 시도의 인구이동 데이터가 없습니다.")
        else:
            st.info("인구이동 데이터를 업데이트하면 이 탭에서 확인할 수 있습니다.")
    
    
    # ============================
    # Tab 4: 수요-공급 분석기 (구 수식 계산기)
    # ============================
if main_tab4:
    st.header("수요-공급 분석기")
    render_tab_usage_guide("수요공급분석")
    st.caption("모든 변수를 사칙연산으로 조합하여 새로운 지표를 계산하고 시각화합니다.")

    # ── 변수 메타데이터 (카테고리/출처 컬럼 추가) ───────────────────
    VAR_META = pd.DataFrame([
        # 거래결과 > 매매
        {"표시명": "매매 평균가격",           "컬럼명": "평균가격",              "단위": "만원",       "연집계룰": "가중평균(거래량)",          "정상범위": "",      "카테고리": "거래결과>매매",                  "출처": "실거래",   "설명": "아파트 매매 평균 거래가 (면적 가중)"},
        {"표시명": "매매 거래량",             "컬럼명": "거래량",                "단위": "건",         "연집계룰": "sum",                       "정상범위": "≥0",   "카테고리": "거래결과>매매",                  "출처": "실거래",   "설명": "매매 건수 — 시장 활성도"},
        {"표시명": "매매 평균단가",           "컬럼명": "평균단가_per_m2",        "단위": "만원/m²",    "연집계룰": "가중평균(거래량)",          "정상범위": "",      "카테고리": "거래결과>매매",                  "출처": "실거래",   "설명": "m²당 매매가 — 면적 무관 가격비교"},
        # 거래결과 > 전세
        {"표시명": "전세 보증금평균",         "컬럼명": "전세_보증금평균",        "단위": "만원",       "연집계룰": "가중평균(전세_거래량)",     "정상범위": "",      "카테고리": "거래결과>전세",                  "출처": "실거래",   "설명": "전세 보증금 평균액 (면적 가중)"},
        {"표시명": "전세 거래량",             "컬럼명": "전세_거래량",            "단위": "건",         "연집계룰": "sum",                       "정상범위": "≥0",   "카테고리": "거래결과>전세",                  "출처": "실거래",   "설명": "전세 계약 건수 — 전세 수요 강도"},
        {"표시명": "전세 보증금단가",         "컬럼명": "전세_보증금단가",        "단위": "만원/m²",    "연집계룰": "가중평균(전세_거래량)",     "정상범위": "",      "카테고리": "거래결과>전세",                  "출처": "실거래",   "설명": "m²당 전세보증금 — 면적 무관 비교"},
        # 거래결과 > 월세
        {"표시명": "월세 보증금평균",         "컬럼명": "월세_보증금평균",        "단위": "만원",       "연집계룰": "가중평균(월세_거래량)",     "정상범위": "",      "카테고리": "거래결과>월세",                  "출처": "실거래",   "설명": "월세 계약 보증금 평균"},
        {"표시명": "월세 거래량",             "컬럼명": "월세_거래량",            "단위": "건",         "연집계룰": "sum",                       "정상범위": "≥0",   "카테고리": "거래결과>월세",                  "출처": "실거래",   "설명": "월세 계약 건수 — 월세 전환 추세"},
        {"표시명": "월세 보증금단가",         "컬럼명": "월세_보증금단가",        "단위": "만원/m²",    "연집계룰": "가중평균(월세_거래량)",     "정상범위": "",      "카테고리": "거래결과>월세",                  "출처": "실거래",   "설명": "m²당 월세 보증금"},
        {"표시명": "월세 평균",               "컬럼명": "월세_월세평균",          "단위": "만원/월",    "연집계룰": "가중평균(월세_거래량)",     "정상범위": "",      "카테고리": "거래결과>월세",                  "출처": "실거래",   "설명": "월 임대료 평균 — 임대수익률 산정 기초"},
        # 거래결과 > 임대전체
        {"표시명": "임대전체 보증금평균",     "컬럼명": "임대전체_보증금평균",    "단위": "만원",       "연집계룰": "가중평균(임대전체_거래량)", "정상범위": "",      "카테고리": "거래결과>임대전체",              "출처": "실거래",   "설명": "전세+월세 전체 보증금 평균"},
        {"표시명": "임대전체 거래량",         "컬럼명": "임대전체_거래량",        "단위": "건",         "연집계룰": "sum",                       "정상범위": "≥0",   "카테고리": "거래결과>임대전체",              "출처": "실거래",   "설명": "전체 임대 건수 — 임대시장 규모"},
        {"표시명": "임대전체 보증금단가",     "컬럼명": "임대전체_보증금단가",    "단위": "만원/m²",    "연집계룰": "가중평균(임대전체_거래량)", "정상범위": "",      "카테고리": "거래결과>임대전체",              "출처": "실거래",   "설명": "전체 임대 m²당 보증금"},
        {"표시명": "임대전체 월세평균",       "컬럼명": "임대전체_월세평균",      "단위": "만원/월",    "연집계룰": "가중평균(임대전체_거래량)", "정상범위": "",      "카테고리": "거래결과>임대전체",              "출처": "실거래",   "설명": "전체 임대 월 임대료 평균"},
        # 수요 > 인구수요
        {"표시명": "총인구",                  "컬럼명": "총인구",                "단위": "명",         "연집계룰": "last",                      "정상범위": "",      "카테고리": "수요>인구수요",                  "출처": "통계청"},
        {"표시명": "남자인구",                "컬럼명": "남자인구",              "단위": "명",         "연집계룰": "last",                      "정상범위": "",      "카테고리": "수요>인구수요",                  "출처": "통계청"},
        {"표시명": "여자인구",                "컬럼명": "여자인구",              "단위": "명",         "연집계룰": "last",                      "정상범위": "",      "카테고리": "수요>인구수요",                  "출처": "통계청"},
        {"표시명": "20대 인구",               "컬럼명": "20대",                  "단위": "명",         "연집계룰": "last",                      "정상범위": "",      "카테고리": "수요>인구수요",                  "출처": "통계청"},
        {"표시명": "20대 남자",               "컬럼명": "남_20대",               "단위": "명",         "연집계룰": "last",                      "정상범위": "",      "카테고리": "수요>인구수요",                  "출처": "통계청"},
        {"표시명": "20대 여자",               "컬럼명": "여_20대",               "단위": "명",         "연집계룰": "last",                      "정상범위": "",      "카테고리": "수요>인구수요",                  "출처": "통계청"},
        {"표시명": "30대 인구",               "컬럼명": "30대",                  "단위": "명",         "연집계룰": "last",                      "정상범위": "",      "카테고리": "수요>인구수요",                  "출처": "통계청"},
        {"표시명": "30대 남자",               "컬럼명": "남_30대",               "단위": "명",         "연집계룰": "last",                      "정상범위": "",      "카테고리": "수요>인구수요",                  "출처": "통계청"},
        {"표시명": "30대 여자",               "컬럼명": "여_30대",               "단위": "명",         "연집계룰": "last",                      "정상범위": "",      "카테고리": "수요>인구수요",                  "출처": "통계청"},
        {"표시명": "40대 인구",               "컬럼명": "40대",                  "단위": "명",         "연집계룰": "last",                      "정상범위": "",      "카테고리": "수요>인구수요",                  "출처": "통계청"},
        {"표시명": "40대 남자",               "컬럼명": "남_40대",               "단위": "명",         "연집계룰": "last",                      "정상범위": "",      "카테고리": "수요>인구수요",                  "출처": "통계청"},
        {"표시명": "40대 여자",               "컬럼명": "여_40대",               "단위": "명",         "연집계룰": "last",                      "정상범위": "",      "카테고리": "수요>인구수요",                  "출처": "통계청"},
        {"표시명": "50대이상 인구",           "컬럼명": "50대이상",              "단위": "명",         "연집계룰": "last",                      "정상범위": "",      "카테고리": "수요>인구수요",                  "출처": "통계청"},
        {"표시명": "50대이상 남자",           "컬럼명": "남_50대이상",           "단위": "명",         "연집계룰": "last",                      "정상범위": "",      "카테고리": "수요>인구수요",                  "출처": "통계청"},
        {"표시명": "50대이상 여자",           "컬럼명": "여_50대이상",           "단위": "명",         "연집계룰": "last",                      "정상범위": "",      "카테고리": "수요>인구수요",                  "출처": "통계청"},
        # 공급
        {"표시명": "지역내총생산(GRDP)",      "컬럼명": "GRDP",                  "단위": "백만원",     "연집계룰": "last",                      "정상범위": "",      "카테고리": "공급",                           "출처": "통계청",    "설명": "지역 경제규모 — 고용·소득 기반"},
        {"표시명": "아파트 인허가",           "컬럼명": "인허가_호수",            "단위": "호",         "연집계룰": "sum",                       "정상범위": "≥0",   "카테고리": "공급",                           "출처": "통계청",    "설명": "신규 인허가 호수 — 2~3년 후 공급 선행지표"},
        # 수요 > 유효수요 > 소득/신용 (국민연금)
        {"표시명": "NPS 가입자수",            "컬럼명": "NPS_가입자수",           "단위": "명",         "연집계룰": "최신 스냅샷",               "정상범위": "≥0",   "카테고리": "수요>유효수요>소득/신용",        "출처": "국민연금",  "설명": "4대보험 직장가입자 수 — 지역 고용규모 대리변수"},
        {"표시명": "NPS 1인당고지금액",       "컬럼명": "NPS_1인당고지금액",      "단위": "원",         "연집계룰": "가중평균(가입자수)",         "정상범위": "",      "카테고리": "수요>유효수요>소득/신용",        "출처": "국민연금",  "설명": "1인당 월 보험료 — 지역 소득수준 대리변수 (소득의 9%)"},
        {"표시명": "NPS 사업장수",            "컬럼명": "NPS_사업장수",           "단위": "개",         "연집계룰": "최신 스냅샷",               "정상범위": "≥0",   "카테고리": "수요>유효수요>소득/신용",        "출처": "국민연금",  "설명": "국민연금 가입 사업장 수 — 지역 사업체 밀도"},
        {"표시명": "NPS 고용증감",            "컬럼명": "NPS_고용증감",           "단위": "명",         "연집계룰": "최신 스냅샷",               "정상범위": "",      "카테고리": "수요>유효수요>소득/신용",        "출처": "국민연금",  "설명": "전월 대비 가입자 증감 — 양수=고용 증가"},
        # 수요 > 유효수요 > 소득/신용 (BOK 주담대)
        {"표시명": "주담대 잔액",             "컬럼명": "주담대_잔액",            "단위": "십억원",     "연집계룰": "12월값(연말잔액)",           "정상범위": "",      "카테고리": "수요>유효수요>소득/신용",        "출처": "BOK",      "설명": "주택담보대출 잔액 — 레버리지 총량 (높을수록 부담↑)"},
        {"표시명": "주담대 증감률",           "컬럼명": "주담대_증감률",          "단위": "%",          "연집계룰": "12월 전월비",               "정상범위": "",      "카테고리": "수요>유효수요>소득/신용",        "출처": "BOK",      "설명": "전월비 대출 증감 — 양수=대출 팽창"},
        {"표시명": "주담대 비중",             "컬럼명": "주담대_비중",            "단위": "%",          "연집계룰": "12월값",                    "정상범위": "0~100", "카테고리": "수요>유효수요>소득/신용",        "출처": "BOK",      "설명": "전체 가계대출 중 주담대 비율 — 주택 쏠림 정도"},
        # 수요 > 유효수요 > 기존자산 (KOSIS)
        {"표시명": "가구 자산평균",           "컬럼명": "가구_자산평균",          "단위": "만원",       "연집계룰": "연간",                      "정상범위": "",      "카테고리": "수요>유효수요>기존자산",         "출처": "KOSIS",    "설명": "가구당 총자산 (부동산+금융+기타) — 구매력"},
        {"표시명": "가구 부채평균",           "컬럼명": "가구_부채평균",          "단위": "만원",       "연집계룰": "연간",                      "정상범위": "",      "카테고리": "수요>유효수요>기존자산",         "출처": "KOSIS",    "설명": "가구당 총부채 — 추가 대출 여력 역지표"},
        {"표시명": "가구 순자산",             "컬럼명": "가구_순자산",            "단위": "만원",       "연집계룰": "연간(자산-부채)",            "정상범위": "",      "카테고리": "수요>유효수요>기존자산",         "출처": "KOSIS",    "설명": "자산-부채 — 실질 구매력"},
        # 수요 > 유효수요 > 소득/신용 (KOSIS)
        {"표시명": "가구 소득평균",           "컬럼명": "가구_소득평균",          "단위": "만원",       "연집계룰": "연간",                      "정상범위": "",      "카테고리": "수요>유효수요>소득/신용",        "출처": "KOSIS",    "설명": "가구당 연 경상소득 — PIR 계산 기초"},
        {"표시명": "부채/소득비율(DSR)",      "컬럼명": "DSR",                   "단위": "%",          "연집계룰": "연간(부채/소득×100)",        "정상범위": "",      "카테고리": "수요>유효수요>소득/신용",        "출처": "KOSIS",    "설명": "소득 대비 부채 비율 — 100% 초과 시 고위험"},
        # 수요 > 유효수요 > 소득/신용 (국세청)
        {"표시명": "근로소득 신고인원",       "컬럼명": "총급여_인원",            "단위": "명",         "연집계룰": "연간(시군구합산)",            "정상범위": "≥0",   "카테고리": "수요>유효수요>소득/신용",        "출처": "국세청",    "설명": "연말정산 신고자 수 — 지역 근로자 규모"},
        {"표시명": "근로소득 총급여",         "컬럼명": "총급여_금액",            "단위": "백만원",     "연집계룰": "연간(시군구합산)",            "정상범위": "≥0",   "카테고리": "수요>유효수요>소득/신용",        "출처": "국세청",    "설명": "지역 전체 근로소득 총액 — 지역 소득 규모"},
        {"표시명": "1인당 평균급여",          "컬럼명": "1인당총급여_백만원",      "단위": "백만원",     "연집계룰": "연간(총급여/인원)",           "정상범위": "",      "카테고리": "수요>유효수요>소득/신용",        "출처": "국세청",    "설명": "근로자 1인당 연 평균급여 — 지역 소득수준"},
        {"표시명": "1인당 결정세액",          "컬럼명": "1인당결정세액_백만원",    "단위": "백만원",     "연집계룰": "연간(결정세액/인원)",         "정상범위": "",      "카테고리": "수요>유효수요>소득/신용",        "출처": "국세청",    "설명": "1인당 납부세액 — 고소득층 밀집도 간접지표"},
        # 시장심리 (KB부동산)
        {"표시명": "KB 매수우위지수",          "컬럼명": "KB_매수우위지수",         "단위": "지수",       "연집계룰": "12월값",                     "정상범위": "0~200", "카테고리": "시장심리",                       "출처": "KB부동산",  "설명": ">100 매수자 우위(하락 압력), <100 매도자 우위(상승 압력)"},
        {"표시명": "KB 매매거래지수",          "컬럼명": "KB_매매거래지수",         "단위": "지수",       "연집계룰": "12월값",                     "정상범위": "0~200", "카테고리": "시장심리",                       "출처": "KB부동산",  "설명": ">100 거래 활발, <100 거래 위축"},
        {"표시명": "KB 전세수급지수",          "컬럼명": "KB_전세수급지수",         "단위": "지수",       "연집계룰": "12월값",                     "정상범위": "0~200", "카테고리": "시장심리",                       "출처": "KB부동산",  "설명": ">100 공급 부족(전세가↑ 압력), <100 공급 충분"},
        # 시장심리 (BOK)
        {"표시명": "소비자심리지수(CCSI)",    "컬럼명": "소비자심리지수",          "단위": "지수",       "연집계룰": "12월값",                     "정상범위": "0~200", "카테고리": "시장심리",                       "출처": "BOK",      "설명": ">100 경기 낙관, <100 비관 — 소비·투자 심리"},
        {"표시명": "주택가격전망CSI",         "컬럼명": "주택가격전망CSI",         "단위": "지수",       "연집계룰": "12월값",                     "정상범위": "0~200", "카테고리": "시장심리",                       "출처": "BOK",      "설명": ">100 상승 전망 우세, <100 하락 전망"},
        {"표시명": "기준금리",                "컬럼명": "기준금리",               "단위": "%",          "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "시장심리",                       "출처": "BOK",      "설명": "한은 기준금리 — 대출이자·자산가격 핵심 변수"},
        {"표시명": "CD 91일",                 "컬럼명": "CD_91일",                "단위": "%",          "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "시장심리",                       "출처": "BOK",      "설명": "양도성예금증서 금리 — 변동금리 대출 기준"},
        {"표시명": "국고채 3년",              "컬럼명": "국고채_3년",             "단위": "%",          "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "시장심리",                       "출처": "BOK",      "설명": "국채 금리 — 고정금리 대출 기준"},
        {"표시명": "국고채 5년",              "컬럼명": "국고채_5년",             "단위": "%",          "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "시장심리",                       "출처": "BOK",      "설명": "국채 금리 — 중기 채권시장 기준"},
        {"표시명": "국고채 10년",             "컬럼명": "국고채_10년",            "단위": "%",          "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "시장심리",                       "출처": "BOK",      "설명": "국채 금리 — 장기 금리 기준"},
        {"표시명": "전월세전환율",            "컬럼명": "전월세전환율",           "단위": "%",          "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "시장심리",                       "출처": "BOK",      "설명": "전세→월세 전환 수익률 — 높으면 월세 부담↑"},
        {"표시명": "지가변동률",              "컬럼명": "지가변동률",             "단위": "%",          "연집계룰": "연간",                       "정상범위": "",      "카테고리": "시장심리",                       "출처": "BOK",      "설명": "전분기비 땅값 변화 — 개발기대·체감경기"},
        {"표시명": "아파트매매가격지수",      "컬럼명": "아파트매매가격지수",      "단위": "지수",       "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "시장심리",                       "출처": "BOK",      "설명": "2017.11=100 기준 — 장기 추세 비교용"},
        {"표시명": "아파트전세가격지수",      "컬럼명": "아파트전세가격지수",      "단위": "지수",       "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "시장심리",                       "출처": "BOK",      "설명": "2017.11=100 기준 — 장기 추세 비교용"},
        # KB 신규 지표 (수요>유효수요>소득/신용)
        {"표시명": "KB PIR(매매)",            "컬럼명": "KB_PIR",                 "단위": "년",         "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "수요>유효수요>소득/신용",       "출처": "KB부동산",  "설명": "주택가격/연소득 배수(중위 기준) — 몇 년치 소득으로 매수 가능한가 [전국/서울]"},
        {"표시명": "KB J-PIR(전세)",          "컬럼명": "KB_J_PIR",               "단위": "년",         "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "수요>유효수요>소득/신용",       "출처": "KB부동산",  "설명": "전세가격/연소득 배수(중위 기준) — 몇 년치 소득으로 전세 가능한가 [전국/서울]"},
        {"표시명": "KB HAI(주택구매력)",      "컬럼명": "KB_HAI",                 "단위": "지수",       "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "수요>유효수요>소득/신용",       "출처": "KB부동산",  "설명": "중간소득 가구의 주택 구입 능력 — 100이상이면 구매 가능"},
        {"표시명": "KB HOI(구매잠재력)",      "컬럼명": "KB_HOI",                 "단위": "지수",       "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "수요>유효수요>소득/신용",       "출처": "KB부동산",  "설명": "구입 가능 아파트 비율 — 서울/경기/인천만 제공"},
        # KB 신규 지표 (시장심리)
        {"표시명": "KB 선도50지수",           "컬럼명": "KB_선도50지수",          "단위": "지수",       "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "시장심리",                       "출처": "KB부동산",  "설명": "선도 아파트 50개 단지 가격지수 — 시장 선행 신호"},
        {"표시명": "KB 월세지수",             "컬럼명": "KB_월세지수",            "단위": "지수",       "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "시장심리",                       "출처": "KB부동산",  "설명": "아파트 월세 가격지수 — 임대차 시장 동향"},
        # 신규 5종 지표
        {"표시명": "부동산소비심리지수",      "컬럼명": "부동산소비심리지수",      "단위": "지수",       "연집계룰": "12월값",                     "정상범위": "0~200", "카테고리": "시장심리",                       "출처": "국토연구원","설명": "부동산 소비자 심리 — >100 강세, <100 약세 (국토연구원 KRIHS)"},
        {"표시명": "M2잔액",                  "컬럼명": "M2잔액",                 "단위": "십억원",     "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "시장심리",                       "출처": "BOK",       "설명": "광의통화(M2) 평잔 — 시중 유동성 총량, 부동산 가격 선행"},
        {"표시명": "M2 YoY",                  "컬럼명": "M2_YoY",                 "단위": "%",          "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "시장심리",                       "출처": "BOK",       "설명": "M2 전년동월비 증감률 — 통화량 팽창/수축 속도"},
        {"표시명": "예대금리차",              "컬럼명": "예대금리차",             "단위": "%p",         "연집계룰": "12월값",                     "정상범위": "1~3",   "카테고리": "시장심리",                       "출처": "BOK",       "설명": "대출금리-예금금리 스프레드 — 은행 수익성/대출 여력 지표"},
        {"표시명": "가계신용잔액",            "컬럼명": "가계신용잔액",           "단위": "십억원",     "연집계룰": "12월값",                     "정상범위": "",      "카테고리": "수요>유효수요>소득/신용",         "출처": "BOK",       "설명": "가계신용 총잔액(분기) — 부채 총량, 높을수록 금리 충격 취약"},
        {"표시명": "가계대출연체율",          "컬럼명": "가계대출연체율",         "단위": "%",          "연집계룰": "12월값",                     "정상범위": "0.2~1", "카테고리": "수요>유효수요>소득/신용",         "출처": "BOK",       "설명": "은행 가계대출 연체율 — 상승 시 신용경색/부동산 하락 신호"},
        {"표시명": "주택보급률",              "컬럼명": "주택보급률",             "단위": "%",          "연집계룰": "12월값",                     "정상범위": "90~110","카테고리": "공급",                           "출처": "국토부",    "설명": "가구수 대비 주택수 비율 — 100 이상이면 공급 충분 (다가구 구분 기준)"},
        # 파생지표
        {"표시명": "전세가율",               "컬럼명": "전세가율",              "단위": "%",          "연집계룰": "계산(전세보증금/매매가×100)", "정상범위": "40~80", "카테고리": "파생지표",                       "출처": "파생",     "설명": "매매가 대비 전세가 — 높으면 갭투자 여지↓, 실수요↑"},
        {"표시명": "PIR(소득대비주택가격)",    "컬럼명": "PIR",                  "단위": "배",         "연집계룰": "계산(매매가/가구소득)",       "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "가구소득 대비 주택가격 배수 — 높을수록 구매부담↑"},
        {"표시명": "PIR(NPS기반)",           "컬럼명": "PIR_NPS",              "단위": "배",         "연집계룰": "계산(매매가/NPS연소득)",      "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "NPS 추정소득 기반 PIR — 시군구 단위 비교 가능"},
        {"표시명": "매매 거래회전율",         "컬럼명": "매매_거래회전율",        "단위": "‰",         "연집계룰": "계산(거래량/인구×1000)",     "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "인구 대비 매매 건수 — 시장 유동성"},
        {"표시명": "전세 거래회전율",         "컬럼명": "전세_거래회전율",        "단위": "‰",         "연집계룰": "계산(전세거래/인구×1000)",    "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "인구 대비 전세 건수 — 전세시장 유동성"},
        {"표시명": "가격변화율(YoY)",        "컬럼명": "가격변화율_YoY",        "단위": "%",          "연집계룰": "계산(전년대비변화)",          "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "전년 대비 매매가 변화율 — 시장 모멘텀"},
        {"표시명": "소득대비대출",           "컬럼명": "소득대비대출",          "단위": "배",         "연집계룰": "계산(주담대/가구소득)",       "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "가구소득 대비 주담대 배수 — 레버리지 부담도"},
        {"표시명": "월세화 비율",             "컬럼명": "월세화비율",            "단위": "%",          "연집계룰": "계산(월세거래/전체임대×100)", "정상범위": "0~100", "카테고리": "파생지표",                       "출처": "파생",     "설명": "전체 임대차 중 월세 비중 — 전세 소멸 진행도"},
        {"표시명": "매매 흡수율",             "컬럼명": "매매흡수율",            "단위": "%",          "연집계룰": "계산(매매/(매매+전세)×100)",  "정상범위": "0~100", "카테고리": "파생지표",                       "출처": "파생",     "설명": "주거수요 중 매매가 차지하는 비중 — 높으면 실수요/투자 강세"},
        {"표시명": "전세가율 YoY모멘텀",      "컬럼명": "전세가율_모멘텀_YoY",   "단위": "%p",         "연집계룰": "계산(전세가율 전년동월차)",    "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "전세가율 12개월 전 대비 변화 — 가속 시 갭투자 유입 신호"},
        {"표시명": "갭투자 비용",             "컬럼명": "갭비용",                "단위": "만원",       "연집계룰": "계산(매매가-전세보증금)",      "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "전세 끼고 매수 시 실투자금 — 낮을수록 갭투자 유입 쉬움"},
        {"표시명": "실질 임대수익률",         "컬럼명": "임대수익률",            "단위": "%",          "연집계룰": "계산(임대료등가×12/매매가)",   "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "연 임대료등가/매매가 — 기준금리와 비교 시 투자매력 판단"},
        {"표시명": "P/R Ratio",              "컬럼명": "PR비율",                "단위": "배",         "연집계룰": "계산(매매가/연임대료등가)",    "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "가격-임대 배수 — 국제표준 버블지표 (20↑ 고평가)"},
        {"표시명": "미분양 소화기간",         "컬럼명": "미분양소화기간",        "단위": "개월",       "연집계룰": "계산(미분양/MA12거래량)",      "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "현재 거래속도로 미분양 소진 기간 — 6개월 균형, 9↑ 과잉"},
        {"표시명": "금리조정 PIR",            "컬럼명": "금리조정PIR",           "단위": "배",         "연집계룰": "계산(PIR×(1+기준금리%))",      "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "금리 부담 반영 PIR — 실질 구매부담 측정"},
    ])
    if "설명" in VAR_META.columns:
        VAR_META["설명"] = VAR_META["설명"].fillna("")
        # 설명 끝에 [출처] 태그 자동 부착
        VAR_META["설명"] = VAR_META.apply(
            lambda r: f'{r["설명"]} [{r["출처"]}]' if r["설명"] else f'[{r["출처"]}]', axis=1
        )

    # ── 변수 라벨 포맷터 ([출처] 표시명) ──────────────────────────────
    def _fmt_var_by_col(col):
        """컬럼명 → '[출처] 표시명' 반환"""
        row = VAR_META[VAR_META["컬럼명"] == col]
        if row.empty:
            meta = INDICATOR_META.get(str(col))
            if meta:
                return f"[{meta['source']}] {meta['label']}"
            return str(col)
        src = row.iloc[0].get("출처", "")
        lbl = row.iloc[0]["표시명"]
        return f"[{src}] {lbl}" if src else lbl

    def _fmt_var_by_label(label):
        """표시명 → '[출처] 표시명' 반환 (이미 [출처]가 붙어있으면 그대로)"""
        if isinstance(label, str) and label.startswith("["):
            return label
        row = VAR_META[VAR_META["표시명"] == label]
        if row.empty:
            return str(label)
        src = row.iloc[0].get("출처", "")
        return f"[{src}] {label}" if src else label

    with st.expander("변수 목록 및 메타데이터"):
        # 카테고리/출처/설명 컬럼 포함하여 표시
        st.dataframe(VAR_META, use_container_width=True, hide_index=True)
        st.caption(
            "연집계룰: 월별→연별 변환 시 적용 기준.  "
            "가중평균 = 거래량 기준 가중산술평균 / last = 해당 연도 마지막 월값 / sum = 월 합산."
        )

    # ── 수식 빌더에 사용할 변수 목록 ──────────────────────────────
    time_col_5 = "연월" if freq == "월별" and "연월" in analysis_df.columns else "연도"
    numeric_cols_5 = sorted([
        c for c in analysis_df.columns
        if c not in ("시도", "연도", "월", "연월", "지역코드")
        and pd.api.types.is_numeric_dtype(analysis_df[c])
        and analysis_df[c].notna().any()
    ])

    ZERO_PRONE_5 = {"거래량", "전세_거래량", "월세_거래량", "임대전체_거래량", "인허가_호수",
                     "NPS_가입자수", "NPS_사업장수", "가구_소득평균",
                     "총급여_인원", "총급여_금액",
                     "매매_거래회전율", "전세_거래회전율"}

    # 프리셋 수식 템플릿 (label, 수식, 단위)
    PRESETS_5 = [
        ("PIR(소득대비)", "평균가격 / (NPS_1인당고지금액 * 12 / 10000)", "배"),
        ("전세가율(%)",   "전세_보증금평균 / 평균가격 * 100",             "%"),
        ("소득대비가격",  "평균가격 / 가구_소득평균",                     "배"),
        ("공급강도",      "인허가_호수 / 총인구 * 1000",                  "호/천명"),
    ]

    # ── 수식 텍스트 세션 상태 초기화 ──────────────────────────────
    MAX_FORMULAS = 4
    for _i in range(MAX_FORMULAS):
        if f"f5_ta_{_i}" not in st.session_state:
            st.session_state[f"f5_ta_{_i}"] = ""

    if not numeric_cols_5:
        st.warning("사용 가능한 수치형 변수가 없습니다.")
    else:
        # ── 수식 정의 ──────────────────────────────────────────────
        st.subheader("수식 정의")
        formulas_5 = []

        for i in range(MAX_FORMULAS):
            with st.expander(f"수식 {i + 1}", expanded=(i < 2)):
                enabled = st.checkbox("활성화", value=(i == 0), key=f"f5_enabled_{i}")
                if not enabled:
                    formulas_5.append(None)
                    continue

                ta_key = f"f5_ta_{i}"

                # ① 변수 선택 (카테고리 → 변수 2단계 선택) → 수식에 삽입
                col_sel, col_ins = st.columns([5, 1])
                with col_sel:
                    # 1단계: 카테고리 선택
                    cat_list = ["(전체)"] + sorted(VAR_META["카테고리"].unique().tolist())
                    sel_cat = st.selectbox("카테고리", cat_list, key=f"cat_{i}")

                    # 2단계: 카테고리 필터
                    if sel_cat == "(전체)":
                        filtered_meta = VAR_META[VAR_META["컬럼명"].isin(numeric_cols_5)].copy()
                    else:
                        filtered_meta = VAR_META[
                            (VAR_META["카테고리"] == sel_cat) &
                            (VAR_META["컬럼명"].isin(numeric_cols_5))
                        ].copy()

                    # 메타에 없는 컬럼 fallback
                    meta_col_set = set(VAR_META["컬럼명"].tolist())
                    extra_cols = [c for c in numeric_cols_5 if c not in meta_col_set]
                    if sel_cat == "(전체)" and extra_cols:
                        extra_rows = pd.DataFrame([
                            {"표시명": c, "컬럼명": c, "카테고리": "(기타)", "출처": "-"}
                            for c in extra_cols
                        ])
                        filtered_meta = pd.concat([filtered_meta, extra_rows], ignore_index=True)

                    # 검색창 (표시명·설명 키워드 필터)
                    _srch = st.text_input(
                        "변수 검색", placeholder="예: 인구, 금리, KB …",
                        key=f"f5_search_{i}", label_visibility="collapsed",
                    )
                    if _srch.strip():
                        _smask = filtered_meta["표시명"].str.contains(_srch, case=False, na=False)
                        if "설명" in filtered_meta.columns:
                            _smask |= filtered_meta["설명"].fillna("").str.contains(_srch, case=False, na=False)
                        filtered_meta = filtered_meta[_smask]

                    # [출처] 접두사 라벨 생성
                    filtered_meta = filtered_meta.copy()
                    filtered_meta["_라벨"] = filtered_meta.apply(
                        lambda r: f"[{r['출처']}] {r['표시명']}" if str(r.get("출처", "-")) not in ("", "-") else r["표시명"],
                        axis=1,
                    )

                    disp_labels = filtered_meta["_라벨"].tolist() if not filtered_meta.empty else numeric_cols_5
                    sel_disp = st.selectbox(
                        "변수 선택 후 [삽입] 클릭",
                        disp_labels,
                        key=f"f5_selvar_{i}",
                    )
                    # 라벨 → 컬럼명 매핑
                    _meta_match = filtered_meta[filtered_meta["_라벨"] == sel_disp]
                    sel_var = _meta_match["컬럼명"].iloc[0] if not _meta_match.empty else sel_disp

                    # 선택 변수 설명 표시 (출처는 이미 라벨에 포함)
                    if not _meta_match.empty:
                        _desc = _meta_match.iloc[0].get("설명", "")
                        if _desc:
                            st.caption(_desc)

                with col_ins:
                    st.write("")
                    if st.button("삽입", key=f"f5_ins_{i}", use_container_width=True):
                        cur = st.session_state.get(ta_key, "")
                        sep = " " if cur and not cur.endswith(" ") else ""
                        st.session_state[ta_key] = cur + sep + sel_var

                # ② 프리셋 수식 템플릿
                st.caption("수식 템플릿:")
                _p_cols = st.columns(len(PRESETS_5))
                for _pi, (_plbl, _pexpr, _punit) in enumerate(PRESETS_5):
                    if _p_cols[_pi].button(_plbl, key=f"f5_preset_{i}_{_pi}", use_container_width=True):
                        st.session_state[ta_key] = _pexpr
                        st.session_state[f"f5_unit_{i}"] = _punit

                # ③ 연산자 단축 버튼
                st.caption("연산자 빠른 삽입:")
                op_cols = st.columns(7)
                for _j, (_lbl, _val) in enumerate([
                    ("+", " + "), ("−", " - "), ("×", " * "), ("÷", " / "),
                    ("(", " ("), (")", ") "), ("지우기", None),
                ]):
                    if op_cols[_j].button(_lbl, key=f"f5_op_{i}_{_j}", use_container_width=True):
                        if _val is None:
                            st.session_state[ta_key] = ""
                        else:
                            st.session_state[ta_key] = st.session_state.get(ta_key, "") + _val

                # ③ 수식 텍스트 입력 (직접 편집 가능)
                formula_str = st.text_area(
                    "수식 입력 (직접 편집 가능)",
                    key=ta_key,
                    height=80,
                    placeholder="예: 평균가격 / GRDP * 12   또는   (전세_보증금평균 + 월세_보증금평균) / 총인구",
                )

                # ④ 실시간 수식 검증
                if formula_str.strip() and not analysis_df.empty:
                    _test_ns = {
                        col: analysis_df[col].astype(float)
                        for col in numeric_cols_5 if col in analysis_df.columns
                    }
                    _test_ns.update({"__builtins__": {}, "abs": np.abs, "sqrt": np.sqrt,
                                     "log": np.log, "log10": np.log10, "exp": np.exp})
                    try:
                        with np.errstate(divide="ignore", invalid="ignore"):
                            _res = eval(formula_str, {"__builtins__": {}}, _test_ns)
                        if hasattr(_res, "dropna"):
                            _valid_n = _res.replace([np.inf, -np.inf], np.nan).dropna().shape[0]
                            _sample = _res.replace([np.inf, -np.inf], np.nan).dropna()
                            _hint = f"{_sample.iloc[0]:,.4f}" if len(_sample) > 0 else "없음"
                            st.success(f"수식 유효  |  유효 데이터 {_valid_n}행  |  첫 유효값: {_hint}")
                        else:
                            st.success(f"수식 유효  |  결과: {float(_res):,.4f}")
                    except Exception as _e:
                        st.error(f"수식 오류: {_e}")

                # ⑤ 0 가능성 경고
                if "/" in formula_str:
                    _warned = [v for v in ZERO_PRONE_5 if v in formula_str]
                    if _warned:
                        st.warning(f"0이 될 수 있는 변수 포함: {', '.join(_warned)} → 0/inf는 NaN 처리됩니다.")

                # ⑥ 수식 이름 및 단위
                _col_nm, _col_ut = st.columns([3, 1])
                with _col_nm:
                    _default_lbl = (formula_str[:40] + "...") if len(formula_str) > 40 else formula_str
                    label = st.text_input("수식 이름 (범례)", value=_default_lbl or f"수식{i+1}",
                                         key=f"f5_label_{i}")
                with _col_ut:
                    unit = st.text_input("단위 (선택)", value="", key=f"f5_unit_{i}",
                                        placeholder="만원, 배율 …")

                formulas_5.append({"expr": formula_str, "label": label, "unit": unit})

        active_formulas_5 = [f for f in formulas_5 if f is not None and f["expr"].strip()]

        if not active_formulas_5:
            st.info("수식을 1개 이상 입력하고 활성화하세요.")
        else:
            # ── 차트 설정 ──────────────────────────────────────────
            st.subheader("차트 설정")
            default_sido_5 = (selected_sido[:3] if len(selected_sido) > 3 else selected_sido) or all_sido[:1]
            sido_for_calc = st.multiselect(
                "시도 선택",
                options=selected_sido if selected_sido else all_sido,
                default=default_sido_5,
                key="calc5_sido",
            )
            chart_mode_5 = st.radio(
                "값 표시 방식",
                ["원값", "Index=100 (기준시점)", "정규화 (0-1)"],
                horizontal=True, key="calc5_mode",
            )
            base_period_5 = None
            if chart_mode_5 == "Index=100 (기준시점)":
                avail_periods = sorted(analysis_df[time_col_5].dropna().unique())
                if avail_periods:
                    base_period_5 = st.selectbox("기준 시점", avail_periods, index=0, key="calc5_base")

            # ── 캐시 키 및 계산 실행 ───────────────────────────────
            formula_params_5 = tuple((f["label"], f["expr"]) for f in active_formulas_5)
            cache_key_5 = (
                f"{freq}_{selected_years[0]}_{selected_years[1]}_"
                f"{'_'.join(sorted(selected_sido or []))}"
            )
            computed_df_5 = _compute_formulas(
                formula_params_5, tuple(numeric_cols_5),
                tuple(sido_for_calc), time_col_5, cache_key_5, analysis_df,
            )
            formula_labels_5 = [f["label"] for f in active_formulas_5]

            # ── 값 변환 함수 ────────────────────────────────────────
            def _transform_5(df, col, mode, base_period, time_col):
                df = df.copy()
                for _s in df["시도"].unique():
                    _m = df["시도"] == _s
                    _v = df.loc[_m, col].astype(float)
                    if mode == "정규화 (0-1)":
                        vmin, vmax = _v.min(), _v.max()
                        df.loc[_m, col] = (_v - vmin) / (vmax - vmin) if vmax > vmin else 0.0
                    elif mode == "Index=100 (기준시점)" and base_period is not None:
                        _base = df.loc[_m & (df[time_col] == base_period), col]
                        if not _base.empty and _base.iloc[0] != 0 and not pd.isna(_base.iloc[0]):
                            df.loc[_m, col] = _v / _base.iloc[0] * 100
                        else:
                            df.loc[_m, col] = np.nan
                return df

            y_axis_label_5 = {
                "원값": "값",
                "Index=100 (기준시점)": "Index (기준=100)",
                "정규화 (0-1)": "정규화값 (0-1)",
            }.get(chart_mode_5, "값")

            # ── 통합 차트: 모든 수식을 하나의 차트에 ──────────────
            st.subheader("통합 차트 (수식 1~4 한 화면)")
            if chart_mode_5 == "원값" and len(active_formulas_5) > 1:
                st.caption(
                    "원값 모드에서 수식 간 단위/스케일이 다를 경우 가독성이 떨어질 수 있습니다. "
                    "아래 [듀얼 Y축] 섹션을 이용하거나 Index=100 / 정규화 모드를 권장합니다."
                )

            combined_sido = st.selectbox(
                "시도 선택 (통합 차트)", options=sido_for_calc or all_sido, key="calc5_combined_sido"
            )
            sido_combined_df = computed_df_5[
                computed_df_5["시도"] == combined_sido
            ].sort_values(time_col_5)

            avail_labels = [
                l for l in formula_labels_5
                if l in sido_combined_df.columns and sido_combined_df[l].notna().any()
            ]
            if avail_labels and not sido_combined_df.empty:
                plot_combined = sido_combined_df[[time_col_5] + avail_labels].copy()
                for lbl in avail_labels:
                    _tmp = _transform_5(
                        plot_combined[[time_col_5, lbl]].assign(시도=combined_sido),
                        lbl, chart_mode_5, base_period_5, time_col_5,
                    )
                    plot_combined[lbl] = _tmp[lbl].values

                melted_combined = plot_combined.melt(
                    id_vars=[time_col_5], var_name="수식", value_name=y_axis_label_5
                )
                fig_combined = px.line(
                    melted_combined.dropna(subset=[y_axis_label_5]).sort_values(time_col_5),
                    x=time_col_5, y=y_axis_label_5, color="수식",
                    title=f"{combined_sido}: 수식 통합 차트",
                    markers=True,
                    labels={time_col_5: "기간"},
                )
                register_fig("수식_통합차트", fig_combined, "수요-공급 분석기")
                st.plotly_chart(fig_combined, use_container_width=True, key="calc5_combined")
            else:
                st.info("계산 결과가 없습니다.")

            # ── 듀얼 Y축 비교 ──────────────────────────────────────
            if len(active_formulas_5) >= 2:
                st.subheader("수식 간 비교 (듀얼 Y축)")
                cmp_sido_5 = st.selectbox(
                    "비교 시도", options=sido_for_calc or all_sido, key="calc5_cmp_sido"
                )
                col_y1, col_y2 = st.columns(2)
                with col_y1:
                    left_f_label = st.selectbox("좌측 Y축 수식", formula_labels_5, key="calc5_y1")
                with col_y2:
                    right_candidates_5 = [l for l in formula_labels_5 if l != left_f_label]
                    right_f_label = (
                        st.selectbox("우측 Y축 수식", right_candidates_5, key="calc5_y2")
                        if right_candidates_5 else None
                    )

                sido_cmp_df = computed_df_5[
                    computed_df_5["시도"] == cmp_sido_5
                ].sort_values(time_col_5)

                if not sido_cmp_df.empty and right_f_label:
                    left_plot = _transform_5(
                        sido_cmp_df[[time_col_5, left_f_label]].dropna(subset=[left_f_label]).assign(시도=cmp_sido_5),
                        left_f_label, chart_mode_5, base_period_5, time_col_5,
                    )
                    right_plot = _transform_5(
                        sido_cmp_df[[time_col_5, right_f_label]].dropna(subset=[right_f_label]).assign(시도=cmp_sido_5),
                        right_f_label, chart_mode_5, base_period_5, time_col_5,
                    )
                    fig_dual = make_subplots(specs=[[{"secondary_y": True}]])
                    fig_dual.add_trace(
                        go.Scatter(
                            x=left_plot[time_col_5], y=left_plot[left_f_label],
                            name=left_f_label, line=dict(color="royalblue"), mode="lines+markers",
                        ),
                        secondary_y=False,
                    )
                    fig_dual.add_trace(
                        go.Scatter(
                            x=right_plot[time_col_5], y=right_plot[right_f_label],
                            name=right_f_label, line=dict(color="tomato", dash="dash"), mode="lines+markers",
                        ),
                        secondary_y=True,
                    )
                    fig_dual.update_layout(
                        title=f"{cmp_sido_5}: {left_f_label} vs {right_f_label}",
                        xaxis_title="기간",
                    )
                    fig_dual.update_yaxes(
                        title_text=f"{left_f_label} ({y_axis_label_5})",
                        secondary_y=False, title_font=dict(color="royalblue"),
                    )
                    fig_dual.update_yaxes(
                        title_text=f"{right_f_label} ({y_axis_label_5})",
                        secondary_y=True, title_font=dict(color="tomato"),
                    )
                    register_fig("수식_듀얼축", fig_dual, "수요-공급 분석기")
                    st.plotly_chart(fig_dual, use_container_width=True, key="calc5_dual")

                    common_5 = pd.merge(
                        left_plot[[time_col_5, left_f_label]],
                        right_plot[[time_col_5, right_f_label]],
                        on=time_col_5,
                    ).dropna()
                    if len(common_5) >= 3:
                        from scipy import stats as sp_stats
                        r5, p5 = sp_stats.pearsonr(common_5[left_f_label], common_5[right_f_label])
                        st.info(f"같이 움직인 정도: **{r5:.4f}** (통계 신뢰도: {1 - p5:.1%}, 표본 {len(common_5)}개)")

            # ── 데이터 테이블 ──────────────────────────────────────
            with st.expander("계산 결과 데이터 테이블"):
                disp_cols_5 = [time_col_5, "시도"] + [
                    l for l in formula_labels_5 if l in computed_df_5.columns
                ]
                fmt_dict_5 = {l: "{:,.4f}" for l in formula_labels_5 if l in computed_df_5.columns}
                st.dataframe(
                    computed_df_5[disp_cols_5].style.format(fmt_dict_5, na_rep="N/A"),
                    use_container_width=True,
                )

    st.divider()
    st.header("전세-매매 선행 신호")
    st.caption("전세와 매매 중 무엇이 먼저 움직였고, 그 뒤 다른 지표가 따라온 패턴이 반복됐는지 확인합니다.")

    lead_src = filtered_monthly if not filtered_monthly.empty else filtered_yearly
    lead_time_col = "연월" if "연월" in lead_src.columns else "연도"
    if lead_src.empty or "평균가격" not in lead_src.columns or "전세_보증금평균" not in lead_src.columns:
        st.info("전세-매매 선행 신호를 계산하려면 매매가격과 전세보증금 데이터가 모두 필요합니다.")
    else:
        ll_c1, ll_c2, ll_c3 = st.columns(3)
        with ll_c1:
            ll_max_lag = st.slider("최대 몇 기간까지 먼저 움직였는지", 1, 12, 6, key="lead_lag_max")
        with ll_c2:
            ll_regions = st.multiselect(
                "지역",
                sorted(lead_src["시도"].dropna().unique()),
                default=(selected_sido[:3] if selected_sido else ["서울"] if "서울" in lead_src["시도"].unique() else []),
                key="lead_lag_regions",
            )
        with ll_c3:
            ll_use_change = st.checkbox("변화율 기준으로 보기", value=True, key="lead_lag_pct")

        ll_input = lead_src[lead_src["시도"].isin(ll_regions)].copy() if ll_regions else lead_src.copy()
        ll_df = compute_lead_lag_signal(
            ll_input,
            sale_col="평균가격",
            jeonse_col="전세_보증금평균",
            time_col=lead_time_col,
            max_lag=ll_max_lag,
            use_pct_change=ll_use_change,
        )
        if ll_df.empty:
            st.warning("선행 신호를 계산할 표본이 부족합니다.")
        else:
            ll_best = ll_df.iloc[0]
            s1, s2, s3, s4 = st.columns(4)
            s1.metric("가장 뚜렷한 지역", ll_best["지역"])
            s2.metric("선행 방향", ll_best["선행방향"])
            s3.metric("먼저 움직인 기간", f"{int(ll_best['먼저움직인기간'])}")
            s4.metric("반복성", ll_best["반복성"])

            display_cols = [
                "지역", "선행방향", "먼저움직인기간", "같이움직인정도",
                "통계신뢰도", "반복성", "표본수", "요약",
            ]
            st.dataframe(
                ll_df[[c for c in display_cols if c in ll_df.columns]].style.format({
                    "같이움직인정도": "{:.3f}",
                    "통계신뢰도": "{:.1%}",
                }, na_rep="N/A"),
                use_container_width=True,
                height=280,
            )

            fig_ll = px.bar(
                ll_df,
                x="지역",
                y="같이움직인정도",
                color="선행방향",
                hover_data=["먼저움직인기간", "반복성", "통계신뢰도"],
                title="지역별 전세-매매 선행 신호",
            )
            fig_ll.add_hline(y=0, line_dash="dash", line_color="gray")
            register_fig("전세매매_선행신호", fig_ll, "수요공급분석")
            st.plotly_chart(fig_ll, use_container_width=True)


# ============================
# Tab 5: 공급분석 (인허가/착공/준공 + 미분양)
# ============================
if main_tab5:
    st.header("공급분석")
    supply_sub1, supply_sub2, supply_sub3 = st.tabs(["입주물량", "인허가/착공/준공", "미분양"])

    with supply_sub1:
        st.caption("asil 입주물량 화면처럼 지역·기간별 예정 세대수와 단지 목록을 단순하게 확인합니다.")
        if movein_sido_monthly_df is not None and not movein_sido_monthly_df.empty:
            available_movein_sido = sorted(movein_sido_monthly_df["시도"].dropna().unique())
            preferred_movein_sido = [s for s in selected_sido if s in available_movein_sido]
            mv_sido_options = list(dict.fromkeys(preferred_movein_sido + available_movein_sido))
            mv_sido = st.selectbox("입주물량 시도", mv_sido_options, key="movein_sido")
            mv_min_year = int(movein_sido_monthly_df["연도"].min())
            mv_max_year = int(movein_sido_monthly_df["연도"].max())
            if mv_min_year == mv_max_year:
                mv_start_year = mv_end_year = mv_min_year
                st.caption(f"입주예정 기간: {mv_min_year}년")
            else:
                mv_start_year, mv_end_year = st.slider(
                    "입주예정 기간",
                    min_value=mv_min_year,
                    max_value=mv_max_year,
                    value=(mv_min_year, mv_max_year),
                    key="movein_year_range",
                )
            mv_monthly = movein_sido_monthly_df[
                (movein_sido_monthly_df["시도"] == mv_sido)
                & movein_sido_monthly_df["연도"].between(mv_start_year, mv_end_year)
            ].sort_values("연월")

            if not mv_monthly.empty:
                mv_total = int(mv_monthly["입주예정_세대수"].fillna(0).sum())
                mv_complexes = int(mv_monthly["입주예정_단지수"].fillna(0).sum())
                mv_peak = mv_monthly.loc[mv_monthly["입주예정_세대수"].idxmax()]
                c1, c2, c3 = st.columns(3)
                c1.metric("기간 합계 세대수", f"{mv_total:,}호")
                c2.metric("기간 합계 단지수", f"{mv_complexes:,}개")
                c3.metric("최대 입주월", f"{mv_peak['연월']}", f"{int(mv_peak['입주예정_세대수']):,}호")

                fig_movein = px.bar(
                    mv_monthly,
                    x="연월",
                    y="입주예정_세대수",
                    text="입주예정_단지수",
                    title=f"{mv_sido}: 월별 입주예정 물량",
                    labels={"입주예정_세대수": "세대수(호)", "연월": "입주예정월", "입주예정_단지수": "단지수"},
                )
                fig_movein.update_traces(texttemplate="%{text}개", textposition="outside")
                register_fig("입주예정_월별물량", fig_movein, "공급분석")
                st.plotly_chart(fig_movein, use_container_width=True)

                if movein_sigungu_monthly_df is not None and not movein_sigungu_monthly_df.empty:
                    mv_sgg = movein_sigungu_monthly_df[
                        (movein_sigungu_monthly_df["시도"] == mv_sido)
                        & movein_sigungu_monthly_df["연도"].between(mv_start_year, mv_end_year)
                    ]
                    if not mv_sgg.empty:
                        mv_sgg_rank = (
                            mv_sgg.groupby("시군구")[["입주예정_세대수", "입주예정_단지수"]]
                            .sum()
                            .reset_index()
                            .sort_values("입주예정_세대수", ascending=False)
                        )
                        fig_movein_rank = px.bar(
                            mv_sgg_rank.head(20),
                            x="시군구",
                            y="입주예정_세대수",
                            color="입주예정_세대수",
                            color_continuous_scale="Blues",
                            title=f"{mv_sido}: 시군구별 입주예정 물량 Top 20",
                        )
                        register_fig("입주예정_시군구순위", fig_movein_rank, "공급분석")
                        st.plotly_chart(fig_movein_rank, use_container_width=True)
                        st.dataframe(mv_sgg_rank.style.format({"입주예정_세대수": "{:,.0f}", "입주예정_단지수": "{:,.0f}"}), use_container_width=True, height=280)

                if movein_plan_df is not None and not movein_plan_df.empty:
                    mv_complex = movein_plan_df[
                        (movein_plan_df["시도"] == mv_sido)
                        & movein_plan_df["연도"].between(mv_start_year, mv_end_year)
                    ].sort_values(["입주예정연월", "시군구", "단지명"])
                    display_cols = [
                        "입주예정연월", "시군구", "단지명", "세대수", "공급세대수",
                        "주소", "사업유형", "시공사", "source_name", "validation_status",
                    ]
                    display_cols = [c for c in display_cols if c in mv_complex.columns]
                    st.subheader("단지 목록")
                    st.dataframe(mv_complex[display_cols], use_container_width=True, height=360)
            else:
                st.info("선택한 기간의 입주예정 데이터가 없습니다.")
        else:
            st.info("data/supply/movein_plan_complex_monthly.csv를 추가하면 입주물량 분석이 활성화됩니다.")

    with supply_sub2:
        # analysis_df에서 공급 관련 컬럼 탐색
        supply_cols = [c for c in ["인허가_호수", "착공_호수", "준공_호수"] if c in analysis_df.columns]
        if supply_cols:
            sup_sido = st.selectbox(
                "시도 선택", selected_sido if selected_sido else all_sido, key="supply_sido"
            )
            sup_time_col = "연월" if freq == "월별" and "연월" in analysis_df.columns else "연도"
            sup_df = analysis_df[analysis_df["시도"] == sup_sido].sort_values(sup_time_col)

            avail_supply = [c for c in supply_cols if sup_df[c].notna().any()]
            if avail_supply:
                sup_melted = sup_df[[sup_time_col] + avail_supply].melt(
                    id_vars=[sup_time_col], var_name="구분", value_name="호수"
                )
                fig_supply = px.line(
                    sup_melted.sort_values(sup_time_col),
                    x=sup_time_col, y="호수", color="구분",
                    title=f"{sup_sido}: 인허가/착공/준공 추이",
                    labels={sup_time_col: "기간", "호수": "호수(호)"},
                    markers=True,
                )
                register_fig("공급_파이프라인", fig_supply, "공급분석")
                st.plotly_chart(fig_supply, use_container_width=True)
            else:
                st.info("선택한 시도의 착공/준공 데이터가 없습니다.")
        else:
            st.info("착공/준공 데이터를 업데이트하면 이 탭에서 확인할 수 있습니다.")

    with supply_sub3:
        # 미분양 데이터 탐색
        unsold_cols = [c for c in ["미분양_호수", "미분양_평균"] if c in analysis_df.columns]
        if unsold_cols:
            unsold_time_col = "연월" if freq == "월별" and "연월" in analysis_df.columns else "연도"
            # 시도별 미분양 시계열
            unsold_var = st.selectbox("미분양 지표", unsold_cols, key="unsold_var", format_func=_fmt_var_by_col)
            unsold_df = analysis_df.groupby(["시도", unsold_time_col])[unsold_var].mean().reset_index()
            fig_unsold = px.line(
                unsold_df.sort_values(unsold_time_col),
                x=unsold_time_col, y=unsold_var, color="시도",
                title=f"시도별 {unsold_var} 추이",
                labels={unsold_time_col: "기간"},
                markers=True,
            )
            register_fig("미분양_추이", fig_unsold, "공급분석")
            st.plotly_chart(fig_unsold, use_container_width=True)

            # 최근 연도 시도별 막대 비교
            unsold_latest_yr = int(analysis_df["연도"].max())
            unsold_latest = analysis_df[analysis_df["연도"] == unsold_latest_yr].groupby("시도")[unsold_var].mean().reset_index()
            unsold_latest = unsold_latest.sort_values(unsold_var, ascending=False)
            fig_unsold_bar = px.bar(
                unsold_latest, x="시도", y=unsold_var,
                color=unsold_var, color_continuous_scale="Reds",
                title=f"{unsold_latest_yr}년 시도별 {unsold_var}",
            )
            register_fig("미분양_바", fig_unsold_bar, "공급분석")
            st.plotly_chart(fig_unsold_bar, use_container_width=True)
        else:
            st.info("미분양 데이터를 업데이트하면 이 탭에서 확인할 수 있습니다.")


# ============================
# Tab 6: 통계분석 (회귀 + 이상치 + 상관관계)
# ============================
if valuation_tab:
    st.header("자유차트")
    render_tab_usage_guide("자유차트")
    st.caption("여러 지역·지표·사용자 지표를 겹쳐 보고, 만든 조건이 과거에 효과 있었는지 바로 검증합니다.")

    super_tab, strategy_tab = st.tabs(["슈퍼차트", "전략검증"])

    with super_tab:
        chart_src = analysis_df.copy()
        if chart_src.empty:
            st.warning("자유차트에 표시할 데이터가 없습니다.")
        else:
            saved_charts = list_saved_charts()
            share_token = st.query_params.get("chart_share") if hasattr(st, "query_params") else None
            if isinstance(share_token, list):
                share_token = share_token[0] if share_token else None
            if share_token and st.session_state.get("loaded_chart_share") != share_token:
                shared_chart = get_shared_chart(str(share_token))
                if shared_chart:
                    _apply_saved_widget_settings(shared_chart.get("settings", {}))
                    st.session_state["loaded_chart_share"] = share_token
                    st.success(f"공유 차트 '{shared_chart['name']}' 설정을 불러왔습니다.")
                    st.rerun()
                else:
                    st.warning("공유 차트 토큰이 없거나 비공개로 전환되었습니다.")
    
            with st.expander("저장된 차트", expanded=False):
                if saved_charts:
                    chart_labels = {f"{row['name']} · {row['updated_at'] or row['created_at']}": row for row in saved_charts}
                    selected_saved_chart = st.selectbox("불러올 차트", list(chart_labels.keys()), key="saved_chart_select")
                    sc_load_col, sc_delete_col, sc_share_col, sc_revoke_col = st.columns(4)
                    selected_saved_row = chart_labels[selected_saved_chart]
                    with sc_load_col:
                        if st.button("차트 불러오기", key="load_saved_chart"):
                            _apply_saved_widget_settings(json.loads(selected_saved_row["settings_json"] or "{}"))
                            st.rerun()
                    with sc_delete_col:
                        if st.button("저장 차트 삭제", key="delete_saved_chart"):
                            delete_saved_chart(int(selected_saved_row["id"]))
                            st.rerun()
                    with sc_share_col:
                        if st.button("공유 링크 만들기", key="share_saved_chart"):
                            token = share_saved_chart(int(selected_saved_row["id"]))
                            st.session_state["last_chart_share_token"] = token
                            st.session_state["last_chart_share_id"] = int(selected_saved_row["id"])
                            st.rerun()
                    with sc_revoke_col:
                        if st.button("공유 해제", key="revoke_saved_chart"):
                            revoke_shared_chart(int(selected_saved_row["id"]))
                            st.session_state.pop("last_chart_share_token", None)
                            st.session_state.pop("last_chart_share_id", None)
                            st.rerun()
    
                    selected_chart_id = int(selected_saved_row["id"])
                    if st.session_state.get("last_chart_share_id") == selected_chart_id:
                        visible_token = st.session_state.get("last_chart_share_token")
                        if not visible_token:
                            visible_token = selected_saved_row.get("share_token")
                    else:
                        visible_token = selected_saved_row.get("share_token")
                    if visible_token:
                        st.text_input(
                            "공유 링크",
                            value=f"?chart_share={visible_token}",
                            key=f"saved_chart_share_link_{selected_chart_id}",
                            help="Streamlit 앱 주소 뒤에 이 값을 붙이면 다른 사용자가 같은 차트 설정을 불러올 수 있습니다.",
                        )
                else:
                    st.info("아직 저장된 차트가 없습니다.")
    
            sc_time_col = "연월" if freq == "월별" and "연월" in chart_src.columns else "연도"
            sc_numeric = _numeric_rule_columns(chart_src)
            sc_defaults = [c for c in ["평균가격", "전세_보증금평균", "전세가율", "거래량", "PIR", "갭비용"] if c in sc_numeric]
    
            sc_c1, sc_c2, sc_c3 = st.columns([1.4, 1.8, 1.2])
            with sc_c1:
                sc_regions = st.multiselect(
                    "지역",
                    sorted(chart_src["시도"].dropna().unique()),
                    default=(selected_sido[:3] if selected_sido else ["서울"] if "서울" in chart_src["시도"].unique() else []),
                    key="super_regions",
                )
            with sc_c2:
                sc_indicators = _render_grouped_indicator_picker(
                    "지표",
                    chart_src,
                    "super_indicators",
                    default_group="가격",
                    default=sc_defaults[:2],
                    multi=True,
                )
            with sc_c3:
                population_cols = {
                    "총인구", "남자인구", "여자인구", "20대", "남_20대", "여_20대",
                    "30대", "남_30대", "여_30대", "40대", "남_40대", "여_40대",
                    "50대이상", "남_50대이상", "여_50대이상",
                }
                is_population_chart = bool(set(sc_indicators or []) & population_cols)
                default_mode_index = 0 if is_population_chart else 1
                sc_mode = st.selectbox(
                    "보기 방식",
                    ["원값", "같은 기준으로 비교", "전년 대비", "전월 대비"],
                    index=default_mode_index,
                    key="super_mode",
                )
                if is_population_chart and sc_time_col == "연월":
                    st.caption("인구 지표는 연간 데이터라 월별 화면에서는 같은 연도 안에서 같은 값이 반복됩니다.")
    
            custom_expr = st.text_input(
                "사용자 지표",
                value="",
                placeholder="예: 평균가격 / 전세_보증금평균 또는 갭비용 / 평균가격 * 100",
                key="super_custom_expr",
            )
            custom_label = st.text_input("사용자 지표 이름", value="사용자 지표", key="super_custom_label")
    
            sc_plot = chart_src[chart_src["시도"].isin(sc_regions)].copy() if sc_regions else chart_src.copy()
            if custom_expr.strip():
                computed_custom = _compute_formulas(
                    ((custom_label, custom_expr),),
                    tuple(sc_numeric),
                    tuple(sc_regions),
                    sc_time_col,
                    f"super_{freq}_{selected_years}",
                    chart_src,
                )
                if not computed_custom.empty and custom_label in computed_custom.columns:
                    sc_plot = sc_plot.merge(
                        computed_custom[[sc_time_col, "시도", custom_label]],
                        on=[sc_time_col, "시도"],
                        how="left",
                    )
                    sc_indicators = list(dict.fromkeys(sc_indicators + [custom_label]))
    
            if not sc_indicators:
                st.info("표시할 지표를 1개 이상 선택하세요.")
            else:
                plot_rows = []
                for region, group in sc_plot.groupby("시도"):
                    group = group.sort_values(sc_time_col).copy()
                    for ind in sc_indicators:
                        if ind not in group.columns:
                            continue
                        series = group[ind].astype(float)
                        if sc_mode == "같은 기준으로 비교":
                            first = series.dropna().iloc[0] if not series.dropna().empty else np.nan
                            value = series / first * 100 if pd.notna(first) and first != 0 else np.nan
                            label = "Index=100"
                        elif sc_mode == "전년 대비":
                            period = 12 if sc_time_col == "연월" else 1
                            value = series.pct_change(period) * 100
                            label = "변화율(%)"
                        elif sc_mode == "전월 대비":
                            value = series.pct_change(1) * 100
                            label = "변화율(%)"
                        else:
                            value = series
                            label = "값"
                        temp = pd.DataFrame({
                            sc_time_col: group[sc_time_col],
                            "지역": region,
                            "지표": ind,
                            "계열": f"{region} · {_indicator_label(ind)}",
                            "표시값": value,
                            "축": label,
                        })
                        plot_rows.append(temp)
    
                sc_long = pd.concat(plot_rows, ignore_index=True) if plot_rows else pd.DataFrame()
                if sc_long.empty or sc_long["표시값"].dropna().empty:
                    st.info("표시 가능한 값이 없습니다.")
                else:
                    fig_super = px.line(
                        sc_long.dropna(subset=["표시값"]).sort_values(sc_time_col),
                        x=sc_time_col,
                        y="표시값",
                        color="계열",
                        line_dash="지표",
                        markers=True,
                        title="자유차트",
                        labels={sc_time_col: "기간", "표시값": sc_long["축"].dropna().iloc[0]},
                    )
                    register_fig("자유차트_슈퍼차트", fig_super, "자유차트")
                    st.plotly_chart(fig_super, use_container_width=True)
                    st.caption(_source_caption(sc_indicators))
                    save_col1, save_col2 = st.columns([2, 1])
                    with save_col1:
                        sc_save_name = st.text_input("차트 저장 이름", value="내 자유차트", key="super_save_name")
                    with save_col2:
                        st.write("")
                        st.write("")
                        if st.button("현재 차트 저장", key="super_save_button"):
                            save_chart_settings(
                                sc_save_name.strip() or "내 자유차트",
                                _chart_save_payload({"time_col": sc_time_col}),
                            )
                            st.success("차트 설정을 저장했습니다.")
    
            with st.expander("전세-매매 선행 신호 같이 보기"):
                if "평균가격" in chart_src.columns and "전세_보증금평균" in chart_src.columns:
                    sc_ll = compute_lead_lag_signal(
                        sc_plot,
                        sale_col="평균가격",
                        jeonse_col="전세_보증금평균",
                        time_col=sc_time_col,
                        max_lag=6,
                    )
                    if sc_ll.empty:
                        st.info("선행 신호를 계산할 표본이 부족합니다.")
                    else:
                        st.dataframe(
                            sc_ll[["지역", "선행방향", "먼저움직인기간", "같이움직인정도", "통계신뢰도", "반복성", "요약"]]
                            .style.format({"같이움직인정도": "{:.3f}", "통계신뢰도": "{:.1%}"}, na_rep="N/A"),
                            use_container_width=True,
                        )
                else:
                    st.info("매매가격과 전세보증금 데이터가 필요합니다.")
    
    with strategy_tab:
        bt_src = analysis_df.copy()
        if bt_src.empty:
            st.warning("전략검증에 사용할 데이터가 없습니다.")
        else:
            bt_time_col = "연월" if freq == "월별" and "연월" in bt_src.columns else "연도"
            bt_lead = compute_lead_lag_signal(
                bt_src,
                sale_col="평균가격",
                jeonse_col="전세_보증금평균",
                time_col=bt_time_col,
                max_lag=6,
            ) if "평균가격" in bt_src.columns and "전세_보증금평균" in bt_src.columns else pd.DataFrame()
            bt_work = prepare_screener_dataset(bt_src, lead_lag_df=bt_lead, time_col=bt_time_col)
            # 최신 스냅샷 조건뿐 아니라 과거 전체 행 조건도 검증할 수 있도록 선행 신호는 지역별로 붙인다.
            if not bt_lead.empty and "지역" in bt_lead.columns:
                bt_src = bt_src.merge(
                    bt_lead[["지역", "선행방향", "먼저움직인기간", "반복성"]],
                    left_on="시도",
                    right_on="지역",
                    how="left",
                )
            period = 12 if bt_time_col == "연월" else 1
            for col, new_col in [("평균가격", "가격_YoY"), ("거래량", "거래량_YoY"), ("전세가율", "전세가율_변화"), ("갭비용", "갭비용_변화")]:
                if col in bt_src.columns:
                    bt_src[new_col] = bt_src.groupby("시도")[col].pct_change(period) * 100
    
            st.subheader("조건 만들기")
            bt_cols = _numeric_rule_columns(bt_src) + [c for c in ["선행방향", "반복성"] if c in bt_src.columns]
            bt_defaults = [
                {"column": "PIR", "op": "<", "value": "15"} if "PIR" in bt_cols else None,
                {"column": "전세가율", "op": ">", "value": "60"} if "전세가율" in bt_cols else None,
                {"column": "선행방향", "op": "==", "value": "전세 선행"} if "선행방향" in bt_cols else None,
            ]
            bt_defaults = [r for r in bt_defaults if r]
            bt_rules, bt_combine = _render_condition_builder("bt", bt_cols, default_rules=bt_defaults)
            with st.expander("조건 저장", expanded=False):
                bt_condition_name = st.text_input("조건 이름", value="전략검증 조건", key="bt_condition_name")
                if st.button("현재 조건 저장", key="bt_save_condition"):
                    if bt_rules:
                        save_condition_set(bt_condition_name.strip() or "전략검증 조건", bt_rules, bt_combine)
                        st.success("조건을 저장했습니다.")
                    else:
                        st.warning("저장할 조건이 없습니다.")
    
            bt_h1, bt_h2, bt_h3, bt_h4, bt_h5 = st.columns(5)
            with bt_h1:
                bt_price_col = _render_indicator_picker(
                    "성과 기준 가격",
                    bt_src,
                    "bt_price_col",
                    groups=["가격"],
                    default="평균가격",
                    allow_extra_numeric=False,
                )
            with bt_h2:
                bt_horizons = st.multiselect("확인 기간", [3, 6, 12, 24], default=[6, 12, 24], key="bt_horizons")
            with bt_h3:
                bt_group = st.selectbox("검증 단위", ["시도"], key="bt_group")
            with bt_h4:
                bt_cooldown = st.number_input(
                    "중복 신호 쉬는 기간",
                    min_value=0,
                    max_value=36,
                    value=6 if bt_time_col == "연월" else 1,
                    step=1,
                    key="bt_cooldown",
                    help="한 번 진입 신호가 켜진 뒤 지정 기간 안의 반복 신호는 같은 매수 기회로 보고 제외합니다.",
                )
            with bt_h5:
                bt_success_threshold = st.number_input(
                    "성공 기준 수익률(%)",
                    value=0.0,
                    step=0.5,
                    key="bt_success_threshold",
                    help="성공률은 진입 신호 이후 선택 기간 수익률이 이 값을 초과한 비율입니다.",
                )
            st.caption("최대하락폭은 신호별 선택 기간 수익률 중 가장 낮은 값을 지역별로 다시 최저 집계한 값입니다.")
    
            if st.button("전략검증 실행", key="bt_run", type="primary"):
                if not bt_rules:
                    st.warning("조건을 1개 이상 켜세요.")
                else:
                    signals, summary = run_region_backtest(
                        bt_src,
                        bt_rules,
                        combine=bt_combine,
                        price_col=bt_price_col,
                        group_col=bt_group,
                        time_col=bt_time_col,
                        horizons=tuple(bt_horizons or [6, 12]),
                        cooldown_periods=int(bt_cooldown),
                        success_threshold=float(bt_success_threshold),
                    )
                    if summary.empty:
                        st.info("조건을 만족한 과거 시점이 없거나 이후 수익률을 계산할 데이터가 부족합니다.")
                    else:
                        st.subheader("지역별 검증 결과")
                        fmt = {c: "{:.1f}%" for c in summary.columns if "수익률" in c or "성공률" in c or c == "최대하락폭"}
                        st.dataframe(summary.style.format(fmt, na_rep="N/A"), use_container_width=True, height=320)
                        st.subheader("진입 신호 상세")
                        sig_fmt = {c: "{:.1f}%" for c in signals.columns if c.endswith("수익률")}
                        st.dataframe(signals.style.format(sig_fmt, na_rep="N/A"), use_container_width=True, height=320)
    
if main_tab6:
    past_tab, stat_verify_tab = st.tabs(["과거분석", "통계검증"])

    with past_tab:
        st.header("과거분석: 역사는 반복된다")
        st.caption("과거에 가격이 오르고 내렸던 국면을 찾고, 수요·공급·금융 지표가 몇 년 앞서 움직였는지 가설을 만든 뒤 검증합니다.")
    
        _past_src = yearly_df.copy()
        if selected_sido and "시도" in _past_src.columns:
            _past_src = _past_src[_past_src["시도"].isin(selected_sido)]
    
        if _past_src.empty or "연도" not in _past_src.columns or "평균가격" not in _past_src.columns:
            st.warning("과거분석에 필요한 연간 가격 데이터가 없습니다.")
        else:
            _past_numeric = [
                c for c in _past_src.columns
                if c not in ("연도", "월", "연월", "시도", "지역코드")
                and pd.api.types.is_numeric_dtype(_past_src[c])
                and _past_src[c].notna().any()
            ]
            _driver_defaults = [
                c for c in [
                    "총인구", "30대", "NPS_가입자수", "NPS_고용증감", "가구_소득평균",
                    "인허가_호수", "착공_호수", "준공_호수", "미분양_평균",
                    "전세가율", "PIR", "PIR_NPS", "갭비용", "기준금리", "주담대_증감률",
                ]
                if c in _past_numeric
            ]
    
            _past_c1, _past_c2, _past_c3, _past_c4 = st.columns(4)
            with _past_c1:
                _past_sido_options = sorted(_past_src["시도"].dropna().unique()) if "시도" in _past_src.columns else []
                _past_sido = st.selectbox(
                    "검증 지역",
                    _past_sido_options,
                    index=_past_sido_options.index("서울") if "서울" in _past_sido_options else 0,
                    key="past_sido",
                ) if _past_sido_options else None
            with _past_c2:
                _past_lag = st.select_slider("선행 시차", options=[0, 1, 2, 3], value=1, key="past_lag")
            with _past_c3:
                _past_rise_th = st.number_input("상승 국면 기준(%)", value=5.0, step=0.5, key="past_rise_th")
            with _past_c4:
                _past_fall_th = st.number_input("하락 국면 기준(%)", value=-3.0, step=0.5, key="past_fall_th")
    
            _hypothesis_vars = st.multiselect(
                "검증할 원인 후보",
                options=[c for c in _past_numeric if c != "평균가격"],
                default=_driver_defaults[:8],
                key="past_hypothesis_vars",
                format_func=_fmt_var_by_col,
            )
    
            if _past_sido and _hypothesis_vars:
                _one = _past_src[_past_src["시도"] == _past_sido].sort_values("연도").copy()
                _one["가격변화율"] = _one["평균가격"].pct_change() * 100
                _one["국면"] = np.select(
                    [_one["가격변화율"] >= _past_rise_th, _one["가격변화율"] <= _past_fall_th],
                    ["상승기", "하락기"],
                    default="중립기",
                )
    
                _rank_rows = []
                for _var in _hypothesis_vars:
                    if _var not in _one.columns:
                        continue
                    _test = _one[["연도", "가격변화율", _var]].copy()
                    _test[f"{_var}_선행"] = _test[_var].shift(_past_lag)
                    _test[f"{_var}_변화율_선행"] = _test[_var].pct_change().shift(_past_lag) * 100
                    for _signal_col, _kind in [(f"{_var}_선행", "레벨"), (f"{_var}_변화율_선행", "변화율")]:
                        _valid = _test[["가격변화율", _signal_col]].replace([np.inf, -np.inf], np.nan).dropna()
                        if len(_valid) >= 4 and _valid[_signal_col].nunique() > 1:
                            _corr = _valid["가격변화율"].corr(_valid[_signal_col])
                            _rank_rows.append({
                                "원인후보": _var,
                                "신호": _kind,
                                "시차": _past_lag,
                                "상관계수": _corr,
                                "검증표본": len(_valid),
                                "가설": f"{_past_sido}에서 {_var} {_kind}이(가) {_past_lag}년 선행하면 가격변화율이 {'같은 방향' if _corr >= 0 else '반대 방향'}으로 움직였을 가능성",
                            })
    
                _rank_df = pd.DataFrame(_rank_rows)
                if _rank_df.empty:
                    st.info("선택한 변수와 시차로 검증 가능한 표본이 부족합니다.")
                else:
                    _rank_df["설명력"] = _rank_df["상관계수"].abs()
                    _rank_df = _rank_df.sort_values("설명력", ascending=False).reset_index(drop=True)
                    _best = _rank_df.iloc[0]
    
                    st.subheader("자동 가설")
                    st.info(_best["가설"])
    
                    _sel_c1, _sel_c2 = st.columns(2)
                    with _sel_c1:
                        _selected_driver = st.selectbox(
                            "상세 검증 변수",
                            _rank_df["원인후보"].drop_duplicates().tolist(),
                            key="past_selected_driver",
                            format_func=_fmt_var_by_col,
                        )
                    with _sel_c2:
                        _selected_signal = st.selectbox("신호 형태", ["변화율", "레벨"], key="past_selected_signal")
    
                    _signal_col = f"{_selected_driver}_변화율_선행" if _selected_signal == "변화율" else f"{_selected_driver}_선행"
                    _plot_df = _one[["연도", "평균가격", "가격변화율", "국면", _selected_driver]].copy()
                    _plot_df[f"{_selected_driver}_선행"] = _plot_df[_selected_driver].shift(_past_lag)
                    _plot_df[f"{_selected_driver}_변화율_선행"] = _plot_df[_selected_driver].pct_change().shift(_past_lag) * 100
    
                    _verify = _plot_df[["연도", "가격변화율", "국면", _signal_col]].replace([np.inf, -np.inf], np.nan).dropna()
                    if len(_verify) >= 4:
                        _corr_val = _verify["가격변화율"].corr(_verify[_signal_col])
                        _v1, _v2, _v3 = st.columns(3)
                        _v1.metric("검증 상관계수", f"{_corr_val:.3f}")
                        _v2.metric("표본 수", f"{len(_verify)}년")
                        _v3.metric("방향", "같은 방향" if _corr_val >= 0 else "반대 방향")
    
                        fig_hyp = px.scatter(
                            _verify,
                            x=_signal_col,
                            y="가격변화율",
                            color="국면",
                            text="연도",
                            trendline="ols" if len(_verify) >= 5 else None,
                            title=f"{_past_sido}: {_selected_driver} {_selected_signal} {_past_lag}년 선행 vs 가격변화율",
                            labels={_signal_col: f"{_selected_driver} {_selected_signal}({_past_lag}년 선행)", "가격변화율": "가격변화율(%)"},
                            color_discrete_map={"상승기": "#ef4444", "하락기": "#2563eb", "중립기": "#64748b"},
                        )
                        fig_hyp.update_traces(textposition="top center")
                        fig_hyp.add_hline(y=0, line_dash="dash", line_color="gray")
                        register_fig("과거가설_검증산점도", fig_hyp, "적정값가상계산")
                        st.plotly_chart(fig_hyp, use_container_width=True)
    
                    _episode_cols = ["연도", "국면", "평균가격", "가격변화율", _selected_driver, f"{_selected_driver}_변화율_선행"]
                    st.subheader("반복 국면 테이블")
                    st.dataframe(
                        _plot_df[[c for c in _episode_cols if c in _plot_df.columns]]
                        .sort_values("연도", ascending=False)
                        .style.format({c: "{:,.2f}" for c in _episode_cols if c not in ["연도", "국면"]}, na_rep="N/A"),
                        use_container_width=True,
                        height=320,
                    )
    
                    with st.expander("원인 후보별 검증 순위"):
                        st.dataframe(
                            _rank_df[["원인후보", "신호", "시차", "상관계수", "검증표본", "가설"]]
                            .style.format({"상관계수": "{:.3f}"}),
                            use_container_width=True,
                            height=320,
                        )
            else:
                st.info("검증 지역과 원인 후보를 선택하세요.")
    
    with stat_verify_tab:
        sub_reg, sub_outlier, sub_corr = st.tabs(["회귀분석", "이상치 탐지", "상관관계 분석"])
    
    with sub_reg:
        st.header("다중회귀 분석")
    
        if analysis_df.empty:
            st.warning("데이터가 없습니다.")
        else:
            valid_num = [v for v in available_vars if v in analysis_df.columns and analysis_df[v].notna().any()]
    
            col_y, col_x = st.columns([1, 2])
            with col_y:
                reg_y = st.selectbox("종속변수 (Y)", [v for v in result_vars if v in valid_num], key="reg_y", format_func=_fmt_var_by_col)
            with col_x:
                reg_x_candidates = [v for v in valid_num if v != reg_y]
                reg_x = st.multiselect("독립변수 (X)", reg_x_candidates,
                                        default=reg_x_candidates[:3] if len(reg_x_candidates) >= 3 else reg_x_candidates,
                                        key="reg_x",
                                        format_func=_fmt_var_by_col)
    
            if reg_x and reg_y:
                try:
                    summary_df, importance_df, r2, adj_r2 = multiple_regression(analysis_df, reg_y, reg_x)
    
                    # KPI
                    cols = st.columns(3)
                    cols[0].metric("R²", f"{r2:.4f}")
                    cols[1].metric("Adjusted R²", f"{adj_r2:.4f}")
                    cols[2].metric("독립변수 수", len(reg_x))
    
                    # 계수 테이블
                    st.subheader("회귀 계수")
                    st.dataframe(summary_df.style.format({
                        "계수": "{:.6f}", "표준오차": "{:.6f}", "t값": "{:.4f}", "p값": "{:.4f}"
                    }), use_container_width=True)
    
                    # 변수 중요도 차트
                    st.subheader("변수 중요도 (표준화 계수)")
                    fig_imp = px.bar(importance_df, x="변수", y="중요도", color="중요도",
                                     color_continuous_scale="Blues", title="변수별 영향력")
                    register_fig("회귀_변수중요도", fig_imp, "통계분석")
                    st.plotly_chart(fig_imp, use_container_width=True)
    
                except (ValueError, ImportError) as e:
                    st.error(str(e))
            else:
                st.info("독립변수를 1개 이상 선택하세요.")
    
    
    # ── 이상치 탐지 서브탭 ──────────────────────────────────────────────
    with sub_outlier:
        st.header("이상치 탐지")
    
        if analysis_df.empty:
            st.warning("데이터가 없습니다.")
        else:
            valid_num = [v for v in available_vars if v in analysis_df.columns and analysis_df[v].notna().any()]
    
            col1, col2, col3 = st.columns(3)
            with col1:
                outlier_var = st.selectbox("분석 변수", valid_num, key="outlier_var", format_func=_fmt_var_by_col)
            with col2:
                outlier_method = st.selectbox("탐지 방법", ["zscore", "iqr"], key="outlier_method")
            with col3:
                outlier_threshold = st.slider("임계값 (Z-score)", 1.5, 4.0, 2.5, 0.1, key="outlier_th")
    
            outlier_df = detect_outliers(analysis_df, outlier_var, method=outlier_method, threshold=outlier_threshold)
            outlier_count = outlier_df["이상치"].sum()
    
            st.metric("탐지된 이상치 수", f"{outlier_count}건 / {len(outlier_df)}건")
    
            if outlier_count > 0:
                # 이상치 표시 산점도
                time_col_7 = "연월" if freq == "월별" and "연월" in outlier_df.columns else "연도"
                fig_outlier = px.scatter(
                    outlier_df, x=time_col_7, y=outlier_var, color="이상치",
                    color_discrete_map={True: "red", False: "lightgray"},
                    hover_data=["시도", "z_score"],
                    title=f"{outlier_var} 이상치 분포",
                )
                register_fig("이상치_산점도", fig_outlier, "통계분석")
                st.plotly_chart(fig_outlier, use_container_width=True)
    
                # 이상치 상세 테이블
                st.subheader("이상치 상세")
                outlier_detail = outlier_df[outlier_df["이상치"]].copy()
                display_cols = [c for c in ["시도", time_col_7, outlier_var, "z_score"] if c in outlier_detail.columns]
                st.dataframe(
                    outlier_detail[display_cols].sort_values("z_score", key=abs, ascending=False),
                    use_container_width=True,
                )
            else:
                st.success("이상치가 탐지되지 않았습니다.")
    
    
    # ── 상관관계 분석 서브탭 (Tab 2에서 이동) ───────────────────────────
    with sub_corr:
        st.header(f"상관관계 분석 ({mode_label})")
    
        if analysis_df.empty:
            st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
        else:
            # 실제 데이터에 존재하는 변수만 필터
            valid_vars = [v for v in available_vars if v in analysis_df.columns and analysis_df[v].notna().any()]
    
            # 히트맵
            st.subheader("상관계수 히트맵")
            corr, pval = correlation_matrix(analysis_df, valid_vars)
            if not corr.empty:
                fig_heatmap = px.imshow(
                    corr, text_auto=".2f",
                    color_continuous_scale="RdBu_r", zmin=-1, zmax=1,
                    title="변수 간 피어슨 상관계수",
                    labels={"color": "상관계수"},
                )
                fig_heatmap.update_layout(width=700, height=600)
                register_fig("상관관계_히트맵", fig_heatmap, "통계분석")
                st.plotly_chart(fig_heatmap, use_container_width=True)
    
                with st.expander("통계 신뢰도 상세"):
                    confidence = 1 - pval
                    st.dataframe(confidence.style.format("{:.1%}"))
            else:
                st.info("상관계수를 계산할 데이터가 부족합니다.")
    
            # 산점도
            st.subheader("산점도 분석")
            col_a, col_b = st.columns(2)
            with col_a:
                default_x = valid_vars.index("GRDP") if "GRDP" in valid_vars else 0
                x_var = st.selectbox("X축 변수", valid_vars, index=default_x, key="corr_x_var", format_func=_fmt_var_by_col)
            with col_b:
                y_var = st.selectbox("Y축 변수", valid_vars, index=0, key="corr_y_var", format_func=_fmt_var_by_col)
    
            scatter_df, slope, intercept, r_sq = scatter_analysis(analysis_df, x_var, y_var)
            if not scatter_df.empty:
                fig_scatter = px.scatter(
                    scatter_df, x=x_var, y=y_var, color="시도",
                    title=f"{x_var} vs {y_var}",
                    trendline="ols" if len(scatter_df) >= 3 else None,
                )
                if r_sq is not None:
                    fig_scatter.add_annotation(
                        text=f"R² = {r_sq:.4f}",
                        xref="paper", yref="paper",
                        x=0.02, y=0.98, showarrow=False, font=dict(size=14),
                    )
                register_fig("상관_산점도", fig_scatter, "통계분석")
                st.plotly_chart(fig_scatter, use_container_width=True)
    
            # 시도별 상관계수
            st.subheader("시도별 상관계수")
            region_corr = correlation_by_region(yearly_df[yearly_df["연도"].between(*selected_years)], x_var, y_var)
            if not region_corr.empty:
                fig_bar = px.bar(
                    region_corr, x="시도", y="상관계수",
                    color="상관계수", color_continuous_scale="RdBu_r", range_color=[-1, 1],
                    title=f"시도별 {x_var}-{y_var} 상관계수",
                )
                register_fig("시도별_상관계수", fig_bar, "통계분석")
                st.plotly_chart(fig_bar, use_container_width=True)
                st.dataframe(region_corr)
    
            # 기간별 상관계수 추이
            st.subheader("연도별 상관계수 추이")
            period_corr = correlation_by_period(yearly_df, x_var, y_var)
            if not period_corr.empty:
                fig_period = px.line(
                    period_corr, x="연도", y="상관계수",
                    title=f"연도별 {x_var}-{y_var} 상관계수 추이", markers=True,
                )
                fig_period.add_hline(y=0, line_dash="dash", line_color="gray")
                register_fig("연도별_상관계수", fig_period, "통계분석")
                st.plotly_chart(fig_period, use_container_width=True)
    
    
    # ============================
    # Tab 7: 고급분석 (클러스터링 + Granger)
    # ============================
if main_tab7:
    st.caption("**클러스터링**: 비슷한 특성의 지역을 자동으로 묶어 그룹별 특징을 파악합니다 | **선행 신호**: 특정 지표가 가격 변화를 몇 달 먼저 움직였는지 확인합니다")
    sub_cluster, sub_granger = st.tabs(["클러스터링", "선행 신호"])

    with sub_cluster:
        st.header("지역 클러스터링")
    
        if analysis_df.empty:
            st.warning("데이터가 없습니다.")
        else:
            valid_num = [v for v in available_vars if v in analysis_df.columns and analysis_df[v].notna().any()]
    
            cluster_features = st.multiselect(
                "클러스터링 변수", valid_num,
                default=[v for v in [price_col, vol_col, "GRDP"] if v in valid_num][:3],
                key="cluster_features",
                format_func=_fmt_var_by_col,
            )
            n_clusters = st.slider("클러스터 수", 2, 8, 4, key="n_clusters")
    
            if len(cluster_features) >= 2:
                try:
                    clustered_df, centers_df = _cached_cluster_regions(analysis_df, tuple(cluster_features), n_clusters)
    
                    # 클러스터 결과 테이블
                    st.subheader("클러스터별 시도 배정")
                    st.dataframe(clustered_df, use_container_width=True)
    
                    # 클러스터 시각화 (첫 2개 변수로 산점도)
                    fig_cluster = px.scatter(
                        clustered_df, x=cluster_features[0], y=cluster_features[1],
                        color=clustered_df["cluster"].astype(str),
                        text="시도", title="지역 클러스터링 결과",
                        labels={"color": "클러스터"},
                    )
                    fig_cluster.update_traces(textposition="top center")
                    register_fig("클러스터링", fig_cluster, "고급분석")
                    st.plotly_chart(fig_cluster, use_container_width=True)
    
                    # 클러스터 중심값
                    st.subheader("클러스터 중심값")
                    st.dataframe(centers_df.style.format(
                        {c: "{:,.2f}" for c in cluster_features}
                    ), use_container_width=True)
    
                    # 레이더 차트
                    if len(cluster_features) >= 3:
                        st.subheader("클러스터 프로파일 (레이더 차트)")
                        # 정규화
                        radar_df = centers_df.copy()
                        for c in cluster_features:
                            vmin, vmax = radar_df[c].min(), radar_df[c].max()
                            radar_df[c] = (radar_df[c] - vmin) / (vmax - vmin) if vmax > vmin else 0
    
                        fig_radar = go.Figure()
                        for _, row in radar_df.iterrows():
                            fig_radar.add_trace(go.Scatterpolar(
                                r=[row[c] for c in cluster_features] + [row[cluster_features[0]]],
                                theta=cluster_features + [cluster_features[0]],
                                name=f"클러스터 {int(row['cluster'])}",
                            ))
                        fig_radar.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
                                                title="클러스터 프로파일")
                        register_fig("레이더차트", fig_radar, "고급분석")
                        st.plotly_chart(fig_radar, use_container_width=True)
    
                except (ValueError, ImportError) as e:
                    st.error(str(e))
            else:
                st.info("변수를 2개 이상 선택하세요.")
    
    
    # ============================
    # Tab 6-2: Granger 인과성 (고급분석 서브탭)
    # ============================
    with sub_granger:
        st.header("선행 신호 반복성")
        st.caption("한 지표가 먼저 움직인 뒤 다른 지표가 따라온 패턴이 반복됐는지 확인합니다.")
    
        # Granger는 월별 시계열 데이터 필요
        granger_src = filtered_monthly if not filtered_monthly.empty else monthly_df
    
        if granger_src.empty:
            st.warning("월별 데이터가 필요합니다.")
        else:
            valid_num_g = [v for v in available_vars if v in granger_src.columns and granger_src[v].notna().any()]
    
            col1, col2, col3 = st.columns(3)
            with col1:
                g_y = st.selectbox("나중에 움직였는지 볼 지표", [v for v in result_vars if v in valid_num_g], key="g_y", format_func=_fmt_var_by_col)
            with col2:
                g_x_candidates = [v for v in valid_num_g if v != g_y]
                g_x = st.selectbox("먼저 움직였는지 볼 지표", g_x_candidates, key="g_x", format_func=_fmt_var_by_col)
            with col3:
                g_max_lag = st.slider("최대 몇 개월 먼저 움직였는지", 1, 12, 4, key="g_lag")
    
            if g_y and g_x:
                try:
                    granger_df = _cached_granger(granger_src, g_y, g_x, g_max_lag)
    
                    if granger_df.empty:
                        st.warning("검정할 수 있는 데이터가 부족합니다.")
                    else:
                        # 요약: 선행 패턴이 반복된 시도 수
                        causal_sido = granger_df[granger_df["인과성"]]["시도"].nunique()
                        total_sido = granger_df["시도"].nunique()
                        st.metric("선행 패턴 발견 지역", f"{causal_sido} / {total_sido}")
    
                        # 히트맵: 시도 × 기간별 통계 신뢰도
                        pivot_g = granger_df.pivot_table(index="시도", columns="lag", values="p값", aggfunc="first")
                        pivot_conf = 1 - pivot_g
                        fig_g = px.imshow(
                            pivot_conf, text_auto=".1%",
                            color_continuous_scale="RdYlGn", zmin=0.9, zmax=1.0,
                            title=f"선행 신호 반복성 ({g_x} 먼저 → {g_y} 나중)",
                            labels={"color": "통계 신뢰도", "lag": "먼저 움직인 개월"},
                        )
                        register_fig("선행신호_반복성_히트맵", fig_g, "자유차트")
                        st.plotly_chart(fig_g, use_container_width=True)
    
                        # 상세 테이블
                        with st.expander("상세 결과"):
                            detail_g = granger_df.rename(columns={
                                "lag": "먼저움직인개월",
                                "p값": "낮을수록_우연가능성",
                                "인과성": "반복패턴있음",
                            })
                            detail_g["통계신뢰도"] = 1 - detail_g["낮을수록_우연가능성"]
                            st.dataframe(detail_g.style.format({
                                "F통계량": "{:.4f}", "낮을수록_우연가능성": "{:.4f}", "통계신뢰도": "{:.1%}"
                            }), use_container_width=True)
    
                except (ValueError, ImportError) as e:
                    st.error(str(e))
    
    
    # ============================
    # Tab 8: 가격 예측 (Prophet)
    # ============================
if main_tab8:
    st.header("AI 가격 예측 (Prophet)")
    st.caption("시계열 분해 + 계절성 모델로 시도별 아파트 가격을 예측합니다.")

    if not HAS_PROPHET:
        st.warning("Prophet이 설치되지 않았습니다. `pip install prophet`으로 설치하세요.")
        st.info("requirements.txt에 prophet이 추가되어 있으니, 가상환경에서 `pip install -r requirements.txt`를 실행하세요.")
        st.stop()

    # ── 파라미터 입력 ───────────────────────────────────────────────────────
    _fc_c1, _fc_c2, _fc_c3 = st.columns(3)
    with _fc_c1:
        _fc_sido_options = sorted(apt_df["시도"].dropna().unique()) if "시도" in apt_df.columns else []
        _fc_sido = st.selectbox(
            "예측 시도",
            _fc_sido_options,
            index=_fc_sido_options.index("서울") if "서울" in _fc_sido_options else 0,
            key="fc_sido",
        )
    with _fc_c2:
        _fc_periods = st.select_slider(
            "예측 기간 (개월)",
            options=[6, 12, 18, 24],
            value=12,
            key="fc_periods",
        )
    with _fc_c3:
        _fc_mode = st.radio(
            "분석 모드",
            ["매매", "전세"],
            horizontal=True,
            key="fc_mode",
        )

    st.divider()

    # ── 데이터 소스 선택 ────────────────────────────────────────────────────
    if _fc_mode == "매매":
        _fc_src_df = apt_df.copy()
        _fc_price_col = "평균가격"
        _fc_unit = "만원"
    else:
        _fc_src_df = jeonse_df.copy() if not jeonse_df.empty else apt_df.copy()
        _fc_price_col = "보증금평균" if "보증금평균" in (_fc_src_df.columns if not jeonse_df.empty else []) else "평균가격"
        _fc_unit = "만원 (보증금)"

    # ── 예측 실행 ────────────────────────────────────────────────────────────
    # Prophet 적합은 가장 무거운 연산이므로 버튼으로 수동 실행한다.
    # (st.tabs는 모든 탭 본문을 매 실행마다 돌리므로, 자동 실행하면 다른 탭을 봐도 매번 재적합된다.)
    if st.button("📈 예측 실행", key="fc_run", type="primary"):
        st.session_state["fc_done"] = True

    if st.session_state.get("fc_done"):
        with st.spinner(f"{_fc_sido} {_fc_mode} 가격 예측 중..."):
            _fc_result = _cached_forecast_price(_fc_src_df, _fc_sido, _fc_periods, _fc_price_col)
    else:
        st.info("'📈 예측 실행' 버튼을 누르면 Prophet 가격 예측을 수행합니다. (무거운 연산이라 자동 실행하지 않습니다.)")
        _fc_result = {"error": "_NOT_RUN_"}

    if "error" in _fc_result:
        if _fc_result["error"] != "_NOT_RUN_":
            st.error(_fc_result["error"])
    else:
        _fc_actual = _fc_result["actual"]
        _fc_forecast = _fc_result["forecast"]
        _fc_forecast_future = _fc_result.get("forecast_future", pd.DataFrame())
        _fc_metrics = _fc_result.get("metrics", {})
        _fc_components = _fc_result.get("components", {})
        _fc_holdout_actual = _fc_result.get("holdout_actual", pd.DataFrame())
        _fc_holdout_pred = _fc_result.get("holdout_pred", pd.DataFrame())

        # ── 결과 1: 예측 정확도 지표 ───────────────────────────────────────
        if _fc_metrics:
            _m1, _m2, _m3 = st.columns(3)
            with _m1:
                st.metric("MAE (평균절대오차)", f"{_fc_metrics.get('mae', 'N/A'):,.0f} 만원")
            with _m2:
                st.metric("MAPE (평균절대백분율오차)", f"{_fc_metrics.get('mape', 'N/A'):.1f} %")
            with _m3:
                st.metric("RMSE (평균제곱근오차)", f"{_fc_metrics.get('rmse', 'N/A'):,.0f} 만원")
            st.caption("※ holdout 검증: 가장 최근 데이터로 예측 정확도를 측정합니다.")

        st.divider()

        # ── 결과 2: 예측 차트 ───────────────────────────────────────────────
        st.subheader(f"{_fc_sido} {_fc_mode} 가격 추이 및 예측")

        _fc_fig = go.Figure()

        # 실제값 (전체)
        _fc_fig.add_trace(go.Scatter(
            x=_fc_actual["ds"],
            y=_fc_actual["y"],
            mode="lines",
            name="실제값",
            line=dict(color="#2196F3", width=2),
        ))

        # 모델 피팅값 (actual 구간)
        _fc_fitted = _fc_forecast[_fc_forecast["ds"].isin(_fc_actual["ds"])]
        _fc_fig.add_trace(go.Scatter(
            x=_fc_fitted["ds"],
            y=_fc_fitted["yhat"],
            mode="lines",
            name="모델 피팅",
            line=dict(color="#FF9800", width=1.5, dash="dot"),
            opacity=0.7,
        ))

        # 미래 예측값 + 신뢰구간
        if not _fc_forecast_future.empty:
            # 신뢰구간 음영 (upper → lower fill)
            _fc_fig.add_trace(go.Scatter(
                x=pd.concat([_fc_forecast_future["ds"], _fc_forecast_future["ds"].iloc[::-1]]),
                y=pd.concat([_fc_forecast_future["yhat_upper"], _fc_forecast_future["yhat_lower"].iloc[::-1]]),
                fill="toself",
                fillcolor="rgba(244, 67, 54, 0.15)",
                line=dict(color="rgba(255,255,255,0)"),
                hoverinfo="skip",
                showlegend=True,
                name="예측 신뢰구간",
            ))
            _fc_fig.add_trace(go.Scatter(
                x=_fc_forecast_future["ds"],
                y=_fc_forecast_future["yhat"],
                mode="lines+markers",
                name="예측값",
                line=dict(color="#F44336", width=2, dash="dash"),
                marker=dict(size=5),
            ))

        # 예측 시작점 수직선 — add_vline annotation이 Python 3.14+에서 sum(str) TypeError 유발
        # add_shape + add_annotation으로 분리하여 우회
        _last_date = _fc_actual["ds"].max()
        _vline_x = _last_date.strftime("%Y-%m-%d") if hasattr(_last_date, "strftime") else str(_last_date)
        _fc_fig.add_shape(
            type="line",
            x0=_vline_x, x1=_vline_x,
            y0=0, y1=1, yref="paper",
            line=dict(dash="dash", color="gray", width=1),
        )
        _fc_fig.add_annotation(
            x=_vline_x, y=1.02, yref="paper",
            text="예측 시작", showarrow=False,
            xanchor="left", font=dict(size=11, color="gray"),
        )

        _fc_fig.update_layout(
            xaxis_title="날짜",
            yaxis_title=f"가격 ({_fc_unit})",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            hovermode="x unified",
            height=450,
        )
        _fc_fig.update_yaxes(tickformat=",")
        st.plotly_chart(_fc_fig, use_container_width=True)

        # ── 결과 3: 미래 예측 테이블 ────────────────────────────────────────
        if not _fc_forecast_future.empty:
            with st.expander("미래 예측 수치 보기"):
                _fc_tbl = _fc_forecast_future.copy()
                _fc_tbl["ds"] = _fc_tbl["ds"].dt.strftime("%Y-%m")
                _fc_tbl.columns = ["연월", "예측가격(만원)", "하한(만원)", "상한(만원)"]
                _fc_tbl = _fc_tbl.set_index("연월")
                st.dataframe(
                    _fc_tbl.style.format("{:,.0f}"),
                    use_container_width=True,
                    height=300,
                )

        st.divider()

        # ── 결과 4: 트렌드 / 계절성 분해 ────────────────────────────────────
        st.subheader("트렌드 및 계절성 분해")
        _comp_c1, _comp_c2 = st.columns(2)

        with _comp_c1:
            if "trend" in _fc_components:
                _trend_df = _fc_components["trend"]
                _trend_fig = go.Figure()
                _trend_fig.add_trace(go.Scatter(
                    x=_trend_df["ds"],
                    y=_trend_df["trend"],
                    mode="lines",
                    line=dict(color="#4CAF50", width=2),
                    name="장기 추세",
                ))
                _trend_fig.update_layout(
                    title="장기 추세 (Trend)",
                    xaxis_title="날짜",
                    yaxis_title=f"가격 ({_fc_unit})",
                    height=300,
                )
                _trend_fig.update_yaxes(tickformat=",")
                st.plotly_chart(_trend_fig, use_container_width=True)
            else:
                st.info("추세 데이터를 추출할 수 없습니다.")

        with _comp_c2:
            if "yearly" in _fc_components:
                _season_df = _fc_components["yearly"].copy()
                # 월별 평균 계절성 집계
                _season_df["월"] = _season_df["ds"].dt.month
                _season_monthly = _season_df.groupby("월")["yearly"].mean().reset_index()
                _month_names = ["1월", "2월", "3월", "4월", "5월", "6월",
                                "7월", "8월", "9월", "10월", "11월", "12월"]
                _season_monthly["월_이름"] = _season_monthly["월"].apply(lambda x: _month_names[x - 1])

                _season_fig = go.Figure()
                _season_fig.add_trace(go.Bar(
                    x=_season_monthly["월_이름"],
                    y=_season_monthly["yearly"],
                    marker_color=["#F44336" if v >= 0 else "#2196F3" for v in _season_monthly["yearly"]],
                    name="월별 계절 효과",
                ))
                _season_fig.update_layout(
                    title="연간 계절성 패턴 (월별 가격 편차)",
                    xaxis_title="월",
                    yaxis_title=f"계절 효과 ({_fc_unit})",
                    height=300,
                )
                _season_fig.update_yaxes(tickformat=",")
                st.plotly_chart(_season_fig, use_container_width=True)
                st.caption("※ 양수(빨강)=해당 월에 가격이 평균보다 높은 경향, 음수(파랑)=낮은 경향")
            else:
                st.info("계절성 데이터를 추출할 수 없습니다.")

        # ── holdout 검증 상세 ────────────────────────────────────────────────
        if not _fc_holdout_actual.empty and not _fc_holdout_pred.empty:
            with st.expander("검증 기간 실제 vs 예측 비교"):
                _hv_merged = _fc_holdout_actual.merge(_fc_holdout_pred, on="ds", how="inner")
                _hv_merged["오차(만원)"] = (_hv_merged["yhat"] - _hv_merged["y"]).round(0)
                _hv_merged["오차율(%)"] = ((_hv_merged["yhat"] - _hv_merged["y"]) / _hv_merged["y"] * 100).round(2)
                _hv_merged["ds"] = _hv_merged["ds"].dt.strftime("%Y-%m")
                _hv_merged = _hv_merged.rename(columns={"ds": "연월", "y": "실제(만원)", "yhat": "예측(만원)"})
                st.dataframe(
                    _hv_merged[["연월", "실제(만원)", "예측(만원)", "오차(만원)", "오차율(%)"]].style.format({
                        "실제(만원)": "{:,.0f}", "예측(만원)": "{:,.0f}", "오차(만원)": "{:,.0f}", "오차율(%)": "{:.2f}"
                    }),
                    use_container_width=True,
                    height=280,
                )


# ============================
# Tab 9: 투자 계산기
# ============================
if main_tab9:
    st.header("투자 수익률 & 세금 계산기")
    st.caption("취득세·양도세·이자비용을 자동 계산하고, 레버리지 포함 투자 수익률을 산출합니다. (2026년 세율 기준)")

    # ── 입력 영역 ────────────────────────────────────────────────────────────
    _tc_left, _tc_right = st.columns([1, 1])

    with _tc_left:
        st.subheader("매수/매도 정보")
        _tc_buy = st.number_input("매수가 (만원)", min_value=0, value=50000, step=1000, key="tc_buy",
                                   help="아파트 매수 금액 (만원)")
        _tc_sell = st.number_input("매도가 (만원)", min_value=0, value=60000, step=1000, key="tc_sell",
                                    help="예상 매도 금액 (만원)")
        _tc_area = st.number_input("전용면적 (m²)", min_value=1.0, value=84.0, step=1.0, key="tc_area",
                                    help="전용면적 (농어촌특별세 면제 기준: 85m² 이하)")
        _tc_holding = st.slider("보유 기간 (년)", min_value=1, max_value=30, value=5, key="tc_holding")
        _tc_residence = st.slider(
            "거주 기간 (년)",
            min_value=0,
            max_value=_tc_holding,
            value=min(2, _tc_holding),
            key="tc_residence",
            help="1세대1주택 비과세: 보유 2년 + 거주 2년 이상 필요 (조정지역)",
        )

        st.subheader("주택 현황")
        _tc_num_homes_str = st.selectbox(
            "매수 후 보유 주택 수",
            ["1주택", "2주택", "3주택 이상"],
            key="tc_num_homes",
        )
        _tc_num_homes = {"1주택": 1, "2주택": 2, "3주택 이상": 3}[_tc_num_homes_str]
        _tc_is_one_home = _tc_num_homes == 1
        _tc_is_first = st.checkbox("생애최초 주택 구입 (취득세 감면)", value=False, key="tc_is_first")
        _tc_is_adjusted = st.checkbox("조정대상지역 여부", value=True, key="tc_is_adjusted",
                                       help="조정대상지역은 2주택 이상 취득세·양도세 중과 적용")

    with _tc_right:
        st.subheader("대출 및 수익 정보")
        _tc_loan = st.number_input("대출금액 (만원)", min_value=0, value=20000, step=1000, key="tc_loan")
        _tc_rate = st.number_input("대출금리 (%)", min_value=0.1, max_value=30.0, value=3.5, step=0.1, key="tc_rate")
        _tc_monthly_income = st.number_input(
            "월 임대소득 (만원, 없으면 0)",
            min_value=0,
            value=0,
            step=10,
            key="tc_monthly_income",
            help="임대로 운용 시 월 임대료 수입",
        )
        _tc_annual_exp = st.number_input(
            "연간 유지비 (만원)",
            min_value=0,
            value=100,
            step=10,
            key="tc_annual_exp",
            help="연간 재산세 + 관리비 + 수선비 등",
        )
        _tc_deductible = st.number_input(
            "필요경비 (만원)",
            min_value=0,
            value=500,
            step=100,
            key="tc_deductible",
            help="양도세 필요경비: 취득 시 중개수수료 + 인테리어 + 등기비용 등 (취득세 제외)",
        )

    st.divider()

    # ── 세금 계산 실행 ──────────────────────────────────────────────────────
    _tc_acq = calc_acquisition_tax(
        price_만원=_tc_buy,
        num_homes=_tc_num_homes,
        area_m2=_tc_area,
        is_first_home=_tc_is_first,
        is_adjusted_zone=_tc_is_adjusted,
    )
    _tc_cgt = calc_capital_gains_tax(
        buy_price_만원=_tc_buy,
        sell_price_만원=_tc_sell,
        holding_years=_tc_holding,
        residence_years=_tc_residence,
        is_one_home=_tc_is_one_home,
        num_homes=_tc_num_homes,
        deductible_costs_만원=_tc_deductible,
    )
    _tc_roi = calc_investment_return(
        buy_price_만원=_tc_buy,
        sell_price_만원=_tc_sell,
        holding_years=_tc_holding,
        loan_amount_만원=_tc_loan,
        loan_rate_pct=_tc_rate,
        monthly_income_만원=_tc_monthly_income,
        annual_expenses_만원=_tc_annual_exp,
        acquisition_tax_만원=_tc_acq["합계(만원)"],
        capital_gains_tax_만원=_tc_cgt["합계(만원)"],
    )

    # ── 결과 카드 ────────────────────────────────────────────────────────────
    st.subheader("계산 결과")

    _res_c1, _res_c2, _res_c3, _res_c4 = st.columns(4)
    with _res_c1:
        st.metric(
            "취득세 합계",
            f"{_tc_acq['합계(만원)']:,.0f} 만원",
            help=_tc_acq["취득세율_설명"],
        )
    with _res_c2:
        if _tc_cgt["비과세여부"]:
            st.metric("양도소득세", "비과세", help=_tc_cgt["비과세사유"])
        else:
            st.metric(
                "양도소득세 합계",
                f"{_tc_cgt['합계(만원)']:,.0f} 만원",
                help=f"세율 {_tc_cgt['세율(%)']:.0f}%, 장특공 {_tc_cgt['장기보유특별공제율(%)']:.0f}%",
            )
    with _res_c3:
        _roi_delta_color = "normal" if _tc_roi["ROE(%)"] >= 0 else "inverse"
        st.metric(
            "자기자본 수익률 (ROE)",
            f"{_tc_roi['ROE(%)']:+.1f} %",
        )
    with _res_c4:
        st.metric(
            "연환산 수익률 (CAGR)",
            f"{_tc_roi['연환산ROE_CAGR(%)']:+.1f} % / 년",
        )

    # ── 순수익 하이라이트 ────────────────────────────────────────────────────
    _net = _tc_roi["순수익(만원)"]
    _net_color = "#4CAF50" if _net >= 0 else "#F44336"
    st.markdown(
        f"""
        <div style="background:{_net_color}22; border-left: 4px solid {_net_color}; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
        <span style="font-size:1.1em; font-weight:600;">순수익:</span>
        <span style="font-size:1.4em; font-weight:700; color:{_net_color}; margin-left:12px;">
            {_net:+,.0f} 만원
        </span>
        <span style="margin-left:24px; color:#666;">
            손익분기 매도가: {_tc_roi['손익분기매도가(만원)']:,.0f} 만원
        </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.divider()

    # ── 비용 breakdown 차트 ─────────────────────────────────────────────────
    st.subheader("비용 구성")
    _breakdown_c1, _breakdown_c2 = st.columns(2)

    with _breakdown_c1:
        # 파이 차트: 총비용 항목별
        _cost_labels = ["취득세", "이자비용", "유지비", "양도세"]
        _cost_values = [
            _tc_acq["합계(만원)"],
            _tc_roi["이자비용(만원)"],
            float(_tc_annual_exp) * _tc_holding,
            _tc_cgt["합계(만원)"],
        ]
        _cost_values_pos = [max(v, 0) for v in _cost_values]
        if sum(_cost_values_pos) > 0:
            _cost_fig = go.Figure(go.Pie(
                labels=_cost_labels,
                values=_cost_values_pos,
                hole=0.4,
                textinfo="label+percent",
                marker=dict(colors=["#FF9800", "#F44336", "#9C27B0", "#2196F3"]),
            ))
            _cost_fig.update_layout(
                title="총비용 구성",
                height=320,
                margin=dict(t=40, b=10, l=10, r=10),
            )
            st.plotly_chart(_cost_fig, use_container_width=True)

    with _breakdown_c2:
        # 막대 차트: 매수가 vs 비용 vs 매도가
        _summary_labels = ["매수가", "취득세", "이자비용", "유지비", "양도세", "임대수입", "매도가"]
        _summary_values = [
            _tc_buy,
            _tc_acq["합계(만원)"],
            _tc_roi["이자비용(만원)"],
            float(_tc_annual_exp) * _tc_holding,
            _tc_cgt["합계(만원)"],
            -_tc_roi["임대수입(만원)"],  # 수입은 음수(비용 절감 효과)
            _tc_sell,
        ]
        _bar_colors = ["#2196F3", "#FF9800", "#F44336", "#9C27B0", "#FF5722", "#4CAF50", "#3F51B5"]
        _summary_fig = go.Figure(go.Bar(
            x=_summary_labels,
            y=_summary_values,
            marker_color=_bar_colors,
            text=[f"{v:,.0f}" for v in _summary_values],
            textposition="outside",
        ))
        _summary_fig.update_layout(
            title="매수가·비용·임대수입·매도가 비교",
            yaxis_title="금액 (만원)",
            height=320,
            yaxis=dict(tickformat=","),
            margin=dict(t=40, b=10),
        )
        st.plotly_chart(_summary_fig, use_container_width=True)

    # ── 상세 내역 ────────────────────────────────────────────────────────────
    with st.expander("취득세 상세 내역"):
        _acq_detail = {
            "항목": ["취득세율", "취득세", "지방교육세", "농어촌특별세", "합계", "실효세율", "세율 설명"],
            "내용": [
                f"{_tc_acq['취득세율(%)']:.4f} %",
                f"{_tc_acq['취득세(만원)']:,.1f} 만원",
                f"{_tc_acq['지방교육세(만원)']:,.1f} 만원",
                f"{_tc_acq['농어촌특별세(만원)']:,.1f} 만원",
                f"{_tc_acq['합계(만원)']:,.1f} 만원",
                f"{_tc_acq['실효세율(%)']:.4f} %",
                _tc_acq["취득세율_설명"],
            ],
        }
        st.dataframe(pd.DataFrame(_acq_detail), use_container_width=True, hide_index=True)

    with st.expander("양도소득세 상세 내역"):
        if _tc_cgt["비과세여부"]:
            st.success(f"비과세: {_tc_cgt['비과세사유']}")
        else:
            _cgt_detail_dict = _tc_cgt["상세내역"]
            _cgt_display = {
                "항목": list(_cgt_detail_dict.keys()),
                "내용": [str(v) for v in _cgt_detail_dict.values()],
            }
            st.dataframe(pd.DataFrame(_cgt_display), use_container_width=True, hide_index=True)

    with st.expander("투자 수익률 상세 내역"):
        _roi_detail = _tc_roi["상세내역"]
        _roi_display = {
            "항목": list(_roi_detail.keys()),
            "내용": [f"{v:,.0f}" if isinstance(v, (int, float)) else str(v) for v in _roi_detail.values()],
        }
        st.dataframe(pd.DataFrame(_roi_display), use_container_width=True, hide_index=True)


# ============================
# Tab 10: 소득-매물 매칭
# ============================
if listing_tab:
    st.header("매물현황")
    render_tab_usage_guide("매물현황")
    st.caption("구매력, 급지, 실거래, 네이버 호가를 함께 보며 반복 국면에서 살 만한 지역과 매물을 찾습니다.")

    st.subheader("지역검색기")
    st.caption("전국 지역을 조건식으로 훑어 투자 후보를 먼저 좁힙니다. 예: PIR < 15 AND 전세가율 > 60")

    screen_src = analysis_df.copy()
    screen_time_col = "연월" if freq == "월별" and "연월" in screen_src.columns else "연도"
    if screen_src.empty:
        st.info("지역검색기에 사용할 데이터가 없습니다.")
    else:
        screen_lead = compute_lead_lag_signal(
            screen_src,
            sale_col="평균가격",
            jeonse_col="전세_보증금평균",
            time_col=screen_time_col,
            max_lag=6,
        ) if "평균가격" in screen_src.columns and "전세_보증금평균" in screen_src.columns else pd.DataFrame()
        screen_df = prepare_screener_dataset(screen_src, lead_lag_df=screen_lead, time_col=screen_time_col)
        if screen_df.empty:
            st.info("지역검색기용 최신 데이터가 없습니다.")
        else:
            screen_cols = _numeric_rule_columns(screen_df) + [c for c in ["선행방향", "반복성"] if c in screen_df.columns]
            screen_defaults = [
                {"column": "PIR", "op": "<", "value": "15"} if "PIR" in screen_cols else None,
                {"column": "전세가율", "op": ">", "value": "60"} if "전세가율" in screen_cols else None,
                {"column": "선행방향", "op": "==", "value": "전세 선행"} if "선행방향" in screen_cols else None,
            ]
            screen_defaults = [r for r in screen_defaults if r]
            with st.expander("검색 조건", expanded=True):
                screen_rules, screen_combine = _render_condition_builder(
                    "screen",
                    screen_cols,
                    default_rules=screen_defaults,
                    max_rules=6,
                )
                screen_condition_name = st.text_input("검색 조건 저장 이름", value="지역검색 조건", key="screen_condition_name")
                if st.button("검색 조건 저장", key="screen_save_condition"):
                    if screen_rules:
                        save_condition_set(screen_condition_name.strip() or "지역검색 조건", screen_rules, screen_combine)
                        st.success("검색 조건을 저장했습니다.")
                    else:
                        st.warning("저장할 조건이 없습니다.")

            if screen_rules:
                screen_mask = evaluate_condition_rules(screen_df, screen_rules, combine=screen_combine)
                screen_result = screen_df[screen_mask].copy()
            else:
                screen_result = screen_df.copy()

            screen_result["충족조건수"] = 0
            for rule in screen_rules:
                screen_result["충족조건수"] += evaluate_condition_rules(screen_result, [rule], combine="AND").astype(int)
            if "선행방향" in screen_result.columns:
                screen_result["선행신호요약"] = screen_result.apply(_format_signal_summary, axis=1)

            sr1, sr2, sr3 = st.columns(3)
            sr1.metric("검색 대상", f"{len(screen_df):,}개 지역")
            sr2.metric("조건 충족", f"{len(screen_result):,}개 지역")
            sr3.metric("조건 조합", screen_combine)

            show_cols = [
                "시도", screen_time_col, "평균가격", "가격_YoY", "거래량", "거래량_YoY",
                "전세가율", "PIR", "갭비용", "미분양소화기간", "선행신호요약", "충족조건수",
            ]
            show_cols = [c for c in show_cols if c in screen_result.columns]
            if screen_result.empty:
                st.info("현재 조건을 만족하는 지역이 없습니다. OR 조건을 쓰거나 기준을 완화해 보세요.")
            else:
                sort_col = "충족조건수" if "충족조건수" in screen_result.columns else show_cols[0]
                screen_result = screen_result.sort_values(sort_col, ascending=False)
                fmt = {
                    c: "{:,.1f}" for c in show_cols
                    if c not in ["시도", screen_time_col, "선행신호요약", "충족조건수"]
                }
                st.dataframe(
                    screen_result[show_cols].style.format(fmt, na_rep="N/A"),
                    use_container_width=True,
                    height=340,
                )
                screen_csv = screen_result[show_cols].to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    "지역검색 결과 CSV 다운로드",
                    data=screen_csv,
                    file_name="region_screener_result.csv",
                    mime="text/csv",
                    key="screen_download",
                )
                watch_col1, watch_col2, watch_col3 = st.columns([2, 1, 1])
                with watch_col1:
                    watch_region = st.selectbox(
                        "관심지역 등록",
                        screen_result["시도"].dropna().astype(str).tolist(),
                        key="screen_watch_region",
                    )
                with watch_col2:
                    watch_alert = st.checkbox("알림 대상", value=False, key="screen_watch_alert")
                with watch_col3:
                    st.write("")
                    st.write("")
                    if st.button("등록", key="screen_watch_add"):
                        upsert_watchlist(watch_region, screen_rules, alert_on=watch_alert)
                        st.success(f"{watch_region}을 관심지역에 등록했습니다.")
                with st.expander("관심지역 목록"):
                    watchlists = list_watchlists()
                    if watchlists:
                        st.dataframe(
                            pd.DataFrame(watchlists)[["region", "alert_on", "updated_at", "created_at"]],
                            use_container_width=True,
                            hide_index=True,
                        )
                    else:
                        st.info("관심지역이 없습니다.")

    st.divider()

    _listing_df = _clean_listing_df(st.session_state.get("naver_listings_df", pd.DataFrame()))
    st.subheader("매물현황")
    st.caption("지역과 조건을 선택하면 현재 매물 수, 호가 분포, 변화 확인 대상 단지를 한 화면에서 확인합니다.")

    if not _listing_df.empty:
        _listing_valid_price = _listing_df["매물가격"].notna() if "매물가격" in _listing_df.columns else pd.Series(False, index=_listing_df.index)
        if not _listing_valid_price.any():
            st.warning("매물가격을 해석할 수 없습니다. 업로드 파일의 가격 컬럼 형식을 확인해 주세요.")
            with st.expander("업로드 원본 매물 목록", expanded=True):
                st.dataframe(_listing_df, use_container_width=True, height=320, hide_index=True)
        else:
            _listing_price_min = int(np.nanmin(_listing_df.loc[_listing_valid_price, "매물가격"]))
            _listing_price_max = int(np.nanmax(_listing_df.loc[_listing_valid_price, "매물가격"]))
            if _listing_price_min == _listing_price_max:
                _listing_price_range = (_listing_price_min, _listing_price_max)
            else:
                _listing_price_range = (_listing_price_min, _listing_price_max)

            with st.expander("조회 조건", expanded=True):
                _lf1, _lf2, _lf3, _lf4 = st.columns([1, 1, 1, 1.3])
                with _lf1:
                    _listing_sido_options = sorted([v for v in _listing_df.get("시도", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if v])
                    _listing_sido = st.multiselect("시도", _listing_sido_options, default=[], key="listing_sido_filter_v2")
                with _lf2:
                    _listing_sigungu_options = sorted([v for v in _listing_df.get("시군구", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if v])
                    _listing_sigungu = st.multiselect("시군구", _listing_sigungu_options, default=[], key="listing_sigungu_filter_v2")
                with _lf3:
                    _listing_trade_options = sorted([v for v in _listing_df.get("거래유형", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if v])
                    _listing_trade = st.multiselect("거래유형", _listing_trade_options, default=[], key="listing_trade_filter_v2")
                with _lf4:
                    _listing_keyword = st.text_input("단지/지역 검색", placeholder="예: 잠실, 헬리오시티", key="listing_keyword_filter_v2")

                if _listing_price_min < _listing_price_max:
                    _listing_price_range = st.slider(
                        "호가 범위(만원)",
                        min_value=_listing_price_min,
                        max_value=_listing_price_max,
                        value=(_listing_price_min, _listing_price_max),
                        step=1000,
                        key="listing_price_range_v2",
                    )
                else:
                    st.caption(f"호가 범위: {_format_listing_price(_listing_price_min)} 단일 가격")

            _filtered_listing = _filter_listing_df(
                _listing_df,
                sido=_listing_sido,
                sigungu=_listing_sigungu,
                trade_types=_listing_trade,
                price_range=_listing_price_range,
                keyword=_listing_keyword,
            )

            _kpis = _summarize_listing_kpis(_filtered_listing)
            _lk1, _lk2, _lk3, _lk4, _lk5 = st.columns(5)
            _lk1.metric("전체 매물", f"{_kpis['매물수']:,}건")
            _lk2.metric("단지 수", f"{_kpis['단지수']:,}개")
            _lk3.metric("지역 수", f"{_kpis['지역수']:,}개")
            _lk4.metric("중위 호가", _format_listing_price(_kpis["중위호가"]))
            _lk5.metric("최저 호가", _format_listing_price(_kpis["최저호가"]))
            if _kpis.get("최근확인일") is not None and pd.notna(_kpis["최근확인일"]):
                st.caption(f"데이터 기준일: {_kpis['최근확인일']:%Y-%m-%d}")

            if _filtered_listing.empty:
                st.info("조회 조건에 맞는 매물이 없습니다. 지역, 가격, 거래유형 조건을 넓혀 다시 조회해보세요.")
            else:
                _chart_df = _filtered_listing.dropna(subset=["매물가격"]).copy()
                _lc1, _lc2 = st.columns(2)
                with _lc1:
                    _fig_price_hist = px.histogram(
                        _chart_df,
                        x="매물가격",
                        nbins=25,
                        title="호가 분포",
                        labels={"매물가격": "호가(만원)"},
                        color_discrete_sequence=["#2563eb"],
                    )
                    _fig_price_hist.update_layout(font=PLOTLY_FONT, height=340, margin=dict(l=10, r=10, t=50, b=10))
                    st.plotly_chart(_fig_price_hist, use_container_width=True)
                    register_fig("매물현황_호가분포", _fig_price_hist, "매물현황")
                with _lc2:
                    if "시군구" in _chart_df.columns and _chart_df["시군구"].replace("", np.nan).notna().any():
                        _region_count = (
                            _chart_df.assign(시군구=_chart_df["시군구"].replace("", "미분류"))
                            .groupby("시군구")
                            .size()
                            .reset_index(name="매물수")
                            .sort_values("매물수", ascending=False)
                            .head(15)
                        )
                        _fig_region_bar = px.bar(
                            _region_count,
                            x="시군구",
                            y="매물수",
                            title="지역별 매물 수 Top 15",
                            text="매물수",
                            color_discrete_sequence=["#0f766e"],
                        )
                        _fig_region_bar.update_traces(textposition="outside")
                        _fig_region_bar.update_layout(font=PLOTLY_FONT, height=340, margin=dict(l=10, r=10, t=50, b=10))
                        st.plotly_chart(_fig_region_bar, use_container_width=True)
                        register_fig("매물현황_지역별매물수", _fig_region_bar, "매물현황")
                    else:
                        _trade_count = _chart_df.groupby("거래유형").size().reset_index(name="매물수") if "거래유형" in _chart_df.columns else pd.DataFrame()
                        if not _trade_count.empty:
                            _fig_trade = px.pie(_trade_count, names="거래유형", values="매물수", hole=0.45, title="거래유형별 비중")
                            _fig_trade.update_layout(font=PLOTLY_FONT, height=340, margin=dict(l=10, r=10, t=50, b=10))
                            st.plotly_chart(_fig_trade, use_container_width=True)

                st.markdown("#### 단지별 매물현황")
                _complex_summary = _build_complex_listing_summary(_filtered_listing)
                if _complex_summary.empty:
                    st.info("단지별로 요약할 수 있는 매물 데이터가 없습니다.")
                else:
                    _complex_display = _complex_summary.copy()
                    for _price_col in ["최저호가", "중위호가", "평균호가", "최고호가"]:
                        if _price_col in _complex_display.columns:
                            _complex_display[_price_col] = _complex_display[_price_col].apply(_format_listing_price)
                    st.dataframe(_complex_display, use_container_width=True, height=340, hide_index=True)
                    st.download_button(
                        "단지별 매물현황 CSV 다운로드",
                        data=_complex_summary.to_csv(index=False).encode("utf-8-sig"),
                        file_name="listing_complex_summary.csv",
                        mime="text/csv",
                        key="listing_complex_download_v2",
                    )

                with st.expander("원본 매물 목록", expanded=False):
                    _raw_listing = _filtered_listing.sort_values("매물가격", na_position="last").copy()
                    _raw_listing["호가표시"] = _raw_listing["매물가격"].apply(_format_listing_price)
                    _display_cols = [c for c in ["시도", "시군구", "단지명", "거래유형", "호가표시", "면적", "층", "동", "향", "중개사", "확인일", "매물URL", "비고"] if c in _raw_listing.columns]
                    st.dataframe(
                        _raw_listing[_display_cols],
                        use_container_width=True,
                        height=360,
                        hide_index=True,
                        column_config={"매물URL": st.column_config.LinkColumn("원문 링크")} if "매물URL" in _display_cols else None,
                    )
                    st.download_button(
                        "필터링 매물 CSV 다운로드",
                        data=_filtered_listing.to_csv(index=False).encode("utf-8-sig"),
                        file_name="filtered_naver_listings.csv",
                        mime="text/csv",
                        key="listing_raw_download_v2",
                    )

                st.caption("매물 정보는 수집 시점 기준 호가입니다. 실매물 여부, 가격, 권리관계는 투자 판단 전 별도 확인이 필요합니다.")
    else:
        st.info("거래현황 탭에서 네이버 매물 CSV/JSON을 업로드하면 ASIL식 매물현황 대시보드가 활성화됩니다.")
        with st.expander("필수 업로드 컬럼 예시"):
            st.write("단지명, 거래유형, 매물가격, 시도, 시군구, 면적, 층, 매물URL, 확인일")

if main_tab10:
    st.header("적정가·구매력")
    st.caption("소득분위별 구매력(순자산 + 주담대 PMT 역산)을 계산하고, 시군구 급지순위·실거래가와 매칭합니다.")
    render_tab_usage_guide("적정가·구매력")

    # ── 파라미터 섹션 ──────────────────────────────────────────────────
    _pm_c1, _pm_c2, _pm_c3, _pm_c4, _pm_c5 = st.columns(5)

    with _pm_c1:
        # quintile_df의 연도 목록에서 기준연도 선택
        if not quintile_df.empty and "연도" in quintile_df.columns:
            _q_years = sorted(quintile_df["연도"].dropna().unique().tolist(), reverse=True)
        elif not apt_df.empty and "연도" in apt_df.columns:
            _q_years = sorted(apt_df["연도"].dropna().unique().tolist(), reverse=True)
        else:
            _q_years = [2023, 2022, 2021]
        _match_year = st.selectbox("기준연도", _q_years, key="match_year")

    with _pm_c2:
        _base_rate = st.number_input("주담대금리 (%)", min_value=0.5, max_value=20.0, value=3.5, step=0.1, key="match_rate")

    with _pm_c3:
        _dsr_pct = st.selectbox("DSR 한도", [40, 50], index=0, key="match_dsr")
        _dsr_limit = _dsr_pct / 100.0

    with _pm_c4:
        _loan_years = st.selectbox("대출기간 (년)", [20, 25, 30], index=2, key="match_loan_years")

    with _pm_c5:
        _apply_ltv_limit = st.checkbox("LTV 규제 반영", value=True, key="match_apply_ltv_limit")
        _ltv_pct = st.number_input("LTV 한도 (%)", min_value=0, max_value=100, value=70, step=5, key="match_ltv_pct")
        _ltv_ratio = _ltv_pct / 100.0
    _ltv_label = f"{_ltv_pct}%" if _apply_ltv_limit else "미적용"

    st.divider()

    # ── 결과 1: 소득분위별 구매력 ──────────────────────────────────────
    st.subheader("퍼센타일별 자금여력")
    st.caption("자금여력 = 순자산 + min(PMT 역산 대출가능액, 대출규제한도). 구매가능가격 역산 시 LTV 한도는 순자산/(1-LTV)로 반영합니다.")
    try:
        # 보간 → 구매력 계산
        _pct_df = interpolate_quintile_to_percentile(quintile_df, year=_match_year)
        _pp_df  = compute_purchasing_power(
            _pct_df,
            base_rate=_base_rate,
            dsr_limit=_dsr_limit,
            loan_years=_loan_years,
            apply_ltv_limit=_apply_ltv_limit,
            ltv_ratio=_ltv_ratio,
        )

        if _pp_df.empty:
            st.warning("구매력 계산 결과가 없습니다. quintile 데이터를 확인하세요.")
        else:
            # 구매력 분포 면적 차트
            _pp_col = next((c for c in ["자금여력_만원", "구매력(만원)", "구매력", "대출가능액_만원", "대출가능액"] if c in _pp_df.columns), None)
            _pct_col = "percentile" if "percentile" in _pp_df.columns else _pp_df.columns[0]

            if _pp_col:
                fig_pp = px.area(
                    _pp_df,
                    x=_pct_col, y=_pp_col,
                    title=f"소득분위별 자금여력 분포 ({_match_year}년, 주담대금리 {_base_rate}%, DSR {_dsr_pct}%, LTV {_ltv_label}, {_loan_years}년)",
                    labels={_pct_col: "소득 퍼센타일 (%)", _pp_col: "자금여력(만원)"},
                    color_discrete_sequence=["#3498db"],
                )
                # 핵심 구간 annotation (상위 1%, 5%, 10%, 50%)
                for _ann_pct in [1, 5, 10, 50]:
                    _ann_row = _pp_df[_pp_df[_pct_col] == _ann_pct]
                    if not _ann_row.empty:
                        _ann_val = float(_ann_row[_pp_col].iloc[0])
                        fig_pp.add_vline(
                            x=_ann_pct, line_dash="dot", line_color="gray",
                            annotation_text=f"상위{_ann_pct}%\n{_ann_val:,.0f}만",
                            annotation_position="top",
                            annotation_font_size=9,
                        )
                fig_pp.update_layout(height=380)
                register_fig("소득분위_구매력", fig_pp, "소득-매물 매칭")
                st.plotly_chart(fig_pp, use_container_width=True)

            # 주요 퍼센타일 테이블
            _pp_disp_cols = [c for c in [_pct_col, "순자산", "연소득", "PMT역산대출가능액_만원", "LTV자기자본구매한도_만원", "대출가능액_만원", "자금여력_만원", "대출한도제약유형", "대출가능액", "구매력(만원)"] if c in _pp_df.columns]
            st.dataframe(
                _pp_df[_pp_disp_cols].style.format({c: "{:,.0f}" for c in _pp_disp_cols if c not in [_pct_col, "대출한도제약유형"]}, na_rep="N/A"),
                use_container_width=True, height=280,
            )
    except Exception as e:
        st.error(f"구매력 계산 오류: {e}")

    st.divider()

    # ── 결과 2: 급지 순위 ─────────────────────────────────────────────
    st.subheader("시군구 급지 순위")
    try:
        _grade_df = _cached_rank_sigungu(apt_df, nps_df, _match_year)

        if _grade_df.empty:
            st.warning("급지 순위 계산 결과가 없습니다. apt/nps 데이터를 확인하세요.")
        else:
            # 상위 30개 수평 막대
            _gd_top = _grade_df.head(30).copy()
            _gd_name_col = "시군구명" if "시군구명" in _gd_top.columns else "지역코드"
            _gd_score_col = "급지스코어" if "급지스코어" in _gd_top.columns else "급지순위"
            _gd_color_col = "급지스코어" if "급지스코어" in _gd_top.columns else None

            _gd_top["표시명"] = _gd_top.apply(
                lambda r: f"{r.get('시군구명', r.get('지역코드',''))}" +
                          (f" ({r['시도']})" if "시도" in r and pd.notna(r.get("시도")) else ""),
                axis=1
            )
            fig_grade = px.bar(
                _gd_top,
                x=_gd_score_col, y="표시명",
                orientation="h",
                color=_gd_color_col,
                color_continuous_scale="RdYlGn",
                title=f"시군구 급지 순위 TOP 30 ({_match_year}년)",
                labels={_gd_score_col: "급지스코어", "표시명": ""},
            )
            fig_grade.update_layout(
                height=500, margin=dict(t=40, b=20, l=10, r=20),
                yaxis=dict(autorange="reversed"),
            )
            register_fig("급지순위_TOP30", fig_grade, "소득-매물 매칭")
            st.plotly_chart(fig_grade, use_container_width=True)

            with st.expander("전체 급지 테이블"):
                _gd_disp = [c for c in ["급지순위", "시군구명", "시도", "급지스코어", "평균단가", "소득수준", "거래량", "성장률_3yr"] if c in _grade_df.columns]
                st.dataframe(_grade_df[_gd_disp], use_container_width=True, height=400)
                # CSV 다운로드 버튼
                _csv_grade = _grade_df[_gd_disp].to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    label="📥 급지순위 CSV 다운로드",
                    data=_csv_grade,
                    file_name=f"sigungu_grade_{_match_year}.csv",
                    mime="text/csv",
                    key="dl_grade",
                )
    except Exception as e:
        st.error(f"급지순위 계산 오류: {e}")

    st.divider()

    # ── 결과 3: 소득-급지 매칭 ────────────────────────────────────────
    st.subheader("소득-급지 매칭")
    try:
        # 구매력 df와 급지 df가 모두 있을 때만 매칭
        if "_pp_df" in dir() and not _pp_df.empty and "_grade_df" in dir() and not _grade_df.empty:
            _match_df = match_income_to_property(_pp_df, _grade_df)

            if _match_df.empty:
                st.info("매칭 결과가 없습니다.")
            else:
                # 매칭 테이블
                _mt_disp_cols = [c for c in [
                    "percentile_구간", "구매력_중앙", "매칭최고급지", "매칭시군구_TOP3", "시장가격", "갭"
                ] if c in _match_df.columns]

                st.dataframe(
                    _match_df[_mt_disp_cols].style.format(
                        {c: "{:,.0f}" for c in ["구매력_중앙", "시장가격", "갭"] if c in _match_df.columns},
                        na_rep="N/A",
                    ),
                    use_container_width=True,
                    height=320,
                    column_config={
                        "매칭시군구_TOP3": st.column_config.TextColumn("추천 시군구 TOP3", width="large"),
                        "갭": st.column_config.NumberColumn("갭(만원)", help="시장가격 - 구매력 (음수=살 수 있음)"),
                    },
                )
                # 매칭 결과 CSV 다운로드
                _csv_match = _match_df[_mt_disp_cols].to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    label="📥 매칭결과 CSV 다운로드",
                    data=_csv_match,
                    file_name=f"income_match_{_match_year}.csv",
                    mime="text/csv",
                    key="dl_match",
                )

                # 계단 차트: 구매력 곡선 vs 급지별 시장가격 오버레이
                _pct_col2 = "percentile" if "percentile" in _pp_df.columns else _pp_df.columns[0]
                _pp_col2  = next((c for c in ["자금여력_만원", "구매력(만원)", "구매력", "대출가능액_만원", "대출가능액"] if c in _pp_df.columns), None)
                _mkt_col  = "시장가격" if "시장가격" in _match_df.columns else None
                _pct_interval_col = "percentile_구간" if "percentile_구간" in _match_df.columns else None

                if _pp_col2 and _mkt_col and _pct_interval_col:
                    fig_match = go.Figure()
                    # 구매력 곡선
                    fig_match.add_trace(go.Scatter(
                        x=_pp_df[_pct_col2], y=_pp_df[_pp_col2],
                        mode="lines",
                        name="구매력",
                        line=dict(color="#3498db", width=2),
                        fill="tozeroy", fillcolor="rgba(52,152,219,0.1)",
                    ))
                    # 급지별 시장가격 (계단형)
                    fig_match.add_trace(go.Scatter(
                        x=_match_df[_pct_interval_col], y=_match_df[_mkt_col],
                        mode="lines+markers",
                        name="급지별 시장가격",
                        line=dict(color="#e74c3c", width=2, shape="hv"),
                        marker=dict(size=6),
                    ))
                    fig_match.update_layout(
                        title="소득 퍼센타일별 구매력 vs 급지 시장가격",
                        xaxis_title="소득 퍼센타일 (%)",
                        yaxis_title="금액(만원)",
                        height=420,
                        legend=dict(orientation="h"),
                        annotations=[dict(
                            text="두 곡선이 만나는 구간 = 해당 소득수준에서 접근 가능한 시장",
                            showarrow=False, x=0.5, y=1.06, xref="paper", yref="paper",
                            font=dict(size=10, color="gray"),
                        )],
                    )
                    register_fig("소득_급지_매칭_차트", fig_match, "소득-매물 매칭")
                    st.plotly_chart(fig_match, use_container_width=True)
        else:
            st.info("구매력 및 급지 데이터가 모두 계산되어야 매칭이 가능합니다.")
    except Exception as e:
        st.error(f"소득-급지 매칭 오류: {e}")


# ============================
# Tab 11: 커뮤니티 게시판
# ============================
if main_tab11:
    st.header("커뮤니티 게시판")
    st.caption("차트와 분석 설정을 저장하고 다른 사용자와 공유합니다.")

    from board import (get_posts, get_post, get_post_count, delete_post,
                       toggle_like, add_comment, get_comments)

    # 세션 초기화
    if "board_view" not in st.session_state:
        st.session_state["board_view"] = ("gallery",)
    if "board_page" not in st.session_state:
        st.session_state["board_page"] = 1
    if "board_session_id" not in st.session_state:
        import uuid
        st.session_state["board_session_id"] = str(uuid.uuid4())

    view_mode = st.session_state["board_view"]

    if view_mode[0] == "detail" and len(view_mode) > 1:
        # --- 상세 뷰 ---
        post = get_post(view_mode[1])
        if post is None:
            st.error("게시글을 찾을 수 없습니다.")
            if st.button("목록으로"):
                st.session_state["board_view"] = ("gallery",)
                st.rerun()
        else:
            if st.button("← 목록으로"):
                st.session_state["board_view"] = ("gallery",)
                st.rerun()

            st.subheader(post["title"])
            col_info, col_like = st.columns([3, 1])
            with col_info:
                st.caption(f"작성자: {post['author']} | {post['created_at'][:16]} | 탭: {post['tab_name']}")
            with col_like:
                if st.button(f"👍 {post['likes']}", key="like_btn"):
                    new_count = toggle_like(post["id"], st.session_state["board_session_id"])
                    st.rerun()

            # 차트 이미지
            img_full = os.path.join(os.path.dirname(__file__), post["image_path"])
            if os.path.exists(img_full):
                st.image(img_full, use_container_width=True)
            else:
                st.warning("이미지 파일을 찾을 수 없습니다.")

            if post["description"]:
                st.markdown(post["description"])

            # 사용된 설정
            with st.expander("사용된 설정 보기"):
                settings = json.loads(post["settings_json"])
                if "global" in settings:
                    st.markdown("**글로벌 필터**")
                    for k, v in settings["global"].items():
                        st.text(f"  {k}: {v}")
                if "tab" in settings:
                    st.markdown(f"**탭 설정 ({settings['tab'].get('name', '')})**")
                    for k, v in settings["tab"].items():
                        if k != "name":
                            st.text(f"  {k}: {v}")

            # 댓글 섹션
            st.divider()
            st.markdown("**댓글**")
            comments = get_comments(post["id"])
            for c in comments:
                st.markdown(f"**{c['author']}** ({c['created_at'][:16]})")
                st.text(c["content"])
                st.markdown("---")

            with st.form(key=f"comment_form_{post['id']}"):
                c_author = st.text_input("닉네임", key="cmt_author")
                c_content = st.text_area("댓글 내용", key="cmt_content", height=80)
                if st.form_submit_button("댓글 등록"):
                    if c_author and c_content:
                        add_comment(post["id"], c_author, c_content)
                        st.rerun()
                    else:
                        st.warning("닉네임과 내용을 모두 입력하세요.")

            # 삭제
            st.divider()
            with st.expander("게시글 삭제"):
                del_pw = st.text_input("비밀번호 확인", type="password", key="del_pw")
                if st.button("삭제", key="del_btn"):
                    if del_pw:
                        if delete_post(post["id"], del_pw):
                            st.success("삭제되었습니다.")
                            st.session_state["board_view"] = ("gallery",)
                            st.rerun()
                        else:
                            st.error("비밀번호가 일치하지 않습니다.")
    else:
        # --- 갤러리 뷰 ---
        total = get_post_count()
        page = st.session_state["board_page"]
        per_page = 12
        posts = get_posts(page=page, per_page=per_page)

        if not posts:
            st.info("아직 게시글이 없습니다. 사이드바에서 차트를 저장해보세요!")
        else:
            cols = st.columns(4)
            for i, post in enumerate(posts):
                with cols[i % 4]:
                    img_path = os.path.join(os.path.dirname(__file__), post["image_path"])
                    if os.path.exists(img_path):
                        st.image(img_path, use_container_width=True)
                    else:
                        st.markdown("*(이미지 없음)*")
                    st.markdown(f"**{post['title']}**")
                    st.caption(f"{post['author']} | 👍 {post['likes']} | {post['created_at'][:10]}")
                    if st.button("상세보기", key=f"view_{post['id']}"):
                        st.session_state["board_view"] = ("detail", post["id"])
                        st.rerun()

            # 페이지네이션
            total_pages = max(1, math.ceil(total / per_page))
            nav_col1, nav_col2, nav_col3 = st.columns([1, 2, 1])
            with nav_col1:
                if page > 1 and st.button("← 이전"):
                    st.session_state["board_page"] = page - 1
                    st.rerun()
            with nav_col2:
                st.markdown(f"<center>{page} / {total_pages}</center>", unsafe_allow_html=True)
            with nav_col3:
                if page < total_pages and st.button("다음 →"):
                    st.session_state["board_page"] = page + 1
                    st.rerun()


# --- 게시판 저장 위젯 (사이드바) ---
with st.sidebar:
    st.divider()
    st.subheader("📌 게시판에 저장")
    board_figs = st.session_state.get("_board_figures", {})
    if board_figs:
        fig_options = list(board_figs.keys())
        save_chart = st.selectbox(
            "저장할 차트",
            options=fig_options,
            format_func=lambda k: f"[{board_figs[k]['tab_name']}] {k}",
            key="board_sel_chart",
        )
        save_title = st.text_input("제목", key="board_save_title")
        save_desc = st.text_area("설명 (선택)", key="board_save_desc", height=80)
        save_author = st.text_input("닉네임", key="board_save_author")
        save_pw = st.text_input("비밀번호", type="password", key="board_save_pw",
                                help="수정/삭제 시 필요")
        if st.button("게시판에 저장", key="board_save_btn", type="primary"):
            if save_title and save_author and save_pw:
                from board import create_post, capture_current_settings
                entry = board_figs[save_chart]
                settings = capture_current_settings()
                post_id = create_post(
                    title=save_title, description=save_desc,
                    author=save_author, password=save_pw,
                    tab_name=entry["tab_name"], fig=entry["fig"],
                    settings=settings,
                )
                st.success(f"저장 완료! (#{post_id})")
            else:
                st.warning("제목, 닉네임, 비밀번호를 모두 입력하세요.")
    else:
        st.info("차트가 표시되면 저장할 수 있습니다.")

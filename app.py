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
from plotly.subplots import make_subplots

from data_loader import load_all_data, load_apt_data, load_rent_data, get_sigungu_name
from board import init_db
from analysis import (
    correlation_matrix,
    correlation_by_region,
    correlation_by_period,
    scatter_analysis,
    multiple_regression,
    detect_outliers,
    cluster_regions,
    granger_causality_test,
)

st.set_page_config(
    page_title="부동산 가격분석 대시보드",
    page_icon="🏠",
    layout="wide",
)


def register_fig(name: str, fig, tab_name: str):
    """Plotly figure를 게시판 저장용으로 등록"""
    if "_board_figures" not in st.session_state:
        st.session_state["_board_figures"] = {}
    st.session_state["_board_figures"][name] = {"fig": fig, "tab_name": tab_name}

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
                "미분양_평균", "미분양_호수", "전월세전환율", "지가변동률",
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
                "미분양_평균", "미분양_호수", "전월세전환율", "지가변동률",
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
    수식 문자열을 eval로 계산하여 캐싱.
    _src_df: 언더스코어 접두어로 Streamlit 해싱 제외 (cache_key가 대신 무효화 담당)
    """
    src = _src_df.copy()
    if sido_list:
        src = src[src["시도"].isin(list(sido_list))]

    _SAFE_GLOBALS = {
        "__builtins__": {},
        "abs": np.abs, "sqrt": np.sqrt,
        "log": np.log, "log10": np.log10, "exp": np.exp,
    }

    parts = []
    for sido_name, group in src.groupby("시도"):
        group = group.sort_values(time_col).reset_index(drop=True)
        row = group[[time_col, "시도"]].copy()
        namespace = {**_SAFE_GLOBALS}
        for col in var_names:
            if col in group.columns:
                namespace[col] = group[col].astype(float)
        for label, expr in formula_strs:
            if not expr.strip():
                row[label] = np.nan
                continue
            try:
                with np.errstate(divide="ignore", invalid="ignore"):
                    result = eval(expr, {"__builtins__": {}}, namespace)
                if isinstance(result, (int, float)):
                    row[label] = float(result)
                elif hasattr(result, "values"):
                    s = pd.Series(result.values, index=group.index)
                    row[label] = s.replace([np.inf, -np.inf], np.nan).values
                else:
                    row[label] = np.nan
            except Exception:
                row[label] = np.nan
        parts.append(row)

    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


# --- 페이지 구성 ---
main_tab1, main_tab2, main_tab3, main_tab4, main_tab5, main_tab6, main_tab7, main_tab8 = st.tabs([
    "Overview", "시장분석", "지역별 분석", "수요-공급 분석기", "공급분석", "통계분석", "고급분석", "커뮤니티 게시판"
])

# ============================
# Tab 1: Overview
# ============================
with main_tab1:
    st.header(f"주요 지표 Overview ({mode_label})")

    if analysis_mode == "매매 분석":
        main_filtered = filtered_apt
    else:
        main_filtered = filtered_rent

    if main_filtered.empty:
        st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
    else:
        # KPI 카드
        cols = st.columns(5)

        if analysis_mode == "매매 분석":
            with cols[0]:
                st.metric("평균 거래가격", f"{filtered_apt['평균가격'].mean():,.0f}만원")
            with cols[1]:
                st.metric("총 거래량", f"{filtered_apt['거래량'].sum():,.0f}건")
            with cols[2]:
                st.metric("평균 단가(m2)", f"{filtered_apt['평균단가_per_m2'].mean():,.0f}만원/m2")
        else:
            with cols[0]:
                st.metric("평균 보증금", f"{filtered_rent['보증금평균'].mean():,.0f}만원")
            with cols[1]:
                st.metric("총 거래량", f"{filtered_rent['임대거래량'].sum():,.0f}건")
            with cols[2]:
                st.metric("보증금 단가(m2)", f"{filtered_rent['보증금단가_per_m2'].mean():,.0f}만원/m2")

        with cols[3]:
            if "총인구" in analysis_df.columns and not analysis_df["총인구"].dropna().empty:
                pop = analysis_df["총인구"].dropna().iloc[-1]
                st.metric("인구(최근)", f"{pop:,.0f}명")
            else:
                st.metric("인구", "N/A")
        with cols[4]:
            if "GRDP" in analysis_df.columns and not analysis_df["GRDP"].dropna().empty:
                grdp_val = analysis_df["GRDP"].dropna().iloc[-1]
                st.metric("GRDP(최근)", f"{grdp_val:,.0f}백만원")
            else:
                st.metric("GRDP", "N/A")

        # 시계열 트렌드
        st.subheader(f"{mode_label} 가격 트렌드")

        if analysis_mode == "매매 분석":
            # 매매 트렌드
            if selected_codes:
                time_col = "연월" if freq == "월별" and "연월" in filtered_apt.columns else "연도"
                chart_df = aggregate_by_code(filtered_apt, time_col)
                color_col = "지역코드"
            else:
                chart_df = analysis_df
                time_col = "연월" if freq == "월별" and "연월" in analysis_df.columns else "연도"
                color_col = "시도"
            _price_col, _vol_col = "평균가격", "거래량"
        else:
            # 임대차 트렌드
            if selected_codes:
                time_col = "연월" if freq == "월별" and "연월" in filtered_rent.columns else "연도"
                chart_df = aggregate_rent_by_code(filtered_rent, time_col)
                color_col = "지역코드"
            else:
                chart_df = analysis_df
                time_col = "연월" if freq == "월별" and "연월" in analysis_df.columns else "연도"
                color_col = "시도"
            _price_col, _vol_col = price_col, vol_col

        if not chart_df.empty and _price_col in chart_df.columns:
            fig_price = px.line(
                chart_df.sort_values([time_col]),
                x=time_col, y=_price_col, color=color_col,
                title=f"{mode_label} 평균 가격 추이",
                labels={_price_col: "가격(만원)", time_col: "기간"},
            )
            register_fig("매매가격_추이", fig_price, "Overview")
            st.plotly_chart(fig_price, use_container_width=True)

        if not chart_df.empty and _vol_col in chart_df.columns:
            fig_vol = px.bar(
                chart_df.sort_values([time_col]),
                x=time_col, y=_vol_col, color=color_col,
                title=f"{mode_label} 거래량 추이",
                labels={_vol_col: "거래건수", time_col: "기간"},
            )
            register_fig("거래량_추이", fig_vol, "Overview")
            st.plotly_chart(fig_vol, use_container_width=True)

        # ── 시장심리 게이지 (KB 매수우위지수, 주택가격전망CSI) ──────
        gauge_cols_check = ["KB_매수우위지수", "주택가격전망CSI"]
        gauge_available = [c for c in gauge_cols_check if c in analysis_df.columns and analysis_df[c].notna().any()]
        if gauge_available:
            st.subheader("시장심리 지표")
            g_col1, g_col2 = st.columns(2)

            # KB 매수우위지수 게이지 (0~200, 100이 중립)
            if "KB_매수우위지수" in gauge_available:
                kb_series = analysis_df["KB_매수우위지수"].dropna()
                kb_val = float(kb_series.iloc[-1]) if not kb_series.empty else None
                if kb_val is not None:
                    fig_gauge_kb = go.Figure(go.Indicator(
                        mode="gauge+number+delta",
                        value=kb_val,
                        title={"text": "KB 매수우위지수"},
                        delta={"reference": 100, "valueformat": ".1f"},
                        gauge={
                            "axis": {"range": [0, 200]},
                            "bar": {"color": "darkblue"},
                            "steps": [
                                {"range": [0, 80],   "color": "lightblue"},
                                {"range": [80, 120],  "color": "lightyellow"},
                                {"range": [120, 200], "color": "lightsalmon"},
                            ],
                            "threshold": {
                                "line": {"color": "red", "width": 4},
                                "thickness": 0.75,
                                "value": 100,
                            },
                        },
                    ))
                    fig_gauge_kb.update_layout(height=300)
                    with g_col1:
                        register_fig("KB_매수우위_게이지", fig_gauge_kb, "Overview")
                        st.plotly_chart(fig_gauge_kb, use_container_width=True)

            # 주택가격전망CSI 게이지 (0~200, 100이 중립)
            if "주택가격전망CSI" in gauge_available:
                csi_series = analysis_df["주택가격전망CSI"].dropna()
                csi_val = float(csi_series.iloc[-1]) if not csi_series.empty else None
                if csi_val is not None:
                    fig_gauge_csi = go.Figure(go.Indicator(
                        mode="gauge+number+delta",
                        value=csi_val,
                        title={"text": "주택가격전망CSI"},
                        delta={"reference": 100, "valueformat": ".1f"},
                        gauge={
                            "axis": {"range": [0, 200]},
                            "bar": {"color": "darkorange"},
                            "steps": [
                                {"range": [0, 80],   "color": "lightblue"},
                                {"range": [80, 120],  "color": "lightyellow"},
                                {"range": [120, 200], "color": "lightsalmon"},
                            ],
                            "threshold": {
                                "line": {"color": "red", "width": 4},
                                "thickness": 0.75,
                                "value": 100,
                            },
                        },
                    ))
                    fig_gauge_csi.update_layout(height=300)
                    with g_col2:
                        register_fig("CSI_게이지", fig_gauge_csi, "Overview")
                        st.plotly_chart(fig_gauge_csi, use_container_width=True)

        # 원인 지표 트렌드 (시도 레벨)
        extra_vars = [v for v in cause_vars if v in analysis_df.columns and analysis_df[v].notna().any()]
        if extra_vars:
            st.subheader("원인 지표 트렌드")
            ts_time_col = "연월" if freq == "월별" and "연월" in analysis_df.columns else "연도"
            for var in extra_vars:
                fig = px.line(
                    analysis_df.sort_values([ts_time_col]),
                    x=ts_time_col, y=var, color="시도",
                    title=f"{var} 추이",
                )
                register_fig("정책이벤트_차트", fig, "Overview")
                st.plotly_chart(fig, use_container_width=True)


# ============================
# Tab 2: 시장분석 (시계열비교 + 가격비교 + 갭분석)
# ============================
with main_tab2:
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
            left_var = st.selectbox("좌측 Y축 (결과)", [v for v in result_vars if v in valid_vars_ts], key="left")
        with col2:
            right_candidates = [v for v in valid_vars_ts if v != left_var]
            right_var = st.selectbox("우측 Y축 (원인)", right_candidates, key="right")
        with col3:
            sido_for_ts = st.selectbox("비교 시도", selected_sido if selected_sido else all_sido, key="ts_sido")

        ts_df = analysis_df[analysis_df["시도"] == sido_for_ts].sort_values("연도")
        time_col = "연월" if freq == "월별" and "연월" in ts_df.columns else "연도"

        if not ts_df.empty and right_var in ts_df.columns:
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
                    x_val = ev["날짜"]
                    if time_col == "연도":
                        x_val = ev["날짜"].year
                    fig_dual.add_vline(
                        x=x_val, line_width=1, line_dash="dot",
                        line_color=colors.get(ev.get("방향", ""), "gray"),
                        annotation_text=ev.get("이벤트명", ""),
                        annotation_position="top",
                        annotation_font_size=8,
                        annotation_textangle=-45,
                    )

            register_fig("시계열_듀얼축", fig_dual, "시장분석")
            st.plotly_chart(fig_dual, use_container_width=True)

            # 상관계수 표시
            valid = ts_df[[left_var, right_var]].dropna()
            if len(valid) >= 3:
                from scipy import stats as sp_stats
                r, p = sp_stats.pearsonr(valid[left_var], valid[right_var])
                st.info(f"상관계수: **{r:.4f}** (p-value: {p:.4f})")
        else:
            st.info("선택한 변수의 데이터가 부족합니다.")

        # 정규화 비교 차트
        st.subheader("정규화 시계열 비교")
        norm_vars = st.multiselect(
            "비교할 변수 선택", valid_vars_ts,
            default=[v for v in [price_col, "GRDP"] if v in valid_vars_ts],
            key="norm_vars",
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
            pc_var = st.selectbox("비교 변수", pc_vars, key="price_cmp_var")
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
with main_tab3:
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
        compare_var = st.selectbox("비교 변수", compare_options, key="compare")

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
with main_tab4:
    st.header("수요-공급 분석기")
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
        {"표시명": "NPS 가입자수",            "컬럼명": "NPS_가입자수",           "단위": "명",         "연집계룰": "sum(시도집계)",              "정상범위": "≥0",   "카테고리": "수요>유효수요>소득/신용",        "출처": "국민연금",  "설명": "4대보험 직장가입자 수 — 지역 고용규모 대리변수"},
        {"표시명": "NPS 1인당고지금액",       "컬럼명": "NPS_1인당고지금액",      "단위": "원",         "연집계룰": "가중평균(가입자수)",         "정상범위": "",      "카테고리": "수요>유효수요>소득/신용",        "출처": "국민연금",  "설명": "1인당 월 보험료 — 지역 소득수준 대리변수 (소득의 9%)"},
        {"표시명": "NPS 사업장수",            "컬럼명": "NPS_사업장수",           "단위": "개",         "연집계룰": "sum(시도집계)",              "정상범위": "≥0",   "카테고리": "수요>유효수요>소득/신용",        "출처": "국민연금",  "설명": "국민연금 가입 사업장 수 — 지역 사업체 밀도"},
        {"표시명": "NPS 고용증감",            "컬럼명": "NPS_고용증감",           "단위": "명",         "연집계룰": "sum(시도집계)",              "정상범위": "",      "카테고리": "수요>유효수요>소득/신용",        "출처": "국민연금",  "설명": "전월 대비 가입자 증감 — 양수=고용 증가"},
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
        # 파생지표
        {"표시명": "전세가율",               "컬럼명": "전세가율",              "단위": "%",          "연집계룰": "계산(전세보증금/매매가×100)", "정상범위": "40~80", "카테고리": "파생지표",                       "출처": "파생",     "설명": "매매가 대비 전세가 — 높으면 갭투자 여지↓, 실수요↑"},
        {"표시명": "PIR(소득대비주택가격)",    "컬럼명": "PIR",                  "단위": "배",         "연집계룰": "계산(매매가/가구소득)",       "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "가구소득 대비 주택가격 배수 — 높을수록 구매부담↑"},
        {"표시명": "PIR(NPS기반)",           "컬럼명": "PIR_NPS",              "단위": "배",         "연집계룰": "계산(매매가/NPS연소득)",      "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "NPS 추정소득 기반 PIR — 시군구 단위 비교 가능"},
        {"표시명": "매매 거래회전율",         "컬럼명": "매매_거래회전율",        "단위": "‰",         "연집계룰": "계산(거래량/인구×1000)",     "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "인구 대비 매매 건수 — 시장 유동성"},
        {"표시명": "전세 거래회전율",         "컬럼명": "전세_거래회전율",        "단위": "‰",         "연집계룰": "계산(전세거래/인구×1000)",    "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "인구 대비 전세 건수 — 전세시장 유동성"},
        {"표시명": "가격변화율(YoY)",        "컬럼명": "가격변화율_YoY",        "단위": "%",          "연집계룰": "계산(전년대비변화)",          "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "전년 대비 매매가 변화율 — 시장 모멘텀"},
        {"표시명": "소득대비대출",           "컬럼명": "소득대비대출",          "단위": "배",         "연집계룰": "계산(주담대/가구소득)",       "정상범위": "",      "카테고리": "파생지표",                       "출처": "파생",     "설명": "가구소득 대비 주담대 배수 — 레버리지 부담도"},
    ])
    if "설명" in VAR_META.columns:
        VAR_META["설명"] = VAR_META["설명"].fillna("")
        # 설명 끝에 [출처] 태그 자동 부착
        VAR_META["설명"] = VAR_META.apply(
            lambda r: f'{r["설명"]} [{r["출처"]}]' if r["설명"] else f'[{r["출처"]}]', axis=1
        )

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

                    # 2단계: 카테고리 필터 후 변수 선택
                    if sel_cat == "(전체)":
                        filtered_meta = VAR_META[VAR_META["컬럼명"].isin(numeric_cols_5)]
                    else:
                        filtered_meta = VAR_META[
                            (VAR_META["카테고리"] == sel_cat) &
                            (VAR_META["컬럼명"].isin(numeric_cols_5))
                        ]

                    # 메타에 없는 컬럼은 표시명=컬럼명으로 fallback
                    meta_col_set = set(VAR_META["컬럼명"].tolist())
                    extra_cols = [c for c in numeric_cols_5 if c not in meta_col_set]
                    if sel_cat == "(전체)" and extra_cols:
                        extra_rows = pd.DataFrame([
                            {"표시명": c, "컬럼명": c, "카테고리": "(기타)", "출처": "-"}
                            for c in extra_cols
                        ])
                        filtered_meta = pd.concat([filtered_meta, extra_rows], ignore_index=True)

                    disp_names = filtered_meta["표시명"].tolist() if not filtered_meta.empty else numeric_cols_5
                    sel_disp = st.selectbox(
                        "변수 선택 후 [삽입] 클릭",
                        disp_names,
                        key=f"f5_selvar_{i}",
                    )
                    # 표시명 → 컬럼명 매핑
                    _meta_match = filtered_meta[filtered_meta["표시명"] == sel_disp]
                    sel_var = _meta_match["컬럼명"].iloc[0] if not _meta_match.empty else sel_disp

                    # 선택 변수의 출처/카테고리 표시
                    if not _meta_match.empty:
                        _row = _meta_match.iloc[0]
                        st.caption(f"출처: {_row.get('출처', '-')} | 카테고리: {_row.get('카테고리', '-')}")

                with col_ins:
                    st.write("")
                    if st.button("삽입", key=f"f5_ins_{i}", use_container_width=True):
                        cur = st.session_state.get(ta_key, "")
                        sep = " " if cur and not cur.endswith(" ") else ""
                        st.session_state[ta_key] = cur + sep + sel_var

                # ② 연산자 단축 버튼
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
                        st.info(f"상관계수: **{r5:.4f}** (p-value: {p5:.4f},  n={len(common_5)})")

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


# ============================
# Tab 5: 공급분석 (인허가/착공/준공 + 미분양)
# ============================
with main_tab5:
    st.header("공급분석")
    supply_sub1, supply_sub2 = st.tabs(["인허가/착공/준공", "미분양"])

    with supply_sub1:
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

    with supply_sub2:
        # 미분양 데이터 탐색
        unsold_cols = [c for c in ["미분양_호수", "미분양_평균"] if c in analysis_df.columns]
        if unsold_cols:
            unsold_time_col = "연월" if freq == "월별" and "연월" in analysis_df.columns else "연도"
            # 시도별 미분양 시계열
            unsold_var = st.selectbox("미분양 지표", unsold_cols, key="unsold_var")
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
with main_tab6:
    sub_reg, sub_outlier, sub_corr = st.tabs(["회귀분석", "이상치 탐지", "상관관계 분석"])

with sub_reg:
    st.header("다중회귀 분석")

    if analysis_df.empty:
        st.warning("데이터가 없습니다.")
    else:
        valid_num = [v for v in available_vars if v in analysis_df.columns and analysis_df[v].notna().any()]

        col_y, col_x = st.columns([1, 2])
        with col_y:
            reg_y = st.selectbox("종속변수 (Y)", [v for v in result_vars if v in valid_num], key="reg_y")
        with col_x:
            reg_x_candidates = [v for v in valid_num if v != reg_y]
            reg_x = st.multiselect("독립변수 (X)", reg_x_candidates,
                                    default=reg_x_candidates[:3] if len(reg_x_candidates) >= 3 else reg_x_candidates,
                                    key="reg_x")

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
            outlier_var = st.selectbox("분석 변수", valid_num, key="outlier_var")
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

            with st.expander("p-value 상세"):
                st.dataframe(pval.style.format("{:.4f}"))
        else:
            st.info("상관계수를 계산할 데이터가 부족합니다.")

        # 산점도
        st.subheader("산점도 분석")
        col_a, col_b = st.columns(2)
        with col_a:
            default_x = valid_vars.index("GRDP") if "GRDP" in valid_vars else 0
            x_var = st.selectbox("X축 변수", valid_vars, index=default_x, key="corr_x_var")
        with col_b:
            y_var = st.selectbox("Y축 변수", valid_vars, index=0, key="corr_y_var")

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
with main_tab7:
    st.caption("**클러스터링**: 비슷한 특성의 지역을 자동으로 묶어 그룹별 특징을 파악합니다 | **Granger 인과성**: 특정 지표가 가격 변화를 몇 달 앞서 예측할 수 있는지 통계적으로 검정합니다")
    sub_cluster, sub_granger = st.tabs(["클러스터링", "Granger 인과성"])

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
        )
        n_clusters = st.slider("클러스터 수", 2, 8, 4, key="n_clusters")

        if len(cluster_features) >= 2:
            try:
                clustered_df, centers_df = cluster_regions(analysis_df, cluster_features, n_clusters)

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
                    st.plotly_chart(fig_radar, use_container_width=True)

            except (ValueError, ImportError) as e:
                st.error(str(e))
        else:
            st.info("변수를 2개 이상 선택하세요.")


# ============================
# Tab 6-2: Granger 인과성 (고급분석 서브탭)
# ============================
with sub_granger:
    st.header("Granger 인과성 검정")
    st.caption("X 변수가 Y 변수를 시간적으로 선행하여 예측하는 데 도움이 되는지 검정합니다.")

    # Granger는 월별 시계열 데이터 필요
    granger_src = filtered_monthly if not filtered_monthly.empty else monthly_df

    if granger_src.empty:
        st.warning("월별 데이터가 필요합니다.")
    else:
        valid_num_g = [v for v in available_vars if v in granger_src.columns and granger_src[v].notna().any()]

        col1, col2, col3 = st.columns(3)
        with col1:
            g_y = st.selectbox("결과변수 (Y)", [v for v in result_vars if v in valid_num_g], key="g_y")
        with col2:
            g_x_candidates = [v for v in valid_num_g if v != g_y]
            g_x = st.selectbox("원인변수 (X)", g_x_candidates, key="g_x")
        with col3:
            g_max_lag = st.slider("최대 시차 (개월)", 1, 12, 4, key="g_lag")

        if g_y and g_x:
            try:
                granger_df = granger_causality_test(granger_src, g_y, g_x, max_lag=g_max_lag)

                if granger_df.empty:
                    st.warning("검정할 수 있는 데이터가 부족합니다.")
                else:
                    # 요약: 인과성 있는 시도 수
                    causal_sido = granger_df[granger_df["인과성"]]["시도"].nunique()
                    total_sido = granger_df["시도"].nunique()
                    st.metric("인과성 발견 시도", f"{causal_sido} / {total_sido}")

                    # 히트맵: 시도 × lag별 p값
                    pivot_g = granger_df.pivot_table(index="시도", columns="lag", values="p값", aggfunc="first")
                    fig_g = px.imshow(
                        pivot_g, text_auto=".3f",
                        color_continuous_scale="RdYlGn_r", zmin=0, zmax=0.1,
                        title=f"Granger 인과성 p값 ({g_x} → {g_y})",
                        labels={"color": "p값"},
                    )
                    st.plotly_chart(fig_g, use_container_width=True)

                    # 상세 테이블
                    with st.expander("상세 결과"):
                        st.dataframe(granger_df.style.format({
                            "F통계량": "{:.4f}", "p값": "{:.4f}"
                        }), use_container_width=True)

            except (ValueError, ImportError) as e:
                st.error(str(e))

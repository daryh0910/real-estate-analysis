"""
부동산 가격분석 서비스 - Streamlit 대시보드
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data_loader import load_all_data, load_apt_data, load_rent_data, get_sigungu_name
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

# --- 데이터 로딩 (캐싱) ---
@st.cache_data(show_spinner=False)
def get_data():
    return load_all_data()


data = get_data()
apt_df = data["apt"]
jeonse_df = data["jeonse"]
wolse_df = data["wolse"]
rent_all_df = data["rent_all"]
pop_df = data["pop"]
grdp_df = data["grdp"]
permit_df = data["permit"]
nps_df = data.get("nps", pd.DataFrame())
loan_df = data.get("loan", pd.DataFrame())
asset_df = data.get("asset", pd.DataFrame())
yearly_df = data["yearly"]
monthly_df = data["monthly"]

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
main_tab1, main_tab2, main_tab3, main_tab4, main_tab5, main_tab6 = st.tabs([
    "Overview", "비교분석", "지역별 분석", "수식 계산기", "통계분석", "고급분석"
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
            st.plotly_chart(fig_price, use_container_width=True)

        if not chart_df.empty and _vol_col in chart_df.columns:
            fig_vol = px.bar(
                chart_df.sort_values([time_col]),
                x=time_col, y=_vol_col, color=color_col,
                title=f"{mode_label} 거래량 추이",
                labels={_vol_col: "거래건수", time_col: "기간"},
            )
            st.plotly_chart(fig_vol, use_container_width=True)

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
                st.plotly_chart(fig, use_container_width=True)


# ============================
# Tab 2: 비교분석 (상관관계 + 시계열)
# ============================
with main_tab2:
    sub_corr, sub_ts = st.tabs(["상관관계 분석", "시계열 비교"])

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
            x_var = st.selectbox("X축 변수", valid_vars, index=default_x)
        with col_b:
            y_var = st.selectbox("Y축 변수", valid_vars, index=0)

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
            st.plotly_chart(fig_period, use_container_width=True)


# ============================
# Tab 2-2: 시계열 비교 (비교분석 서브탭)
# ============================
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
                st.plotly_chart(fig_norm, use_container_width=True)


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
            st.plotly_chart(fig_region, use_container_width=True)

        # 시군구별 요약 테이블
        st.subheader("시군구별 요약 통계")
        summary = (
            tab4_df.groupby(["지역코드"]).agg(**agg_dict)
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
                st.plotly_chart(fig_compare, use_container_width=True)


# ============================
# Tab 4: 수식 계산기
# ============================
with main_tab4:
    st.header("수식 계산기")
    st.caption("모든 변수를 사칙연산으로 조합하여 새로운 지표를 계산하고 시각화합니다.")

    # ── 변수 메타데이터 ────────────────────────────────────────────
    VAR_META = pd.DataFrame([
        {"표시명": "매매 평균가격",           "컬럼명": "평균가격",              "단위": "만원",       "연집계룰": "가중평균(거래량)",          "정상범위": ""},
        {"표시명": "매매 거래량",             "컬럼명": "거래량",                "단위": "건",         "연집계룰": "sum",                       "정상범위": "≥0"},
        {"표시명": "매매 평균단가",           "컬럼명": "평균단가_per_m2",        "단위": "만원/m²",    "연집계룰": "가중평균(거래량)",          "정상범위": ""},
        {"표시명": "전세 보증금평균",         "컬럼명": "전세_보증금평균",        "단위": "만원",       "연집계룰": "가중평균(전세_거래량)",     "정상범위": ""},
        {"표시명": "전세 거래량",             "컬럼명": "전세_거래량",            "단위": "건",         "연집계룰": "sum",                       "정상범위": "≥0"},
        {"표시명": "전세 보증금단가",         "컬럼명": "전세_보증금단가",        "단위": "만원/m²",    "연집계룰": "가중평균(전세_거래량)",     "정상범위": ""},
        {"표시명": "월세 보증금평균",         "컬럼명": "월세_보증금평균",        "단위": "만원",       "연집계룰": "가중평균(월세_거래량)",     "정상범위": ""},
        {"표시명": "월세 거래량",             "컬럼명": "월세_거래량",            "단위": "건",         "연집계룰": "sum",                       "정상범위": "≥0"},
        {"표시명": "월세 보증금단가",         "컬럼명": "월세_보증금단가",        "단위": "만원/m²",    "연집계룰": "가중평균(월세_거래량)",     "정상범위": ""},
        {"표시명": "월세 평균",               "컬럼명": "월세_월세평균",          "단위": "만원/월",    "연집계룰": "가중평균(월세_거래량)",     "정상범위": ""},
        {"표시명": "임대전체 보증금평균",     "컬럼명": "임대전체_보증금평균",    "단위": "만원",       "연집계룰": "가중평균(임대전체_거래량)", "정상범위": ""},
        {"표시명": "임대전체 거래량",         "컬럼명": "임대전체_거래량",        "단위": "건",         "연집계룰": "sum",                       "정상범위": "≥0"},
        {"표시명": "임대전체 보증금단가",     "컬럼명": "임대전체_보증금단가",    "단위": "만원/m²",    "연집계룰": "가중평균(임대전체_거래량)", "정상범위": ""},
        {"표시명": "임대전체 월세평균",       "컬럼명": "임대전체_월세평균",      "단위": "만원/월",    "연집계룰": "가중평균(임대전체_거래량)", "정상범위": ""},
        {"표시명": "총인구",                  "컬럼명": "총인구",                "단위": "명",         "연집계룰": "last",                      "정상범위": ""},
        {"표시명": "지역내총생산(GRDP)",      "컬럼명": "GRDP",                  "단위": "백만원",     "연집계룰": "last",                      "정상범위": ""},
        {"표시명": "아파트 인허가",           "컬럼명": "인허가_호수",            "단위": "호",         "연집계룰": "sum",                       "정상범위": "≥0"},
        # 수요 데이터 — 소득(국민연금)
        {"표시명": "NPS 가입자수",            "컬럼명": "NPS_가입자수",           "단위": "명",         "연집계룰": "sum(시도집계)",              "정상범위": "≥0"},
        {"표시명": "NPS 1인당고지금액",       "컬럼명": "NPS_1인당고지금액",      "단위": "원",         "연집계룰": "가중평균(가입자수)",         "정상범위": ""},
        {"표시명": "NPS 사업장수",            "컬럼명": "NPS_사업장수",           "단위": "개",         "연집계룰": "sum(시도집계)",              "정상범위": "≥0"},
        {"표시명": "NPS 고용증감",            "컬럼명": "NPS_고용증감",           "단위": "명",         "연집계룰": "sum(시도집계)",              "정상범위": ""},
        # 수요 데이터 — 대출(BOK)
        {"표시명": "주담대 잔액",             "컬럼명": "주담대_잔액",            "단위": "십억원",     "연집계룰": "12월값(연말잔액)",           "정상범위": ""},
        {"표시명": "주담대 증감률",           "컬럼명": "주담대_증감률",          "단위": "%",          "연집계룰": "12월 전월비",               "정상범위": ""},
        {"표시명": "주담대 비중",             "컬럼명": "주담대_비중",            "단위": "%",          "연집계룰": "12월값",                    "정상범위": "0~100"},
        # 수요 데이터 — 자산(KOSIS)
        {"표시명": "가구 자산평균",           "컬럼명": "가구_자산평균",          "단위": "만원",       "연집계룰": "연간",                      "정상범위": ""},
        {"표시명": "가구 부채평균",           "컬럼명": "가구_부채평균",          "단위": "만원",       "연집계룰": "연간",                      "정상범위": ""},
        {"표시명": "가구 순자산",             "컬럼명": "가구_순자산",            "단위": "만원",       "연집계룰": "연간(자산-부채)",            "정상범위": ""},
        {"표시명": "가구 소득평균",           "컬럼명": "가구_소득평균",          "단위": "만원",       "연집계룰": "연간",                      "정상범위": ""},
        {"표시명": "부채/소득비율(DSR)",      "컬럼명": "DSR",                   "단위": "%",          "연집계룰": "연간(부채/소득×100)",        "정상범위": ""},
    ])

    with st.expander("변수 목록 및 메타데이터"):
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
                     "NPS_가입자수", "NPS_사업장수", "가구_소득평균"}

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

                # ① 변수 선택 → 수식에 삽입
                col_sel, col_ins = st.columns([5, 1])
                with col_sel:
                    sel_var = st.selectbox(
                        "변수 선택 후 [삽입] 클릭", numeric_cols_5, key=f"f5_selvar_{i}"
                    )
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
# Tab 5: 통계분석 (회귀 + 이상치)
# ============================
with main_tab5:
    sub_reg, sub_outlier = st.tabs(["회귀분석", "이상치 탐지"])

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
                st.plotly_chart(fig_imp, use_container_width=True)

            except (ValueError, ImportError) as e:
                st.error(str(e))
        else:
            st.info("독립변수를 1개 이상 선택하세요.")


# ============================
# Tab 5-2: 이상치 탐지 (통계분석 서브탭)
# ============================
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


# ============================
# Tab 6: 고급분석 (클러스터링 + Granger)
# ============================
with main_tab6:
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

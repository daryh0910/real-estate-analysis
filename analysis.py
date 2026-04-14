"""
부동산 가격분석 서비스 - 상관관계 분석 모듈
"""
import pandas as pd
import numpy as np
from scipy import stats

# PCHIP 보간용
try:
    from scipy.interpolate import PchipInterpolator
    HAS_SCIPY_INTERP = True
except ImportError:
    HAS_SCIPY_INTERP = False

# 다중회귀/Granger 인과검정용
try:
    import statsmodels.api as sm
    from statsmodels.tsa.stattools import grangercausalitytests
    HAS_STATSMODELS = True
except ImportError:
    HAS_STATSMODELS = False

# 클러스터링용
try:
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False


def correlation_matrix(df, columns=None):
    """
    전체 변수 간 피어슨 상관계수 행렬
    Returns: (corr_matrix, p_value_matrix)
    """
    if columns is None:
        columns = ["평균가격", "거래량", "평균단가_per_m2", "총인구", "GRDP", "인허가_호수"]
    cols = [c for c in columns if c in df.columns]
    numeric_df = df[cols].dropna()

    if numeric_df.empty or len(numeric_df) < 3:
        return pd.DataFrame(), pd.DataFrame()

    n = len(cols)
    corr = pd.DataFrame(np.zeros((n, n)), index=cols, columns=cols)
    pval = pd.DataFrame(np.ones((n, n)), index=cols, columns=cols)

    for i in range(n):
        for j in range(n):
            if i == j:
                corr.iloc[i, j] = 1.0
                pval.iloc[i, j] = 0.0
            elif i < j:
                valid = numeric_df[[cols[i], cols[j]]].dropna()
                if len(valid) >= 3:
                    r, p = stats.pearsonr(valid[cols[i]], valid[cols[j]])
                    corr.iloc[i, j] = r
                    corr.iloc[j, i] = r
                    pval.iloc[i, j] = p
                    pval.iloc[j, i] = p

    return corr, pval


def correlation_by_region(df, var_x="GRDP", var_y="평균가격"):
    """
    시도별 상관계수 계산
    Returns: DataFrame [시도, 상관계수, p_value, 데이터수]
    """
    results = []
    for sido, group in df.groupby("시도"):
        valid = group[[var_x, var_y]].dropna()
        if len(valid) >= 3:
            r, p = stats.pearsonr(valid[var_x], valid[var_y])
            results.append({
                "시도": sido,
                "상관계수": round(r, 4),
                "p_value": round(p, 4),
                "데이터수": len(valid),
            })
    return pd.DataFrame(results).sort_values("상관계수", ascending=False)


def correlation_by_period(df, var_x="GRDP", var_y="평균가격", period_col="연도"):
    """
    기간별(연도별) 상관계수 추이 (각 기간 내 시도 간 상관)
    Returns: DataFrame [기간, 상관계수, p_value, 데이터수]
    """
    results = []
    for period, group in df.groupby(period_col):
        valid = group[[var_x, var_y]].dropna()
        if len(valid) >= 3:
            r, p = stats.pearsonr(valid[var_x], valid[var_y])
            results.append({
                period_col: period,
                "상관계수": round(r, 4),
                "p_value": round(p, 4),
                "데이터수": len(valid),
            })
    return pd.DataFrame(results)


def scatter_analysis(df, var_x, var_y, group_col="시도"):
    """
    산점도 데이터 + 전체 회귀선 계산
    Returns: (scatter_df, slope, intercept, r_squared)
    """
    valid = df[[var_x, var_y, group_col]].dropna()
    if len(valid) < 3:
        return valid, None, None, None

    slope, intercept, r_value, p_value, std_err = stats.linregress(
        valid[var_x], valid[var_y]
    )
    return valid, slope, intercept, r_value ** 2


def rolling_correlation(df, var_x, var_y, window=12, sort_col="연월"):
    """
    이동 상관계수 (월별 데이터용, 전체 시도 합산 기준)
    Returns: DataFrame [연월, rolling_corr]
    """
    # 전체 시도 합산 월별 데이터
    monthly_agg = (
        df.groupby(sort_col)
        .agg({var_x: "mean", var_y: "mean"})
        .sort_index()
        .dropna()
    )
    if len(monthly_agg) < window:
        return pd.DataFrame()

    rolling_corr = (
        monthly_agg[var_x]
        .rolling(window)
        .corr(monthly_agg[var_y])
        .reset_index()
    )
    rolling_corr.columns = [sort_col, "rolling_corr"]
    return rolling_corr.dropna()


# ── 다중회귀 분석 ──────────────────────────────────────────────────────────

def multiple_regression(df, y_var, x_vars):
    """
    OLS 다중회귀 분석
    Args:
        df: 분석 대상 DataFrame
        y_var: 종속변수 컬럼명
        x_vars: 독립변수 컬럼명 리스트
    Returns:
        (model_summary_df, feature_importance_df, r_squared, adj_r_squared)
        - model_summary_df: [변수, 계수, 표준오차, t값, p값]
        - feature_importance_df: [변수, 중요도] (표준화 계수의 절대값)
        - r_squared: 결정계수
        - adj_r_squared: 수정 결정계수
    """
    if not HAS_STATSMODELS:
        raise ImportError("statsmodels가 설치되어 있지 않습니다. pip install statsmodels")

    # 사용할 컬럼만 추출 후 결측치 제거
    use_cols = [y_var] + list(x_vars)
    clean_df = df[use_cols].dropna()

    if len(clean_df) < 10:
        raise ValueError(f"관측치가 {len(clean_df)}개로 부족합니다 (최소 10개 필요)")

    y = clean_df[y_var]
    X = clean_df[x_vars]

    # 상수항 추가 후 OLS 적합
    X_with_const = sm.add_constant(X)
    model = sm.OLS(y, X_with_const).fit()

    # 모델 요약 DataFrame
    summary_data = []
    # 상수항 포함
    param_names = ["상수항"] + list(x_vars)
    for i, name in enumerate(param_names):
        summary_data.append({
            "변수": name,
            "계수": round(model.params.iloc[i], 6),
            "표준오차": round(model.bse.iloc[i], 6),
            "t값": round(model.tvalues.iloc[i], 4),
            "p값": round(model.pvalues.iloc[i], 4),
        })
    model_summary_df = pd.DataFrame(summary_data)

    # 변수 중요도: 표준화 계수의 절대값 (상수항 제외)
    # 표준화 계수 = 계수 * (X의 표준편차 / Y의 표준편차)
    y_std = y.std()
    importance_data = []
    for var in x_vars:
        x_std = X[var].std()
        # 해당 변수의 계수 (상수항 제외이므로 인덱스 조회)
        coef = model.params[var]
        standardized = abs(coef * x_std / y_std) if y_std != 0 else 0.0
        importance_data.append({
            "변수": var,
            "중요도": round(standardized, 4),
        })
    feature_importance_df = pd.DataFrame(importance_data).sort_values(
        "중요도", ascending=False
    ).reset_index(drop=True)

    return (
        model_summary_df,
        feature_importance_df,
        round(model.rsquared, 4),
        round(model.rsquared_adj, 4),
    )


# ── 이상치 탐지 ──────────────────────────────────────────────────────────

def detect_outliers(df, target_col, group_col="시도", method="zscore", threshold=2.5):
    """
    가격 변동 기반 이상치 지역/기간 탐지
    Args:
        df: 분석 대상 DataFrame
        target_col: 이상치 판별 대상 컬럼
        group_col: 그룹 컬럼 (기본: "시도")
        method: "zscore" 또는 "iqr"
        threshold: Z-score 방식의 임계값 (기본: 2.5)
    Returns:
        DataFrame — 원본 컬럼 + [이상치(bool), z_score]
    """
    result = df.copy()
    result["이상치"] = False
    result["z_score"] = np.nan

    for _, group in result.groupby(group_col):
        idx = group.index
        values = group[target_col]

        # 결측치가 있는 행은 건너뜀
        valid_mask = values.notna()
        valid_values = values[valid_mask]
        valid_idx = idx[valid_mask]

        if len(valid_values) < 3:
            continue

        mean = valid_values.mean()
        std = valid_values.std()

        if std == 0:
            result.loc[valid_idx, "z_score"] = 0.0
            continue

        # Z-score 계산 (두 방식 모두 사용)
        z_scores = (valid_values - mean) / std
        result.loc[valid_idx, "z_score"] = z_scores.round(4)

        if method == "zscore":
            # Z-score 방식: 절대값이 threshold 초과
            outlier_mask = z_scores.abs() > threshold
            result.loc[valid_idx[outlier_mask], "이상치"] = True

        elif method == "iqr":
            # IQR 방식
            q1 = valid_values.quantile(0.25)
            q3 = valid_values.quantile(0.75)
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            outlier_mask = (valid_values < lower) | (valid_values > upper)
            result.loc[valid_idx[outlier_mask], "이상치"] = True

    return result


# ── 지역 클러스터링 ────────────────────────────────────────────────────────

def cluster_regions(df, features, n_clusters=4, method="kmeans"):
    """
    시도별 특성 기반 클러스터링
    Args:
        df: 분석 대상 DataFrame
        features: 클러스터링에 사용할 변수 리스트
        n_clusters: 클러스터 수 (기본: 4)
        method: 클러스터링 방법 (기본: "kmeans")
    Returns:
        (clustered_df, cluster_centers_df)
        - clustered_df: [시도, cluster, ...features]
        - cluster_centers_df: [cluster, ...features] — 각 클러스터 중심값
    """
    if not HAS_SKLEARN:
        raise ImportError("scikit-learn이 설치되어 있지 않습니다. pip install scikit-learn")

    # 시도별 평균으로 집계
    agg_df = df.groupby("시도")[features].mean().dropna()

    if len(agg_df) < n_clusters:
        raise ValueError(
            f"시도 수({len(agg_df)})가 클러스터 수({n_clusters})보다 적습니다"
        )

    # 표준화
    scaler = StandardScaler()
    scaled = scaler.fit_transform(agg_df[features])

    # KMeans 클러스터링
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = kmeans.fit_predict(scaled)

    # 결과 DataFrame 구성
    clustered_df = agg_df.reset_index()
    clustered_df["cluster"] = labels

    # 클러스터 중심값 (원래 스케일로 역변환)
    centers_original = scaler.inverse_transform(kmeans.cluster_centers_)
    cluster_centers_df = pd.DataFrame(centers_original, columns=features)
    cluster_centers_df.insert(0, "cluster", range(n_clusters))

    # 컬럼 순서 정리: 시도, cluster, ...features
    clustered_df = clustered_df[["시도", "cluster"] + features].sort_values(
        "cluster"
    ).reset_index(drop=True)

    return clustered_df, cluster_centers_df


# ── Granger 인과성 검정 ────────────────────────────────────────────────────

def granger_causality_test(df, y_var, x_var, max_lag=4, group_col="시도"):
    """
    Granger 인과성 검정: x_var가 y_var를 Granger-cause 하는지 검정
    Args:
        df: 시계열 데이터가 포함된 DataFrame
        y_var: 종속변수 (결과 변수)
        x_var: 독립변수 (원인 변수)
        max_lag: 최대 시차 (기본: 4)
        group_col: 그룹 컬럼 (기본: "시도")
    Returns:
        DataFrame [시도, lag, F통계량, p값, 인과성(bool, p<0.05)]
    """
    if not HAS_STATSMODELS:
        raise ImportError("statsmodels가 설치되어 있지 않습니다. pip install statsmodels")

    results = []

    for sido, group in df.groupby(group_col):
        # 연월 기준 시간순 정렬
        if "연월" in group.columns:
            group = group.sort_values("연월")
        elif "연도" in group.columns:
            group = group.sort_values("연도")

        # 사용할 컬럼의 결측치 제거
        ts_data = group[[y_var, x_var]].dropna()

        # Granger 검정에는 최소 max_lag*2 + 1 정도의 관측치 필요
        min_obs = max_lag * 2 + 1
        if len(ts_data) < min_obs:
            continue

        try:
            # grangercausalitytests: 2열 배열 [y, x] 형태
            test_result = grangercausalitytests(
                ts_data[[y_var, x_var]].values,
                maxlag=max_lag,
                verbose=False,
            )

            # 각 lag별 결과 수집
            for lag in range(1, max_lag + 1):
                # F-test 결과 사용 (ssr_ftest)
                f_stat = test_result[lag][0]["ssr_ftest"][0]
                p_value = test_result[lag][0]["ssr_ftest"][1]
                results.append({
                    "시도": sido,
                    "lag": lag,
                    "F통계량": round(f_stat, 4),
                    "p값": round(p_value, 4),
                    "인과성": p_value < 0.05,
                })
        except Exception:
            # 특이 행렬 등 수치 오류 발생 시 해당 시도 건너뜀
            continue

    return pd.DataFrame(results)


# ── 밸류 스코어 ────────────────────────────────────────────────────────────

def compute_value_score(apt_df, jeonse_df, nps_df, nts_df=None, year=None):
    """
    시군구별 저평가/고평가 밸류 스코어 계산

    Args:
        apt_df: 매매 실거래 DataFrame [지역코드, 시도, 연도, 평균가격, 거래량, 평균단가_per_m2]
        jeonse_df: 전세 실거래 DataFrame [지역코드, 시도, 연도, 보증금평균, 보증금단가_per_m2]
        nps_df: NPS DataFrame [지역코드, 시도, 연도, NPS_가입자수, NPS_1인당고지금액]
        nts_df: 국세청 근로소득 DataFrame (optional) [지역코드, 시도, 연도, 1인당총급여_백만원]
        year: 기준 연도. None이면 최신 연도.

    Returns:
        DataFrame [지역코드, 시군구명, 시도, 밸류스코어, 전세가율, PIR_NPS, 거래회전율_proxy, 가격모멘텀]
    """
    from data_loader import get_sigungu_name

    # 기준 연도 결정
    if year is None:
        year = int(apt_df["연도"].max())

    # ── 매매 집계 (해당 연도 시군구별)
    apt_yr = apt_df[apt_df["연도"] == year].copy()
    apt_agg = (
        apt_yr.groupby(["지역코드", "시도"], as_index=False)
        .agg(평균가격=("평균가격", "mean"), 평균단가=("평균단가_per_m2", "mean"), 거래량=("거래량", "sum"))
    )

    # ── 전세 집계
    jeonse_yr = jeonse_df[jeonse_df["연도"] == year].copy() if "연도" in jeonse_df.columns else pd.DataFrame()
    if not jeonse_yr.empty and "보증금평균" in jeonse_yr.columns:
        jeonse_agg = (
            jeonse_yr.groupby("지역코드", as_index=False)
            .agg(보증금평균=("보증금평균", "mean"))
        )
        apt_agg = apt_agg.merge(jeonse_agg, on="지역코드", how="left")
    else:
        apt_agg["보증금평균"] = np.nan

    # 전세가율 (%)
    apt_agg["전세가율"] = np.where(
        apt_agg["평균가격"] > 0,
        apt_agg["보증금평균"] / apt_agg["평균가격"] * 100,
        np.nan,
    )

    # ── NPS 집계 (연도별 가중평균 — 가입자수 기준)
    nps_col_year = "연도" if "연도" in nps_df.columns else None
    nps_col_amount = "NPS_1인당고지금액" if "NPS_1인당고지금액" in nps_df.columns else None
    nps_col_sub = "NPS_가입자수" if "NPS_가입자수" in nps_df.columns else None

    if nps_col_year and nps_col_amount and nps_col_sub:
        nps_yr = nps_df[nps_df[nps_col_year] == year].copy()
        # NPS에 해당 연도가 없으면 가장 가까운 이전 연도 사용
        if nps_yr.empty:
            avail_years = sorted(nps_df[nps_col_year].unique())
            prev_years = [y for y in avail_years if y <= year]
            fallback_year = prev_years[-1] if prev_years else (avail_years[-1] if avail_years else None)
            if fallback_year is not None:
                nps_yr = nps_df[nps_df[nps_col_year] == fallback_year].copy()
        # 시군구별 가중평균
        nps_yr = nps_yr.dropna(subset=[nps_col_amount, nps_col_sub])
        nps_yr["_weighted"] = nps_yr[nps_col_amount] * nps_yr[nps_col_sub]
        nps_agg = nps_yr.groupby("지역코드", as_index=False).agg(
            _w_sum=("_weighted", "sum"),
            _sub_sum=(nps_col_sub, "sum"),
        )
        nps_agg["NPS_1인당고지금액"] = np.where(
            nps_agg["_sub_sum"] > 0,
            nps_agg["_w_sum"] / nps_agg["_sub_sum"],
            np.nan,
        )
        apt_agg = apt_agg.merge(nps_agg[["지역코드", "NPS_1인당고지금액"]], on="지역코드", how="left")
    else:
        apt_agg["NPS_1인당고지금액"] = np.nan

    # PIR_NPS: 평균가격(만원) / (NPS 월고지금액(원) → 연소득(만원))
    # NPS_1인당고지금액 단위가 원이므로: 연소득(만원) = 월고지금액 * 12 / 10000
    apt_agg["PIR_NPS"] = np.where(
        apt_agg["NPS_1인당고지금액"] > 0,
        apt_agg["평균가격"] / (apt_agg["NPS_1인당고지금액"] * 12 / 10000),
        np.nan,
    )

    # 거래회전율 proxy = 거래량 (시도 내 min-max 정규화에서 활용)
    apt_agg["거래회전율_proxy"] = apt_agg["거래량"]

    # ── 가격모멘텀: (year 가격 - year-2 가격) / year-2 가격 * 100
    apt_prev = apt_df[apt_df["연도"] == year - 2].copy()
    if not apt_prev.empty and "평균가격" in apt_prev.columns:
        apt_prev_agg = apt_prev.groupby("지역코드", as_index=False).agg(평균가격_prev=("평균가격", "mean"))
        apt_agg = apt_agg.merge(apt_prev_agg, on="지역코드", how="left")
        apt_agg["가격모멘텀"] = np.where(
            apt_agg["평균가격_prev"] > 0,
            (apt_agg["평균가격"] - apt_agg["평균가격_prev"]) / apt_agg["평균가격_prev"] * 100,
            np.nan,
        )
    else:
        apt_agg["가격모멘텀"] = np.nan

    # ── 시도 내 z-score 정규화 헬퍼
    def zscore_within_sido(df, col):
        """시도 내 z-score 정규화 (결측치는 그대로 유지)"""
        result = df[col].copy().astype(float)
        for sido, grp in df.groupby("시도"):
            idx = grp.index
            vals = grp[col].dropna()
            if len(vals) < 2:
                continue
            mu, sigma = vals.mean(), vals.std()
            if sigma > 0:
                result.loc[idx] = (df.loc[idx, col] - mu) / sigma
        return result

    apt_agg["전세가율_z"] = zscore_within_sido(apt_agg, "전세가율")
    apt_agg["PIR_inv_z"] = zscore_within_sido(apt_agg.assign(PIR_inv=1 / apt_agg["PIR_NPS"].replace(0, np.nan)), "PIR_inv")
    apt_agg["거래회전율_z"] = zscore_within_sido(apt_agg, "거래회전율_proxy")
    apt_agg["모멘텀_neg_z"] = zscore_within_sido(apt_agg.assign(모멘텀_neg=-apt_agg["가격모멘텀"]), "모멘텀_neg")

    # ── 밸류스코어 가중합
    apt_agg["밸류스코어"] = (
        0.35 * apt_agg["전세가율_z"].fillna(0)
        + 0.30 * apt_agg["PIR_inv_z"].fillna(0)
        + 0.15 * apt_agg["거래회전율_z"].fillna(0)
        + 0.20 * apt_agg["모멘텀_neg_z"].fillna(0)
    )

    # ── 시군구명 추가
    apt_agg["시군구명"] = apt_agg["지역코드"].apply(get_sigungu_name)

    # 반환 컬럼 선택
    result_cols = ["지역코드", "시군구명", "시도", "밸류스코어", "전세가율", "PIR_NPS", "거래회전율_proxy", "가격모멘텀"]
    return apt_agg[[c for c in result_cols if c in apt_agg.columns]].sort_values("밸류스코어", ascending=False).reset_index(drop=True)


# ── 시장 온도 스코어 ────────────────────────────────────────────────────────

def compute_market_temperature(analysis_df):
    """
    시장 종합 온도 스코어 계산 (0~100)

    Args:
        analysis_df: 병합된 분석 DataFrame (시도별, 연도별)

    Returns:
        tuple (score: float, delta: float, breakdown: dict)
        - score: 0~100 (0=침체, 100=과열)
        - delta: 전년 대비 변동분
        - breakdown: 각 지표별 기여도 dict
    """
    def _calc_score(df_yr):
        """단일 연도 데이터에서 온도 스코어 계산"""
        weights = {}
        scores = {}

        # 1. KB 매수우위지수 (0~200): 낮을수록 매수자 우세 → 과열
        if "KB_매수우위지수" in df_yr.columns:
            vals = df_yr["KB_매수우위지수"].dropna()
            if not vals.empty:
                mean_val = vals.mean()
                # 반전: 높으면(매도자우세) 침체, 낮으면(매수자우세) 과열
                # 정규화: 0=침체(val=200), 100=과열(val=0)
                scores["KB_매수우위"] = max(0, min(100, (200 - mean_val) / 200 * 100))
                weights["KB_매수우위"] = 0.25

        # 2. 주택가격전망CSI (0~200): 높을수록 상승기대 → 과열
        if "주택가격전망CSI" in df_yr.columns:
            vals = df_yr["주택가격전망CSI"].dropna()
            if not vals.empty:
                mean_val = vals.mean()
                scores["CSI"] = max(0, min(100, mean_val / 200 * 100))
                weights["CSI"] = 0.20

        # 3. 가격변화율_YoY (%): clamp(-20~+20) → 0~100
        if "가격변화율_YoY" in df_yr.columns:
            vals = df_yr["가격변화율_YoY"].dropna()
            if not vals.empty:
                mean_val = vals.mean()
                clamped = max(-20, min(20, mean_val))
                scores["가격변화율"] = (clamped + 20) / 40 * 100
                weights["가격변화율"] = 0.25

        # 4. 거래회전율 (있으면 매매거래량 사용): min-max 정규화
        turnover_col = next((c for c in ["거래회전율", "매매_거래량", "거래량"] if c in df_yr.columns), None)
        if turnover_col:
            vals = df_yr[turnover_col].dropna()
            if len(vals) >= 2:
                v_min, v_max = vals.min(), vals.max()
                if v_max > v_min:
                    mean_val = vals.mean()
                    scores["거래회전율"] = (mean_val - v_min) / (v_max - v_min) * 100
                    weights["거래회전율"] = 0.15

        # 5. 미분양_호수: 반전 정규화 (적을수록 과열)
        if "미분양_호수" in df_yr.columns:
            vals = df_yr["미분양_호수"].dropna()
            if len(vals) >= 2:
                v_min, v_max = vals.min(), vals.max()
                if v_max > v_min:
                    mean_val = vals.mean()
                    # 반전: 미분양 적을수록(=min에 가까울수록) 과열 → 100
                    scores["미분양"] = (v_max - mean_val) / (v_max - v_min) * 100
                    weights["미분양"] = 0.15

        if not weights:
            return 50.0, {}

        # 가중치 재배분 (누락 지표 제외)
        total_weight = sum(weights.values())
        final_score = sum(scores[k] * weights[k] for k in scores) / total_weight
        breakdown = {k: round(scores[k], 2) for k in scores}
        return round(final_score, 2), breakdown

    if analysis_df.empty:
        return 50.0, 0.0, {}

    # 최신 연도
    year_col = "연도" if "연도" in analysis_df.columns else None
    if year_col is None:
        return 50.0, 0.0, {}

    latest_year = int(analysis_df[year_col].max())
    df_latest = analysis_df[analysis_df[year_col] == latest_year]
    score, breakdown = _calc_score(df_latest)

    # 전년도 delta
    df_prev = analysis_df[analysis_df[year_col] == latest_year - 1]
    if not df_prev.empty:
        prev_score, _ = _calc_score(df_prev)
        delta = round(score - prev_score, 2)
    else:
        delta = 0.0

    return score, delta, breakdown


# ── 소득5분위 → 퍼센타일 보간 ─────────────────────────────────────────────

def interpolate_quintile_to_percentile(quintile_df, year, columns=None):
    """
    KOSIS 소득5분위 데이터를 1% 단위(1~99)로 PCHIP 보간

    Args:
        quintile_df: DataFrame [연도, 소득분위, 가구_자산평균, 가구_부채평균, ...]
        year: 보간 대상 연도
        columns: 보간할 컬럼 리스트. None이면 숫자형 컬럼 전부.

    Returns:
        DataFrame [percentile(1~99), 각 컬럼 보간값]
    """
    if not HAS_SCIPY_INTERP:
        raise ImportError("scipy.interpolate가 설치되어 있지 않습니다. pip install scipy")

    if quintile_df.empty:
        return pd.DataFrame()

    # 해당 연도, "전체" 제외한 5개 분위 추출
    year_col = "연도" if "연도" in quintile_df.columns else quintile_df.columns[0]
    quintile_col = "소득분위" if "소득분위" in quintile_df.columns else quintile_df.columns[1]

    df_yr = quintile_df[quintile_df[year_col] == year].copy()
    # "전체" 등 비분위 행 제거
    df_yr = df_yr[~df_yr[quintile_col].astype(str).str.contains("전체|평균|합계", na=False)]

    if len(df_yr) == 0:
        return pd.DataFrame()

    # 소득분위 숫자 추출 (1~5분위)
    df_yr["_분위_num"] = df_yr[quintile_col].astype(str).str.extract(r"(\d)").astype(float)
    df_yr = df_yr.dropna(subset=["_분위_num"]).sort_values("_분위_num").reset_index(drop=True)

    # 대표 퍼센타일 매핑
    quintile_percentiles = {1: 10, 2: 30, 3: 50, 4: 70, 5: 90}
    df_yr["_pct"] = df_yr["_분위_num"].map(quintile_percentiles)
    df_yr = df_yr.dropna(subset=["_pct"])

    if len(df_yr) < 3:
        return pd.DataFrame()

    # 보간 대상 컬럼 결정
    if columns is None:
        columns = [c for c in df_yr.select_dtypes(include=[np.number]).columns
                   if c not in [year_col, "_분위_num", "_pct"]]

    x_known = df_yr["_pct"].values
    target_pct = np.arange(1, 100)  # 1~99
    result = {"percentile": target_pct}

    for col in columns:
        if col not in df_yr.columns:
            continue
        y_known = df_yr[col].values.astype(float)
        # 결측치가 있으면 해당 포인트 제외
        valid_mask = ~np.isnan(y_known)
        if valid_mask.sum() < 3:
            result[col] = np.full(len(target_pct), np.nan)
            continue
        interp_fn = PchipInterpolator(x_known[valid_mask], y_known[valid_mask])
        y_interp = interp_fn(target_pct)
        # 음수 클램핑 (자산/소득은 음수 불가)
        y_interp = np.maximum(y_interp, 0)
        result[col] = y_interp

    return pd.DataFrame(result)


# ── 퍼센타일별 구매력 계산 ─────────────────────────────────────────────────

def compute_purchasing_power(percentile_df, base_rate=3.5, dsr_limit=0.40, loan_years=30):
    """
    퍼센타일별 구매력(구매가능가격) 계산

    Args:
        percentile_df: interpolate_quintile_to_percentile() 결과
        base_rate: 대출 금리 (%, 예: 3.5)
        dsr_limit: DSR 한도 (0~1, 예: 0.40)
        loan_years: 대출 기간 (년)

    Returns:
        DataFrame [percentile, 순자산, 연소득, 대출가능액, 구매력(만원)]
    """
    if percentile_df.empty:
        return pd.DataFrame()

    df = percentile_df.copy()

    # 순자산 계산
    if "가구_순자산" in df.columns:
        df["순자산"] = df["가구_순자산"]
    elif "가구_자산평균" in df.columns and "가구_부채평균" in df.columns:
        df["순자산"] = df["가구_자산평균"] - df["가구_부채평균"]
    else:
        df["순자산"] = 0.0

    # 연소득
    income_col = next((c for c in ["가구_소득평균", "가구소득평균", "소득평균"] if c in df.columns), None)
    df["연소득"] = df[income_col] if income_col else 0.0

    # 현재 DSR (이미 % 저장 가정 → 0~1로 변환)
    dsr_col = next((c for c in ["DSR", "부채상환비율", "가구_DSR"] if c in df.columns), None)
    if dsr_col:
        current_dsr = df[dsr_col] / 100.0
    else:
        current_dsr = pd.Series(0.0, index=df.index)

    # 여유 DSR
    slack_dsr = (dsr_limit - current_dsr).clip(lower=0)

    # 연 상환 가능액 (만원)
    annual_repayment = df["연소득"] * slack_dsr

    # 대출가능액 계산 (원리금균등상환, 단위: 만원)
    monthly_rate = base_rate / 100 / 12
    n_payments = loan_years * 12
    if monthly_rate > 0:
        # PV = PMT * [1 - (1+r)^(-n)] / r
        loan_possible = (annual_repayment / 12) * (1 - (1 + monthly_rate) ** (-n_payments)) / monthly_rate
    else:
        loan_possible = annual_repayment * loan_years

    df["대출가능액"] = loan_possible.fillna(0)
    df["구매력(만원)"] = (df["순자산"] + df["대출가능액"]).clip(lower=0)

    result_cols = ["percentile", "순자산", "연소득", "대출가능액", "구매력(만원)"]
    return df[[c for c in result_cols if c in df.columns]].reset_index(drop=True)


# ── 시군구 급지 순위 ───────────────────────────────────────────────────────

def rank_sigungu_grade(apt_df, nps_df, nts_df=None, year=None):
    """
    시군구 급지 순위 산출

    Args:
        apt_df, nps_df, nts_df: 데이터프레임
        year: 기준 연도

    Returns:
        DataFrame [지역코드, 시군구명, 시도, 급지순위, 급지스코어,
                   평균단가, 소득수준, 거래량, 3yr성장률]
    """
    from data_loader import get_sigungu_name

    if year is None:
        year = int(apt_df["연도"].max())

    # ── 매매 집계 (해당 연도)
    apt_yr = apt_df[apt_df["연도"] == year].copy()
    agg = (
        apt_yr.groupby(["지역코드", "시도"], as_index=False)
        .agg(평균단가=("평균단가_per_m2", "mean"), 거래량=("거래량", "sum"), 평균가격=("평균가격", "mean"))
    )

    # ── NPS 소득 집계
    nps_col_year = "연도" if "연도" in nps_df.columns else None
    nps_col_amount = "NPS_1인당고지금액" if "NPS_1인당고지금액" in nps_df.columns else None
    nps_col_sub = "NPS_가입자수" if "NPS_가입자수" in nps_df.columns else None

    if nps_col_year and nps_col_amount and nps_col_sub:
        nps_yr = nps_df[nps_df[nps_col_year] == year].copy()
        # NPS에 해당 연도가 없으면 가장 가까운 이전 연도 사용
        if nps_yr.empty:
            avail_years = sorted(nps_df[nps_col_year].unique())
            prev_years = [y for y in avail_years if y <= year]
            fallback_year = prev_years[-1] if prev_years else (avail_years[-1] if avail_years else None)
            if fallback_year is not None:
                nps_yr = nps_df[nps_df[nps_col_year] == fallback_year].copy()
        nps_yr = nps_yr.dropna(subset=[nps_col_amount, nps_col_sub])
        nps_yr["_w"] = nps_yr[nps_col_amount] * nps_yr[nps_col_sub]
        nps_agg = nps_yr.groupby("지역코드", as_index=False).agg(
            _w_sum=("_w", "sum"), _sub_sum=(nps_col_sub, "sum")
        )
        nps_agg["소득수준"] = np.where(
            nps_agg["_sub_sum"] > 0, nps_agg["_w_sum"] / nps_agg["_sub_sum"], np.nan
        )
        agg = agg.merge(nps_agg[["지역코드", "소득수준"]], on="지역코드", how="left")
    elif nts_df is not None and "1인당총급여_백만원" in nts_df.columns:
        # NPS 없으면 NTS 소득 사용
        nts_yr = nts_df[nts_df["연도"] == year].copy() if "연도" in nts_df.columns else pd.DataFrame()
        if not nts_yr.empty:
            nts_agg = nts_yr.groupby("지역코드", as_index=False).agg(소득수준=("1인당총급여_백만원", "mean"))
            agg = agg.merge(nts_agg, on="지역코드", how="left")
        else:
            agg["소득수준"] = np.nan
    else:
        agg["소득수준"] = np.nan

    # ── 3년 성장률
    apt_3yr_ago = apt_df[apt_df["연도"] == year - 3].copy()
    if not apt_3yr_ago.empty:
        apt_3yr_agg = apt_3yr_ago.groupby("지역코드", as_index=False).agg(평균가격_3yr=("평균가격", "mean"))
        agg = agg.merge(apt_3yr_agg, on="지역코드", how="left")
        agg["3yr성장률"] = np.where(
            agg["평균가격_3yr"] > 0,
            (agg["평균가격"] - agg["평균가격_3yr"]) / agg["평균가격_3yr"] * 100,
            np.nan,
        )
    else:
        agg["3yr성장률"] = np.nan

    # ── 전국 min-max 정규화
    def minmax_norm(series):
        v_min, v_max = series.min(), series.max()
        if v_max == v_min:
            return pd.Series(0.5, index=series.index)
        return (series - v_min) / (v_max - v_min)

    agg["단가_norm"] = minmax_norm(agg["평균단가"].fillna(agg["평균단가"].median()))
    agg["소득_norm"] = minmax_norm(agg["소득수준"].fillna(agg["소득수준"].median()))
    agg["거래량_norm"] = minmax_norm(agg["거래량"].fillna(0))
    agg["성장률_norm"] = minmax_norm(agg["3yr성장률"].fillna(agg["3yr성장률"].median()))

    # 급지스코어
    agg["급지스코어"] = (
        0.40 * agg["단가_norm"]
        + 0.25 * agg["소득_norm"]
        + 0.20 * agg["거래량_norm"]
        + 0.15 * agg["성장률_norm"]
    )

    agg = agg.sort_values("급지스코어", ascending=False).reset_index(drop=True)
    agg["급지순위"] = agg.index + 1
    agg["시군구명"] = agg["지역코드"].apply(get_sigungu_name)

    result_cols = ["지역코드", "시군구명", "시도", "급지순위", "급지스코어", "평균단가", "소득수준", "거래량", "3yr성장률"]
    return agg[[c for c in result_cols if c in agg.columns]].reset_index(drop=True)


# ── 소득 퍼센타일 → 시군구 급지 매칭 ─────────────────────────────────────

def match_income_to_property(purchasing_power_df, grade_df):
    """
    소득 퍼센타일 → 시군구 급지 매칭

    Args:
        purchasing_power_df: compute_purchasing_power() 결과
        grade_df: rank_sigungu_grade() 결과

    Returns:
        DataFrame [percentile_group, 구매력_대표, 매칭급지순위, 매칭시군구목록, 시장가격, 갭]
    """
    if purchasing_power_df.empty or grade_df.empty:
        return pd.DataFrame()

    # grade_df를 급지순위 오름차순 정렬 (1 = 최고급지)
    grade_sorted = grade_df.sort_values("급지순위").reset_index(drop=True)

    # apt 가격 컬럼 확인 (rank_sigungu_grade에서 평균가격 보존 여부 체크)
    price_col = next((c for c in ["평균가격", "평균단가"] if c in grade_sorted.columns), None)

    results = []
    # 10% 구간별 그룹핑
    bins = list(range(1, 100, 10)) + [100]
    labels = [f"{b}~{min(b+9, 99)}%" for b in bins[:-1]]

    for i, (lo, hi) in enumerate(zip(bins[:-1], bins[1:])):
        grp = purchasing_power_df[
            (purchasing_power_df["percentile"] >= lo) &
            (purchasing_power_df["percentile"] < hi)
        ]
        if grp.empty:
            continue

        rep_power = grp["구매력(만원)"].median()

        # 구매력으로 살 수 있는 시군구 (평균가격 <= 구매력)
        if price_col:
            affordable = grade_sorted[grade_sorted[price_col] <= rep_power]
        else:
            affordable = pd.DataFrame()

        if not affordable.empty:
            best_grade = int(affordable["급지순위"].min())
            sigungu_list = affordable[affordable["급지순위"] == best_grade]["시군구명"].tolist()
            market_price = affordable.loc[affordable["급지순위"] == best_grade, price_col].median() if price_col else np.nan
            gap = rep_power - market_price if not np.isnan(market_price) else np.nan
        else:
            best_grade = None
            sigungu_list = []
            market_price = np.nan
            gap = np.nan

        results.append({
            "percentile_group": labels[i],
            "구매력_대표(만원)": round(rep_power, 0),
            "매칭급지순위": best_grade,
            "매칭시군구목록": ", ".join(sigungu_list[:5]) if sigungu_list else "구매불가",
            "시장가격_중앙값(만원)": round(market_price, 0) if not np.isnan(market_price) else None,
            "갭(만원)": round(gap, 0) if gap is not None and not np.isnan(gap) else None,
        })

    return pd.DataFrame(results)

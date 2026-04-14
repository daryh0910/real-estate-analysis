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

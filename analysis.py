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

# Prophet 시계열 예측용
try:
    from prophet import Prophet
    HAS_PROPHET = True
except ImportError:
    HAS_PROPHET = False

# 다중회귀/Granger 인과검정용
try:
    import statsmodels.api as sm
    from statsmodels.tsa.stattools import adfuller, grangercausalitytests
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


# ── 전략 연구 / 지역검색기 공통 엔진 ────────────────────────────────────────

def _infer_time_col(df):
    if "연월" in df.columns:
        return "연월"
    if "연도" in df.columns:
        return "연도"
    return None


def _pct_change_by_group(df, group_col, value_col, periods=1):
    if value_col not in df.columns:
        return pd.Series(np.nan, index=df.index)
    return df.groupby(group_col)[value_col].pct_change(periods=periods, fill_method=None) * 100


def _series_is_stationary(series, alpha=0.05):
    """ADF 검정으로 정상성 여부를 보수적으로 확인한다."""
    if not HAS_STATSMODELS:
        return None
    clean = pd.Series(series).replace([np.inf, -np.inf], np.nan).dropna()
    if len(clean) < 8 or clean.nunique() <= 1:
        return None
    try:
        return bool(adfuller(clean, autolag="AIC")[1] <= alpha)
    except Exception:
        return None


def _stationary_series(series, alpha=0.05):
    """비정상 시계열이면 1차 차분해 허위 선행 신호를 줄인다."""
    clean = pd.Series(series).astype(float).replace([np.inf, -np.inf], np.nan)
    stationary = _series_is_stationary(clean, alpha=alpha)
    if stationary is False:
        return clean.diff(), "ADF차분"
    return clean, "원자료" if stationary is True else "ADF미확인"


def _group_transform(df, group_col, col, func):
    if group_col and group_col in df.columns:
        return df.groupby(group_col, group_keys=False)[col].transform(func)
    return func(df[col])


def rolling_consecutive_change(df, col, n=3, direction="up", group_col=None, min_change=0):
    """
    n기간 연속 상승/하락 여부를 계산한다.

    min_change는 각 기간 변화율 기준(%)이다. 예: n=3, direction="up", min_change=0이면
    3기간 연속 전기 대비 상승한 행만 True다.
    """
    if df.empty or col not in df.columns:
        return pd.Series(False, index=df.index)
    n = int(max(1, n))
    min_change = float(min_change or 0)

    def _calc(s):
        pct = pd.to_numeric(s, errors="coerce").pct_change(fill_method=None) * 100
        hit = pct >= min_change if direction == "up" else pct <= -abs(min_change)
        return hit.rolling(n, min_periods=n).sum().eq(n)

    return _group_transform(df, group_col, col, _calc).fillna(False).astype(bool)


def vs_moving_avg(df, col, window=12, direction="above", threshold_pct=0, group_col=None):
    """현재 값이 이동평균보다 일정 비율 이상 위/아래인지 계산한다."""
    if df.empty or col not in df.columns:
        return pd.Series(False, index=df.index)
    window = int(max(1, window))
    threshold_pct = float(threshold_pct or 0)

    def _calc(s):
        numeric = pd.to_numeric(s, errors="coerce")
        ma = numeric.rolling(window, min_periods=window).mean()
        ratio = (numeric / ma - 1) * 100
        return ratio >= threshold_pct if direction == "above" else ratio <= -abs(threshold_pct)

    return _group_transform(df, group_col, col, _calc).fillna(False).astype(bool)


def pct_from_peak(df, col, window=None, group_col=None):
    """현재 값이 최근/누적 고점 대비 몇 % 아래인지 계산한다. 고점이면 0, 하락이면 음수다."""
    if df.empty or col not in df.columns:
        return pd.Series(np.nan, index=df.index)
    window = int(window) if window else None

    def _calc(s):
        numeric = pd.to_numeric(s, errors="coerce")
        peak = numeric.rolling(window, min_periods=1).max() if window else numeric.cummax()
        return (numeric / peak - 1) * 100

    return _group_transform(df, group_col, col, _calc)


def peak_drawdown_then_rebound(df, col, drawdown_pct=-20, rebound_pct=5, window=None, group_col=None):
    """고점 대비 충분히 하락한 뒤 직전 저점에서 반등했는지 계산한다."""
    if df.empty or col not in df.columns:
        return pd.Series(False, index=df.index)
    drawdown_pct = float(drawdown_pct)
    rebound_pct = float(rebound_pct)
    window = int(window) if window else None

    def _calc(s):
        numeric = pd.to_numeric(s, errors="coerce")
        peak = numeric.rolling(window, min_periods=1).max() if window else numeric.cummax()
        drawdown = (numeric / peak - 1) * 100
        trough = numeric.cummin()
        if window:
            trough = numeric.rolling(window, min_periods=1).min()
        rebound = (numeric / trough - 1) * 100
        return (drawdown <= drawdown_pct) & (rebound >= rebound_pct)

    return _group_transform(df, group_col, col, _calc).fillna(False).astype(bool)


def _compare_series(s, op, value, value2=None):
    try:
        if op == ">":
            return s.astype(float) > float(value)
        if op == ">=":
            return s.astype(float) >= float(value)
        if op == "<":
            return s.astype(float) < float(value)
        if op == "<=":
            return s.astype(float) <= float(value)
        if op == "between":
            lo, hi = sorted([float(value), float(value2)])
            return s.astype(float).between(lo, hi)
        if op == "==":
            return s.astype(str) == str(value)
        if op == "contains":
            return s.astype(str).str.contains(str(value), na=False)
    except Exception:
        pass
    return pd.Series(False, index=s.index)


def compute_lead_lag_signal(
    df,
    sale_col="평균가격",
    jeonse_col="전세_보증금평균",
    group_col="시도",
    time_col=None,
    max_lag=12,
    use_pct_change=True,
):
    """
    전세와 매매 중 무엇이 먼저 움직이는지 계산한다.

    화면에서는 "선행 신호"로 표시하고, 내부적으로는 시차 상관과 Granger 검정을 함께 사용한다.
    lag > 0: 전세가 lag기간 먼저 움직인 뒤 매매가 따라온 패턴
    lag < 0: 매매가 abs(lag)기간 먼저 움직인 뒤 전세가 따라온 패턴
    """
    time_col = time_col or _infer_time_col(df)
    required = [group_col, sale_col, jeonse_col]
    if time_col:
        required.append(time_col)
    if any(c not in df.columns for c in required):
        return pd.DataFrame()

    rows = []
    max_lag = int(max(1, max_lag))

    for name, group in df[required].dropna(subset=[sale_col, jeonse_col]).groupby(group_col):
        group = group.sort_values(time_col).copy() if time_col else group.copy()
        if use_pct_change:
            sale = group[sale_col].astype(float).pct_change() * 100
            jeonse = group[jeonse_col].astype(float).pct_change() * 100
            sale_preprocess = "변화율"
            jeonse_preprocess = "변화율"
        else:
            sale, sale_preprocess = _stationary_series(group[sale_col])
            jeonse, jeonse_preprocess = _stationary_series(group[jeonse_col])

        lag_rows = []
        for lag in range(-max_lag, max_lag + 1):
            shifted_jeonse = jeonse.shift(lag)
            valid = pd.concat([sale, shifted_jeonse], axis=1).replace([np.inf, -np.inf], np.nan).dropna()
            if len(valid) < 6 or valid.iloc[:, 0].nunique() <= 1 or valid.iloc[:, 1].nunique() <= 1:
                continue
            corr, p_value = stats.pearsonr(valid.iloc[:, 0], valid.iloc[:, 1])
            lag_rows.append({
                "지역": name,
                "시차": lag,
                "같이움직인정도": round(float(corr), 4),
                "통계신뢰도": round(float(1 - p_value), 4),
                "p값": round(float(p_value), 4),
                "표본수": int(len(valid)),
            })

        if not lag_rows:
            continue

        lag_df = pd.DataFrame(lag_rows)
        best = lag_df.iloc[lag_df["같이움직인정도"].abs().argmax()].copy()
        best_lag = int(best["시차"])
        if best_lag > 0:
            direction = "전세 선행"
            summary = f"전세가 {best_lag}기간 먼저 움직인 뒤 매매가 따라온 패턴"
        elif best_lag < 0:
            direction = "매매 선행"
            summary = f"매매가 {abs(best_lag)}기간 먼저 움직인 뒤 전세가 따라온 패턴"
        else:
            direction = "동행"
            summary = "전세와 매매가 거의 같은 시점에 움직인 패턴"

        consistency = "높음" if abs(best["같이움직인정도"]) >= 0.6 and best["통계신뢰도"] >= 0.95 else (
            "보통" if abs(best["같이움직인정도"]) >= 0.4 and best["통계신뢰도"] >= 0.9 else "낮음"
        )

        jeonse_to_sale_p = np.nan
        sale_to_jeonse_p = np.nan
        if HAS_STATSMODELS and len(group[[sale_col, jeonse_col]].dropna()) >= max_lag * 2 + 3:
            try:
                js = grangercausalitytests(group[[sale_col, jeonse_col]].dropna().values, maxlag=max_lag)
                jeonse_to_sale_p = min(float(js[i][0]["ssr_ftest"][1]) for i in range(1, max_lag + 1))
            except Exception:
                pass
            try:
                sj = grangercausalitytests(group[[jeonse_col, sale_col]].dropna().values, maxlag=max_lag)
                sale_to_jeonse_p = min(float(sj[i][0]["ssr_ftest"][1]) for i in range(1, max_lag + 1))
            except Exception:
                pass

        rows.append({
            "지역": name,
            "선행방향": direction,
            "먼저움직인기간": abs(best_lag),
            "최적시차": best_lag,
            "같이움직인정도": best["같이움직인정도"],
            "통계신뢰도": best["통계신뢰도"],
            "반복성": consistency,
            "표본수": int(best["표본수"]),
            "전세→매매_p값": round(jeonse_to_sale_p, 4) if pd.notna(jeonse_to_sale_p) else np.nan,
            "매매→전세_p값": round(sale_to_jeonse_p, 4) if pd.notna(sale_to_jeonse_p) else np.nan,
            "가격전처리": sale_preprocess,
            "전세전처리": jeonse_preprocess,
            "요약": summary,
        })

    result = pd.DataFrame(rows)
    if result.empty:
        return result
    result["_반복성순위"] = result["반복성"].map({"높음": 0, "보통": 1, "낮음": 2}).fillna(3)
    result = result.sort_values(["_반복성순위", "통계신뢰도"], ascending=[True, False]).drop(columns="_반복성순위")
    return result.reset_index(drop=True)


def prepare_screener_dataset(df, lead_lag_df=None, group_col="시도", time_col=None):
    """지역검색기와 전략검증에서 쓰는 최신 스냅샷 + 변화율 데이터."""
    time_col = time_col or _infer_time_col(df)
    if df.empty or group_col not in df.columns or time_col is None or time_col not in df.columns:
        return pd.DataFrame()

    work = df.copy().sort_values([group_col, time_col])
    period = 12 if time_col == "연월" else 1
    for col, new_col in [
        ("평균가격", "가격_YoY"),
        ("거래량", "거래량_YoY"),
        ("전세_보증금평균", "전세_YoY"),
        ("전세가율", "전세가율_변화"),
        ("갭비용", "갭비용_변화"),
        ("미분양소화기간", "미분양소화기간_변화"),
    ]:
        if col in work.columns:
            work[new_col] = _pct_change_by_group(work, group_col, col, periods=period)

    latest = work.groupby(group_col, as_index=False).tail(1).copy()
    if lead_lag_df is not None and not lead_lag_df.empty:
        latest = latest.merge(
            lead_lag_df,
            left_on=group_col,
            right_on="지역",
            how="left",
            suffixes=("", "_선행신호"),
        )
    return latest.reset_index(drop=True)


def evaluate_condition_rules(df, rules, combine="AND"):
    """구조화된 조건 목록을 안전하게 평가한다."""
    if df.empty:
        return pd.Series(False, index=df.index)
    if isinstance(rules, dict):
        rules = [rules]
    masks = []
    for rule in rules:
        if "children" in rule:
            child_mask = evaluate_condition_rules(
                df,
                rule.get("children", []),
                combine=rule.get("combine", "AND"),
            )
            masks.append(child_mask.fillna(False))
            continue
        rule_type = rule.get("type", "compare")
        col = rule.get("column") or rule.get("col")
        op = rule.get("op")
        value = rule.get("value")
        value2 = rule.get("value2")
        if not col or col not in df.columns:
            masks.append(pd.Series(False, index=df.index))
            continue

        if rule_type == "consecutive_up":
            mask = rolling_consecutive_change(
                df, col, n=rule.get("n", value or 3), direction="up",
                group_col=rule.get("group_col"), min_change=rule.get("min_change", 0),
            )
        elif rule_type == "consecutive_down":
            mask = rolling_consecutive_change(
                df, col, n=rule.get("n", value or 3), direction="down",
                group_col=rule.get("group_col"), min_change=rule.get("min_change", 0),
            )
        elif rule_type == "vs_moving_avg":
            mask = vs_moving_avg(
                df, col, window=rule.get("window", value or 12),
                direction=rule.get("direction", "above"),
                threshold_pct=rule.get("threshold_pct", 0),
                group_col=rule.get("group_col"),
            )
        elif rule_type == "pct_from_peak":
            derived = pct_from_peak(df, col, window=rule.get("window"), group_col=rule.get("group_col"))
            mask = _compare_series(derived, op or "<=", value if value is not None else -10, value2)
        elif rule_type == "peak_drawdown_then_rebound":
            mask = peak_drawdown_then_rebound(
                df, col,
                drawdown_pct=rule.get("drawdown_pct", value or -20),
                rebound_pct=rule.get("rebound_pct", value2 or 5),
                window=rule.get("window"),
                group_col=rule.get("group_col"),
            )
        else:
            mask = _compare_series(df[col], op, value, value2)
        if mask is None:
            mask = pd.Series(False, index=df.index)
        masks.append(mask.fillna(False))

    if not masks:
        return pd.Series(True, index=df.index)
    result = masks[0]
    for mask in masks[1:]:
        result = (result | mask) if combine == "OR" else (result & mask)
    return result.fillna(False)


def run_region_backtest(
    df,
    rules,
    combine="AND",
    price_col="평균가격",
    group_col="시도",
    time_col=None,
    horizons=(6, 12, 24),
    cooldown_periods=0,
    success_threshold=0.0,
):
    """조건이 켜진 뒤 지역 가격이 어떻게 움직였는지 검증한다."""
    time_col = time_col or _infer_time_col(df)
    if df.empty or price_col not in df.columns or group_col not in df.columns or time_col not in df.columns:
        return pd.DataFrame(), pd.DataFrame()

    work = df.copy().sort_values([group_col, time_col]).reset_index(drop=True)
    work["진입신호"] = evaluate_condition_rules(work, rules, combine=combine)

    rows = []
    for region, group in work.groupby(group_col):
        group = group.sort_values(time_col).reset_index(drop=True)
        signal_idx = group.index[group["진입신호"]].tolist()
        if cooldown_periods and cooldown_periods > 0:
            cooled = []
            last_idx = -10**9
            for idx in signal_idx:
                if idx - last_idx >= int(cooldown_periods):
                    cooled.append(idx)
                    last_idx = idx
            signal_idx = cooled
        for idx, row in group.loc[signal_idx].iterrows():
            entry_price = row.get(price_col)
            if pd.isna(entry_price) or entry_price == 0:
                continue
            result = {
                "지역": region,
                "진입시점": row[time_col],
                "진입가격": entry_price,
                "쿨다운기간": int(cooldown_periods or 0),
            }
            horizon_returns = []
            for h in horizons:
                future_idx = idx + int(h)
                if future_idx < len(group):
                    future_price = group.loc[future_idx, price_col]
                    return_pct = (future_price / entry_price - 1) * 100 if pd.notna(future_price) else np.nan
                    result[f"{h}기간후수익률"] = return_pct
                    if pd.notna(return_pct):
                        horizon_returns.append(return_pct)
                else:
                    result[f"{h}기간후수익률"] = np.nan
            result["신호별최저수익률"] = min(horizon_returns) if horizon_returns else np.nan
            rows.append(result)

    signals = pd.DataFrame(rows)
    if signals.empty:
        return signals, pd.DataFrame()

    summary_rows = []
    for region, group in signals.groupby("지역"):
        summary = {"지역": region, "반복횟수": len(group)}
        for h in horizons:
            col = f"{h}기간후수익률"
            valid = group[col].dropna()
            summary[f"{h}기간후_평균수익률"] = valid.mean() if len(valid) else np.nan
            summary[f"{h}기간후_성공률"] = (valid > float(success_threshold)).mean() * 100 if len(valid) else np.nan
            summary[f"{h}기간후_검증표본"] = int(len(valid))
        signal_lows = group["신호별최저수익률"].dropna() if "신호별최저수익률" in group.columns else pd.Series(dtype=float)
        summary["최대하락폭"] = signal_lows.min() if len(signal_lows) else np.nan
        summary["성공률정의"] = f"각 진입 후 해당 기간 수익률이 {float(success_threshold):.1f}% 초과인 비율"
        summary["최대하락폭정의"] = "신호별 선택 기간 수익률 중 최저값의 지역 내 최저치"
        summary_rows.append(summary)

    summary_df = pd.DataFrame(summary_rows)
    sort_col = f"{horizons[1] if len(horizons) > 1 else horizons[0]}기간후_평균수익률"
    if sort_col in summary_df.columns:
        summary_df = summary_df.sort_values([sort_col, "반복횟수"], ascending=[False, False])
    return signals, summary_df.reset_index(drop=True)


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


# ── 대출/자금여력 계산 ─────────────────────────────────────────────────────

def calculate_mortgage_loan_capacity(annual_income, annual_rate_pct, dsr_limit=0.40, loan_years=30):
    """
    연소득과 주담대금리로 원리금균등상환 기준 대출가능액(PV)을 계산한다.

    단위는 입력 연소득 단위를 그대로 따른다. 예: 연소득이 만원이면 결과도 만원.
    공식: 월 PMT = 연소득 * DSR / 12, PV = PMT * [1 - (1+r)^(-n)] / r
    """
    income = pd.to_numeric(annual_income, errors="coerce")
    rate = pd.to_numeric(annual_rate_pct, errors="coerce")
    monthly_payment = income.fillna(0) * dsr_limit / 12 if isinstance(income, pd.Series) else (0 if pd.isna(income) else income) * dsr_limit / 12
    monthly_rate = rate / 100 / 12
    n_payments = loan_years * 12

    if isinstance(monthly_rate, pd.Series):
        factor = pd.Series(float(loan_years * 12), index=monthly_rate.index, dtype=float)
        positive_rate = monthly_rate > 0
        factor.loc[positive_rate] = (1 - (1 + monthly_rate.loc[positive_rate]) ** (-n_payments)) / monthly_rate.loc[positive_rate]
        return (monthly_payment * factor).fillna(0)

    if pd.isna(monthly_rate) or monthly_rate <= 0:
        return monthly_payment * loan_years * 12
    return monthly_payment * (1 - (1 + monthly_rate) ** (-n_payments)) / monthly_rate


def calculate_ltv_loan_limit(price, ltv_ratio=0.70):
    """
    규제상 LTV 대출한도 = 주택가격 × LTV.

    단위는 입력 price 단위를 그대로 따른다. 예: price가 만원이면 결과도 만원.
    """
    price_num = pd.to_numeric(price, errors="coerce")
    ltv_num = pd.to_numeric(ltv_ratio, errors="coerce")

    if isinstance(price_num, pd.Series):
        if isinstance(ltv_num, pd.Series):
            ltv_num = ltv_num.reindex(price_num.index).fillna(0)
        elif pd.isna(ltv_num):
            ltv_num = 0
        return (price_num.fillna(0).clip(lower=0) * ltv_num).clip(lower=0)

    if pd.isna(price_num) or pd.isna(ltv_num):
        return 0
    return max(price_num, 0) * max(ltv_num, 0)


def compute_financing_capacity(
    df,
    net_asset_col="순자산",
    income_col="연소득",
    mortgage_rate_col="주담대금리",
    price_col=None,
    dsr_limit=0.40,
    loan_years=30,
    default_mortgage_rate=3.5,
    apply_ltv_limit=False,
    ltv_ratio=0.70,
    ltv_ratio_col=None,
):
    """
    자금여력 = 순자산 + min(PMT 역산 대출가능액, 규제상 LTV 대출한도).

    apply_ltv_limit=False이면 기존 동작처럼 PMT 역산액만 대출가능액으로 쓴다.

    Returns 원본 df에 다음 컬럼을 추가:
    - PMT역산대출가능액_만원
    - LTV규제한도_만원(apply_ltv_limit=True and price_col 제공 시)
    - 대출가능액_만원
    - 대출한도제약유형: PMT/LTV/PMT_ONLY
    - 자금여력_만원
    - 자금여력_매매가커버리지(price_col 제공 시)
    """
    if df.empty:
        return df.copy()

    result = df.copy()
    net_assets = pd.to_numeric(result.get(net_asset_col, 0), errors="coerce").fillna(0)
    annual_income = pd.to_numeric(result.get(income_col, 0), errors="coerce").fillna(0)
    if mortgage_rate_col in result.columns:
        mortgage_rate = pd.to_numeric(result[mortgage_rate_col], errors="coerce").fillna(default_mortgage_rate)
    else:
        mortgage_rate = pd.Series(default_mortgage_rate, index=result.index, dtype=float)

    pmt_capacity = calculate_mortgage_loan_capacity(
        annual_income=annual_income,
        annual_rate_pct=mortgage_rate,
        dsr_limit=dsr_limit,
        loan_years=loan_years,
    ).clip(lower=0)
    result["PMT역산대출가능액_만원"] = pmt_capacity

    if apply_ltv_limit and price_col and price_col in result.columns:
        price = pd.to_numeric(result[price_col], errors="coerce").fillna(0)
        if ltv_ratio_col and ltv_ratio_col in result.columns:
            row_ltv_ratio = pd.to_numeric(result[ltv_ratio_col], errors="coerce").fillna(ltv_ratio)
        else:
            row_ltv_ratio = pd.Series(ltv_ratio, index=result.index, dtype=float)
        ltv_limit = calculate_ltv_loan_limit(price, row_ltv_ratio)
        result["LTV규제한도_만원"] = ltv_limit
        result["대출가능액_만원"] = np.minimum(pmt_capacity, ltv_limit)
        result["대출한도제약유형"] = np.where(ltv_limit < pmt_capacity, "LTV", "PMT")
    else:
        result["대출가능액_만원"] = pmt_capacity
        result["대출한도제약유형"] = "PMT_ONLY"

    result["자금여력_만원"] = (net_assets + result["대출가능액_만원"]).clip(lower=0)

    if price_col and price_col in result.columns:
        price = pd.to_numeric(result[price_col], errors="coerce")
        result["자금여력_매매가커버리지"] = result["자금여력_만원"] / price.replace(0, np.nan)

    return result


# ── 퍼센타일별 구매력 계산 ─────────────────────────────────────────────────

def compute_purchasing_power(
    percentile_df,
    base_rate=3.5,
    dsr_limit=0.40,
    loan_years=30,
    apply_ltv_limit=False,
    ltv_ratio=0.70,
):
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

    # 자금여력 = 순자산 + PMT(연소득 × DSR 한도, 30년 원리금균등, 주담대금리)
    # LTV 적용 시 최대 구매가능가격은 min(순자산 + PMT한도, 순자산 / (1 - LTV))로 역산한다.
    pmt_capacity = calculate_mortgage_loan_capacity(
        annual_income=df["연소득"],
        annual_rate_pct=base_rate,
        dsr_limit=dsr_limit,
        loan_years=loan_years,
    ).clip(lower=0)
    df["PMT역산대출가능액_만원"] = pmt_capacity

    pmt_based_power = (df["순자산"] + pmt_capacity).clip(lower=0)
    if apply_ltv_limit:
        safe_ltv = min(max(float(ltv_ratio), 0.0), 0.999999)
        ltv_purchase_limit = (df["순자산"] / (1 - safe_ltv)).replace([np.inf, -np.inf], np.nan).fillna(0).clip(lower=0)
        df["LTV자기자본구매한도_만원"] = ltv_purchase_limit
        df["자금여력_만원"] = np.minimum(pmt_based_power, ltv_purchase_limit)
        df["대출한도제약유형"] = np.where(ltv_purchase_limit < pmt_based_power, "LTV", "PMT")
        df["대출가능액_만원"] = (df["자금여력_만원"] - df["순자산"]).clip(lower=0)
    else:
        df["대출가능액_만원"] = pmt_capacity
        df["자금여력_만원"] = pmt_based_power
        df["대출한도제약유형"] = "PMT_ONLY"

    # 기존 화면/매칭 로직 호환 alias
    df["대출가능액"] = df["대출가능액_만원"]
    df["구매력(만원)"] = df["자금여력_만원"]
    df["구매력"] = df["자금여력_만원"]

    result_cols = [
        "percentile", "순자산", "연소득",
        "PMT역산대출가능액_만원", "LTV자기자본구매한도_만원",
        "대출가능액_만원", "자금여력_만원", "대출한도제약유형",
        "대출가능액", "구매력(만원)", "구매력",
    ]
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


# ── Prophet 가격 예측 ─────────────────────────────────────────────────────

def forecast_price(df, sido, periods=12, freq="MS", price_col="평균가격"):
    """
    Prophet으로 시도별 아파트 평균가격 예측

    Args:
        df: 매매/임대 실거래 DataFrame (시도, 연도, 월, 평균가격 포함)
        sido: 예측할 시도명
        periods: 예측 기간 (개월)
        freq: 시계열 빈도 ("MS" = month start)
        price_col: 가격 컬럼명 (기본: "평균가격")

    Returns:
        dict {
            "forecast": DataFrame [ds, yhat, yhat_lower, yhat_upper],
            "forecast_future": 미래 기간만 추출한 DataFrame,
            "actual": DataFrame [ds, y],
            "holdout_actual": 검증용 holdout DataFrame,
            "holdout_pred": holdout 예측값 DataFrame,
            "metrics": dict {mae, mape, rmse},
            "components": dict {trend, yearly},
            "error": str (에러 발생 시)
        }
    """
    if not HAS_PROPHET:
        return {"error": "Prophet이 설치되지 않았습니다. pip install prophet으로 설치하세요."}

    # 시도 필터링
    sido_df = df[df["시도"] == sido].copy() if "시도" in df.columns else df.copy()

    if sido_df.empty:
        return {"error": f"'{sido}' 데이터가 없습니다."}

    # 월별 집계를 위한 날짜 컬럼 생성
    if "연도" in sido_df.columns and "월" in sido_df.columns:
        sido_df["_year"] = sido_df["연도"].astype(int)
        sido_df["_month"] = sido_df["월"].astype(int)
        sido_df["ds"] = pd.to_datetime(
            sido_df["_year"].astype(str) + "-" + sido_df["_month"].astype(str).str.zfill(2) + "-01"
        )
    elif "연월" in sido_df.columns:
        # 연월 컬럼이 YYYYMM 형식인 경우
        sido_df["ds"] = pd.to_datetime(sido_df["연월"].astype(str), format="%Y%m", errors="coerce")
    else:
        return {"error": "월별 날짜 정보(연도+월 또는 연월)가 없습니다."}

    # 월별 평균가격 집계
    if price_col not in sido_df.columns:
        return {"error": f"가격 컬럼 '{price_col}'이 없습니다."}

    ts = (
        sido_df.groupby("ds")[price_col]
        .mean()
        .reset_index()
        .rename(columns={price_col: "y"})
        .dropna()
        .sort_values("ds")
    )

    if len(ts) < 24:
        return {"error": f"데이터가 부족합니다. (현재 {len(ts)}개월, 최소 24개월 필요)"}

    # holdout: 최근 12개월을 검증용으로 분리 (전체의 1/4 또는 12개월 중 작은 값)
    holdout_n = min(12, len(ts) // 4)
    train_ts = ts.iloc[:-holdout_n].copy()
    holdout_ts = ts.iloc[-holdout_n:].copy()

    # 1) holdout 검증용 모델 학습
    try:
        model_holdout = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
            changepoint_prior_scale=0.05,
            seasonality_prior_scale=10,
        )
        model_holdout.fit(train_ts)

        # holdout 기간 예측 (future에 holdout 날짜 포함)
        future_holdout = model_holdout.make_future_dataframe(periods=holdout_n, freq=freq)
        forecast_holdout_all = model_holdout.predict(future_holdout)

        # holdout 구간만 추출
        holdout_pred = forecast_holdout_all[
            forecast_holdout_all["ds"].isin(holdout_ts["ds"])
        ][["ds", "yhat"]].copy()

        # 성능 지표 계산
        merged_holdout = holdout_ts.merge(holdout_pred, on="ds", how="inner")
        metrics = {}
        if not merged_holdout.empty:
            actual_vals = merged_holdout["y"].values
            pred_vals = merged_holdout["yhat"].values
            mae = float(np.mean(np.abs(actual_vals - pred_vals)))
            rmse = float(np.sqrt(np.mean((actual_vals - pred_vals) ** 2)))
            nonzero_mask = actual_vals != 0
            if nonzero_mask.any():
                mape = float(
                    np.mean(np.abs((actual_vals[nonzero_mask] - pred_vals[nonzero_mask]) / actual_vals[nonzero_mask])) * 100
                )
            else:
                mape = np.nan
            metrics = {"mae": round(mae, 1), "mape": round(mape, 2), "rmse": round(rmse, 1)}

        # 2) 전체 데이터로 재학습 후 미래 예측
        model_full = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
            changepoint_prior_scale=0.05,
            seasonality_prior_scale=10,
        )
        model_full.fit(ts)
        future_full = model_full.make_future_dataframe(periods=periods, freq=freq)
        forecast_full = model_full.predict(future_full)

        # 미래 기간만 추출 (마지막 actual 날짜 이후)
        last_actual_date = ts["ds"].max()
        forecast_future = forecast_full[forecast_full["ds"] > last_actual_date][
            ["ds", "yhat", "yhat_lower", "yhat_upper"]
        ].copy()

        # 전체 예측 DataFrame (actual 구간 포함 — 차트용)
        forecast_df = forecast_full[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()

        # 계절성 컴포넌트 추출
        components = {}
        if "trend" in forecast_full.columns:
            components["trend"] = forecast_full[["ds", "trend"]].copy()
        if "yearly" in forecast_full.columns:
            components["yearly"] = forecast_full[["ds", "yearly"]].copy()

        return {
            "forecast": forecast_df,
            "forecast_future": forecast_future,
            "actual": ts,
            "holdout_actual": holdout_ts,
            "holdout_pred": holdout_pred,
            "metrics": metrics,
            "components": components,
        }

    except Exception as e:
        return {"error": f"Prophet 예측 오류: {str(e)}"}


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

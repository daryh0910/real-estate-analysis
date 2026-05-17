"""매수판단 v1 현재 가격 위치와 가격 추세 분석."""

from __future__ import annotations

import math

import pandas as pd

from .scoring import safe_divide, weighted_sum_score


def _numeric(df: pd.DataFrame, value_col: str) -> pd.Series:
    if value_col not in df.columns:
        return pd.Series(dtype="float64")
    return pd.to_numeric(df[value_col], errors="coerce").dropna()


def calculate_percentile_position(df: pd.DataFrame, value_col: str) -> float:
    """최신 값의 과거 분포 내 백분위(0~100)를 반환한다."""
    values = _numeric(df, value_col)
    if values.empty:
        return 50.0
    latest = values.iloc[-1]
    return float((values <= latest).mean() * 100)


def calculate_z_score_position(df: pd.DataFrame, value_col: str) -> float:
    """최신 값의 z-score를 반환한다. 표준편차 계산 불가 시 0."""
    values = _numeric(df, value_col)
    if values.empty:
        return 0.0
    if len(values) < 2:
        return 0.0
    std = values.std(ddof=0)
    if std == 0 or pd.isna(std):
        return 0.0
    return float((values.iloc[-1] - values.mean()) / std)


def calculate_drawdown_from_peak(df: pd.DataFrame, value_col: str) -> float:
    """최신 값의 고점 대비 등락률(%)을 반환한다. 하락 시 음수."""
    values = _numeric(df, value_col)
    if values.empty:
        return 0.0
    peak = values.max()
    if peak == 0 or pd.isna(peak):
        return 0.0
    return float((values.iloc[-1] - peak) / peak * 100)


def calculate_trend_gap(df: pd.DataFrame, value_col: str, window: int = 12) -> float:
    """최신 값과 최근 이동평균의 괴리율(%)을 반환한다."""
    values = _numeric(df, value_col)
    if values.empty:
        return 0.0
    recent = values.tail(max(1, int(window)))
    moving_average = recent.mean()
    gap = safe_divide(values.iloc[-1] - moving_average, moving_average)
    if math.isnan(gap):
        return 0.0
    return float(gap * 100)


def _position_to_score(percentile: float, z_score: float, drawdown: float, trend_gap: float) -> float:
    # 낮은 가격 위치·고점 대비 조정은 가점, 과도한 추세 이탈은 감점한다.
    percentile_score = max(0.0, min(100.0, 100.0 - percentile))
    z_score_component = max(0.0, min(100.0, 50.0 - z_score * 15.0))
    drawdown_component = max(0.0, min(100.0, 50.0 + abs(min(drawdown, 0.0)) * 2.0 - max(drawdown, 0.0)))
    trend_component = max(0.0, min(100.0, 50.0 - trend_gap))
    return weighted_sum_score(
        {
            "percentile": percentile_score,
            "z_score": z_score_component,
            "drawdown": drawdown_component,
            "trend_gap": trend_component,
        },
        {"percentile": 35, "z_score": 20, "drawdown": 30, "trend_gap": 15},
    )


def calculate_current_position_score(region_df: pd.DataFrame) -> dict:
    """가격 위치/추세만 반영한 현재위치 축 점수를 계산한다."""
    data_quality: list[str] = []
    if "평균가격" not in region_df.columns:
        return {"score": float("nan"), "components": {}, "data_quality": ["필수 컬럼 누락: 평균가격"]}

    values = _numeric(region_df, "평균가격")
    if values.empty:
        data_quality.append("평균가격 유효 데이터 부족")

    components = {
        "percentile_position": calculate_percentile_position(region_df, "평균가격"),
        "z_score": calculate_z_score_position(region_df, "평균가격"),
        "drawdown_from_peak": calculate_drawdown_from_peak(region_df, "평균가격"),
        "trend_gap": calculate_trend_gap(region_df, "평균가격"),
    }
    score = _position_to_score(
        components["percentile_position"],
        components["z_score"],
        components["drawdown_from_peak"],
        components["trend_gap"],
    )
    return {"score": score, "components": components, "data_quality": data_quality}

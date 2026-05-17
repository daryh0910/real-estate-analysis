"""매수판단 v1 공통 점수화 유틸리티."""

from __future__ import annotations

import math
from collections.abc import Mapping

import pandas as pd


def _is_missing(value: object) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value)) or pd.isna(value)


def _clamp_score(value: float) -> float:
    return max(0.0, min(100.0, float(value)))


def safe_divide(numerator: float | int | None, denominator: float | int | None) -> float:
    """0 또는 결측 분모를 만나면 예외 대신 NaN을 반환한다."""
    if _is_missing(numerator) or _is_missing(denominator) or denominator == 0:
        return float("nan")
    return numerator / denominator


def percentile_score(series: pd.Series, higher_is_better: bool = True) -> pd.Series:
    """시리즈 값을 백분위 점수(0~100)로 변환한다.

    동률은 평균 순위를 사용하고, 결측은 결측으로 유지한다.
    """
    values = pd.to_numeric(series, errors="coerce")
    ascending = bool(higher_is_better)
    scores = values.rank(pct=True, ascending=ascending) * 100
    return scores.clip(lower=0, upper=100)


def minmax_score(series: pd.Series, higher_is_better: bool = True) -> pd.Series:
    """시리즈 값을 min-max 점수(0~100)로 변환한다."""
    values = pd.to_numeric(series, errors="coerce")
    valid = values.dropna()
    scores = pd.Series(float("nan"), index=series.index, dtype="float64")
    if valid.empty:
        return scores

    minimum = valid.min()
    maximum = valid.max()
    if minimum == maximum:
        scores.loc[valid.index] = 50.0
        return scores

    normalized = (values - minimum) / (maximum - minimum) * 100
    if not higher_is_better:
        normalized = 100 - normalized
    return normalized.clip(lower=0, upper=100)


def rebalance_weights_for_available_scores(
    scores: Mapping[str, float | int | None], weights: Mapping[str, float | int]
) -> dict[str, float]:
    """결측 점수를 제외하고 사용 가능한 축의 가중치 합이 100이 되도록 재배분한다."""
    available_weights = {
        axis: float(weight)
        for axis, weight in weights.items()
        if axis in scores and not _is_missing(scores[axis]) and float(weight) > 0
    }
    total_weight = sum(available_weights.values())
    if total_weight <= 0:
        return {}
    return {axis: weight / total_weight * 100 for axis, weight in available_weights.items()}


def weighted_sum_score(scores: Mapping[str, float | int | None], weights: Mapping[str, float | int]) -> float:
    """사용 가능한 점수만 가중치 재배분 후 0~100 범위의 종합 점수를 계산한다."""
    rebalanced_weights = rebalance_weights_for_available_scores(scores, weights)
    if not rebalanced_weights:
        return float("nan")

    total = 0.0
    for axis, weight in rebalanced_weights.items():
        total += _clamp_score(float(scores[axis])) * (weight / 100)
    return _clamp_score(total)

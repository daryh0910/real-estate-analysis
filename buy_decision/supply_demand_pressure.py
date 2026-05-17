"""매수판단 v1 수요공급압력, 전세지지력, 거래량회복, 입주부담 점수."""

from __future__ import annotations

import math

import pandas as pd

from .scoring import safe_divide, weighted_sum_score


def _series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(dtype="float64")
    return pd.to_numeric(df[col], errors="coerce").dropna()


def _latest(df: pd.DataFrame, col: str) -> float:
    values = _series(df, col)
    return float(values.iloc[-1]) if not values.empty else float("nan")


def _trend_pct(df: pd.DataFrame, col: str, window: int = 3) -> float:
    values = _series(df, col)
    if len(values) < 2:
        return 0.0
    baseline = values.iloc[:-1].tail(window).mean()
    pct = safe_divide(values.iloc[-1] - baseline, baseline)
    return 0.0 if math.isnan(pct) else float(pct * 100)


def _score_from_growth(growth_pct: float, neutral: float = 50.0, scale: float = 2.0, inverse: bool = False) -> float:
    direction = -1 if inverse else 1
    return max(0.0, min(100.0, neutral + direction * growth_pct * scale))


def _neutral(message: str) -> dict:
    return {"score": 50.0, "components": {}, "data_quality": [message]}


def calculate_movein_burden_score(region_df: pd.DataFrame) -> dict:
    """입주예정 물량 부담 점수. 높을수록 부담이 낮다."""
    data_quality: list[str] = []
    if "입주예정_세대수" not in region_df.columns:
        return _neutral("선택 컬럼 누락: 입주예정_세대수")

    latest_movein = _latest(region_df, "입주예정_세대수")
    movein_growth = _trend_pct(region_df, "입주예정_세대수")
    burden_per_population = float("nan")
    if "총인구" in region_df.columns:
        population = _latest(region_df, "총인구")
        burden_per_population = safe_divide(latest_movein, population) * 1000
    else:
        data_quality.append("선택 컬럼 누락: 총인구")

    growth_score = _score_from_growth(movein_growth, inverse=True)
    level_score = 50.0 if math.isnan(burden_per_population) else max(0.0, min(100.0, 90.0 - burden_per_population * 2.0))
    score = weighted_sum_score({"growth": growth_score, "level": level_score}, {"growth": 60, "level": 40})
    return {
        "score": score,
        "components": {"movein_growth_pct": movein_growth, "movein_per_1000_people": burden_per_population},
        "data_quality": data_quality,
    }


def calculate_supply_pressure_score(region_df: pd.DataFrame) -> dict:
    """준공·미분양·입주예정 부담을 종합한다. 높을수록 공급압력이 낮다."""
    available = [col for col in ["입주예정_세대수", "준공_호수", "미분양_호수"] if col in region_df.columns]
    if not available:
        return _neutral("선택 컬럼 누락: 입주예정_세대수, 준공_호수, 미분양_호수")

    component_scores = {col: _score_from_growth(_trend_pct(region_df, col), inverse=True) for col in available}
    score = weighted_sum_score(component_scores, {col: 1 for col in component_scores})
    data_quality = [f"선택 컬럼 누락: {col}" for col in ["입주예정_세대수", "준공_호수", "미분양_호수"] if col not in region_df.columns]
    return {"score": score, "components": component_scores, "data_quality": data_quality}


def calculate_demand_strength_score(region_df: pd.DataFrame) -> dict:
    """NPS·인구 기반 수요 강도 점수. 높을수록 수요 지표가 양호하다."""
    available = [col for col in ["NPS_가입자수", "NPS_1인당고지금액", "NPS_고용증감", "총인구"] if col in region_df.columns]
    if not available:
        return _neutral("선택 컬럼 누락: NPS_가입자수, NPS_1인당고지금액, NPS_고용증감, 총인구")

    component_scores = {}
    for col in available:
        if col == "NPS_고용증감":
            latest = _latest(region_df, col)
            component_scores[col] = max(0.0, min(100.0, 50.0 + latest)) if not math.isnan(latest) else 50.0
        else:
            component_scores[col] = _score_from_growth(_trend_pct(region_df, col), scale=3.0)
    score = weighted_sum_score(component_scores, {col: 1 for col in component_scores})
    data_quality = [f"선택 컬럼 누락: {col}" for col in ["NPS_가입자수", "NPS_1인당고지금액", "NPS_고용증감", "총인구"] if col not in region_df.columns]
    return {"score": score, "components": component_scores, "data_quality": data_quality}


def calculate_jeonse_support_score(region_df: pd.DataFrame) -> dict:
    """전세가율·전세거래·갭비용 기반 전세 지지력 점수."""
    available = [col for col in ["전세가율", "전세_거래량", "갭비용"] if col in region_df.columns]
    if not available:
        return _neutral("선택 컬럼 누락: 전세가율, 전세_거래량, 갭비용")

    scores = {}
    if "전세가율" in available:
        ratio = _latest(region_df, "전세가율")
        scores["전세가율"] = max(0.0, min(100.0, (ratio - 35.0) * 3.0)) if not math.isnan(ratio) else 50.0
    if "전세_거래량" in available:
        scores["전세_거래량"] = _score_from_growth(_trend_pct(region_df, "전세_거래량"), scale=2.0)
    if "갭비용" in available:
        scores["갭비용"] = _score_from_growth(_trend_pct(region_df, "갭비용"), inverse=True, scale=1.5)
    score = weighted_sum_score(scores, {"전세가율": 80, "전세_거래량": 15, "갭비용": 5})
    data_quality = [f"선택 컬럼 누락: {col}" for col in ["전세가율", "전세_거래량", "갭비용"] if col not in region_df.columns]
    return {"score": score, "components": scores, "data_quality": data_quality}


def calculate_transaction_recovery_score(region_df: pd.DataFrame) -> dict:
    """거래량 회복 축의 최종 점수를 계산한다."""
    if "거래량" not in region_df.columns:
        return {"score": float("nan"), "components": {}, "data_quality": ["필수 컬럼 누락: 거래량"]}
    values = _series(region_df, "거래량")
    if values.empty:
        return {"score": float("nan"), "components": {}, "data_quality": ["거래량 유효 데이터 부족"]}
    recovery_pct = _trend_pct(region_df, "거래량")
    score = _score_from_growth(recovery_pct, scale=1.5)
    return {"score": score, "components": {"transaction_recovery_pct": recovery_pct}, "data_quality": []}

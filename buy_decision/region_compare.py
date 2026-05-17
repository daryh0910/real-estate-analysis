"""지역 A/B 매수판단 비교 모델."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd

from .current_position import calculate_current_position_score
from .schemas import PURPOSE_WEIGHTS, SCORE_AXES, classify_score
from .scoring import weighted_sum_score
from .supply_demand_pressure import (
    calculate_demand_strength_score,
    calculate_jeonse_support_score,
    calculate_movein_burden_score,
    calculate_supply_pressure_score,
    calculate_transaction_recovery_score,
)


def _region_name(df: pd.DataFrame, fallback: str = "지역") -> str:
    for col in ["시군구", "지역", "시도", "지역코드"]:
        if col in df.columns and not df[col].dropna().empty:
            return str(df[col].dropna().iloc[0])
    return fallback


def _merge_quality(*results: dict) -> list[str]:
    messages: list[str] = []
    for result in results:
        for message in result.get("data_quality", []):
            if message not in messages:
                messages.append(message)
    return messages


def score_region_for_purpose(region_df: pd.DataFrame, purpose: str) -> dict[str, Any]:
    """목적별 가중치를 적용해 단일 지역의 5대 축 점수와 총점을 계산한다."""
    if purpose not in PURPOSE_WEIGHTS:
        raise ValueError(f"지원하지 않는 목적: {purpose}")

    current = calculate_current_position_score(region_df)
    supply = calculate_supply_pressure_score(region_df)
    demand = calculate_demand_strength_score(region_df)
    jeonse = calculate_jeonse_support_score(region_df)
    transaction = calculate_transaction_recovery_score(region_df)
    movein = calculate_movein_burden_score(region_df)

    supply_demand = weighted_sum_score({"supply": supply["score"], "demand": demand["score"]}, {"supply": 45, "demand": 55})
    axis_scores = {
        "현재위치": current["score"],
        "수요공급압력": supply_demand,
        "전세지지력": jeonse["score"],
        "거래량회복": transaction["score"],
        "입주물량부담": movein["score"],
    }
    total_score = weighted_sum_score(axis_scores, PURPOSE_WEIGHTS[purpose])
    return {
        "region_name": _region_name(region_df),
        "purpose": purpose,
        "total_score": total_score,
        "grade": classify_score(total_score) if not math.isnan(total_score) else "판단 보류 또는 리스크 우위",
        "axis_scores": axis_scores,
        "details": {
            "현재위치": current,
            "공급압력": supply,
            "수요강도": demand,
            "전세지지력": jeonse,
            "거래량회복": transaction,
            "입주물량부담": movein,
        },
        "data_quality": _merge_quality(current, supply, demand, jeonse, transaction, movein),
    }


def build_comparison_rows(compare_result: dict[str, Any]) -> list[dict[str, Any]]:
    """UI 표시에 적합한 축별 비교 행을 만든다."""
    rows = []
    a_scores = compare_result["region_a"]["axis_scores"]
    b_scores = compare_result["region_b"]["axis_scores"]
    for axis in SCORE_AXES:
        a = a_scores.get(axis, float("nan"))
        b = b_scores.get(axis, float("nan"))
        if pd.isna(a) and pd.isna(b):
            winner = "판단 보류"
        elif pd.isna(b) or (not pd.isna(a) and a > b + 3):
            winner = compare_result["region_a"]["region_name"]
        elif pd.isna(a) or b > a + 3:
            winner = compare_result["region_b"]["region_name"]
        else:
            winner = "유사"
        rows.append({"axis": axis, "region_a": a, "region_b": b, "winner": winner})
    return rows


def generate_compare_summary(compare_result: dict[str, Any]) -> str:
    """확정 예측·투자 권유 없이 비교 요약 문구를 생성한다."""
    purpose = compare_result["purpose"]
    winner = compare_result["winner"]
    if compare_result["winner_type"] == "tie":
        summary = f"{purpose} 관점에서는 두 지역의 종합 점수가 유사해 판단 보류로 분류됩니다. 과거 및 현재 데이터 기준의 비교 참고입니다."
    else:
        summary = f"{purpose} 관점에서는 {winner}이 상대적으로 우위입니다. 과거 및 현재 데이터 기준의 비교 참고입니다."
    compare_result["summary"] = summary
    return summary


def _top_reasons(compare_result: dict[str, Any]) -> list[str]:
    if compare_result["winner_type"] == "tie":
        return ["종합 점수 차이가 작아 우열을 단정하기 어렵습니다."]
    winner_key = "region_a" if compare_result["winner"] == compare_result["region_a"]["region_name"] else "region_b"
    loser_key = "region_b" if winner_key == "region_a" else "region_a"
    reasons = []
    for axis in SCORE_AXES:
        diff = compare_result[winner_key]["axis_scores"].get(axis, float("nan")) - compare_result[loser_key]["axis_scores"].get(axis, float("nan"))
        if not pd.isna(diff) and diff > 3:
            reasons.append(f"{axis} 축에서 상대적으로 양호합니다.")
    return reasons[:3] or ["종합 점수 기준으로 상대 비교 우위가 관찰됩니다."]


def compare_regions(region_a_df: pd.DataFrame, region_b_df: pd.DataFrame, purpose: str) -> dict[str, Any]:
    """두 지역을 목적별로 비교하고 winner, 이유, 주의사항을 반환한다."""
    region_a = score_region_for_purpose(region_a_df, purpose)
    region_b = score_region_for_purpose(region_b_df, purpose)
    a_score = region_a["total_score"]
    b_score = region_b["total_score"]
    if pd.isna(a_score) and pd.isna(b_score):
        winner = "판단 보류"
        winner_type = "tie"
    elif pd.isna(b_score) or (not pd.isna(a_score) and a_score > b_score + 3):
        winner = region_a["region_name"]
        winner_type = "relative"
    elif pd.isna(a_score) or b_score > a_score + 3:
        winner = region_b["region_name"]
        winner_type = "relative"
    else:
        winner = "판단 보류"
        winner_type = "tie"

    result = {
        "purpose": purpose,
        "winner": winner,
        "winner_type": winner_type,
        "region_a": region_a,
        "region_b": region_b,
        "summary": "",
        "reasons": [],
        "cautions": list(dict.fromkeys(region_a["data_quality"] + region_b["data_quality"])),
        "comparison_rows": [],
    }
    result["comparison_rows"] = build_comparison_rows(result)
    result["reasons"] = _top_reasons(result)
    generate_compare_summary(result)
    return result

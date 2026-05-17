"""Streamlit 매수판단 탭에서 바로 사용할 ViewModel 생성기."""

from __future__ import annotations

from typing import Any

import pandas as pd

from .region_compare import compare_regions

REGION_COLUMNS = ["지역코드", "시군구", "지역", "시도"]
OPTIONAL_COLUMNS = [
    "전세가율",
    "전세_거래량",
    "갭비용",
    "NPS_가입자수",
    "NPS_1인당고지금액",
    "NPS_고용증감",
    "입주예정_세대수",
    "입주예정_단지수",
    "준공_호수",
    "미분양_호수",
    "총인구",
]
COMMON_CAUTION = "본 화면은 과거 및 현재 데이터 기반의 비교 참고 자료이며, 투자 판단을 대신하지 않습니다."


def _region_column(df: pd.DataFrame) -> str | None:
    for col in REGION_COLUMNS:
        if col in df.columns:
            return col
    return None


def _filter_region(df: pd.DataFrame, region: Any) -> pd.DataFrame:
    col = _region_column(df)
    if col is None:
        return df.iloc[0:0].copy()
    filtered = df[df[col].astype(str) == str(region)].copy()
    if "연월" in filtered.columns:
        filtered = filtered.sort_values("연월")
    return filtered


def _display_name(region_df: pd.DataFrame, fallback: Any) -> str:
    for col in ["시군구", "지역", "시도", "지역코드"]:
        if col in region_df.columns and not region_df[col].dropna().empty:
            return str(region_df[col].dropna().iloc[0])
    return str(fallback)


def validate_data_availability(df: pd.DataFrame, required_columns: list[str]) -> list[str]:
    """최소 입력 계약과 지역 식별 가능성을 검증해 data_quality 메시지를 반환한다."""
    messages: list[str] = []
    for col in required_columns:
        if col not in df.columns:
            messages.append(f"필수 컬럼 누락: {col}")
    if _region_column(df) is None:
        messages.append("필수 컬럼 누락: 지역 식별 컬럼(지역코드/시군구/지역/시도)")
    for col in OPTIONAL_COLUMNS:
        if col not in df.columns:
            messages.append(f"선택 컬럼 누락: {col}")
    return messages


def _quality_label(messages: list[str]) -> str:
    if any("필수 컬럼 누락" in message or "데이터 부족" in message for message in messages):
        return "데이터 부족"
    if messages:
        return "부분 확인 필요"
    return "양호"


def _group_data_quality(messages: list[str]) -> dict[str, Any]:
    grouped: dict[str, Any] = {"필수": [], "선택": [], "주의": []}
    for message in messages:
        if "필수 컬럼 누락" in message or "지역 A 데이터 부족" in message or "지역 B 데이터 부족" in message:
            grouped["필수"].append(message)
        elif "선택 컬럼 누락" in message:
            grouped["선택"].append(message)
        else:
            grouped["주의"].append(message)
    grouped["label"] = _quality_label(messages)
    return grouped


def _axis_label(score: float) -> str:
    if pd.isna(score):
        return "자료 부족"
    if score >= 70:
        return "강점"
    if score <= 45:
        return "주의축"
    return "중립"


def _region_key_points(region_result: dict[str, Any]) -> list[dict[str, Any]]:
    points = []
    for axis, score in region_result.get("axis_scores", {}).items():
        points.append({"axis": axis, "score": score, "label": _axis_label(score)})
    return sorted(points, key=lambda item: -1 if pd.isna(item["score"]) else item["score"], reverse=True)


def _region_card(region_result: dict[str, Any]) -> dict[str, Any]:
    key_points = _region_key_points(region_result)
    strengths = [item["axis"] for item in key_points if item["label"] == "강점"][:2]
    weaknesses = [item["axis"] for item in reversed(key_points) if item["label"] == "주의축"][:2]
    if not strengths and key_points:
        strengths = [key_points[0]["axis"]]
    if not weaknesses and len(key_points) > 1:
        weaknesses = [key_points[-1]["axis"]]
    return {
        "region_name": region_result["region_name"],
        "total_score": region_result["total_score"],
        "grade": region_result["grade"],
        "axis_scores": region_result["axis_scores"],
        "strengths": strengths,
        "weaknesses": weaknesses,
        "key_points": key_points,
    }


def _next_checks(compare_result: dict[str, Any]) -> list[str]:
    checks = ["최근 실거래가와 호가 차이를 같은 평형 기준으로 확인", "입주예정 물량의 실제 사용승인·입주 지연 여부 확인"]
    purpose = compare_result.get("purpose")
    if purpose == "전세끼고 매수":
        checks.append("전세가율과 갭비용이 같은 생활권 내 단지에서도 유지되는지 확인")
    elif purpose == "실거주":
        checks.append("교통·학군·직주근접 등 비정량 생활 조건 확인")
    else:
        checks.append("거래량 회복이 일시적 반등인지 3~6개월 추가 추세 확인")
    return checks


def build_decision_cards(compare_result: dict[str, Any], data_quality: list[str] | None = None) -> dict[str, Any]:
    """종합 카드와 지역 카드를 만든다."""
    quality_messages = data_quality if data_quality is not None else compare_result.get("cautions", [])
    headline = "판단 보류" if compare_result["winner_type"] == "tie" else f"{compare_result['winner']} 상대 우위"
    return {
        "summary_card": {
            "title": "종합 판단",
            "headline": headline,
            "winner": compare_result["winner"],
            "winner_type": compare_result["winner_type"],
            "summary": compare_result["summary"],
            "reasons": compare_result.get("reasons", [])[:3],
            "cautions": compare_result.get("cautions", [])[:3],
            "next_checks": _next_checks(compare_result),
            "data_quality_label": _quality_label(quality_messages),
            "caution": COMMON_CAUTION,
        },
        "region_cards": [_region_card(compare_result[key]) for key in ["region_a", "region_b"]],
    }


def _chart_points(region_df: pd.DataFrame, region_name: str, value_col: str) -> dict[str, Any] | None:
    if value_col not in region_df.columns or "연월" not in region_df.columns:
        return None
    frame = region_df[["연월", value_col]].dropna().sort_values("연월")
    if frame.empty:
        return None
    return {
        "region_name": region_name,
        "value_col": value_col,
        "points": [{"연월": str(row["연월"]), "value": float(row[value_col])} for _, row in frame.iterrows()],
    }


def build_chart_frames(df: pd.DataFrame, region_a: Any, region_b: Any) -> dict[str, list[dict[str, Any]]]:
    """2x2 차트용 직렬화 가능한 시계열 프레임을 만든다."""
    a_df = _filter_region(df, region_a)
    b_df = _filter_region(df, region_b)
    region_frames = [(a_df, _display_name(a_df, region_a)), (b_df, _display_name(b_df, region_b))]
    specs = {
        "price": "평균가격",
        "jeonse_ratio": "전세가율",
        "transaction_volume": "거래량",
        "movein_volume": "입주예정_세대수",
    }
    charts: dict[str, list[dict[str, Any]]] = {}
    for chart_key, value_col in specs.items():
        chart_items = []
        for region_df, name in region_frames:
            item = _chart_points(region_df, name, value_col)
            if item is not None:
                chart_items.append(item)
        charts[chart_key] = chart_items
    return charts


def build_buy_decision_view_model(df: pd.DataFrame, region_a: Any, region_b: Any, purpose: str) -> dict[str, Any]:
    """매수판단 UI가 바로 사용할 단일 ViewModel을 반환한다."""
    required = ["연월", "평균가격", "거래량"]
    data_quality = validate_data_availability(df, required)
    a_df = _filter_region(df, region_a)
    b_df = _filter_region(df, region_b)
    if a_df.empty:
        data_quality.append(f"지역 A 데이터 부족: {region_a}")
    if b_df.empty:
        data_quality.append(f"지역 B 데이터 부족: {region_b}")

    compare_result = compare_regions(a_df, b_df, purpose)
    merged_quality = list(dict.fromkeys(data_quality + compare_result.get("cautions", [])))
    cards = build_decision_cards(compare_result, merged_quality)
    return {
        "summary_card": cards["summary_card"],
        "region_cards": cards["region_cards"],
        "comparison_rows": compare_result["comparison_rows"],
        "charts": build_chart_frames(df, region_a, region_b),
        "data_quality": merged_quality,
        "data_quality_grouped": _group_data_quality(merged_quality),
        "compare_result": compare_result,
    }

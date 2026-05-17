import pandas as pd

from buy_decision.view_model import (
    build_buy_decision_view_model,
    build_chart_frames,
    build_decision_cards,
    validate_data_availability,
)

FORBIDDEN_WORDS = ["오릅니다", "매수 적기", "투자 유망", "확실", "보장", "추천합니다", "공급 폭탄"]


def sample_df(include_optional=True):
    rows = []
    for code, name, price, jeonse in [("11110", "종로구", 100, 68), ("11590", "동작구", 90, 58)]:
        for i, ym in enumerate(["2025-01", "2025-02", "2025-03", "2025-04"]):
            row = {
                "지역코드": code,
                "시군구": name,
                "연월": ym,
                "평균가격": price + i,
                "거래량": 100 + i * 10,
            }
            if include_optional:
                row.update(
                    {
                        "전세가율": jeonse + i,
                        "전세_거래량": 80 + i,
                        "갭비용": 30 - i,
                        "NPS_가입자수": 1000 + i,
                        "NPS_고용증감": i,
                        "입주예정_세대수": 100 - i,
                        "총인구": 10000,
                    }
                )
            rows.append(row)
    return pd.DataFrame(rows)


def collect_strings(value):
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        strings = []
        for item in value.values():
            strings.extend(collect_strings(item))
        return strings
    if isinstance(value, list):
        strings = []
        for item in value:
            strings.extend(collect_strings(item))
        return strings
    return []


def test_view_model_contains_required_ui_sections():
    vm = build_buy_decision_view_model(sample_df(), "11110", "11590", "실거주")

    assert {"summary_card", "region_cards", "comparison_rows", "charts", "data_quality"} <= set(vm)
    assert len(vm["region_cards"]) == 2
    assert len(vm["comparison_rows"]) == 5
    assert {"price", "jeonse_ratio", "transaction_volume", "movein_volume"} <= set(vm["charts"])


def test_missing_optional_columns_are_reported_in_data_quality_not_failure():
    vm = build_buy_decision_view_model(sample_df(include_optional=False), "11110", "11590", "실거주")

    assert vm["summary_card"]
    assert any("선택 컬럼 누락" in message for message in vm["data_quality"])
    assert any("전세가율" in message for message in vm["data_quality"])


def test_validate_data_availability_requires_minimum_contract():
    quality = validate_data_availability(pd.DataFrame({"연월": ["2025-01"], "시도": ["서울"], "평균가격": [1]}), ["연월", "평균가격", "거래량"])

    assert any("거래량" in message for message in quality)
    assert not any("지역 식별" in message for message in quality)


def test_region_code_filtering_uses_code_but_display_name_uses_mapping_column():
    vm = build_buy_decision_view_model(sample_df(), "11110", "11590", "실거주")

    names = [card["region_name"] for card in vm["region_cards"]]
    assert names == ["종로구", "동작구"]


def test_chart_frames_return_serializable_frames_for_available_and_missing_optional_charts():
    charts = build_chart_frames(sample_df(include_optional=False), "11110", "11590")

    assert charts["price"][0]["region_name"] == "종로구"
    assert charts["price"][0]["points"]
    assert charts["jeonse_ratio"] == []
    assert charts["movein_volume"] == []


def test_decision_cards_and_view_model_strings_avoid_forbidden_words():
    vm = build_buy_decision_view_model(sample_df(), "11110", "11590", "전세끼고 매수")
    cards = build_decision_cards(vm["compare_result"])

    all_strings = collect_strings(vm) + collect_strings(cards)
    for text in all_strings:
        for word in FORBIDDEN_WORDS:
            assert word not in text


def test_summary_card_exposes_actionable_judgment_sections():
    vm = build_buy_decision_view_model(sample_df(), "11110", "11590", "전세끼고 매수")
    summary = vm["summary_card"]

    assert {"headline", "reasons", "cautions", "next_checks", "data_quality_label"} <= set(summary)
    assert summary["headline"]
    assert 1 <= len(summary["reasons"]) <= 3
    assert len(summary["next_checks"]) >= 2
    assert summary["data_quality_label"] in {"양호", "부분 확인 필요", "데이터 부족"}


def test_region_cards_expose_strengths_and_weaknesses_for_card_ui():
    vm = build_buy_decision_view_model(sample_df(), "11110", "11590", "실거주")

    for card in vm["region_cards"]:
        assert {"strengths", "weaknesses", "key_points"} <= set(card)
        assert len(card["key_points"]) >= 2
        assert all("axis" in item and "score" in item and "label" in item for item in card["key_points"])


def test_view_model_provides_grouped_data_quality_for_reader_friendly_ui():
    vm = build_buy_decision_view_model(sample_df(include_optional=False), "11110", "11590", "투자")

    grouped = vm["data_quality_grouped"]
    assert {"필수", "선택", "주의"} <= set(grouped)
    assert any("전세가율" in message for message in grouped["선택"])
    assert grouped["label"] in {"양호", "부분 확인 필요", "데이터 부족"}

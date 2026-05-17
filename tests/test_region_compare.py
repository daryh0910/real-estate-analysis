import pandas as pd

from buy_decision.region_compare import (
    build_comparison_rows,
    compare_regions,
    generate_compare_summary,
    score_region_for_purpose,
)

FORBIDDEN_WORDS = ["오릅니다", "매수 적기", "투자 유망", "확실", "보장", "추천합니다", "공급 폭탄"]


def make_region(name, jeonse_ratio, nps_growth, volume_latest, movein_latest):
    return pd.DataFrame(
        {
            "지역": [name] * 4,
            "연월": pd.period_range("2025-01", periods=4, freq="M").astype(str),
            "평균가격": [100, 100, 100, 96],
            "거래량": [100, 100, 100, volume_latest],
            "전세가율": [jeonse_ratio - 2, jeonse_ratio - 1, jeonse_ratio, jeonse_ratio],
            "전세_거래량": [100, 100, 100, 110],
            "갭비용": [100 - jeonse_ratio] * 4,
            "NPS_고용증감": [0, 0, 0, nps_growth],
            "NPS_가입자수": [1000, 1010, 1020, 1030 + nps_growth],
            "입주예정_세대수": [100, 100, 100, movein_latest],
            "총인구": [10000] * 4,
        }
    )


def test_purpose_weights_can_change_winner_between_jeonse_and_investment():
    jeonse_strong = make_region("전세강점", jeonse_ratio=78, nps_growth=0, volume_latest=80, movein_latest=80)
    growth_strong = make_region("성장강점", jeonse_ratio=52, nps_growth=80, volume_latest=220, movein_latest=80)

    jeonse_result = compare_regions(jeonse_strong, growth_strong, "전세끼고 매수")
    invest_result = compare_regions(jeonse_strong, growth_strong, "투자")

    assert jeonse_result["winner"] == "전세강점"
    assert invest_result["winner"] == "성장강점"


def test_similar_scores_return_hold_judgment():
    a = make_region("A", jeonse_ratio=60, nps_growth=10, volume_latest=100, movein_latest=100)
    b = make_region("B", jeonse_ratio=60, nps_growth=10, volume_latest=100, movein_latest=100)

    result = compare_regions(a, b, "실거주")

    assert result["winner"] == "판단 보류"
    assert result["winner_type"] == "tie"


def test_summary_avoids_forbidden_prediction_or_recommendation_words():
    a = make_region("A", jeonse_ratio=70, nps_growth=30, volume_latest=140, movein_latest=80)
    b = make_region("B", jeonse_ratio=50, nps_growth=-10, volume_latest=70, movein_latest=300)

    result = compare_regions(a, b, "실거주")
    summary = generate_compare_summary(result)

    assert summary == result["summary"]
    for word in FORBIDDEN_WORDS:
        assert word not in summary


def test_score_region_and_comparison_rows_have_expected_axes():
    region = make_region("A", jeonse_ratio=65, nps_growth=10, volume_latest=120, movein_latest=90)
    scored = score_region_for_purpose(region, "실거주")
    rows = build_comparison_rows(compare_regions(region, region, "실거주"))

    assert set(scored["axis_scores"]) == {"현재위치", "수요공급압력", "전세지지력", "거래량회복", "입주물량부담"}
    assert 0 <= scored["total_score"] <= 100
    assert len(rows) == 5
    assert {"axis", "region_a", "region_b", "winner"} <= set(rows[0])

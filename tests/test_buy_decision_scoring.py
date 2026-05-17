import math

import pandas as pd
import pytest

from buy_decision.schemas import (
    PURPOSES,
    PURPOSE_WEIGHTS,
    SCORE_AXES,
    classify_score,
)

from buy_decision.scoring import (
    minmax_score,
    percentile_score,
    rebalance_weights_for_available_scores,
    safe_divide,
    weighted_sum_score,
)


def assert_series_close(actual, expected):
    assert list(actual.index) == list(expected.index)
    for actual_value, expected_value in zip(actual.tolist(), expected.tolist()):
        if pd.isna(expected_value):
            assert pd.isna(actual_value)
        else:
            assert actual_value == pytest.approx(expected_value)


def test_task1_purpose_weights_cover_all_axes_and_sum_to_100():
    assert PURPOSES == ["실거주", "투자", "전세끼고 매수", "갈아타기", "장기보유"]
    assert SCORE_AXES == ["현재위치", "수요공급압력", "전세지지력", "거래량회복", "입주물량부담"]

    expected_weights = {
        "실거주": {"현재위치": 25, "수요공급압력": 25, "전세지지력": 20, "거래량회복": 15, "입주물량부담": 15},
        "투자": {"현재위치": 25, "수요공급압력": 25, "전세지지력": 15, "거래량회복": 25, "입주물량부담": 10},
        "전세끼고 매수": {"현재위치": 15, "수요공급압력": 15, "전세지지력": 40, "거래량회복": 10, "입주물량부담": 20},
        "갈아타기": {"현재위치": 25, "수요공급압력": 20, "전세지지력": 15, "거래량회복": 25, "입주물량부담": 15},
        "장기보유": {"현재위치": 20, "수요공급압력": 35, "전세지지력": 15, "거래량회복": 10, "입주물량부담": 20},
    }
    assert PURPOSE_WEIGHTS == expected_weights

    for purpose, weights in PURPOSE_WEIGHTS.items():
        assert set(weights) == set(SCORE_AXES), purpose
        assert sum(weights.values()) == 100, purpose


@pytest.mark.parametrize(
    ("score", "expected"),
    [
        (100, "관심 우선"),
        (80, "관심 우선"),
        (79.999, "추가 검토"),
        (65, "추가 검토"),
        (64.999, "중립"),
        (50, "중립"),
        (49.999, "주의 관찰"),
        (35, "주의 관찰"),
        (34.999, "판단 보류 또는 리스크 우위"),
        (0, "판단 보류 또는 리스크 우위"),
    ],
)
def test_task1_classify_score_thresholds(score, expected):
    assert classify_score(score) == expected


def test_task2_safe_divide_handles_zero_and_missing_denominator():
    assert safe_divide(10, 2) == pytest.approx(5)
    assert math.isnan(safe_divide(10, 0))
    assert math.isnan(safe_divide(10, None))
    assert math.isnan(safe_divide(None, 10))


def test_task2_percentile_score_keeps_range_and_reverses_direction():
    series = pd.Series([10, 20, 30, None], index=["a", "b", "c", "d"])

    assert_series_close(
        percentile_score(series, higher_is_better=True),
        pd.Series([pytest.approx(100 / 3), pytest.approx(200 / 3), 100, None], index=series.index),
    )
    assert_series_close(
        percentile_score(series, higher_is_better=False),
        pd.Series([100, pytest.approx(200 / 3), pytest.approx(100 / 3), None], index=series.index),
    )


def test_task2_minmax_score_keeps_range_reverses_direction_and_handles_constant():
    series = pd.Series([10, 20, 30, None], index=["a", "b", "c", "d"])

    assert_series_close(
        minmax_score(series, higher_is_better=True),
        pd.Series([0, 50, 100, None], index=series.index),
    )
    assert_series_close(
        minmax_score(series, higher_is_better=False),
        pd.Series([100, 50, 0, None], index=series.index),
    )
    assert_series_close(
        minmax_score(pd.Series([7, 7, None])),
        pd.Series([50, 50, None]),
    )


def test_task2_rebalance_weights_for_available_scores_ignores_nan_scores():
    scores = {"현재위치": 80, "수요공급압력": None, "전세지지력": float("nan")}
    weights = {"현재위치": 25, "수요공급압력": 25, "전세지지력": 50}

    assert rebalance_weights_for_available_scores(scores, weights) == {"현재위치": 100.0}


def test_task2_weighted_sum_score_rebalances_available_scores_and_clamps_range():
    scores = {"현재위치": 80, "수요공급압력": None, "전세지지력": 40}
    weights = {"현재위치": 25, "수요공급압력": 25, "전세지지력": 50}

    assert weighted_sum_score(scores, weights) == pytest.approx((80 * (25 / 75)) + (40 * (50 / 75)))
    assert weighted_sum_score({"현재위치": -10, "전세지지력": 150}, {"현재위치": 50, "전세지지력": 50}) == 50
    assert math.isnan(weighted_sum_score({"현재위치": None}, {"현재위치": 100}))

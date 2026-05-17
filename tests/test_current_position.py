import math

import pandas as pd
import pytest

from buy_decision.current_position import (
    calculate_current_position_score,
    calculate_drawdown_from_peak,
    calculate_percentile_position,
    calculate_trend_gap,
    calculate_z_score_position,
)


def test_drawdown_from_peak_uses_latest_value_against_historical_peak():
    df = pd.DataFrame({"평균가격": [100, 120, 90]})

    assert calculate_drawdown_from_peak(df, "평균가격") == pytest.approx(-25.0)


def test_trend_gap_uses_latest_value_against_moving_average():
    df = pd.DataFrame({"평균가격": [100, 100, 100, 110]})

    moving_average = (100 + 100 + 110) / 3
    assert calculate_trend_gap(df, "평균가격", window=3) == pytest.approx((110 - moving_average) / moving_average * 100)


def test_position_functions_handle_short_and_missing_data_without_exception():
    df = pd.DataFrame({"평균가격": [None, 100]})

    assert calculate_percentile_position(df, "평균가격") == pytest.approx(100.0)
    assert calculate_z_score_position(df, "평균가격") == pytest.approx(0.0)
    assert calculate_drawdown_from_peak(pd.DataFrame({"평균가격": [None]}), "평균가격") == pytest.approx(0.0)
    assert calculate_trend_gap(pd.DataFrame({"평균가격": [None]}), "평균가격") == pytest.approx(0.0)


def test_current_position_score_returns_price_only_components_and_quality_flags():
    df = pd.DataFrame({"평균가격": [100, 120, 90], "거래량": [1, 10, 100]})

    result = calculate_current_position_score(df)

    assert set(result) == {"score", "components", "data_quality"}
    assert 0 <= result["score"] <= 100
    assert set(result["components"]) == {"percentile_position", "z_score", "drawdown_from_peak", "trend_gap"}
    assert "거래량회복" not in result["components"]
    assert result["data_quality"] == []


def test_current_position_score_records_missing_required_column():
    result = calculate_current_position_score(pd.DataFrame({"거래량": [1, 2, 3]}))

    assert math.isnan(result["score"])
    assert any("평균가격" in message for message in result["data_quality"])

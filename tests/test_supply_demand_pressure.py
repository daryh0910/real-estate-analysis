import math

import pandas as pd
import pytest

from buy_decision.supply_demand_pressure import (
    calculate_demand_strength_score,
    calculate_jeonse_support_score,
    calculate_movein_burden_score,
    calculate_supply_pressure_score,
    calculate_transaction_recovery_score,
)


def test_movein_burden_score_decreases_when_movein_volume_increases():
    low_supply = pd.DataFrame({"입주예정_세대수": [100, 100, 100], "총인구": [10000, 10000, 10000]})
    high_supply = pd.DataFrame({"입주예정_세대수": [100, 100, 600], "총인구": [10000, 10000, 10000]})

    low_result = calculate_movein_burden_score(low_supply)
    high_result = calculate_movein_burden_score(high_supply)

    assert high_result["score"] < low_result["score"]
    assert low_result["score"] > 50


def test_jeonse_support_rises_with_better_jeonse_ratio_and_volume():
    weak = pd.DataFrame({"전세가율": [45, 46, 47], "전세_거래량": [100, 90, 80], "갭비용": [600, 650, 700]})
    strong = pd.DataFrame({"전세가율": [65, 68, 70], "전세_거래량": [100, 110, 130], "갭비용": [600, 560, 500]})

    assert calculate_jeonse_support_score(strong)["score"] > calculate_jeonse_support_score(weak)["score"]


def test_optional_columns_missing_return_neutral_score_and_quality_messages():
    df = pd.DataFrame({"평균가격": [100, 101], "거래량": [10, 12]})

    for func in [calculate_supply_pressure_score, calculate_demand_strength_score, calculate_jeonse_support_score, calculate_movein_burden_score]:
        result = func(df)
        assert result["score"] == pytest.approx(50.0)
        assert result["data_quality"]


def test_transaction_recovery_score_is_owned_by_supply_demand_module():
    recovering = pd.DataFrame({"거래량": [100, 100, 100, 180]})
    weak = pd.DataFrame({"거래량": [100, 100, 100, 60]})

    assert calculate_transaction_recovery_score(recovering)["score"] > calculate_transaction_recovery_score(weak)["score"]


def test_missing_transaction_volume_returns_nan_when_required_column_absent():
    result = calculate_transaction_recovery_score(pd.DataFrame({"평균가격": [1, 2, 3]}))

    assert math.isnan(result["score"])
    assert any("거래량" in message for message in result["data_quality"])

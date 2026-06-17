import numpy as np
import pandas as pd

from analysis import (
    calculate_mortgage_loan_capacity,
    compute_financing_capacity,
    compute_lead_lag_signal,
    compute_purchasing_power,
    evaluate_condition_rules,
    peak_drawdown_then_rebound,
    pct_from_peak,
    prepare_screener_dataset,
    rolling_consecutive_change,
    run_region_backtest,
    vs_moving_avg,
)


def test_mortgage_capacity_uses_pmt_present_value_formula():
    loan = calculate_mortgage_loan_capacity(
        annual_income=10_000,
        annual_rate_pct=4.0,
        dsr_limit=0.40,
        loan_years=30,
    )
    monthly_payment = 10_000 * 0.40 / 12
    monthly_rate = 0.04 / 12
    expected = monthly_payment * (1 - (1 + monthly_rate) ** -360) / monthly_rate

    assert np.isclose(loan, expected)


def test_financing_capacity_adds_net_assets_and_mortgage_capacity():
    df = pd.DataFrame({
        "가구_순자산": [80_000],
        "가구_소득평균": [12_000],
        "주담대금리": [4.5],
        "매매가_만원": [150_000],
    })

    result = compute_financing_capacity(
        df,
        net_asset_col="가구_순자산",
        income_col="가구_소득평균",
        mortgage_rate_col="주담대금리",
        price_col="매매가_만원",
        dsr_limit=0.40,
        loan_years=30,
    )

    expected_loan = calculate_mortgage_loan_capacity(12_000, 4.5, 0.40, 30)
    assert np.isclose(result.loc[0, "대출가능액_만원"], expected_loan)
    assert np.isclose(result.loc[0, "자금여력_만원"], 80_000 + expected_loan)
    assert np.isclose(result.loc[0, "자금여력_매매가커버리지"], (80_000 + expected_loan) / 150_000)


def test_purchasing_power_uses_full_income_based_loan_capacity_columns():
    df = pd.DataFrame({
        "percentile": [50],
        "가구_순자산": [70_000],
        "가구_소득평균": [10_000],
        "DSR": [20],
    })

    result = compute_purchasing_power(df, base_rate=4.0, dsr_limit=0.40, loan_years=30)
    expected_loan = calculate_mortgage_loan_capacity(10_000, 4.0, 0.40, 30)

    assert np.isclose(result.loc[0, "대출가능액_만원"], expected_loan)
    assert np.isclose(result.loc[0, "자금여력_만원"], 70_000 + expected_loan)
    assert np.isclose(result.loc[0, "구매력(만원)"], 70_000 + expected_loan)
    assert np.isclose(result.loc[0, "구매력"], 70_000 + expected_loan)


def sample_monthly_df():
    rows = []
    for region in ["A", "B"]:
        base = 100 if region == "A" else 200
        for i in range(18):
            rows.append({
                "시도": region,
                "연월": f"2024-{i + 1:02d}",
                "평균가격": base + i * (3 if region == "A" else -1),
                "전세_보증금평균": base * 0.6 + max(i - 2, 0) * (3 if region == "A" else -1),
                "전세가율": 55 + i if region == "A" else 75 - i,
                "거래량": 10 + i,
                "PIR": 12 if region == "A" else 18,
            })
    return pd.DataFrame(rows)


def test_prepare_screener_dataset_keeps_latest_and_yoy_columns():
    df = sample_monthly_df()
    latest = prepare_screener_dataset(df, time_col="연월")

    assert set(latest["시도"]) == {"A", "B"}
    assert "가격_YoY" in latest.columns
    assert latest.loc[latest["시도"] == "A", "연월"].iloc[0] == "2024-18"


def test_basic_and_nested_condition_rules():
    df = sample_monthly_df()
    nested = {
        "combine": "OR",
        "children": [
            {"column": "PIR", "op": "<", "value": "13"},
            {"column": "전세가율", "op": ">", "value": "80"},
        ],
    }

    mask = evaluate_condition_rules(df, [nested], combine="AND")

    assert mask.any()
    assert mask[df["시도"] == "A"].all()


def test_time_series_condition_helpers():
    df = sample_monthly_df()

    up = rolling_consecutive_change(df, "전세가율", n=3, group_col="시도")
    above = vs_moving_avg(df, "평균가격", window=3, direction="above", group_col="시도")
    drawdown = pct_from_peak(df, "평균가격", group_col="시도")

    assert up[df["시도"] == "A"].tail(1).iloc[0]
    assert not up[df["시도"] == "B"].tail(1).iloc[0]
    assert above[df["시도"] == "A"].tail(1).iloc[0]
    assert drawdown[df["시도"] == "A"].max() == 0
    assert drawdown[df["시도"] == "B"].min() < 0


def test_peak_drawdown_then_rebound_rule():
    df = pd.DataFrame({
        "시도": ["A"] * 7,
        "연월": [f"2024-{i + 1:02d}" for i in range(7)],
        "가격": [100, 95, 80, 70, 74, 78, 82],
    })

    mask = peak_drawdown_then_rebound(df, "가격", drawdown_pct=-20, rebound_pct=5, group_col="시도")

    assert mask.iloc[4]


def test_lead_lag_signal_returns_direction_columns():
    df = sample_monthly_df()
    result = compute_lead_lag_signal(df, max_lag=3, time_col="연월")

    assert not result.empty
    assert {"선행방향", "반복성", "가격전처리", "전세전처리"}.issubset(result.columns)


def test_backtest_cooldown_reduces_duplicate_signals_and_uses_threshold():
    df = sample_monthly_df()
    rules = [{"column": "PIR", "op": "<", "value": "13"}]

    signals_no_cooldown, summary_no_cooldown = run_region_backtest(
        df, rules, time_col="연월", horizons=(3,), cooldown_periods=0, success_threshold=1.0
    )
    signals_cooldown, summary_cooldown = run_region_backtest(
        df, rules, time_col="연월", horizons=(3,), cooldown_periods=6, success_threshold=1.0
    )

    assert len(signals_cooldown) < len(signals_no_cooldown)
    assert "신호별최저수익률" in signals_cooldown.columns
    assert "성공률정의" in summary_cooldown.columns
    assert summary_no_cooldown["반복횟수"].iloc[0] > summary_cooldown["반복횟수"].iloc[0]

import pandas as pd
import pytest

import data_loader
from leader_apartment import get_leader_apartment_flow, select_leader_apartments


def _row(
    region_code,
    apartment,
    volume,
    unit_price,
    month="2025-12",
    legal_dong="테스트동",
    sido="서울",
    average_price=100_000,
):
    return {
        "시도": sido,
        "지역코드": region_code,
        "법정동": legal_dong,
        "아파트": apartment,
        "연월": month,
        "거래량": volume,
        "평균가격": average_price,
        "평균단가_per_m2": unit_price,
    }


def test_selects_highest_unit_price_among_high_volume_candidates():
    df = pd.DataFrame(
        [
            _row("11680", "초고가1건", 1, 2_000),
            _row("11680", "활발한고가", 10, 1_000),
            _row("11680", "최다거래중가", 20, 700),
            _row("11710", "송파대표", 8, 900, legal_dong="잠실동"),
        ]
    )

    leaders = select_leader_apartments(
        df, lookback_months=24, min_transactions=3, volume_quantile=0.70
    )

    assert leaders.set_index("지역코드").loc["11680", "아파트"] == "활발한고가"
    assert leaders.set_index("지역코드").loc["11710", "아파트"] == "송파대표"
    assert "초고가1건" not in leaders["아파트"].tolist()


def test_selection_uses_only_requested_lookback_window():
    df = pd.DataFrame(
        [
            _row("11680", "과거대장", 100, 3_000, month="2022-01"),
            _row("11680", "현재대장", 5, 900, month="2025-12"),
        ]
    )

    leaders = select_leader_apartments(
        df, lookback_months=12, min_transactions=3, volume_quantile=0.70, as_of="2025-12"
    )

    assert leaders.iloc[0]["아파트"] == "현재대장"
    assert leaders.iloc[0]["관찰시작"] == "2025-01"
    assert leaders.iloc[0]["관찰종료"] == "2025-12"


def test_stale_region_uses_its_latest_month_and_is_flagged():
    df = pd.DataFrame(
        [
            _row("11680", "현재단지", 5, 1_000, month="2025-12"),
            _row("41590", "지연단지", 8, 800, month="2024-01", sido="경기"),
        ]
    )

    leaders = select_leader_apartments(
        df, lookback_months=12, min_transactions=1, volume_quantile=0, as_of="2025-12"
    ).set_index("지역코드")

    assert leaders.loc["41590", "관찰종료"] == "2024-01"
    assert leaders.loc["41590", "데이터경과개월"] == 23
    assert leaders.loc["41590", "데이터상태"] == "23개월 지연"


def test_tie_break_is_deterministic_and_same_name_in_different_dong_is_separate():
    df = pd.DataFrame(
        [
            _row("11680", "동명단지", 5, 1_000, legal_dong="나동"),
            _row("11680", "동명단지", 5, 1_000, legal_dong="가동"),
        ]
    )

    leaders = select_leader_apartments(df, min_transactions=1, volume_quantile=0)

    assert leaders.iloc[0]["법정동"] == "가동"
    assert leaders.iloc[0]["전체단지수"] == 2


def test_flow_reaggregates_duplicate_months_with_volume_weights():
    df = pd.DataFrame(
        [
            _row("11680", "대표", 1, 1_000, month="2025-01", average_price=100_000),
            _row("11680", "대표", 3, 2_000, month="2025-01", average_price=200_000),
            _row("11680", "대표", 2, 2_500, month="2025-02", average_price=250_000),
        ]
    )

    flow = get_leader_apartment_flow(df, "11680", "테스트동", "대표", 2025, 2025)

    january = flow.set_index("연월").loc["2025-01"]
    assert january["거래량"] == 4
    assert january["평균가격"] == pytest.approx(175_000)
    assert january["평균단가_per_m2"] == pytest.approx(1_750)


def test_missing_required_column_raises_clear_error():
    with pytest.raises(ValueError, match="필수 컬럼"):
        select_leader_apartments(pd.DataFrame({"연월": ["2025-01"]}))


def test_complex_loader_builds_cache_and_excludes_cancelled_trade(tmp_path, monkeypatch):
    raw_path = tmp_path / "apt.csv"
    cache_path = tmp_path / "apt_complex_monthly.parquet"
    raw = pd.DataFrame(
        [
            {
                "년": "2025", "월": "1", "지역코드": "11680", "법정동": "대치동",
                "아파트": "테스트", "단지": "", "전용면적": "84", "거래금액": "120,000",
                "해제여부": "",
            },
            {
                "년": "2025", "월": "1", "지역코드": "11680", "법정동": "대치동",
                "아파트": "테스트", "단지": "", "전용면적": "84", "거래금액": "130,000",
                "해제여부": "O",
            },
        ]
    )
    raw.to_csv(raw_path, index=False, encoding="cp949")
    monkeypatch.setattr(data_loader, "APT_PATH", str(raw_path))
    monkeypatch.setattr(data_loader, "APT_COMPLEX_CACHE_PARQUET", str(cache_path))

    result = data_loader.load_apt_complex_data(chunksize=1, force_rebuild=True)

    assert result.shape[0] == 1
    assert result.iloc[0]["거래량"] == 1
    assert result.iloc[0]["평균가격"] == pytest.approx(120_000)
    assert cache_path.exists()
    cached = data_loader.load_apt_complex_data(force_rebuild=False)
    pd.testing.assert_frame_equal(cached, result)

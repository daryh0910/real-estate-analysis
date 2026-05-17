import pandas as pd

import data_loader


def test_load_movein_plan_data_normalizes_types_and_filters_invalid(tmp_path, monkeypatch):
    csv_path = tmp_path / "movein_plan_complex_monthly.csv"
    pd.DataFrame([
        {
            "source": "reb_movein_plan",
            "단지명": "테스트아파트",
            "주소": "경기도 화성시 반송동 1",
            "시도": "경기도",
            "시군구": "화성시",
            "입주예정연월": "2026-03",
            "세대수": "1,234",
            "공급세대수": "1,200",
            "validation_status": "ok",
        },
        {
            "source": "bad",
            "단지명": "보류단지",
            "주소": "",
            "시도": "서울특별시",
            "시군구": "강남구",
            "입주예정연월": "",
            "세대수": "100",
            "공급세대수": "100",
            "validation_status": "error",
        },
    ]).to_csv(csv_path, index=False, encoding="utf-8-sig")
    monkeypatch.setattr(data_loader, "MOVEIN_PLAN_PATH", str(csv_path))

    df = data_loader.load_movein_plan_data(include_invalid=False)

    assert len(df) == 1
    assert df.loc[0, "시도"] == "경기"
    assert df.loc[0, "연도"] == 2026
    assert df.loc[0, "월"] == 3
    assert df.loc[0, "집계세대수"] == 1234


def test_aggregate_movein_sigungu_monthly_sums_households_and_counts_complexes():
    df = pd.DataFrame([
        {"complex_id": "a", "단지명": "A", "시도": "경기", "시군구": "화성시", "입주예정연월": "2026-03", "세대수": 100, "공급세대수": 90, "validation_status": "ok"},
        {"complex_id": "b", "단지명": "B", "시도": "경기", "시군구": "화성시", "입주예정연월": "2026-03", "세대수": None, "공급세대수": 50, "validation_status": "warn"},
        {"complex_id": "c", "단지명": "C", "시도": "경기", "시군구": "수원시", "입주예정연월": "2026-04", "세대수": 70, "공급세대수": 70, "validation_status": "error"},
    ])

    agg = data_loader.aggregate_movein_sigungu_monthly(df)

    assert len(agg) == 1
    row = agg.iloc[0]
    assert row["연월"] == "2026-03"
    assert row["시도"] == "경기"
    assert row["시군구"] == "화성시"
    assert row["입주예정_세대수"] == 150
    assert row["입주예정_단지수"] == 2


def test_aggregate_movein_sido_monthly_sums_sigungu_rows():
    df = pd.DataFrame([
        {"complex_id": "a", "시도": "경기", "시군구": "화성시", "입주예정연월": "2026-03", "세대수": 100, "공급세대수": 100, "validation_status": "ok"},
        {"complex_id": "b", "시도": "경기", "시군구": "수원시", "입주예정연월": "2026-03", "세대수": 50, "공급세대수": 50, "validation_status": "ok"},
    ])

    agg = data_loader.aggregate_movein_sido_monthly(df)

    assert len(agg) == 1
    assert agg.iloc[0]["입주예정_세대수"] == 150
    assert agg.iloc[0]["입주예정_단지수"] == 2


def test_movein_loader_rejects_invalid_month(tmp_path, monkeypatch):
    csv_path = tmp_path / "movein_plan_complex_monthly.csv"
    pd.DataFrame([
        {"단지명": "정상", "시도": "경기도", "시군구": "화성시", "입주예정연월": "2026-12", "세대수": "100", "validation_status": "ok"},
        {"단지명": "비정상", "시도": "경기도", "시군구": "화성시", "입주예정연월": "2026-99", "세대수": "50", "validation_status": "ok"},
    ]).to_csv(csv_path, index=False, encoding="utf-8-sig")
    monkeypatch.setattr(data_loader, "MOVEIN_PLAN_PATH", str(csv_path))

    df = data_loader.load_movein_plan_data()

    assert set(df["입주예정연월"]) == {"2026-12"}
    assert df["월"].between(1, 12).all()


def test_aggregate_movein_normalizes_sido_and_empty_schema_includes_year_month():
    df = pd.DataFrame([
        {"complex_id": "a", "시도": "경기도", "시군구": "화성시", "입주예정연월": "2026-03", "세대수": 100, "validation_status": "ok"},
        {"complex_id": "bad", "시도": "경기도", "시군구": "화성시", "입주예정연월": "2026-99", "세대수": 50, "validation_status": "ok"},
    ])

    agg = data_loader.aggregate_movein_sido_monthly(df)
    empty = data_loader.aggregate_movein_sido_monthly(pd.DataFrame())

    assert len(agg) == 1
    assert agg.iloc[0]["시도"] == "경기"
    assert agg.iloc[0]["월"] == 3
    assert {"연도", "월"}.issubset(empty.columns)


def test_aggregate_movein_uses_completion_month_fallback_and_counts_blank_complex_ids():
    df = pd.DataFrame([
        {"complex_id": "", "단지명": "A", "시도": "경기도", "시군구": "화성시", "입주예정연월": "", "준공연월": "2026-05", "세대수": 100, "validation_status": "ok"},
        {"complex_id": "", "단지명": "B", "시도": "경기도", "시군구": "화성시", "입주예정연월": "", "준공연월": "2026-05", "세대수": 50, "validation_status": "ok"},
    ])

    agg = data_loader.aggregate_movein_sigungu_monthly(df)

    assert len(agg) == 1
    assert agg.iloc[0]["연월"] == "2026-05"
    assert agg.iloc[0]["입주예정_세대수"] == 150
    assert agg.iloc[0]["입주예정_단지수"] == 2

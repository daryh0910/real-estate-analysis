"""시군구별 대장아파트 선정과 월별 흐름 추출."""

from __future__ import annotations

import re
from collections.abc import Mapping

import pandas as pd


PYEONG_PER_M2 = 3.305785
COMPLEX_KEYS = ["시도", "지역코드", "법정동", "아파트"]
REQUIRED_COLUMNS = [*COMPLEX_KEYS, "연월", "거래량", "평균가격", "평균단가_per_m2"]
_MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _validate_columns(df: pd.DataFrame) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"단지 데이터 필수 컬럼이 없습니다: {', '.join(missing)}")


def _prepare_complex_data(df: pd.DataFrame) -> pd.DataFrame:
    """선정에 사용할 타입과 유효값을 정리한다."""
    _validate_columns(df)
    prepared = df.copy()
    prepared["지역코드"] = prepared["지역코드"].astype(str).str.strip().str.zfill(5)
    for column in ("시도", "법정동", "아파트"):
        prepared[column] = prepared[column].fillna("").astype(str).str.strip()
    for column in ("거래량", "평균가격", "평균단가_per_m2"):
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    prepared["연월_period"] = pd.to_datetime(
        prepared["연월"].astype(str), format="%Y-%m", errors="coerce"
    ).dt.to_period("M")
    prepared = prepared.dropna(
        subset=["연월_period", "거래량", "평균가격", "평균단가_per_m2"]
    )
    prepared = prepared[
        (prepared["지역코드"] != "00000")
        & (prepared["법정동"] != "")
        & (prepared["아파트"] != "")
        & (prepared["거래량"] > 0)
        & (prepared["평균가격"] > 0)
        & (prepared["평균단가_per_m2"] > 0)
    ].copy()
    prepared["평균평당가격"] = prepared["평균단가_per_m2"] * PYEONG_PER_M2
    return prepared


def _aggregate_complexes(df: pd.DataFrame) -> pd.DataFrame:
    """월별 평균을 거래량 가중 평균으로 단지별 재집계한다."""
    weighted = df.assign(
        _가격합계=df["평균가격"] * df["거래량"],
        _단가합계=df["평균단가_per_m2"] * df["거래량"],
    )
    aggregated = (
        weighted.groupby(COMPLEX_KEYS, as_index=False, observed=True)
        .agg(
            거래량=("거래량", "sum"),
            _가격합계=("_가격합계", "sum"),
            _단가합계=("_단가합계", "sum"),
        )
    )
    aggregated["평균가격"] = aggregated["_가격합계"] / aggregated["거래량"]
    aggregated["평균단가_per_m2"] = aggregated["_단가합계"] / aggregated["거래량"]
    aggregated["평균평당가격"] = aggregated["평균단가_per_m2"] * PYEONG_PER_M2
    return aggregated.drop(columns=["_가격합계", "_단가합계"])


def _parse_month_period(value: str | pd.Period, label: str) -> pd.Period:
    """YYYY-MM 문자열 또는 월 단위 Period를 검증해 반환한다."""
    if isinstance(value, pd.Period):
        if value.freqstr != "M":
            raise ValueError(f"{label}은 월 단위 Period여야 합니다.")
        return value
    if not isinstance(value, str) or not _MONTH_PATTERN.fullmatch(value.strip()):
        raise ValueError(f"{label}은 YYYY-MM 형식이어야 합니다: {value!r}")
    return pd.Period(value.strip(), freq="M")


def _parse_period_range(
    start_period: str | pd.Period | None,
    end_period: str | pd.Period | None,
) -> tuple[pd.Period, pd.Period] | None:
    """선택 월 경계가 둘 다 있고 순서가 올바른지 검증한다."""
    if (start_period is None) != (end_period is None):
        raise ValueError("시작월과 종료월은 함께 제공해야 합니다.")
    if start_period is None:
        return None
    start = _parse_month_period(start_period, "시작월")
    end = _parse_month_period(end_period, "종료월")
    if start > end:
        raise ValueError("시작월은 종료월보다 늦을 수 없습니다.")
    return start, end


def _aggregate_monthly_flow(df: pd.DataFrame) -> pd.DataFrame:
    """단지 또는 지역 데이터를 월별 거래량 가중 평균으로 집계한다."""
    if df.empty:
        return pd.DataFrame()
    weighted = df.assign(
        _가격합계=df["평균가격"] * df["거래량"],
        _단가합계=df["평균단가_per_m2"] * df["거래량"],
    )
    flow = (
        weighted.groupby("연월_period", as_index=False, observed=True)
        .agg(
            거래량=("거래량", "sum"),
            _가격합계=("_가격합계", "sum"),
            _단가합계=("_단가합계", "sum"),
        )
        .sort_values("연월_period")
    )
    flow["연월"] = flow["연월_period"].astype(str)
    flow["평균가격"] = flow["_가격합계"] / flow["거래량"]
    flow["평균단가_per_m2"] = flow["_단가합계"] / flow["거래량"]
    flow["평균평당가격"] = flow["평균단가_per_m2"] * PYEONG_PER_M2
    flow["거래량"] = flow["거래량"].round().astype(int)
    return flow[
        ["연월", "거래량", "평균가격", "평균단가_per_m2", "평균평당가격"]
    ].reset_index(drop=True)


def select_leader_apartments(
    df: pd.DataFrame,
    lookback_months: int = 24,
    min_transactions: int = 3,
    volume_quantile: float = 0.70,
    as_of: str | pd.Period | None = None,
    start_period: str | pd.Period | None = None,
    end_period: str | pd.Period | None = None,
) -> pd.DataFrame:
    """시군구별 거래량 상위 후보 중 평당가격이 가장 높은 단지를 선정한다.

    후보 기준은 관찰기간 거래량이 구별 ``volume_quantile`` 분위 이상이면서
    ``min_transactions``건 이상인 단지다. 동률은 거래량 내림차순, 법정동과
    아파트명 오름차순으로 정해 실행마다 같은 결과를 보장한다.
    """
    if lookback_months < 1:
        raise ValueError("관찰기간은 1개월 이상이어야 합니다.")
    if min_transactions < 1:
        raise ValueError("최소 거래건수는 1건 이상이어야 합니다.")
    if not 0 <= volume_quantile <= 1:
        raise ValueError("거래량 분위 기준은 0과 1 사이여야 합니다.")
    explicit_period = _parse_period_range(start_period, end_period)

    prepared = _prepare_complex_data(df)
    if prepared.empty:
        return pd.DataFrame()

    latest = (
        explicit_period[1]
        if explicit_period is not None
        else pd.Period(as_of, freq="M") if as_of is not None else prepared["연월_period"].max()
    )
    leader_rows: list[pd.Series] = []
    for _, region_history in prepared.groupby(["시도", "지역코드"], sort=True, observed=True):
        if explicit_period is not None:
            region_start, requested_end = explicit_period
            region_window = region_history[
                region_history["연월_period"].between(
                    region_start, requested_end, inclusive="both"
                )
            ]
            if region_window.empty:
                continue
            region_latest = region_window["연월_period"].max()
        else:
            region_latest = min(latest, region_history["연월_period"].max())
            region_start = region_latest - (lookback_months - 1)
            region_window = region_history[
                region_history["연월_period"].between(
                    region_start, region_latest, inclusive="both"
                )
            ]
        if region_window.empty:
            continue
        region = _aggregate_complexes(region_window)
        volume_cutoff = float(region["거래량"].quantile(volume_quantile, interpolation="lower"))
        threshold = max(float(min_transactions), volume_cutoff)
        candidates = region[region["거래량"] >= threshold].copy()
        if candidates.empty:
            continue
        candidates = candidates.sort_values(
            ["평균평당가격", "거래량", "법정동", "아파트"],
            ascending=[False, False, True, True],
            kind="mergesort",
        )
        leader = candidates.iloc[0].copy()
        leader["거래량기준"] = threshold
        leader["후보단지수"] = len(candidates)
        leader["전체단지수"] = len(region)
        data_lag_months = latest.ordinal - region_latest.ordinal
        leader["관찰시작"] = str(region_start)
        leader["관찰종료"] = str(region_latest)
        leader["전체최신월"] = str(latest)
        leader["데이터경과개월"] = data_lag_months
        leader["데이터상태"] = "최신" if data_lag_months == 0 else f"{data_lag_months}개월 지연"
        leader_rows.append(leader)

    if not leader_rows:
        return pd.DataFrame()

    result = pd.DataFrame(leader_rows)
    result["거래량"] = result["거래량"].round().astype(int)
    result["후보단지수"] = result["후보단지수"].astype(int)
    result["전체단지수"] = result["전체단지수"].astype(int)
    result["데이터경과개월"] = result["데이터경과개월"].astype(int)
    return result.sort_values(["시도", "지역코드"], kind="mergesort").reset_index(drop=True)


def get_leader_apartment_flow(
    df: pd.DataFrame,
    region_code: str,
    legal_dong: str,
    apartment: str,
    start_year: int | None = None,
    end_year: int | None = None,
    start_period: str | pd.Period | None = None,
    end_period: str | pd.Period | None = None,
) -> pd.DataFrame:
    """선정된 단지의 월별 평균가격·평당가격·거래량 흐름을 반환한다."""
    explicit_period = _parse_period_range(start_period, end_period)
    prepared = _prepare_complex_data(df)
    target = prepared[
        (prepared["지역코드"] == str(region_code).strip().zfill(5))
        & (prepared["법정동"] == str(legal_dong).strip())
        & (prepared["아파트"] == str(apartment).strip())
    ].copy()
    if start_year is not None:
        target = target[target["연월_period"].dt.year >= int(start_year)]
    if end_year is not None:
        target = target[target["연월_period"].dt.year <= int(end_year)]
    if explicit_period is not None:
        target = target[
            target["연월_period"].between(*explicit_period, inclusive="both")
        ]
    return _aggregate_monthly_flow(target)


def get_region_market_flow(
    df: pd.DataFrame,
    region_code: str,
    start_period: str | pd.Period | None = None,
    end_period: str | pd.Period | None = None,
) -> pd.DataFrame:
    """시군구 전체의 월별 가격·평당가격·총거래량 흐름을 반환한다."""
    explicit_period = _parse_period_range(start_period, end_period)
    prepared = _prepare_complex_data(df)
    target = prepared[
        prepared["지역코드"] == str(region_code).strip().zfill(5)
    ].copy()
    if explicit_period is not None:
        target = target[
            target["연월_period"].between(*explicit_period, inclusive="both")
        ]
    return _aggregate_monthly_flow(target)


def map_region_code(region_code: str | int) -> str | None:
    """실거래 지역코드를 현재 GeoJSON의 시군구 코드로 변환한다."""
    code = str(region_code).strip().zfill(5)
    if code in {"41192", "41194", "41196"}:
        return None
    if code == "27720":
        return "47720"
    if code.startswith("51"):
        return "42" + code[2:]
    if code.startswith("52"):
        return "45" + code[2:]
    return code


def _get_field(value: object, field: str) -> object:
    if isinstance(value, Mapping):
        return value.get(field)
    return getattr(value, field, None)


def _normalize_selected_code(value: object) -> str | None:
    if value is None:
        return None
    code = str(value).strip()
    if code.endswith(".0"):
        code = code[:-2]
    code = code.zfill(5)
    return code if re.fullmatch(r"\d{5}", code) else None


def extract_selected_region_code(selection: object) -> str | None:
    """Streamlit Plotly 선택 이벤트에서 첫 번째 시군구 코드를 추출한다."""
    selection_payload = _get_field(selection, "selection")
    if selection_payload is None:
        selection_payload = selection
    points = _get_field(selection_payload, "points")
    if not points:
        return None
    point = points[0]
    customdata = _get_field(point, "customdata")
    if isinstance(customdata, (list, tuple)) and customdata:
        code = _normalize_selected_code(customdata[0])
        if code is not None:
            return code
    return _normalize_selected_code(_get_field(point, "location"))

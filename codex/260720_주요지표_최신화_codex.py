"""주요 동적 지표를 로컬 스테이징에서 수집하고 검증 후 data/에 병합한다."""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
STAGING_ROOT = PROJECT_ROOT / "codex" / "260720_주요지표_최신화_codex"
STAGING_DATA = STAGING_ROOT / "staging_data"
BACKUP_DATA = STAGING_ROOT / "backup_data"
TARGET_DATA = PROJECT_ROOT / "data"


DATA_KEYS = {
    "unsold_housing_sido_monthly.csv": ["연월", "시도"],
    "population_migration_sido_monthly.csv": ["연월", "시도"],
    "base_rate_monthly.csv": ["연월"],
    "housing_price_index_sido_monthly.csv": ["연월", "시도"],
    "land_price_change_sido_monthly.csv": ["연월", "시도"],
    "construction_pipeline_sido_monthly.csv": ["연월", "시도"],
    "csi_monthly.csv": ["연월"],
    "bok_housing_loan_sido_monthly.csv": ["연월", "시도"],
    "kb_market_supply_demand_monthly.csv": ["연월", "시도"],
    "kb_indicators_regional_monthly.csv": ["연월", "지역명"],
    "kb_indicators_national_monthly.csv": ["연월"],
    "kosis_household_asset_sido_yearly.csv": ["연도", "시도"],
    "kosis_household_asset_quintile_yearly.csv": ["연도", "소득분위"],
    "nts_income_sigungu_yearly.csv": ["연도", "지역코드"],
    "bok_money_supply_monthly.csv": ["연월"],
    "bok_deposit_loan_spread_monthly.csv": ["연월"],
    "bok_household_credit_quarterly.csv": ["연월"],
    "krihs_sentiment_sido_monthly.csv": ["연월", "지역"],
    "mlit_housing_supply_sido_yearly.csv": ["연도", "시도"],
}


def _latest(frame: pd.DataFrame) -> str:
    for col in ("연월", "연도"):
        if col in frame.columns and frame[col].notna().any():
            return str(frame[col].dropna().max())
    return "N/A"


def _upsert_file(filename: str) -> dict[str, object]:
    staged_path = STAGING_DATA / filename
    target_path = TARGET_DATA / filename
    if not staged_path.exists():
        raise FileNotFoundError(f"수집 산출물 없음: {filename}")

    new = pd.read_csv(staged_path)
    if new.empty:
        raise ValueError(f"수집 결과가 비었습니다: {filename}")
    keys = DATA_KEYS[filename]
    missing = [key for key in keys if key not in new.columns]
    if missing:
        raise ValueError(f"필수 키 누락({filename}): {missing}")
    new = new.drop_duplicates(keys, keep="last")

    if target_path.exists():
        old = pd.read_csv(target_path)
        old_latest = _latest(old)
        key_index = pd.MultiIndex.from_frame(new[keys].astype(str))
        old_index = pd.MultiIndex.from_frame(old[keys].astype(str))
        old = old[~old_index.isin(key_index)].copy()
        merged = pd.concat([old, new], ignore_index=True, sort=False)
        old_columns = list(pd.read_csv(target_path, nrows=0).columns)
    else:
        old = pd.DataFrame()
        old_latest = "N/A"
        merged = new.copy()
        old_columns = []

    if merged.duplicated(keys).any():
        raise ValueError(f"병합 후 중복 키 발생: {filename}")

    # 과거 가계자산 파일의 단일 컬럼명을 현재 표준 컬럼과 연결한다.
    if (
        filename == "kosis_household_asset_sido_yearly.csv"
        and "전가구 평균" in merged.columns
    ):
        legacy_asset = pd.to_numeric(merged["전가구 평균"], errors="coerce")
        if "가구_자산평균" not in merged.columns:
            merged["가구_자산평균"] = legacy_asset
        else:
            merged["가구_자산평균"] = pd.to_numeric(
                merged["가구_자산평균"], errors="coerce"
            ).fillna(legacy_asset)

    new_latest = _latest(new)
    merged_latest = _latest(merged)
    if old_latest != "N/A" and merged_latest < old_latest:
        raise ValueError(f"최신 시점 역행({filename}): {old_latest} -> {merged_latest}")

    columns = old_columns + [col for col in merged.columns if col not in old_columns]
    merged = merged[columns]
    sort_keys = [key for key in keys if key in merged.columns]
    merged = merged.sort_values(sort_keys, kind="mergesort").reset_index(drop=True)

    BACKUP_DATA.mkdir(parents=True, exist_ok=True)
    if target_path.exists() and not (BACKUP_DATA / filename).exists():
        shutil.copy2(target_path, BACKUP_DATA / filename)
    temp_path = target_path.with_suffix(target_path.suffix + ".tmp")
    merged.to_csv(temp_path, index=False, encoding="utf-8-sig")
    os.replace(temp_path, target_path)
    return {
        "file": filename,
        "old_rows": len(old),
        "new_rows": len(new),
        "merged_rows": len(merged),
        "old_latest": old_latest,
        "fetched_latest": new_latest,
        "latest": merged_latest,
    }


def _run_task(label: str, function, kwargs: dict, outputs: list[str], results: list) -> None:
    print(f"\n=== {label} ===")
    try:
        function(**kwargs)
        task_results = [_upsert_file(filename) for filename in outputs]
        results.extend(task_results)
        for result in task_results:
            print(f"  병합 완료: {result}")
    except Exception as exc:
        results.append(
            {"file": ",".join(outputs), "status": "실패", "error": type(exc).__name__}
        )
        print(f"  수집/병합 실패: {type(exc).__name__}")


def main() -> None:
    import download_demand_data as demand
    import download_public_data as public

    STAGING_DATA.mkdir(parents=True, exist_ok=True)
    public.OUTPUT_DIR = str(STAGING_DATA)
    demand.OUTPUT_DIR = str(STAGING_DATA)

    # KOSIS 실패 시 인증키가 명령행에 포함되는 PowerShell 폴백은 사용하지 않는다.
    def _no_powershell_fallback(*_args, **_kwargs):
        raise RuntimeError("PowerShell KOSIS 폴백 비활성화")

    demand._kosis_api_via_powershell = _no_powershell_fallback

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--only",
        action="append",
        default=[],
        help="실행할 작업 라벨. 여러 번 지정 가능하며 미지정 시 전체 실행",
    )
    args = parser.parse_args()

    results: list[dict[str, object]] = []
    tasks = [
        # 국토부 원 API는 현재 HTTP 오류가 반복되어 동일 지표의 BOK ECOS 대체원을 사용한다.
        ("미분양", public._fetch_unsold_housing_alt, {"start_ym": "202501", "end_ym": "202605"}, ["unsold_housing_sido_monthly.csv"]),
        ("인구이동", public.fetch_population_migration, {"start_year": 2025, "end_year": 2026}, ["population_migration_sido_monthly.csv"]),
        ("기준금리·시장금리", public.fetch_base_rate, {"start_ym": "202501", "end_ym": "202606"}, ["base_rate_monthly.csv"]),
        ("주택가격지수", public.fetch_housing_price_index, {"start_ym": "202501", "end_ym": "202606"}, ["housing_price_index_sido_monthly.csv"]),
        ("지가변동률", public.fetch_land_price_change, {"start_ym": "202501", "end_ym": "202605"}, ["land_price_change_sido_monthly.csv"]),
        ("착공·준공", public.fetch_construction_pipeline, {"start_ym": "202501", "end_ym": "202605"}, ["construction_pipeline_sido_monthly.csv"]),
        ("소비자심리지수", public.fetch_csi, {"start_ym": "202501", "end_ym": "202606"}, ["csi_monthly.csv"]),
        ("지역별 주택담보대출", demand.fetch_bok_housing_loan, {"start_ym": "202501", "end_ym": "202605"}, ["bok_housing_loan_sido_monthly.csv"]),
        ("KB 수급지표", demand.fetch_kb_market_data, {}, ["kb_market_supply_demand_monthly.csv"]),
        ("KB 핵심지표", demand.fetch_kb_indicators, {}, ["kb_indicators_regional_monthly.csv", "kb_indicators_national_monthly.csv"]),
        ("가계자산", demand.fetch_kosis_household_asset, {"start_year": 2024, "end_year": 2025}, ["kosis_household_asset_sido_yearly.csv"]),
        ("소득분위별 가계자산", demand.fetch_kosis_household_asset_quintile, {"start_year": 2024, "end_year": 2025}, ["kosis_household_asset_quintile_yearly.csv"]),
        ("국세청 근로소득", demand.fetch_nts_income_data, {"start_year": 2023, "end_year": 2024}, ["nts_income_sigungu_yearly.csv"]),
        ("M2", public.fetch_m2_money_supply, {"start_ym": "202501", "end_ym": "202605"}, ["bok_money_supply_monthly.csv"]),
        ("예대금리차", public.fetch_deposit_loan_spread, {"start_ym": "202501", "end_ym": "202605"}, ["bok_deposit_loan_spread_monthly.csv"]),
        ("가계신용", public.fetch_household_credit, {"start_ym": "202501", "end_ym": "202606"}, ["bok_household_credit_quarterly.csv"]),
        ("부동산 소비심리", demand.fetch_krihs_sentiment, {"start_ym": "202501", "end_ym": "202606"}, ["krihs_sentiment_sido_monthly.csv"]),
        ("주택보급률", demand.fetch_housing_supply_rate, {"start_year": 2023, "end_year": 2024}, ["mlit_housing_supply_sido_yearly.csv"]),
    ]

    selected = set(args.only)
    for label, function, kwargs, outputs in tasks:
        if selected and label not in selected:
            continue
        _run_task(label, function, kwargs, outputs, results)

    print("\n주요 지표 최신화 요약")
    for result in results:
        print(result)


if __name__ == "__main__":
    main()

"""2026-05~06 실거래를 로컬 스테이징에서 수집해 배포 캐시를 최신화한다.

OneDrive 원천 CSV는 법정동코드 조회에만 사용하고 수정하지 않는다. 새 원천은
프로젝트 codex 폴더에 저장한 뒤 기존 Parquet와 월 단위로 병합한다.
"""

from __future__ import annotations

import csv
import os
import shutil
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
STAGING_ROOT = PROJECT_ROOT / "codex" / "260720_실거래캐시_최신화_codex"
RAW_DIR = STAGING_ROOT / "raw"
BUILD_DIR = STAGING_ROOT / "build_cache"
BACKUP_DIR = STAGING_ROOT / "backup_cache"

START_YM = 202605
END_YM = 202606
TARGET_MONTHS = {"2026-05", "2026-06"}


def _ensure_csv_header(path: Path, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", newline="", encoding="cp949") as file_obj:
        csv.writer(file_obj).writerow(columns)


def _download_raw() -> tuple[Path, Path]:
    import update_data

    trade_path = RAW_DIR / "260720_아파트매매_202605_202606_codex.csv"
    rent_path = RAW_DIR / "260720_아파트임대차_202605_202606_codex.csv"
    progress_path = STAGING_ROOT / "260720_실거래수집진행_codex.json"
    _ensure_csv_header(trade_path, update_data.TRADE_COLUMNS)
    _ensure_csv_header(rent_path, update_data.RENT_COLUMNS)

    update_data.APT_CSV = str(trade_path)
    update_data.RENT_CSV = str(rent_path)
    update_data.PROGRESS_FILE = str(progress_path)

    region_codes = update_data.load_region_codes()
    date_range = update_data.generate_date_range(START_YM, END_YM)
    progress = update_data.load_progress()
    progress.setdefault("trade_done", [])
    progress.setdefault("rent_done", [])

    results = {}
    for data_type in ("trade", "rent"):
        results[data_type] = update_data.download_data(
            data_type,
            region_codes,
            date_range,
            progress,
            dry_run=False,
            sleep_sec=0.15,
        )

    expected_pairs = len(region_codes) * len(date_range)
    for data_type, path in (("trade", trade_path), ("rent", rent_path)):
        existing_pairs, _ = update_data._read_existing_pairs(
            str(path), target_months=date_range
        )
        progress_pairs = {
            tuple(item) for item in progress.get(f"{data_type}_done", [])
        }
        completed_pairs = existing_pairs | progress_pairs
        if len(completed_pairs) != expected_pairs:
            raise RuntimeError(
                f"{data_type} 수집 미완료: {len(completed_pairs)}/{expected_pairs} 지역-월"
            )
        if results[data_type]["new_rows"] == 0 and path.stat().st_size <= 200:
            raise RuntimeError(f"{data_type} 신규 행이 없습니다.")

    return trade_path, rent_path


def _weighted_collapse(
    frame: pd.DataFrame,
    keys: list[str],
    weight_col: str,
    value_cols: list[str],
) -> pd.DataFrame:
    frame = frame.copy()
    frame[weight_col] = pd.to_numeric(frame[weight_col], errors="coerce").fillna(0)
    for col in value_cols:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
        frame[f"__weighted_{col}"] = frame[col] * frame[weight_col]

    agg_spec = {weight_col: "sum"}
    agg_spec.update({f"__weighted_{col}": "sum" for col in value_cols})
    result = frame.groupby(keys, dropna=False, observed=True, as_index=False).agg(agg_spec)
    for col in value_cols:
        numerator = result.pop(f"__weighted_{col}")
        result[col] = numerator / result[weight_col].where(result[weight_col] > 0)
    result[weight_col] = result[weight_col].astype(int)
    return result


def _merge_cache(
    target_path: Path,
    staged_path: Path,
    keys: list[str],
    weight_col: str,
    value_cols: list[str],
) -> dict[str, object]:
    old = pd.read_parquet(target_path)
    new = pd.read_parquet(staged_path)
    if new.empty:
        raise RuntimeError(f"신규 캐시가 비었습니다: {staged_path.name}")
    if set(new["연월"].astype(str).unique()) != TARGET_MONTHS:
        raise RuntimeError(
            f"신규 월 범위 오류({staged_path.name}): {sorted(new['연월'].unique())}"
        )

    base = old[~old["연월"].astype(str).isin(TARGET_MONTHS)].copy()
    merged = pd.concat([base, new], ignore_index=True)
    merged = _weighted_collapse(merged, keys, weight_col, value_cols)

    if "연도" not in merged.columns:
        merged["연도"] = merged["연월"].astype(str).str[:4].astype(int)
    if "월" not in merged.columns:
        merged["월"] = merged["연월"].astype(str).str[5:7].astype(int)

    column_order = [col for col in old.columns if col in merged.columns]
    extra_columns = [col for col in merged.columns if col not in column_order]
    merged = merged[column_order + extra_columns].sort_values(keys, kind="mergesort")

    if merged.duplicated(keys).any():
        raise RuntimeError(f"병합 후 중복 키 발생: {target_path.name}")
    if merged[weight_col].le(0).any():
        raise RuntimeError(f"병합 후 비양수 거래량 발생: {target_path.name}")
    if merged["연월"].max() != "2026-06":
        raise RuntimeError(f"최신월 검증 실패: {target_path.name}")

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    backup_path = BACKUP_DIR / target_path.name
    if not backup_path.exists():
        shutil.copy2(target_path, backup_path)

    temp_path = target_path.with_suffix(target_path.suffix + ".tmp")
    merged.to_parquet(temp_path, index=False)
    os.replace(temp_path, target_path)

    return {
        "file": target_path.name,
        "old_rows": len(old),
        "new_rows": len(merged),
        "latest": merged["연월"].max(),
        "target_rows": int(merged["연월"].isin(TARGET_MONTHS).sum()),
    }


def _build_and_merge(trade_path: Path, rent_path: Path) -> list[dict[str, object]]:
    import data_loader

    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    valid_trade_path = RAW_DIR / "260720_아파트매매_유효거래_202605_202606_codex.csv"
    trade = pd.read_csv(trade_path, encoding="cp949", dtype=str, low_memory=False)
    cancel_flag = trade["해제여부"].fillna("").astype(str).str.strip()
    trade = trade[cancel_flag == ""].copy()
    trade.to_csv(valid_trade_path, index=False, encoding="cp949")

    data_loader.APT_PATH = str(valid_trade_path)
    data_loader.JEONSE_PATH = str(rent_path)
    data_loader.CACHE_DIR = str(BUILD_DIR)

    cache_names = {
        "apt": "apt_sigungu_monthly.parquet",
        "complex": "apt_complex_monthly.parquet",
        "detail": "apt_sigungu_monthly_detail.parquet",
        "jeonse": "jeonse_sigungu_monthly.parquet",
        "wolse": "wolse_sigungu_monthly.parquet",
        "rent_all": "rent_all_sigungu_monthly.parquet",
    }
    for attr, filename in (
        ("APT_CACHE_PARQUET", cache_names["apt"]),
        ("APT_COMPLEX_CACHE_PARQUET", cache_names["complex"]),
        ("APT_DETAIL_CACHE_PARQUET", cache_names["detail"]),
        ("JEONSE_CACHE_PARQUET", cache_names["jeonse"]),
        ("WOLSE_CACHE_PARQUET", cache_names["wolse"]),
        ("RENT_ALL_CACHE_PARQUET", cache_names["rent_all"]),
    ):
        setattr(data_loader, attr, str(BUILD_DIR / filename))

    data_loader.load_apt_data(force_rebuild=True, chunksize=500_000)
    data_loader.load_apt_complex_data(force_rebuild=True, chunksize=500_000)
    data_loader.load_apt_data_detail(force_rebuild=True, chunksize=500_000)
    for rent_type in ("jeonse", "wolse", "all"):
        data_loader.load_rent_data(rent_type, force_rebuild=True, chunksize=500_000)

    specs = [
        (
            cache_names["apt"],
            ["시도", "지역코드", "연월", "연도", "월"],
            "거래량",
            ["평균가격", "평균단가_per_m2"],
        ),
        (
            cache_names["complex"],
            ["시도", "지역코드", "법정동", "아파트", "연월", "연도", "월"],
            "거래량",
            ["평균가격", "평균단가_per_m2", "평균평당가격"],
        ),
        (
            cache_names["detail"],
            ["시도", "지역코드", "연월", "연도", "월", "건축년도"],
            "거래량",
            ["평균가격", "평균단가_per_m2", "평균준공년차"],
        ),
        (
            cache_names["jeonse"],
            ["시도", "지역코드", "연월", "연도", "월"],
            "임대거래량",
            ["보증금평균", "보증금단가_per_m2"],
        ),
        (
            cache_names["wolse"],
            ["시도", "지역코드", "연월", "연도", "월"],
            "임대거래량",
            ["보증금평균", "보증금단가_per_m2", "월세평균"],
        ),
        (
            cache_names["rent_all"],
            ["시도", "지역코드", "연월", "연도", "월"],
            "임대거래량",
            ["보증금평균", "보증금단가_per_m2", "월세평균"],
        ),
    ]

    results = []
    for filename, keys, weight_col, value_cols in specs:
        results.append(
            _merge_cache(
                PROJECT_ROOT / "cache" / filename,
                BUILD_DIR / filename,
                keys,
                weight_col,
                value_cols,
            )
        )
    return results


def main() -> None:
    STAGING_ROOT.mkdir(parents=True, exist_ok=True)
    trade_path, rent_path = _download_raw()
    results = _build_and_merge(trade_path, rent_path)
    print("\n실거래 캐시 최신화 결과")
    for result in results:
        print(result)


if __name__ == "__main__":
    main()

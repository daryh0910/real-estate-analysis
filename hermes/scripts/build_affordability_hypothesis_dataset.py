#!/usr/bin/env python3
"""
가설검증용 데이터셋 생성기

목표:
- 지역(시군구), 전용면적 범위, 기간을 변수로 두고 아파트 매매가를 산출한다.
- KOSIS 가계금융복지조사 소득분위별 소득/순자산과 결합한다.
- 기본 예시: 서울 시군구별 84㎡ 타입 매매가 vs 소득5분위(현 공개 확보분) 소득/순자산.

주의:
- 현재 확보된 KOSIS DT_1HDAAA10은 소득5분위(상위 20%) 기준이다.
- '상위 10% 소득 + 상위 10% 순자산'은 별도 소득10분위/순자산10분위 원천을 추가 확보해야 정확해진다.
  이 스크립트는 우선 소득5분위를 proxy로 쓰고, 추후 decile 데이터가 확보되면 dimension만 교체한다.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except Exception:
    pass

import data_loader  # noqa: E402


def _read_csv_auto(path: str | Path, **kwargs) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return pd.read_csv(path, encoding=enc, **kwargs)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, **kwargs)


def _clean_amount(s):
    if pd.isna(s):
        return np.nan
    return pd.to_numeric(str(s).replace(",", "").strip(), errors="coerce")


def _resolve_quintile_path() -> Path:
    candidates = [
        Path(getattr(data_loader, "KOSIS_QUINTILE_PATH", "")),
        ROOT / "data" / "kosis_household_asset_quintile_yearly.csv",
        ROOT / "hermes" / "data" / "kosis_household_asset_quintile_yearly.csv",
    ]
    for p in candidates:
        if str(p) and p.exists():
            return p
    raise FileNotFoundError("kosis_household_asset_quintile_yearly.csv를 찾을 수 없습니다. download_demand_data.py --quintile 실행 필요")


def _price_stat(series: pd.Series, stat: str) -> float:
    if series.empty:
        return np.nan
    if stat == "mean":
        return float(series.mean())
    if stat == "median":
        return float(series.median())
    if stat.startswith("q"):
        q = float(stat[1:]) / 100.0
        return float(series.quantile(q))
    raise ValueError(f"지원하지 않는 price_stat: {stat}")


def build_price_dataset(
    sido: str = "서울",
    sigungu: Optional[str] = None,
    region_code: Optional[str] = None,
    area_min: float = 82.0,
    area_max: float = 86.0,
    start_year: int = 2017,
    end_year: int = 2024,
    price_stat: str = "median",
    chunksize: int = 500_000,
) -> pd.DataFrame:
    """원천 실거래 CSV에서 지역/면적/기간 변수 기반 연간 매매가를 산출한다."""
    apt_path = Path(data_loader.APT_PATH)
    if not apt_path.exists():
        raise FileNotFoundError(f"아파트 실거래 원천 파일 없음: {apt_path}")

    usecols = ["년", "월", "지역코드", "법정동", "아파트", "전용면적", "거래금액"]
    rows = []

    reader = pd.read_csv(
        apt_path,
        encoding="cp949",
        dtype=str,
        usecols=usecols,
        chunksize=chunksize,
        on_bad_lines="skip",
    )

    sido_norm = sido.replace("특별시", "").replace("광역시", "").replace("특별자치시", "").replace("특별자치도", "").replace("도", "")
    sigungu_norm = sigungu.strip() if sigungu else None
    region_code_norm = str(region_code).strip() if region_code else None

    for chunk in reader:
        chunk["연도"] = pd.to_numeric(chunk["년"], errors="coerce")
        chunk["월"] = pd.to_numeric(chunk["월"], errors="coerce")
        chunk["전용면적_num"] = pd.to_numeric(chunk["전용면적"], errors="coerce")
        chunk["거래금액_만원"] = chunk["거래금액"].map(_clean_amount)
        chunk["지역코드"] = chunk["지역코드"].astype(str).str.zfill(5)
        chunk = chunk.dropna(subset=["연도", "월", "전용면적_num", "거래금액_만원", "지역코드"])
        chunk = chunk[(chunk["연도"] >= start_year) & (chunk["연도"] <= end_year)]
        chunk = chunk[(chunk["전용면적_num"] >= area_min) & (chunk["전용면적_num"] <= area_max)]
        if chunk.empty:
            continue

        chunk["시도"] = chunk["지역코드"].str[:2].astype(int).map(data_loader.SIDO_CODE_MAP).map(data_loader._normalize_sido)
        chunk["시군구"] = chunk["지역코드"].map(data_loader.SIGUNGU_NAME_MAP)
        chunk = chunk[chunk["시도"] == sido_norm]
        if sigungu_norm:
            chunk = chunk[chunk["시군구"].astype(str).str.contains(sigungu_norm, na=False)]
        if region_code_norm:
            chunk = chunk[chunk["지역코드"] == region_code_norm.zfill(5)]
        if chunk.empty:
            continue
        rows.append(chunk[["연도", "월", "시도", "시군구", "지역코드", "법정동", "아파트", "전용면적_num", "거래금액_만원"]])

    if not rows:
        return pd.DataFrame(columns=[
            "연도", "시도", "시군구", "지역코드", "면적범위", "가격통계", "거래량", "매매가_만원", "매매가_억원"
        ])

    df = pd.concat(rows, ignore_index=True)
    group_cols = ["연도", "시도", "시군구", "지역코드"]
    out = (
        df.groupby(group_cols)
        .agg(
            거래량=("거래금액_만원", "count"),
            평균전용면적=("전용면적_num", "mean"),
            매매가_만원=("거래금액_만원", lambda s: _price_stat(s, price_stat)),
            평균매매가_만원=("거래금액_만원", "mean"),
            중위매매가_만원=("거래금액_만원", "median"),
        )
        .reset_index()
    )
    out["매매가_억원"] = out["매매가_만원"] / 10000.0
    out["면적범위"] = f"{area_min:g}~{area_max:g}㎡"
    out["가격통계"] = price_stat
    return out.sort_values(["연도", "시도", "시군구"]).reset_index(drop=True)


def build_affordability_dataset(price_df: pd.DataFrame, target_quintile: str = "소득5분위") -> pd.DataFrame:
    qpath = _resolve_quintile_path()
    qdf = _read_csv_auto(qpath)
    qdf = qdf[qdf["소득분위"] == target_quintile].copy()
    keep = [
        "연도", "소득분위", "가구_소득평균", "가구_순자산", "가구_자산평균", "가구_부채평균", "DSR"
    ]
    qdf = qdf[[c for c in keep if c in qdf.columns]].copy()

    merged = price_df.merge(qdf, on="연도", how="left")
    # KOSIS 금액 단위는 기존 수집 결과상 만원으로 해석한다.
    if "가구_소득평균" in merged.columns:
        merged["매매가_소득배율"] = merged["매매가_만원"] / merged["가구_소득평균"]
    if "가구_순자산" in merged.columns:
        merged["매매가_순자산배율"] = merged["매매가_만원"] / merged["가구_순자산"]
        merged["순자산_매매가커버리지"] = merged["가구_순자산"] / merged["매매가_만원"]
    if {"가구_순자산", "가구_소득평균", "매매가_만원"}.issubset(merged.columns):
        merged["구매여력_단순점수"] = (merged["가구_순자산"] + merged["가구_소득평균"]) / merged["매매가_만원"]
    merged["비교대상_현재"] = target_quintile
    merged["상위10퍼센트_정확도"] = "미확보: 현재는 소득5분위(상위20%) proxy"
    return merged


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--sido", default="서울", help="시도명. 기본 서울")
    p.add_argument("--sigungu", default=None, help="시군구명 부분검색. 예: 강남구. 미지정 시 시도 내 전체 시군구")
    p.add_argument("--region-code", default=None, help="법정동 지역코드 5자리. 지정 시 우선 필터")
    p.add_argument("--area-min", type=float, default=82.0)
    p.add_argument("--area-max", type=float, default=86.0)
    p.add_argument("--start-year", type=int, default=2017)
    p.add_argument("--end-year", type=int, default=2024)
    p.add_argument("--price-stat", default="median", choices=["mean", "median", "q25", "q75", "q90"])
    p.add_argument("--target-quintile", default="소득5분위")
    p.add_argument("--out-dir", default=str(ROOT / "hermes" / "output" / "affordability_hypothesis"))
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    price = build_price_dataset(
        sido=args.sido,
        sigungu=args.sigungu,
        region_code=args.region_code,
        area_min=args.area_min,
        area_max=args.area_max,
        start_year=args.start_year,
        end_year=args.end_year,
        price_stat=args.price_stat,
    )
    dataset = build_affordability_dataset(price, target_quintile=args.target_quintile)

    scope = args.sido + (f"_{args.sigungu}" if args.sigungu else "_전체시군구")
    scope = scope.replace(" ", "")
    area = f"{args.area_min:g}-{args.area_max:g}m2"
    f1 = out_dir / f"affordability_price_{scope}_{area}_{args.start_year}-{args.end_year}.csv"
    f2 = out_dir / f"affordability_hypothesis_{scope}_{area}_{args.start_year}-{args.end_year}.csv"
    price.to_csv(f1, index=False, encoding="utf-8-sig")
    dataset.to_csv(f2, index=False, encoding="utf-8-sig")

    print(f"price_rows={len(price):,} path={f1}")
    print(f"dataset_rows={len(dataset):,} path={f2}")
    if len(dataset):
        print(dataset.head(10).to_string(index=False))


if __name__ == "__main__":
    main()

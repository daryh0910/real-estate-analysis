#!/usr/bin/env python3
"""
지역/면적/소득분위/설명변수 선택형 가설검증 데이터셋 생성기.

지원 가설 예시:
1) 선택 시군구·선택 면적대 아파트 매매가 vs 선택 소득분위의 소득/순자산
2) 시군구 GRDP 증가율 vs 집값 상승률
3) 국민연금 가입자수 증가율/1인당고지금액 증가율 vs 집값 상승률

기본 산출 단위: 시군구-연도.
"""

from __future__ import annotations

import argparse
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


def read_csv_auto(path: str | Path, **kwargs) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return pd.read_csv(path, encoding=enc, **kwargs)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, **kwargs)


def clean_amount(x):
    if pd.isna(x):
        return np.nan
    return pd.to_numeric(str(x).replace(",", "").strip(), errors="coerce")


def normalize_sido_name(s: str) -> str:
    return str(s).replace("특별시", "").replace("광역시", "").replace("특별자치시", "").replace("특별자치도", "").replace("도", "")


def price_stat(series: pd.Series, stat: str) -> float:
    if series.empty:
        return np.nan
    if stat == "mean":
        return float(series.mean())
    if stat == "median":
        return float(series.median())
    if stat.startswith("q"):
        return float(series.quantile(float(stat[1:]) / 100.0))
    raise ValueError(f"unsupported price_stat={stat}")


def build_area_price_annual(
    sido: str = "서울",
    sigungu: Optional[str] = None,
    region_code: Optional[str] = None,
    area_min: float = 82.0,
    area_max: float = 86.0,
    start_year: int = 2017,
    end_year: int = 2024,
    price_stat_name: str = "median",
    chunksize: int = 500_000,
) -> pd.DataFrame:
    apt_path = Path(data_loader.APT_PATH)
    if not apt_path.exists():
        raise FileNotFoundError(f"아파트 실거래 원천 파일 없음: {apt_path}")

    sido_short = normalize_sido_name(sido)
    region_code = str(region_code).zfill(5) if region_code else None
    sigungu = sigungu.strip() if sigungu else None

    usecols = ["년", "월", "지역코드", "법정동", "아파트", "전용면적", "거래금액"]
    rows = []
    for chunk in pd.read_csv(
        apt_path,
        encoding="cp949",
        dtype=str,
        usecols=usecols,
        chunksize=chunksize,
        on_bad_lines="skip",
    ):
        chunk["연도"] = pd.to_numeric(chunk["년"], errors="coerce")
        chunk["월"] = pd.to_numeric(chunk["월"], errors="coerce")
        chunk["지역코드"] = chunk["지역코드"].astype(str).str.zfill(5)
        chunk["전용면적_num"] = pd.to_numeric(chunk["전용면적"], errors="coerce")
        chunk["거래금액_만원"] = chunk["거래금액"].map(clean_amount)
        chunk = chunk.dropna(subset=["연도", "월", "지역코드", "전용면적_num", "거래금액_만원"])
        chunk = chunk[(chunk["연도"] >= start_year) & (chunk["연도"] <= end_year)]
        chunk = chunk[(chunk["전용면적_num"] >= area_min) & (chunk["전용면적_num"] <= area_max)]
        if chunk.empty:
            continue
        chunk["시도"] = chunk["지역코드"].str[:2].astype(int).map(data_loader.SIDO_CODE_MAP).map(data_loader._normalize_sido)
        chunk["시군구"] = chunk["지역코드"].map(data_loader.SIGUNGU_NAME_MAP)
        chunk = chunk[chunk["시도"] == sido_short]
        if sigungu:
            chunk = chunk[chunk["시군구"].astype(str).str.contains(sigungu, na=False)]
        if region_code:
            chunk = chunk[chunk["지역코드"] == region_code]
        if chunk.empty:
            continue
        rows.append(chunk[["연도", "시도", "시군구", "지역코드", "전용면적_num", "거래금액_만원"]])

    cols = ["연도", "시도", "시군구", "지역코드", "거래량", "평균전용면적", "매매가_만원", "매매가_억원"]
    if not rows:
        return pd.DataFrame(columns=cols)
    df = pd.concat(rows, ignore_index=True)
    out = df.groupby(["연도", "시도", "시군구", "지역코드"]).agg(
        거래량=("거래금액_만원", "count"),
        평균전용면적=("전용면적_num", "mean"),
        매매가_만원=("거래금액_만원", lambda s: price_stat(s, price_stat_name)),
        평균매매가_만원=("거래금액_만원", "mean"),
        중위매매가_만원=("거래금액_만원", "median"),
    ).reset_index()
    out["매매가_억원"] = out["매매가_만원"] / 10000.0
    out["면적범위"] = f"{area_min:g}~{area_max:g}㎡"
    out["가격통계"] = price_stat_name
    out = out.sort_values(["지역코드", "연도"]).reset_index(drop=True)
    out["매매가_전년대비증가율"] = out.groupby("지역코드")["매매가_만원"].pct_change() * 100
    return out


def load_quintile(quintile: str) -> pd.DataFrame:
    candidates = [
        Path(getattr(data_loader, "KOSIS_QUINTILE_PATH", "")),
        ROOT / "data" / "kosis_household_asset_quintile_yearly.csv",
    ]
    path = next((p for p in candidates if str(p) and p.exists()), None)
    if path is None:
        return pd.DataFrame()
    df = read_csv_auto(path)
    if "소득분위" not in df.columns:
        return pd.DataFrame()
    return df[df["소득분위"] == quintile].copy()


def load_nps_annual() -> pd.DataFrame:
    candidates = [Path(getattr(data_loader, "NPS_AGG_PATH", "")), ROOT / "data" / "nps_sigungu_monthly.csv"]
    path = next((p for p in candidates if str(p) and p.exists()), None)
    if path is None:
        return pd.DataFrame()
    df = read_csv_auto(path)
    need = {"지역코드", "연도", "NPS_가입자수", "NPS_고지금액합계", "NPS_1인당고지금액"}
    if not need.issubset(df.columns):
        return pd.DataFrame()
    for c in ["NPS_가입자수", "NPS_고지금액합계", "NPS_1인당고지금액"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["지역코드"] = df["지역코드"].astype(str).str.zfill(5)
    df["_w"] = df["NPS_1인당고지금액"] * df["NPS_가입자수"]
    ann = df.groupby(["지역코드", "연도"]).agg(
        NPS_가입자수=("NPS_가입자수", "mean"),
        NPS_고지금액합계=("NPS_고지금액합계", "mean"),
        _w=("_w", "sum"),
        _sub=("NPS_가입자수", "sum"),
    ).reset_index()
    ann["NPS_1인당고지금액"] = np.where(ann["_sub"] > 0, ann["_w"] / ann["_sub"], np.nan)
    ann = ann.drop(columns=["_w", "_sub"])
    ann = ann.sort_values(["지역코드", "연도"])
    for c in ["NPS_가입자수", "NPS_고지금액합계", "NPS_1인당고지금액"]:
        ann[f"{c}_전년대비증가율"] = ann.groupby("지역코드")[c].pct_change(fill_method=None) * 100
    return ann


def load_sido_monthly_annual(path: str | Path, value_cols: list[str], how: str = "mean") -> pd.DataFrame:
    """시도-월별 CSV를 시도-연도 패널로 변환."""
    p = Path(path)
    if not p.exists():
        p = ROOT / "data" / str(path)
    if not p.exists():
        return pd.DataFrame()
    df = read_csv_auto(p)
    if "시도" not in df.columns or "연도" not in df.columns:
        return pd.DataFrame()
    df["시도"] = df["시도"].map(data_loader._normalize_sido)
    keep_cols = [c for c in value_cols if c in df.columns]
    if not keep_cols:
        return pd.DataFrame()
    for c in keep_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    agg = {c: how for c in keep_cols}
    out = df.groupby(["시도", "연도"], dropna=False).agg(agg).reset_index()
    out = out.sort_values(["시도", "연도"])
    for c in keep_cols:
        if c in ["전입", "전출", "순이동", "착공_호수", "준공_호수", "미분양_호수"]:
            out[f"{c}_전년대비증가율"] = out.groupby("시도")[c].pct_change(fill_method=None) * 100
    return out


def load_csi_annual() -> pd.DataFrame:
    p = ROOT / "data" / "csi_monthly.csv"
    if not p.exists():
        return pd.DataFrame()
    df = read_csv_auto(p)
    if "연도" not in df.columns:
        return pd.DataFrame()
    value_cols = [c for c in ["소비자심리지수", "주택가격전망CSI"] if c in df.columns]
    for c in value_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.groupby("연도")[value_cols].mean().reset_index() if value_cols else pd.DataFrame()


def load_kb_national_annual() -> pd.DataFrame:
    p = ROOT / "data" / "kb_indicators_national_monthly.csv"
    if not p.exists():
        return pd.DataFrame()
    df = read_csv_auto(p)
    if "연월" in df.columns and "연도" not in df.columns:
        df["연도"] = df["연월"].astype(str).str[:4].astype(int)
    value_cols = [c for c in ["KB_선도50지수"] if c in df.columns]
    for c in value_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.groupby("연도")[value_cols].mean().reset_index() if value_cols else pd.DataFrame()


def load_grdp_long() -> pd.DataFrame:
    path = Path(data_loader.GRDP_PATH)
    if not path.exists():
        path = ROOT / "data" / "grdp.csv"
    if not path.exists():
        return pd.DataFrame()
    raw = read_csv_auto(path, header=None)
    # 원천은 1~2행 헤더, 이후 행정구역별(1), 행정구역별(2), 연도별 당해년/기준년 반복 구조
    header_years = list(raw.iloc[0])
    header_kind = list(raw.iloc[1])
    data = raw.iloc[2:].copy()
    rows = []
    for _, r in data.iterrows():
        sido_full = r.iloc[0]
        sigungu = r.iloc[1]
        if pd.isna(sido_full) or pd.isna(sigungu) or sigungu == "소계":
            continue
        sido_short = normalize_sido_name(sido_full)
        code = None
        for k, v in data_loader.SIGUNGU_NAME_MAP.items():
            if data_loader._normalize_sido(data_loader.SIDO_CODE_MAP.get(int(str(k)[:2]), "")) == sido_short and str(v) == str(sigungu):
                code = str(k).zfill(5)
                break
        if code is None:
            continue
        for i in range(2, len(r)):
            year = header_years[i]
            kind = header_kind[i]
            if not str(year).isdigit():
                continue
            # 기준년가격을 우선 사용, 없으면 당해년가격도 보존
            val = pd.to_numeric(str(r.iloc[i]).replace(",", ""), errors="coerce")
            rows.append({"지역코드": code, "시도": sido_short, "시군구": sigungu, "연도": int(year), "GRDP_가격기준": kind, "GRDP": val})
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    base = df[df["GRDP_가격기준"].astype(str).str.contains("기준년", na=False)].copy()
    if base.empty:
        base = df[df["GRDP_가격기준"].astype(str).str.contains("당해", na=False)].copy()
    base = base.dropna(subset=["GRDP"])
    base = base.sort_values(["지역코드", "연도"])
    base["GRDP_전년대비증가율"] = base.groupby("지역코드")["GRDP"].pct_change() * 100
    return base[["지역코드", "연도", "GRDP", "GRDP_전년대비증가율", "GRDP_가격기준"]]


def build_dataset(args) -> pd.DataFrame:
    price = build_area_price_annual(
        sido=args.sido,
        sigungu=args.sigungu,
        region_code=args.region_code,
        area_min=args.area_min,
        area_max=args.area_max,
        start_year=args.start_year,
        end_year=args.end_year,
        price_stat_name=args.price_stat,
        chunksize=args.chunksize,
    )
    out = price.copy()

    q = load_quintile(args.income_quantile)
    if not q.empty:
        keep = [c for c in ["연도", "소득분위", "가구_소득평균", "가구_순자산", "가구_자산평균", "가구_부채평균", "DSR"] if c in q.columns]
        out = out.merge(q[keep], on="연도", how="left")
        out["소득분위_선택값"] = args.income_quantile
        if "가구_소득평균" in out.columns:
            out["매매가_소득배율"] = out["매매가_만원"] / out["가구_소득평균"]
        if "가구_순자산" in out.columns:
            out["매매가_순자산배율"] = out["매매가_만원"] / out["가구_순자산"]
            out["순자산_매매가커버리지"] = out["가구_순자산"] / out["매매가_만원"]
    else:
        out["소득분위_선택값"] = args.income_quantile
        out["소득분위_데이터상태"] = "미확보"

    nps = load_nps_annual()
    if not nps.empty:
        out = out.merge(nps, on=["지역코드", "연도"], how="left")

    grdp = load_grdp_long()
    if not grdp.empty:
        out = out.merge(grdp, on=["지역코드", "연도"], how="left")

    # 시도 단위 월별 설명변수는 연평균/연합계로 집계 후 시군구 패널에 broadcast.
    sido_datasets = [
        ("population_migration_sido_monthly.csv", ["전입", "전출", "순이동"], "sum"),
        ("unsold_housing_sido_monthly.csv", ["미분양_호수"], "mean"),
        ("bok_housing_loan_sido_monthly.csv", ["주담대_잔액", "주담대_증감률", "주담대_비중"], "mean"),
        ("construction_pipeline_sido_monthly.csv", ["착공_호수", "준공_호수", "착공_아파트", "준공_아파트"], "sum"),
        ("land_price_change_sido_monthly.csv", ["지가변동률"], "mean"),
        ("kb_market_supply_demand_monthly.csv", ["KB_매수우위지수", "KB_매매거래지수", "KB_전세수급지수"], "mean"),
    ]
    for fname, cols, how in sido_datasets:
        ann = load_sido_monthly_annual(ROOT / "data" / fname, cols, how=how)
        if not ann.empty:
            out = out.merge(ann, on=["시도", "연도"], how="left")

    csi = load_csi_annual()
    if not csi.empty:
        out = out.merge(csi, on="연도", how="left")

    kb_nat = load_kb_national_annual()
    if not kb_nat.empty:
        out = out.merge(kb_nat, on="연도", how="left")

    if "순이동" in out.columns and "NPS_가입자수" in out.columns:
        out["순이동_NPS가입자대비"] = out["순이동"] / out["NPS_가입자수"]
    if "미분양_호수" in out.columns and "거래량" in out.columns:
        out["미분양_거래량배율"] = out["미분양_호수"] / out["거래량"].replace(0, np.nan)
    if "준공_호수" in out.columns and "거래량" in out.columns:
        out["준공_거래량배율"] = out["준공_호수"] / out["거래량"].replace(0, np.nan)

    corr_vars = [
        "가구_소득평균", "가구_순자산", "NPS_가입자수", "NPS_가입자수_전년대비증가율",
        "NPS_1인당고지금액", "NPS_1인당고지금액_전년대비증가율", "GRDP", "GRDP_전년대비증가율",
        "전입", "전출", "순이동", "미분양_호수", "주담대_잔액", "주담대_증감률", "착공_호수", "준공_호수",
        "지가변동률", "소비자심리지수", "주택가격전망CSI", "KB_매수우위지수", "KB_매매거래지수", "KB_전세수급지수",
    ]
    for x in corr_vars:
        if x in out.columns:
            out[f"corr_var__{x}"] = out[x]
    out["상위10퍼센트_정확도"] = "현재 KOSIS 확보분은 소득5분위(상위20%) proxy; 소득10분위/순자산10분위 추가 확보 필요"
    return out


def build_correlation_summary(df: pd.DataFrame) -> pd.DataFrame:
    y_candidates = ["매매가_만원", "매매가_전년대비증가율"]
    x_candidates = [
        "가구_소득평균", "가구_순자산", "매매가_소득배율", "매매가_순자산배율",
        "NPS_가입자수", "NPS_가입자수_전년대비증가율", "NPS_1인당고지금액", "NPS_1인당고지금액_전년대비증가율",
        "GRDP", "GRDP_전년대비증가율", "전입", "전출", "순이동", "순이동_NPS가입자대비",
        "미분양_호수", "미분양_거래량배율", "주담대_잔액", "주담대_증감률", "착공_호수", "준공_호수", "준공_거래량배율",
        "지가변동률", "소비자심리지수", "주택가격전망CSI", "KB_매수우위지수", "KB_매매거래지수", "KB_전세수급지수", "KB_선도50지수",
    ]
    rows = []
    for y in y_candidates:
        if y not in df.columns:
            continue
        for x in x_candidates:
            if x not in df.columns:
                continue
            tmp = df[[x, y]].replace([np.inf, -np.inf], np.nan).dropna()
            rows.append({
                "x": x,
                "y": y,
                "n": len(tmp),
                "pearson_corr": tmp[x].corr(tmp[y]) if len(tmp) >= 3 else np.nan,
            })
    return pd.DataFrame(rows).sort_values(["y", "pearson_corr"], ascending=[True, False]) if rows else pd.DataFrame()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--sido", default="서울")
    p.add_argument("--sigungu", default=None)
    p.add_argument("--region-code", default=None)
    p.add_argument("--area-min", type=float, default=82.0)
    p.add_argument("--area-max", type=float, default=86.0)
    p.add_argument("--start-year", type=int, default=2017)
    p.add_argument("--end-year", type=int, default=2024)
    p.add_argument("--price-stat", default="median", choices=["mean", "median", "q25", "q75", "q90"])
    p.add_argument("--income-quantile", default="소득5분위")
    p.add_argument("--chunksize", type=int, default=500_000)
    p.add_argument("--out-dir", default=str(ROOT / "hermes" / "output" / "regional_hypothesis"))
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ds = build_dataset(args)
    corr = build_correlation_summary(ds)

    scope = args.sido + (f"_{args.sigungu}" if args.sigungu else "_전체시군구")
    scope = scope.replace(" ", "")
    area = f"{args.area_min:g}-{args.area_max:g}m2"
    stem = f"regional_hypothesis_{scope}_{area}_{args.income_quantile}_{args.start_year}-{args.end_year}_{args.price_stat}"
    ds_path = out_dir / f"{stem}.csv"
    corr_path = out_dir / f"{stem}_correlation_summary.csv"
    ds.to_csv(ds_path, index=False, encoding="utf-8-sig")
    corr.to_csv(corr_path, index=False, encoding="utf-8-sig")
    print(f"dataset_rows={len(ds):,} path={ds_path}")
    print(f"corr_rows={len(corr):,} path={corr_path}")
    if len(ds):
        print(ds.head(8).to_string(index=False))
    if len(corr):
        print(corr.to_string(index=False))


if __name__ == "__main__":
    main()

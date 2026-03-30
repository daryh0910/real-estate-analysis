"""
통합 데이터 업데이트 스크립트 — 4개 수집 스크립트를 순차 실행

실행 순서:
    1. download_public_data.py  (공공데이터 7종)
    2. download_demand_data.py  (수요데이터 5종)
    3. update_data.py           (실거래 — 선택적)
    4. build_cache.py           (Parquet 캐시)

사용법:
    python update_all.py                    # 전체 업데이트
    python update_all.py --incremental      # 증분 업데이트 (기존 CSV의 max 이후만)
    python update_all.py --skip-trade       # 실거래 업데이트 건너뛰기
    python update_all.py --dry-run          # 실제 실행 없이 계획만 출력
    python update_all.py --incremental --skip-trade  # 조합 가능
"""

import os
import sys
import time
import argparse
import traceback
from datetime import datetime

import pandas as pd

# 프로젝트 루트를 sys.path에 추가
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)


# ═══════════════════════════════════════════════════════
# 경로 설정
# ═══════════════════════════════════════════════════════

import glob as _glob

def _detect_onedrive():
    """OneDrive 경로 자동 감지"""
    pattern = "/mnt/c/Users/*/OneDrive - (주)에스티/101. 신사업폴더백업/부동산Tradingview"
    matches = _glob.glob(pattern)
    if matches:
        return matches[0]
    raise FileNotFoundError(f"OneDrive 경로를 찾을 수 없습니다. 패턴: {pattern}")

BASE_ONEDRIVE = _detect_onedrive()
BACKDATA = os.path.join(BASE_ONEDRIVE, "실거래_데이터/BackData(거래이외 Table)")
DEMAND_DIR = os.path.join(BACKDATA, "수요/수요_집계")


# ═══════════════════════════════════════════════════════
# CSV 파일 → 시점 정보 매핑
# ═══════════════════════════════════════════════════════

# 월별 데이터 (시점 컬럼: "연월", 형식: "YYYY-MM")
MONTHLY_CSV_MAP = {
    # download_public_data.py 관할
    "unsold_housing_sido_monthly.csv": {
        "col": "연월", "func_module": "public", "func_name": "fetch_unsold_housing",
        "param_type": "ym",  # start_ym="YYYYMM", end_ym="YYYYMM"
        "label": "미분양주택",
    },
    "base_rate_monthly.csv": {
        "col": "연월", "func_module": "public", "func_name": "fetch_base_rate",
        "param_type": "ym",
        "label": "기준금리",
    },
    "land_price_change_sido_monthly.csv": {
        "col": "연월", "func_module": "public", "func_name": "fetch_land_price_change",
        "param_type": "ym",
        "label": "지가변동률",
    },
    "jeonwolse_conversion_rate_sido_monthly.csv": {
        "col": "연월", "func_module": "public", "func_name": "fetch_jeonwolse_rate",
        "param_type": "ym",
        "label": "전월세전환율",
    },
    "housing_price_index_sido_monthly.csv": {
        "col": "연월", "func_module": "public", "func_name": "fetch_housing_price_index",
        "param_type": "ym",
        "label": "주택가격지수",
    },
    "population_migration_sido_monthly.csv": {
        "col": "연월", "func_module": "public", "func_name": "fetch_population_migration",
        "param_type": "year",  # start_year, end_year (int)
        "label": "인구이동",
    },
    "construction_pipeline_sido_monthly.csv": {
        "col": "연월", "func_module": "public", "func_name": "fetch_construction_pipeline",
        "param_type": "ym",
        "label": "착공/준공",
    },
    # download_demand_data.py 관할
    "bok_housing_loan_sido_monthly.csv": {
        "col": "연월", "func_module": "demand", "func_name": "fetch_bok_housing_loan",
        "param_type": "ym",
        "label": "주담대(BOK)",
    },
    "nps_sigungu_monthly.csv": {
        "col": "연월", "func_module": "demand", "func_name": "process_nps_data",
        "param_type": "none",  # 증분 불가, 항상 전체
        "label": "국민연금(NPS)",
    },
}

# 연간 데이터 (시점 컬럼: "연도", 형식: 정수)
YEARLY_CSV_MAP = {
    "kosis_household_asset_sido_yearly.csv": {
        "col": "연도", "func_module": "demand", "func_name": "fetch_kosis_household_asset",
        "param_type": "year",  # start_year, end_year (int)
        "label": "가계자산(KOSIS)",
    },
    "kosis_household_asset_quintile_yearly.csv": {
        "col": "연도", "func_module": "demand", "func_name": "fetch_kosis_household_asset_quintile",
        "param_type": "year",
        "label": "소득5분위자산(KOSIS)",
    },
    "nts_income_sigungu_yearly.csv": {
        "col": "연도", "func_module": "demand", "func_name": "fetch_nts_income_data",
        "param_type": "year",
        "label": "근로소득(국세청)",
    },
}


# ═══════════════════════════════════════════════════════
# 증분 업데이트 유틸리티
# ═══════════════════════════════════════════════════════

def _read_csv_safe(path):
    """인코딩 자동 감지 CSV 로더"""
    for enc in ("utf-8", "utf-8-sig", "cp949", "euc-kr"):
        try:
            return pd.read_csv(path, encoding=enc)
        except (UnicodeDecodeError, UnicodeError):
            continue
    return None


def get_max_ym(csv_path, col="연월"):
    """
    CSV에서 max(연월)을 읽어 YYYYMM(int) 반환.
    연월 형식: "YYYY-MM" → int 202601
    없으면 None 반환
    """
    if not os.path.exists(csv_path):
        return None
    try:
        df = _read_csv_safe(csv_path)
        if df is None or col not in df.columns:
            return None
        max_val = df[col].dropna().max()
        if pd.isna(max_val):
            return None
        # "YYYY-MM" → YYYYMM
        s = str(max_val).replace("-", "")
        return int(s[:6])
    except Exception:
        return None


def get_max_year(csv_path, col="연도"):
    """
    CSV에서 max(연도)를 읽어 int 반환.
    없으면 None 반환
    """
    if not os.path.exists(csv_path):
        return None
    try:
        df = _read_csv_safe(csv_path)
        if df is None or col not in df.columns:
            return None
        max_val = df[col].dropna().max()
        if pd.isna(max_val):
            return None
        return int(max_val)
    except Exception:
        return None


def next_month(ym_int):
    """YYYYMM(int) → 다음 월의 YYYYMM(int)"""
    y, m = divmod(ym_int, 100)
    m += 1
    if m > 12:
        m = 1
        y += 1
    return y * 100 + m


def ym_to_str(ym_int):
    """YYYYMM(int) → "YYYYMM"(str)"""
    return f"{ym_int:06d}"


def ym_to_display(ym_int):
    """YYYYMM(int) → "YYYY-MM"(str) 표시용"""
    return f"{ym_int // 100}-{ym_int % 100:02d}"


# ═══════════════════════════════════════════════════════
# 함수 import 및 호출
# ═══════════════════════════════════════════════════════

def _import_public_func(func_name):
    """download_public_data.py에서 함수 가져오기"""
    from download_public_data import (
        fetch_unsold_housing,
        fetch_population_migration,
        fetch_base_rate,
        fetch_jeonwolse_rate,
        fetch_housing_price_index,
        fetch_land_price_change,
        fetch_construction_pipeline,
    )
    func_map = {
        "fetch_unsold_housing": fetch_unsold_housing,
        "fetch_population_migration": fetch_population_migration,
        "fetch_base_rate": fetch_base_rate,
        "fetch_jeonwolse_rate": fetch_jeonwolse_rate,
        "fetch_housing_price_index": fetch_housing_price_index,
        "fetch_land_price_change": fetch_land_price_change,
        "fetch_construction_pipeline": fetch_construction_pipeline,
    }
    return func_map[func_name]


def _import_demand_func(func_name):
    """download_demand_data.py에서 함수 가져오기"""
    from download_demand_data import (
        process_nps_data,
        fetch_bok_housing_loan,
        fetch_kosis_household_asset,
        fetch_kosis_household_asset_quintile,
        fetch_nts_income_data,
    )
    func_map = {
        "process_nps_data": process_nps_data,
        "fetch_bok_housing_loan": fetch_bok_housing_loan,
        "fetch_kosis_household_asset": fetch_kosis_household_asset,
        "fetch_kosis_household_asset_quintile": fetch_kosis_household_asset_quintile,
        "fetch_nts_income_data": fetch_nts_income_data,
    }
    return func_map[func_name]


def _get_func(module_type, func_name):
    """모듈 타입에 따라 함수 가져오기"""
    if module_type == "public":
        return _import_public_func(func_name)
    elif module_type == "demand":
        return _import_demand_func(func_name)
    raise ValueError(f"알 수 없는 모듈: {module_type}")


# ═══════════════════════════════════════════════════════
# 단계별 실행
# ═══════════════════════════════════════════════════════

# 현재 연월 기본 종료값
NOW = datetime.now()
DEFAULT_END_YM = int(f"{NOW.year}{NOW.month:02d}")
DEFAULT_END_YEAR = NOW.year


def _calc_incremental_params(info, csv_path, incremental=False):
    """
    증분 모드일 때 start 파라미터를 계산.
    반환: (kwargs_dict, prev_max_display)
    """
    param_type = info["param_type"]

    if not incremental or param_type == "none":
        # 전체 실행 (기본 파라미터 사용)
        return {}, None

    if param_type == "ym":
        max_ym = get_max_ym(csv_path, info["col"])
        if max_ym is not None:
            start = next_month(max_ym)
            end = DEFAULT_END_YM
            if start > end:
                # 이미 최신 데이터까지 수집됨
                return None, ym_to_display(max_ym)
            return {
                "start_ym": ym_to_str(start),
                "end_ym": ym_to_str(end),
            }, ym_to_display(max_ym)
        else:
            return {}, None

    elif param_type == "year":
        max_year = get_max_year(csv_path, info.get("col", "연도"))
        if max_year is None:
            # 연월 컬럼에서 연도 추출 시도
            max_ym = get_max_ym(csv_path, "연월")
            if max_ym is not None:
                max_year = max_ym // 100

        if max_year is not None:
            # 연간 데이터: 같은 연도부터 재수집 (최신 반영)
            start_year = max_year
            end_year = DEFAULT_END_YEAR
            # fetch_population_migration은 start_year/end_year를 받음
            return {
                "start_year": start_year,
                "end_year": end_year,
            }, str(max_year)
        else:
            return {}, None

    return {}, None


def run_data_collection(csv_map, incremental=False, dry_run=False):
    """
    CSV 맵의 각 항목에 대해 수집 함수를 실행하고 결과를 반환.
    반환: list of dict (각 항목별 결과)
    """
    results = []

    for csv_name, info in csv_map.items():
        csv_path = os.path.join(DEMAND_DIR, csv_name)
        label = info["label"]

        # 이전 행 수 (있으면)
        prev_rows = None
        if os.path.exists(csv_path):
            try:
                df_prev = _read_csv_safe(csv_path)
                if df_prev is not None:
                    prev_rows = len(df_prev)
            except Exception:
                pass

        # 증분 파라미터 계산
        kwargs, prev_max = _calc_incremental_params(info, csv_path, incremental)

        # 이미 최신 (kwargs가 None)
        if kwargs is None:
            print(f"\n  [{label}] 이미 최신 (마지막: {prev_max}) — 건너뜀")
            results.append({
                "label": label,
                "status": "건너뜀(최신)",
                "prev_end": prev_max,
                "curr_end": prev_max,
                "added_rows": 0,
            })
            continue

        # dry-run 모드
        if dry_run:
            if incremental and kwargs:
                param_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
                print(f"  [DRY-RUN] {label}: {info['func_name']}({param_str})")
            else:
                print(f"  [DRY-RUN] {label}: {info['func_name']}() — 전체 수집")
            results.append({
                "label": label,
                "status": "dry-run",
                "prev_end": prev_max or "-",
                "curr_end": "-",
                "added_rows": "-",
            })
            continue

        # 실제 실행
        print(f"\n{'─' * 50}")
        print(f"  [{label}] 수집 시작...")
        if kwargs:
            param_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
            print(f"  파라미터: {param_str}")

        t0 = time.time()
        try:
            func = _get_func(info["func_module"], info["func_name"])
            result_df = func(**kwargs)
            elapsed = time.time() - t0

            if result_df is not None:
                new_rows = len(result_df)
                # 현재 max 시점 확인
                if info["col"] in result_df.columns:
                    curr_max = str(result_df[info["col"]].dropna().max())
                else:
                    curr_max = "?"

                # 전체 CSV 기준 행 수 (증분이면 기존 + 신규가 합쳐져 저장됨)
                # 각 함수가 내부에서 CSV를 직접 덮어쓰므로 실제 행 수는 result_df 크기
                added = new_rows - (prev_rows or 0) if not incremental else new_rows

                results.append({
                    "label": label,
                    "status": f"성공 ({elapsed:.1f}초)",
                    "prev_end": prev_max or "-",
                    "curr_end": curr_max,
                    "added_rows": new_rows,
                })
            else:
                results.append({
                    "label": label,
                    "status": f"실패/없음 ({time.time()-t0:.1f}초)",
                    "prev_end": prev_max or "-",
                    "curr_end": "-",
                    "added_rows": 0,
                })

        except Exception as e:
            elapsed = time.time() - t0
            print(f"  [ERROR] {label}: {e}")
            traceback.print_exc()
            results.append({
                "label": label,
                "status": f"에러 ({elapsed:.1f}초)",
                "prev_end": prev_max or "-",
                "curr_end": "-",
                "added_rows": 0,
            })

    return results


# ═══════════════════════════════════════════════════════
# 실거래 업데이트 (update_data.py)
# ═══════════════════════════════════════════════════════

def run_trade_update(dry_run=False):
    """update_data.py의 main() 호출 — 자체 증분 시스템 내장"""
    if dry_run:
        print("  [DRY-RUN] 실거래 업데이트 (update_data.main())")
        return {"label": "실거래(매매/임대차)", "status": "dry-run",
                "prev_end": "-", "curr_end": "-", "added_rows": "-"}

    print(f"\n{'═' * 60}")
    print("  [실거래] 매매/임대차 업데이트 시작...")
    print(f"{'═' * 60}")

    t0 = time.time()
    try:
        from update_data import main as update_main
        update_main()
        elapsed = time.time() - t0
        return {"label": "실거래(매매/임대차)", "status": f"성공 ({elapsed:.1f}초)",
                "prev_end": "-", "curr_end": "-", "added_rows": "-"}
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  [ERROR] 실거래: {e}")
        traceback.print_exc()
        return {"label": "실거래(매매/임대차)", "status": f"에러 ({elapsed:.1f}초)",
                "prev_end": "-", "curr_end": "-", "added_rows": "-"}


# ═══════════════════════════════════════════════════════
# 캐시 빌드 (build_cache.py)
# ═══════════════════════════════════════════════════════

def run_cache_build(dry_run=False):
    """build_cache.py 로직 실행 — 항상 전체 재빌드"""
    if dry_run:
        print("  [DRY-RUN] Parquet 캐시 재빌드 (build_cache)")
        return {"label": "Parquet 캐시", "status": "dry-run",
                "prev_end": "-", "curr_end": "-", "added_rows": "-"}

    print(f"\n{'═' * 60}")
    print("  [캐시] Parquet 캐시 재빌드 시작...")
    print(f"{'═' * 60}")

    t0 = time.time()
    try:
        from data_loader import (
            load_apt_data, load_apt_data_detail, load_rent_data,
        )
        # 매매 캐시
        print("\n  [1/5] 매매 시군구 월별 집계...")
        load_apt_data(force_rebuild=True, chunksize=500_000)

        print("  [2/5] 매매 상세 (건축년도 포함)...")
        load_apt_data_detail(force_rebuild=True, chunksize=500_000)

        # 임대차 캐시
        for i, rtype in enumerate(["jeonse", "wolse", "all"], 3):
            print(f"  [{i}/5] {rtype} 캐시...")
            load_rent_data(rtype, force_rebuild=True, chunksize=500_000)

        elapsed = time.time() - t0
        return {"label": "Parquet 캐시", "status": f"성공 ({elapsed:.1f}초)",
                "prev_end": "-", "curr_end": "-", "added_rows": "-"}

    except Exception as e:
        elapsed = time.time() - t0
        print(f"  [ERROR] 캐시 빌드: {e}")
        traceback.print_exc()
        return {"label": "Parquet 캐시", "status": f"에러 ({elapsed:.1f}초)",
                "prev_end": "-", "curr_end": "-", "added_rows": "-"}


# ═══════════════════════════════════════════════════════
# 결과 요약 출력
# ═══════════════════════════════════════════════════════

def print_summary(all_results):
    """최종 결과 요약 테이블 출력"""
    print(f"\n{'═' * 80}")
    print("  최종 결과 요약")
    print(f"{'═' * 80}")

    # 헤더
    print(f"  {'데이터':<25} {'상태':<20} {'이전종료':<12} {'현재종료':<12} {'행수':>8}")
    print(f"  {'─' * 25} {'─' * 20} {'─' * 12} {'─' * 12} {'─' * 8}")

    for r in all_results:
        label = r["label"]
        status = r["status"]
        prev_end = str(r.get("prev_end", "-"))
        curr_end = str(r.get("curr_end", "-"))
        added = str(r.get("added_rows", "-"))
        print(f"  {label:<25} {status:<20} {prev_end:<12} {curr_end:<12} {added:>8}")

    print(f"{'═' * 80}")

    # 성공/실패 카운트
    success = sum(1 for r in all_results if "성공" in str(r["status"]))
    failed = sum(1 for r in all_results if "에러" in str(r["status"]) or "실패" in str(r["status"]))
    skipped = sum(1 for r in all_results if "건너뜀" in str(r["status"]) or "dry-run" in str(r["status"]))

    print(f"\n  성공: {success}  실패: {failed}  건너뜀: {skipped}  합계: {len(all_results)}")


# ═══════════════════════════════════════════════════════
# 메인
# ═══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="통합 데이터 업데이트: 공공데이터 + 수요데이터 + 실거래 + 캐시",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
    python update_all.py                           # 전체 업데이트
    python update_all.py --incremental             # 증분 업데이트
    python update_all.py --incremental --skip-trade # 증분 + 실거래 건너뛰기
    python update_all.py --dry-run                 # 계획만 출력
        """,
    )
    parser.add_argument(
        "--incremental", action="store_true",
        help="증분 업데이트: 기존 CSV의 max(연월) 이후부터만 수집",
    )
    parser.add_argument(
        "--skip-trade", action="store_true",
        help="실거래(매매/임대차) 업데이트 건너뛰기 (시간이 오래 걸림)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="실제 실행 없이 계획만 출력",
    )
    args = parser.parse_args()

    total_start = time.time()

    print("=" * 60)
    print("  부동산 데이터 통합 업데이트")
    print(f"  실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  모드: {'증분' if args.incremental else '전체'}"
          f" | 실거래: {'건너뜀' if args.skip_trade else '포함'}"
          f" | {'DRY-RUN' if args.dry_run else '실행'}")
    print(f"  데이터 디렉토리: {DEMAND_DIR}")
    print("=" * 60)

    all_results = []

    # ── 1단계: 공공데이터 (7종) ──
    print(f"\n{'▶' * 3} 1단계: 공공데이터 수집 (7종)")
    public_csvs = {k: v for k, v in MONTHLY_CSV_MAP.items()
                   if v["func_module"] == "public"}
    results = run_data_collection(public_csvs, args.incremental, args.dry_run)
    all_results.extend(results)

    # ── 2단계: 수요데이터 (5종) ──
    print(f"\n{'▶' * 3} 2단계: 수요데이터 수집 (5종)")
    demand_monthly = {k: v for k, v in MONTHLY_CSV_MAP.items()
                      if v["func_module"] == "demand"}
    demand_yearly = {k: v for k, v in YEARLY_CSV_MAP.items()
                     if v["func_module"] == "demand"}

    results = run_data_collection(demand_monthly, args.incremental, args.dry_run)
    all_results.extend(results)
    results = run_data_collection(demand_yearly, args.incremental, args.dry_run)
    all_results.extend(results)

    # ── 3단계: 실거래 (선택적) ──
    if args.skip_trade:
        print(f"\n{'▶' * 3} 3단계: 실거래 업데이트 — 건너뜀 (--skip-trade)")
        all_results.append({
            "label": "실거래(매매/임대차)",
            "status": "건너뜀(--skip-trade)",
            "prev_end": "-", "curr_end": "-", "added_rows": "-",
        })
    else:
        print(f"\n{'▶' * 3} 3단계: 실거래 업데이트")
        result = run_trade_update(args.dry_run)
        all_results.append(result)

    # ── 4단계: Parquet 캐시 재빌드 ──
    print(f"\n{'▶' * 3} 4단계: Parquet 캐시 재빌드")
    result = run_cache_build(args.dry_run)
    all_results.append(result)

    # ── 최종 요약 ──
    total_elapsed = time.time() - total_start
    print_summary(all_results)
    print(f"\n  총 소요 시간: {total_elapsed:.1f}초 ({total_elapsed/60:.1f}분)")
    print()


if __name__ == "__main__":
    main()

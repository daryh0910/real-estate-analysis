"""
수요 데이터 수집 스크립트 — 소득(국민연금) · 대출(BOK) · 자산(KOSIS) · 근로소득(국세청)

사용법:
    python download_demand_data.py             # 전체 실행
    python download_demand_data.py --nps       # 국민연금만
    python download_demand_data.py --bok       # BOK 주담대만
    python download_demand_data.py --kosis     # KOSIS 가계자산만
    python download_demand_data.py --nts       # 국세청 근로소득만

출력 파일:
    {BACKDATA}/수요/수요_집계/nps_sigungu_monthly.csv
    {BACKDATA}/수요/수요_집계/bok_housing_loan_sido_monthly.csv
    {BACKDATA}/수요/수요_집계/kosis_household_asset_sido_yearly.csv
    {BACKDATA}/수요/수요_집계/nts_income_sigungu_yearly.csv
"""

import os
import sys
import time
import json
import argparse
import subprocess
import requests
import pandas as pd
import numpy as np
import glob as _glob

from dotenv import load_dotenv
load_dotenv()


# ═══════════════════════════════════════════════════════
# 경로 / 상수
# ═══════════════════════════════════════════════════════

def _detect_onedrive():
    """Windows 사용자명 자동 감지 → OneDrive 경로 반환"""
    pattern = "/mnt/c/Users/*/OneDrive - (주)에스티/101. 신사업폴더백업/부동산Tradingview"
    matches = _glob.glob(pattern)
    if matches:
        return matches[0]
    raise FileNotFoundError(f"OneDrive 경로를 찾을 수 없습니다. 패턴: {pattern}")


BASE_ONEDRIVE = _detect_onedrive()
BACKDATA = os.path.join(BASE_ONEDRIVE, "실거래_데이터/BackData(거래이외 Table)")
NPS_DIR = os.path.join(BACKDATA, "수요/#2. 수요_정책이외/2.유효수요/국민연금")
OUTPUT_DIR = os.path.join(BACKDATA, "수요/수요_집계")
KEY_XLSX = os.path.join(
    BASE_ONEDRIVE, "5. coding/##. 250719_전달파일/250711_주요인증키.xlsx"
)

BOK_API_KEY = os.environ.get("BOK_API_KEY", "")
BOK_BASE_URL = "https://ecos.bok.or.kr/api"

# 시도코드(앞 2자리) → 짧은 시도명
SIDO_CODE_MAP = {
    11: "서울", 26: "부산", 27: "대구", 28: "인천",
    29: "광주", 30: "대전", 31: "울산", 36: "세종",
    41: "경기", 42: "강원", 43: "충북", 44: "충남",
    45: "전북", 46: "전남", 47: "경북", 48: "경남",
    50: "제주", 51: "강원", 52: "전북",
}


def _get_kosis_key():
    """KOSIS API 키를 환경변수에서 로드 (base64 인코딩 상태 그대로 사용)"""
    key = os.environ.get("KOSIS_API_KEY", "")
    if key:
        return key
    # 로컬 엑셀 파일에서 추출 시도 (fallback)
    try:
        import openpyxl
        wb = openpyxl.load_workbook(KEY_XLSX, data_only=True)
        ws = wb.active
        for row in ws.iter_rows(values_only=True):
            for i, cell in enumerate(row):
                if cell and "kosis" in str(cell).lower():
                    for j in range(i + 1, len(row)):
                        if row[j] and len(str(row[j])) > 10:
                            return str(row[j])
    except Exception:
        pass
    raise ValueError("KOSIS_API_KEY 환경변수를 설정하세요. (.env 파일 또는 export KOSIS_API_KEY=...)")


# ═══════════════════════════════════════════════════════
# 유틸리티
# ═══════════════════════════════════════════════════════

def read_csv_auto(path, **kwargs):
    """인코딩 자동 감지 CSV 로더 (utf-8 → cp949 → euc-kr)"""
    for enc in ("utf-8", "cp949", "euc-kr"):
        try:
            return pd.read_csv(path, encoding=enc, **kwargs)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"인코딩 감지 실패: {path}")


def _api_get(url, params=None, retries=3, delay=1.0):
    """GET 요청 with retry"""
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt < retries - 1:
                print(f"  재시도 ({attempt+1}/{retries}): {e}")
                time.sleep(delay * (attempt + 1))
            else:
                raise


# ═══════════════════════════════════════════════════════
# 1. 국민연금 CSV 전처리
# ═══════════════════════════════════════════════════════

NPS_USECOLS = [
    "자료생성년월",
    "법정동주소광역시도코드",
    "법정동주소광역시시군구코드",
    "가입자수",
    "당월고지금액",
    "신규취득자수",
    "상실가입자수",
]


def _find_nps_files():
    """NPS CSV 파일 탐색 (합본 + 합본 이후 개별 파일 모두 반환)"""
    result_files = []

    # 1) 합본 파일
    merged = os.path.join(NPS_DIR, "combine_NPS", "merged2_files.csv")
    if os.path.exists(merged):
        result_files.append(merged)

    # 2) 개별 파일 (합본에 포함되지 않은 최신 파일 추가)
    individual = sorted(_glob.glob(os.path.join(NPS_DIR, "국민연금*.csv")))
    # 합본이 있으면 합본 이후 파일만 추가 (파일명에 2024 이후 포함)
    if result_files:
        for f in individual:
            fname = os.path.basename(f)
            if "2024" in fname or "2025" in fname or "2026" in fname:
                result_files.append(f)
    else:
        result_files = individual if individual else []

    if not result_files:
        # fallback: 모든 csv
        all_csv = sorted(_glob.glob(os.path.join(NPS_DIR, "*.csv")))
        return all_csv

    return result_files


def process_nps_data(chunksize=200_000):
    """
    기존 국민연금 CSV → 시군구별 월별 집계
    출력: {OUTPUT_DIR}/nps_sigungu_monthly.csv
    """
    print("=" * 60)
    print("[1/3] 국민연금 CSV 전처리")
    print("=" * 60)

    nps_files = _find_nps_files()
    if not nps_files:
        print(f"  NPS CSV 파일 없음: {NPS_DIR}")
        return None

    print(f"  대상 파일: {len(nps_files)}개")

    all_chunks = []

    for fpath in nps_files:
        fname = os.path.basename(fpath)
        fsize = os.path.getsize(fpath) / (1024 * 1024)
        print(f"  처리 중: {fname} ({fsize:.0f} MB)")

        # usecols로 필요한 컬럼만 읽기 시도
        try:
            reader = read_csv_auto(
                fpath,
                chunksize=chunksize,
                usecols=NPS_USECOLS,
                dtype=str,
                on_bad_lines="skip",
            )
        except (ValueError, KeyError) as e:
            # usecols 매칭 실패 → 전체 컬럼 읽기
            print(f"    usecols 실패 ({e}), 전체 컬럼 시도")
            try:
                reader = read_csv_auto(
                    fpath,
                    chunksize=chunksize,
                    dtype=str,
                    on_bad_lines="skip",
                )
            except Exception as e2:
                print(f"    파일 읽기 실패: {e2}")
                continue

        chunk_count = 0
        for chunk in reader:
            chunk_count += 1

            # 필요한 컬럼 확인 및 매핑 (부분 일치 포함)
            col_map = {}
            for needed in NPS_USECOLS:
                if needed in chunk.columns:
                    col_map[needed] = needed
                else:
                    for col in chunk.columns:
                        if needed in col or col in needed:
                            col_map[needed] = col
                            break

            missing = [c for c in NPS_USECOLS if c not in col_map]
            if missing:
                if chunk_count == 1:
                    print(f"    누락 컬럼: {missing}")
                    print(f"    사용 가능: {list(chunk.columns)}")
                continue

            # 컬럼명 정규화
            df = chunk[[col_map[c] for c in NPS_USECOLS]].copy()
            df.columns = NPS_USECOLS

            # 스키마 행 및 비정상 데이터 필터 (yyyymm 또는 yyyy-mm 형식만 유효)
            ym = df["자료생성년월"].str.strip()
            valid_mask = ym.str.match(r"^\d{6}$", na=False) | ym.str.match(r"^\d{4}-\d{2}$", na=False)
            df = df[valid_mask].copy()
            if df.empty:
                continue

            # 지역코드 생성 (시도 2자리 + 시군구 3자리 = 5자리)
            df["시도코드"] = df["법정동주소광역시도코드"].astype(str).str.strip().str.zfill(2)
            df["시군구코드"] = df["법정동주소광역시시군구코드"].astype(str).str.strip().str.zfill(3)
            df["지역코드"] = df["시도코드"] + df["시군구코드"]

            # 연월 파싱 (yyyymm → yyyy-mm, yyyy-mm → 그대로)
            ym_norm = df["자료생성년월"].str.strip()
            df["연월"] = ym_norm.where(
                ym_norm.str.match(r"^\d{4}-\d{2}$"),
                ym_norm.str[:4] + "-" + ym_norm.str[4:6]
            )

            # 숫자 변환
            for col in ["가입자수", "당월고지금액", "신규취득자수", "상실가입자수"]:
                df[col] = pd.to_numeric(
                    df[col].str.replace(",", ""), errors="coerce"
                ).fillna(0)

            # 시군구 + 연월 단위 집계
            agg = (
                df.groupby(["지역코드", "연월"])
                .agg(
                    NPS_가입자수=("가입자수", "sum"),
                    NPS_고지금액합계=("당월고지금액", "sum"),
                    NPS_사업장수=("가입자수", "count"),
                    NPS_신규취득=("신규취득자수", "sum"),
                    NPS_상실=("상실가입자수", "sum"),
                )
                .reset_index()
            )

            all_chunks.append(agg)

        print(f"    {chunk_count} 청크 처리 완료")

    if not all_chunks:
        print("  집계 데이터 없음")
        return None

    # 청크 간 재집계
    result = pd.concat(all_chunks, ignore_index=True)
    result = (
        result.groupby(["지역코드", "연월"])
        .agg(
            NPS_가입자수=("NPS_가입자수", "sum"),
            NPS_고지금액합계=("NPS_고지금액합계", "sum"),
            NPS_사업장수=("NPS_사업장수", "sum"),
            NPS_신규취득=("NPS_신규취득", "sum"),
            NPS_상실=("NPS_상실", "sum"),
        )
        .reset_index()
    )

    # 파생 지표
    result["NPS_1인당고지금액"] = np.where(
        result["NPS_가입자수"] > 0,
        result["NPS_고지금액합계"] / result["NPS_가입자수"],
        np.nan,
    )
    result["NPS_고용증감"] = result["NPS_신규취득"] - result["NPS_상실"]

    # 시도, 연도, 월 추가
    result["시도"] = result["지역코드"].str[:2].apply(
        lambda x: SIDO_CODE_MAP.get(int(x), None) if x.isdigit() else None
    )
    result["연도"] = result["연월"].str[:4].astype(int)
    result["월"] = result["연월"].str[5:7].astype(int)

    # 내부 집계용 컬럼 제거
    result = result.drop(columns=["NPS_신규취득", "NPS_상실"])

    result = result.sort_values(["연월", "지역코드"]).reset_index(drop=True)

    # 저장
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "nps_sigungu_monthly.csv")
    result.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  저장: {out_path}")
    print(
        f"    행 수: {len(result):,}, "
        f"지역코드 수: {result['지역코드'].nunique()}, "
        f"기간: {result['연월'].min()} ~ {result['연월'].max()}"
    )

    return result


# ═══════════════════════════════════════════════════════
# 2. BOK 주담대 데이터
# ═══════════════════════════════════════════════════════

def fetch_bok_housing_loan(start_ym="200612", end_ym="202602"):
    """
    BOK ECOS API → 151Y003 지역별 예금은행 가계대출(말잔)
    출력: {OUTPUT_DIR}/bok_housing_loan_sido_monthly.csv
    """
    print("=" * 60)
    print("[2/3] BOK 주담대 데이터 수집")
    print("=" * 60)

    # 11110A0: 주택담보대출, 11110B0: 기타대출
    item_codes = {"11110A0": "주담대", "11110B0": "기타대출"}
    all_rows = []

    for item_code, item_label in item_codes.items():
        url = (
            f"{BOK_BASE_URL}/StatisticSearch/{BOK_API_KEY}/json/kr/"
            f"1/99999/151Y003/M/{start_ym}/{end_ym}/{item_code}////"
        )
        print(f"  요청: {item_label} ({item_code})")

        try:
            resp = _api_get(url)
            data = resp.json()
        except Exception as e:
            print(f"  API 요청 실패: {e}")
            continue

        # 응답 파싱
        if "StatisticSearch" not in data:
            err_msg = data.get("RESULT", {}).get("MESSAGE", "알 수 없는 오류")
            print(f"  API 오류: {err_msg}")
            continue

        rows = data["StatisticSearch"]["row"]
        print(f"    {len(rows)} 행 수신")

        for row in rows:
            # 지역명 추출 (ITEM_NAME 차원 중 시도명이 있는 것)
            region_name = None
            for dim in ["ITEM_NAME2", "ITEM_NAME3", "ITEM_NAME4", "ITEM_NAME1"]:
                name = row.get(dim, "").strip()
                if name and name not in ("", item_label, "주택담보대출", "기타대출",
                                         "예금은행", "가계대출"):
                    region_name = name
                    break

            if region_name is None:
                continue

            time_str = row.get("TIME", "")
            value_str = row.get("DATA_VALUE", "")

            try:
                value = float(value_str.replace(",", ""))
            except (ValueError, AttributeError):
                continue

            # 연월 변환 (YYYYMM → YYYY-MM)
            if len(time_str) >= 6:
                ym = time_str[:4] + "-" + time_str[4:6]
            else:
                continue

            all_rows.append({
                "연월": ym,
                "지역명_raw": region_name,
                "대출유형": item_label,
                "금액_십억원": value,
            })

    if not all_rows:
        print("  데이터 없음")
        return None

    df = pd.DataFrame(all_rows)

    # 시도명 정규화 (BOK 지역명 → 표준 짧은 시도명)
    sido_norm = {
        "전국": "전국", "서울": "서울", "부산": "부산", "대구": "대구",
        "인천": "인천", "광주": "광주", "대전": "대전", "울산": "울산",
        "세종": "세종", "경기": "경기", "강원": "강원", "충북": "충북",
        "충남": "충남", "전북": "전북", "전남": "전남", "경북": "경북",
        "경남": "경남", "제주": "제주",
    }
    df["시도"] = df["지역명_raw"].map(sido_norm)
    df = df.dropna(subset=["시도"])

    # 피벗: 대출유형별 → 컬럼으로
    pivot = df.pivot_table(
        index=["연월", "시도"],
        columns="대출유형",
        values="금액_십억원",
        aggfunc="first",
    ).reset_index()
    pivot.columns.name = None

    # 컬럼명 정리
    rename = {}
    if "주담대" in pivot.columns:
        rename["주담대"] = "주담대_잔액"
    if "기타대출" in pivot.columns:
        rename["기타대출"] = "기타대출_잔액"
    pivot = pivot.rename(columns=rename)

    # 파생 지표
    if "주담대_잔액" in pivot.columns:
        pivot = pivot.sort_values(["시도", "연월"])
        pivot["주담대_증감률"] = (
            pivot.groupby("시도")["주담대_잔액"].pct_change() * 100
        )
        if "기타대출_잔액" in pivot.columns:
            total = pivot["주담대_잔액"] + pivot["기타대출_잔액"]
            pivot["주담대_비중"] = np.where(
                total > 0, pivot["주담대_잔액"] / total * 100, np.nan
            )

    # 연도, 월 추가
    pivot["연도"] = pivot["연월"].str[:4].astype(int)
    pivot["월"] = pivot["연월"].str[5:7].astype(int)

    pivot = pivot.sort_values(["연월", "시도"]).reset_index(drop=True)

    # 전국 제외 (시도 레벨 분석용)
    pivot_sido = pivot[pivot["시도"] != "전국"].copy()

    # 저장
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "bok_housing_loan_sido_monthly.csv")
    pivot_sido.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  저장: {out_path}")
    print(
        f"    행 수: {len(pivot_sido):,}, "
        f"시도 수: {pivot_sido['시도'].nunique()}, "
        f"기간: {pivot_sido['연월'].min()} ~ {pivot_sido['연월'].max()}"
    )

    # 전국 포함 버전도 저장
    out_full = os.path.join(OUTPUT_DIR, "bok_housing_loan_full_monthly.csv")
    pivot.to_csv(out_full, index=False, encoding="utf-8-sig")

    return pivot_sido


# ═══════════════════════════════════════════════════════
# 3. KOSIS 가계금융복지조사
# ═══════════════════════════════════════════════════════

KOSIS_API_URL = "https://kosis.kr/openapi/Param/statisticsParameterData.do"

# 가계금융복지조사 후보 테이블 ID (2017년 이후 신규 ID)
KOSIS_HH_TABLES = [
    ("101", "DT_1HDAAA01", "지역별 자산, 부채, 소득 현황"),
    ("101", "DT_1HDAAD01", "가구특성별 자산·부채"),
]


def fetch_kosis_household_asset(start_year=2012, end_year=2024):
    """
    KOSIS API → 시도별 가계 자산/부채/소득 연간 데이터
    출력: {OUTPUT_DIR}/kosis_household_asset_sido_yearly.csv
    """
    print("=" * 60)
    print("[3/3] KOSIS 가계금융복지조사 수집")
    print("=" * 60)

    api_key = _get_kosis_key()
    print(f"  API Key: {api_key[:8]}...")

    raw_data = None
    used_tbl = None

    for org_id, tbl_id, desc in KOSIS_HH_TABLES:
        print(f"  시도: {desc} (orgId={org_id}, tblId={tbl_id})")

        params = {
            "method": "getList",
            "apiKey": api_key,
            "itmId": "ALL",
            "objL1": "ALL",
            "objL2": "ALL",
            "objL3": "ALL",
            "prdSe": "Y",
            "startPrdDe": str(start_year),
            "endPrdDe": str(end_year),
            "orgId": org_id,
            "tblId": tbl_id,
            "format": "json",
            "jsonVD": "Y",
        }

        try:
            resp = _api_get(KOSIS_API_URL, params=params)
            data = resp.json()
        except Exception as e:
            print(f"    API 요청 실패: {e}")
            continue

        if isinstance(data, list) and len(data) > 0:
            raw_data = data
            used_tbl = tbl_id
            print(f"    {len(data)} 행 수신")
            break
        elif isinstance(data, dict):
            err = data.get("err", data.get("RESULT", ""))
            print(f"    API 오류: {err}")
            continue
        else:
            print(f"    빈 응답")
            continue

    if raw_data is None:
        print("  모든 테이블 시도 실패")
        print("  KOSIS 테이블 ID 확인 필요: https://kosis.kr")
        print("    가계금융복지조사 > 시도별 > 테이블 ID를 "
              "KOSIS_HH_TABLES에 추가하세요")
        return None

    df = pd.DataFrame(raw_data)

    # 원본 JSON 백업 저장
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    backup = os.path.join(OUTPUT_DIR, "kosis_household_raw.json")
    with open(backup, "w", encoding="utf-8") as f:
        json.dump(raw_data[:100], f, ensure_ascii=False, indent=2)
    print(f"  원본 샘플 백업: {backup}")

    # 데이터 파싱
    required_cols = ["PRD_DE", "DT"]
    for rc in required_cols:
        if rc not in df.columns:
            print(f"  필수 컬럼 누락: {rc}")
            print(f"    사용 가능 컬럼: {list(df.columns)}")
            return None

    # 지역 차원 찾기 (C1_NM, C2_NM 중 시도명 포함)
    region_col = None
    region_name_col = None
    sido_keywords = {"서울", "부산", "경기", "전국", "대구", "인천"}
    for prefix in ["C1", "C2", "C3"]:
        name_col = f"{prefix}_NM"
        if name_col in df.columns:
            sample_names = set(df[name_col].dropna().unique())
            if sample_names & sido_keywords:
                region_col = prefix
                region_name_col = name_col
                break

    if region_col is None:
        print("  지역 차원을 식별할 수 없음")
        print(f"    컬럼: {list(df.columns)}")
        # 컬럼 샘플 출력
        for c in df.columns:
            if "_NM" in c:
                print(f"    {c}: {df[c].dropna().unique()[:5]}")
        return None

    # 항목명 컬럼 찾기
    # DT_1HDAAA01: C3_NM이 자산/부채/소득 분류, ITM_NM은 "전가구 평균" 등
    # → C3_NM에 자산/부채/소득 키워드가 있으면 C3_NM을 항목 컬럼으로 사용
    item_name_col = None
    if "C3_NM" in df.columns:
        c3_sample = set(df["C3_NM"].dropna().unique())
        if any("자산" in str(v) or "부채" in str(v) or "소득" in str(v) for v in c3_sample):
            item_name_col = "C3_NM"
    if item_name_col is None and "ITM_NM" in df.columns:
        itm_sample = set(df["ITM_NM"].dropna().unique())
        if any("자산" in str(v) or "부채" in str(v) or "소득" in str(v) for v in itm_sample):
            item_name_col = "ITM_NM"
    # fallback: C3_NM이 있으면 사용
    if item_name_col is None and "C3_NM" in df.columns:
        item_name_col = "C3_NM"
    elif item_name_col is None:
        item_name_col = "ITM_NM" if "ITM_NM" in df.columns else None

    # 데이터 값 숫자 변환
    df["값"] = pd.to_numeric(
        df["DT"].astype(str).str.replace(",", ""), errors="coerce"
    )
    df["연도"] = pd.to_numeric(df["PRD_DE"], errors="coerce")

    # 시도명 정규화 (풀네임 → 짧은 이름)
    sido_norm = {
        "전국": "전국",
        "서울특별시": "서울", "부산광역시": "부산", "대구광역시": "대구",
        "인천광역시": "인천", "광주광역시": "광주", "대전광역시": "대전",
        "울산광역시": "울산", "세종특별자치시": "세종", "경기도": "경기",
        "강원도": "강원", "강원특별자치도": "강원",
        "충청북도": "충북", "충청남도": "충남",
        "전라북도": "전북", "전북특별자치도": "전북",
        "전라남도": "전남", "경상북도": "경북", "경상남도": "경남",
        "제주특별자치도": "제주",
        # 짧은 이름 그대로 매핑
        "서울": "서울", "부산": "부산", "대구": "대구", "인천": "인천",
        "광주": "광주", "대전": "대전", "울산": "울산", "세종": "세종",
        "경기": "경기", "강원": "강원", "충북": "충북", "충남": "충남",
        "전북": "전북", "전남": "전남", "경북": "경북", "경남": "경남",
        "제주": "제주",
    }

    df["시도"] = df[region_name_col].map(sido_norm)
    df = df.dropna(subset=["시도", "연도", "값"])

    # 전국 제외
    df = df[df["시도"] != "전국"]

    if item_name_col and item_name_col in df.columns:
        items = df[item_name_col].unique()
        print(f"  항목 종류 ({len(items)}개): {list(items)[:20]}")

        # 항목 중 자산/부채/소득 키워드 매칭
        # KOSIS DT_1HDAAA01 실제 항목명: "자산", "부채", "경상소득(전년도)", "순자산액"
        target_items = {}
        for item in items:
            s = str(item).strip()
            # 자산: "자산", "총자산", "자산액" (단, 하위항목 "금융자산", "실물자산" 제외)
            if s in ("자산", "총자산", "자산액") or ("자산" in s and "총" in s):
                target_items[item] = "가구_자산평균"
            # 부채: "부채", "총부채", "부채액" (단, 하위항목 "금융부채" 등 제외)
            elif s in ("부채", "총부채", "부채액") or ("부채" in s and "총" in s):
                target_items[item] = "가구_부채평균"
            # 소득: "경상소득(전년도)", "소득", "총소득"
            elif s in ("소득", "총소득") or ("소득" in s and ("경상" in s or "총" in s)):
                target_items[item] = "가구_소득평균"

        if not target_items:
            print("  관심 항목 자동 매칭 실패, 전체 항목으로 피벗")
            pivot = df.pivot_table(
                index=["연도", "시도"],
                columns=item_name_col,
                values="값",
                aggfunc="first",
            ).reset_index()
        else:
            print(f"  매칭된 항목: {target_items}")
            df_filtered = df[df[item_name_col].isin(target_items.keys())].copy()
            df_filtered["지표명"] = df_filtered[item_name_col].map(target_items)

            # 가구특성 차원이 있으면 '전체가구'만 필터
            # (항목 컬럼과 지역 컬럼은 제외)
            for dim in ["C1_NM", "C2_NM", "C3_NM"]:
                if (dim in df_filtered.columns
                        and dim != region_name_col
                        and dim != item_name_col):
                    vals = df_filtered[dim].unique()
                    total_vals = [v for v in vals if "전체" in str(v) or "계" == str(v)]
                    if total_vals:
                        df_filtered = df_filtered[df_filtered[dim].isin(total_vals)]
                        break

            # ITM_NM이 항목 컬럼이 아닌 경우, "전가구 평균"만 필터
            if "ITM_NM" in df_filtered.columns and item_name_col != "ITM_NM":
                itm_vals = df_filtered["ITM_NM"].unique()
                avg_vals = [v for v in itm_vals if "평균" in str(v)]
                if avg_vals:
                    df_filtered = df_filtered[df_filtered["ITM_NM"].isin(avg_vals)]

            pivot = df_filtered.pivot_table(
                index=["연도", "시도"],
                columns="지표명",
                values="값",
                aggfunc="first",
            ).reset_index()
    else:
        pivot = df[["연도", "시도", "값"]].copy()
        pivot = pivot.rename(columns={"값": "가구_자산평균"})

    pivot.columns.name = None

    # 파생 지표
    if "가구_자산평균" in pivot.columns and "가구_부채평균" in pivot.columns:
        pivot["가구_순자산"] = pivot["가구_자산평균"] - pivot["가구_부채평균"]
    if "가구_부채평균" in pivot.columns and "가구_소득평균" in pivot.columns:
        pivot["DSR"] = np.where(
            pivot["가구_소득평균"] > 0,
            pivot["가구_부채평균"] / pivot["가구_소득평균"] * 100,
            np.nan,
        )

    pivot = pivot.sort_values(["연도", "시도"]).reset_index(drop=True)

    # 저장
    out_path = os.path.join(OUTPUT_DIR, "kosis_household_asset_sido_yearly.csv")
    pivot.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  저장: {out_path}")
    print(
        f"    행 수: {len(pivot):,}, "
        f"시도 수: {pivot['시도'].nunique()}, "
        f"기간: {int(pivot['연도'].min())} ~ {int(pivot['연도'].max())}"
    )

    return pivot


# ═══════════════════════════════════════════════════════
# 4. 국세청 근로소득 시군구별 (KOSIS)
# ═══════════════════════════════════════════════════════

# KOSIS 국세청 통계표: 시·군·구별 근로소득 연말정산 신고현황 [2016~]
NTS_ORG_ID = "133"
NTS_TBL_ID = "DT_133001N_4215"

# KOSIS C1 코드(시도) → 표준 시도명 매핑
_NTS_SIDO_MAP = {
    "A00": "전국", "A01": "서울", "A02": "인천", "A03": "경기",
    "A04": "강원", "A05": "대전", "A06": "충북", "A07": "충남",
    "A08": "세종", "A09": "광주", "A10": "전북", "A11": "전남",
    "A12": "대구", "A13": "경북", "A14": "부산", "A15": "울산",
    "A16": "경남", "A17": "제주", "A18": "기타",
}


def _kosis_api_via_powershell(params, timeout=120):
    """
    WSL에서 KOSIS 접속 불가 시 PowerShell을 통해 API 호출.
    requests 실패 시 폴백으로 사용.
    """
    # URL 파라미터 조합
    query_parts = [f"{k}={v}" for k, v in params.items()]
    url = KOSIS_API_URL + "?" + "&".join(query_parts)

    ps_script = (
        f"$r = Invoke-RestMethod -Uri '{url}' -TimeoutSec {timeout}; "
        f"$r | ConvertTo-Json -Depth 5 -Compress"
    )

    try:
        result = subprocess.run(
            ["powershell.exe", "-Command", ps_script],
            capture_output=True, text=True, timeout=timeout + 30,
            encoding="utf-8", errors="replace",
        )
        if result.returncode != 0:
            raise RuntimeError(f"PowerShell 오류: {result.stderr[:500]}")

        raw = result.stdout.strip()
        # BOM 제거
        if raw.startswith("\ufeff"):
            raw = raw[1:]
        data = json.loads(raw)
        return data

    except subprocess.TimeoutExpired:
        raise TimeoutError("PowerShell 호출 타임아웃")
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON 파싱 실패: {e}")


def _kosis_api_get(params, retries=2):
    """
    KOSIS API 호출 — requests 시도 후 실패 시 PowerShell 폴백.
    """
    # 1차: requests 직접 호출
    try:
        resp = _api_get(KOSIS_API_URL, params=params, retries=retries)
        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            return data
        if isinstance(data, dict) and "err" not in data:
            return data
        # 에러 응답이면 PowerShell 폴백
        err = data.get("err", "") if isinstance(data, dict) else ""
        print(f"    requests 응답 오류 (err={err}), PowerShell 폴백 시도")
    except Exception as e:
        print(f"    requests 실패 ({e}), PowerShell 폴백 시도")

    # 2차: PowerShell 폴백
    return _kosis_api_via_powershell(params)


def fetch_nts_income_data(start_year=2016, end_year=2024):
    """
    KOSIS API → 국세청 시·군·구별 근로소득 연말정산 신고현황
    통계표 ID: DT_133001N_4215 (orgId=133)

    차원 구조:
        C1 = 행정구역(시군구)별 — A00(전국), A01(서울), A0101(강남구) ...
        C2 = 신고현황별 — B01(급여총계), B02(과세대상근로소득/총급여),
                          B03(과세표준), B04(결정세액)
        ITM = T001(인원/명), T002(금액/백만원)

    출력: {OUTPUT_DIR}/nts_income_sigungu_yearly.csv
    컬럼: [지역코드, 시도, 시군구, 연도, 급여총계_인원, 급여총계_금액,
           총급여_인원, 총급여_금액, 과세표준_인원, 과세표준_금액,
           결정세액_인원, 결정세액_금액, 1인당총급여]
    """
    print("=" * 60)
    print("[4/4] 국세청 근로소득 시군구별 수집 (KOSIS)")
    print("=" * 60)

    api_key = _get_kosis_key()
    print(f"  API Key: {api_key[:8]}...")

    # KOSIS 40,000셀 제한 확인
    # 1년당: 247(지역) x 4(항목) x 2(인원/금액) = 1,976행
    # 최대 약 20년 가능 → 분할 불필요
    year_span = end_year - start_year + 1
    est_rows = 247 * 4 * 2 * year_span
    print(f"  조회 기간: {start_year}~{end_year} ({year_span}년)")
    print(f"  예상 행 수: {est_rows:,} (40,000셀 제한)")

    # 40,000셀 초과 시 분할 요청
    if est_rows > 38000:
        print("  40,000셀 제한 초과 우려 → 기간 분할 요청")
        all_data = []
        # 약 19년씩 분할
        chunk_years = max(1, 38000 // (247 * 4 * 2))
        for yr_start in range(start_year, end_year + 1, chunk_years):
            yr_end = min(yr_start + chunk_years - 1, end_year)
            print(f"    분할 요청: {yr_start}~{yr_end}")

            params = {
                "method": "getList",
                "apiKey": api_key,
                "itmId": "ALL",
                "objL1": "ALL",
                "objL2": "ALL",
                "prdSe": "Y",
                "startPrdDe": str(yr_start),
                "endPrdDe": str(yr_end),
                "orgId": NTS_ORG_ID,
                "tblId": NTS_TBL_ID,
                "format": "json",
                "jsonVD": "Y",
            }

            try:
                chunk = _kosis_api_get(params)
                if isinstance(chunk, list):
                    all_data.extend(chunk)
                    print(f"      {len(chunk)} 행 수신")
                else:
                    err = chunk.get("err", "") if isinstance(chunk, dict) else ""
                    print(f"      오류 (err={err})")
            except Exception as e:
                print(f"      요청 실패: {e}")

            time.sleep(1)  # API 부하 방지

        raw_data = all_data if all_data else None
    else:
        params = {
            "method": "getList",
            "apiKey": api_key,
            "itmId": "ALL",
            "objL1": "ALL",
            "objL2": "ALL",
            "prdSe": "Y",
            "startPrdDe": str(start_year),
            "endPrdDe": str(end_year),
            "orgId": NTS_ORG_ID,
            "tblId": NTS_TBL_ID,
            "format": "json",
            "jsonVD": "Y",
        }

        try:
            raw_data = _kosis_api_get(params)
            if isinstance(raw_data, list):
                print(f"  {len(raw_data)} 행 수신")
            elif isinstance(raw_data, dict):
                err = raw_data.get("err", raw_data.get("errMsg", ""))
                print(f"  API 오류: {err}")
                raw_data = None
            else:
                print(f"  빈 응답")
                raw_data = None
        except Exception as e:
            print(f"  API 요청 실패: {e}")
            raw_data = None

    if not raw_data:
        print("  데이터 수신 실패")
        return None

    # DataFrame 변환
    df = pd.DataFrame(raw_data)

    # 원본 JSON 백업 (샘플)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    backup = os.path.join(OUTPUT_DIR, "nts_income_raw_sample.json")
    with open(backup, "w", encoding="utf-8") as f:
        json.dump(raw_data[:50], f, ensure_ascii=False, indent=2)
    print(f"  원본 샘플 백업: {backup}")

    # 필수 컬럼 확인
    required = ["C1", "C1_NM", "C2", "C2_NM", "ITM_ID", "ITM_NM", "DT", "PRD_DE"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"  필수 컬럼 누락: {missing}")
        print(f"  사용 가능: {list(df.columns)}")
        return None

    # 값 숫자 변환
    df["값"] = pd.to_numeric(
        df["DT"].astype(str).str.replace(",", ""), errors="coerce"
    )
    df["연도"] = pd.to_numeric(df["PRD_DE"], errors="coerce").astype("Int64")

    # 항목 확인
    c2_items = df[["C2", "C2_NM"]].drop_duplicates().sort_values("C2")
    print(f"  C2 항목: {dict(zip(c2_items['C2'], c2_items['C2_NM']))}")

    itm_items = df[["ITM_ID", "ITM_NM"]].drop_duplicates().sort_values("ITM_ID")
    print(f"  ITM 항목: {dict(zip(itm_items['ITM_ID'], itm_items['ITM_NM']))}")

    # 피벗: C2(신고현황) x ITM(인원/금액) 조합을 컬럼으로
    # 레이블 생성: "급여총계_인원", "급여총계_금액" 등
    c2_label_map = {
        "B01": "급여총계",
        "B02": "총급여",
        "B03": "과세표준",
        "B04": "결정세액",
    }
    itm_label_map = {
        "T001": "인원",
        "T002": "금액",
    }

    df["지표명"] = (
        df["C2"].map(c2_label_map).fillna(df["C2_NM"]) + "_" +
        df["ITM_ID"].map(itm_label_map).fillna(df["ITM_NM"])
    )

    # 피벗 (지역 x 연도 → 지표)
    pivot = df.pivot_table(
        index=["C1", "C1_NM", "연도"],
        columns="지표명",
        values="값",
        aggfunc="first",
    ).reset_index()
    pivot.columns.name = None

    # 지역 코드 → 시도 매핑
    pivot["지역코드"] = pivot["C1"]
    pivot["시군구"] = pivot["C1_NM"]

    # 시도 결정: C1 코드 앞 3자리로 시도 매핑
    pivot["시도코드"] = pivot["C1"].str[:3]
    pivot["시도"] = pivot["시도코드"].map(_NTS_SIDO_MAP)

    # 시도 레벨 행은 시군구를 비워둠 (또는 시도명 그대로)
    # C1 길이가 3이면 시도 레벨, 그 이상이면 시군구
    is_sido_level = pivot["C1"].str.len() <= 3
    pivot.loc[is_sido_level, "시군구"] = pivot.loc[is_sido_level, "시도"]

    # 전국/기타 제외 (선택사항: 일단 포함하되 구분)
    pivot["레벨"] = np.where(
        pivot["C1"] == "A00", "전국",
        np.where(is_sido_level, "시도", "시군구")
    )

    # 1인당 총급여 계산 (총급여_금액 / 총급여_인원)
    if "총급여_금액" in pivot.columns and "총급여_인원" in pivot.columns:
        pivot["1인당총급여_백만원"] = np.where(
            pivot["총급여_인원"] > 0,
            pivot["총급여_금액"] / pivot["총급여_인원"],
            np.nan,
        )

    # 1인당 결정세액 계산
    if "결정세액_금액" in pivot.columns and "결정세액_인원" in pivot.columns:
        pivot["1인당결정세액_백만원"] = np.where(
            pivot["결정세액_인원"] > 0,
            pivot["결정세액_금액"] / pivot["결정세액_인원"],
            np.nan,
        )

    # 컬럼 정리 및 정렬
    col_order = ["지역코드", "시도", "시군구", "레벨", "연도"]
    metric_cols = [c for c in pivot.columns if c.endswith(("_인원", "_금액", "_백만원"))]
    col_order += sorted(metric_cols)
    # 존재하는 컬럼만 선택
    col_order = [c for c in col_order if c in pivot.columns]
    pivot = pivot[col_order].copy()

    pivot = pivot.sort_values(["연도", "지역코드"]).reset_index(drop=True)

    # 전국/기타 제외 버전 저장 (시군구 분석용)
    pivot_sigungu = pivot[
        (pivot["레벨"] == "시군구") & (pivot["시도"] != "기타")
    ].copy()

    # 저장: 시군구 레벨
    out_path = os.path.join(OUTPUT_DIR, "nts_income_sigungu_yearly.csv")
    pivot_sigungu.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  저장 (시군구): {out_path}")
    print(
        f"    행 수: {len(pivot_sigungu):,}, "
        f"시군구 수: {pivot_sigungu['시군구'].nunique()}, "
        f"기간: {int(pivot_sigungu['연도'].min())} ~ {int(pivot_sigungu['연도'].max())}"
    )

    # 저장: 전체 (전국/시도/시군구 포함)
    out_full = os.path.join(OUTPUT_DIR, "nts_income_full_yearly.csv")
    pivot.to_csv(out_full, index=False, encoding="utf-8-sig")
    print(f"  저장 (전체): {out_full}")
    print(f"    행 수: {len(pivot):,}")

    return pivot_sigungu


# ═══════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="수요 데이터 수집: 소득(국민연금) · 대출(BOK) · 자산(KOSIS) · 근로소득(국세청)"
    )
    parser.add_argument("--nps", action="store_true", help="국민연금 CSV 전처리만")
    parser.add_argument("--bok", action="store_true", help="BOK 주담대만")
    parser.add_argument("--kosis", action="store_true", help="KOSIS 가계자산만")
    parser.add_argument("--nts", action="store_true", help="국세청 근로소득만")
    parser.add_argument(
        "--bok-start", default="200612", help="BOK 조회 시작월 (기본: 200612)"
    )
    parser.add_argument(
        "--bok-end", default="202602", help="BOK 조회 종료월 (기본: 202602)"
    )
    parser.add_argument(
        "--kosis-start", type=int, default=2012, help="KOSIS 시작년도 (기본: 2012)"
    )
    parser.add_argument(
        "--kosis-end", type=int, default=2024, help="KOSIS 종료년도 (기본: 2024)"
    )
    parser.add_argument(
        "--nts-start", type=int, default=2016, help="국세청 시작년도 (기본: 2016)"
    )
    parser.add_argument(
        "--nts-end", type=int, default=2024, help="국세청 종료년도 (기본: 2024)"
    )
    args = parser.parse_args()

    run_all = not (args.nps or args.bok or args.kosis or args.nts)

    print(f"\n데이터 출력 디렉토리: {OUTPUT_DIR}\n")

    results = {}

    if run_all or args.nps:
        results["nps"] = process_nps_data()

    if run_all or args.bok:
        results["bok"] = fetch_bok_housing_loan(args.bok_start, args.bok_end)

    if run_all or args.kosis:
        results["kosis"] = fetch_kosis_household_asset(
            args.kosis_start, args.kosis_end
        )

    if run_all or args.nts:
        results["nts"] = fetch_nts_income_data(
            args.nts_start, args.nts_end
        )

    print("\n" + "=" * 60)
    print("완료 요약:")
    for key, val in results.items():
        status = f"{len(val):,}행" if val is not None else "실패/없음"
        print(f"  {key}: {status}")
    print("=" * 60)


if __name__ == "__main__":
    main()

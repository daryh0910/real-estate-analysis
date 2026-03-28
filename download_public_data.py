"""
공공데이터 API 수집 스크립트 — 미분양, 인구이동, 기준금리, 전월세전환율, 주택건설실적, 지가변동률

사용법:
    python download_public_data.py                # 전체 실행
    python download_public_data.py --unsold       # 미분양만
    python download_public_data.py --migration    # 인구이동만
    python download_public_data.py --rate         # 기준금리만
    python download_public_data.py --construction # 주택건설실적만
    python download_public_data.py --land-price   # 지가변동률만

출력 파일:
    {DEMAND_DIR}/unsold_housing_sido_monthly.csv
    {DEMAND_DIR}/population_migration_sido_monthly.csv
    {DEMAND_DIR}/base_rate_monthly.csv
    {DEMAND_DIR}/construction_pipeline_sido_monthly.csv
    {DEMAND_DIR}/land_price_change_sido_monthly.csv
"""

import os
import sys
import time
import argparse
import requests
import pandas as pd
import numpy as np
import glob as _glob
import xml.etree.ElementTree as ET

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
OUTPUT_DIR = os.path.join(BACKDATA, "수요/수요_집계")

# API 키 — 환경변수 또는 .env 파일에서 로드
DATA_GO_KR_KEY = os.environ.get("DATA_GO_KR_KEY", "")
BOK_API_KEY = os.environ.get("BOK_API_KEY", "")
BOK_BASE_URL = "https://ecos.bok.or.kr/api"

# 시도코드 매핑
SIDO_CODE_MAP = {
    11: "서울", 26: "부산", 27: "대구", 28: "인천",
    29: "광주", 30: "대전", 31: "울산", 36: "세종",
    41: "경기", 42: "강원", 43: "충북", 44: "충남",
    45: "전북", 46: "전남", 47: "경북", 48: "경남",
    50: "제주", 51: "강원", 52: "전북",
}

# 시도명 정규화
SIDO_NORM = {
    "서울특별시": "서울", "부산광역시": "부산", "대구광역시": "대구",
    "인천광역시": "인천", "광주광역시": "광주", "대전광역시": "대전",
    "울산광역시": "울산", "세종특별자치시": "세종", "경기도": "경기",
    "강원도": "강원", "강원특별자치도": "강원",
    "충청북도": "충북", "충청남도": "충남",
    "전라북도": "전북", "전북특별자치도": "전북",
    "전라남도": "전남", "경상북도": "경북", "경상남도": "경남",
    "제주특별자치도": "제주",
    "서울": "서울", "부산": "부산", "대구": "대구", "인천": "인천",
    "광주": "광주", "대전": "대전", "울산": "울산", "세종": "세종",
    "경기": "경기", "강원": "강원", "충북": "충북", "충남": "충남",
    "전북": "전북", "전남": "전남", "경북": "경북", "경남": "경남",
    "제주": "제주",
}


def _normalize_sido(name):
    """시도명 정규화"""
    if pd.isna(name):
        return None
    name = str(name).strip()
    return SIDO_NORM.get(name, name)


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
# 1. 미분양주택현황 (국토교통부 API)
# ═══════════════════════════════════════════════════════

def fetch_unsold_housing(start_ym="200801", end_ym="202602"):
    """
    국토교통부 미분양주택현황 API → 시도별 월별 미분양 호수
    API: 미분양주택현황보고 (서비스키: 1613000/KMTL_003)
    출력: {OUTPUT_DIR}/unsold_housing_sido_monthly.csv
    """
    print("=" * 60)
    print("[1] 미분양주택현황 수집 (국토교통부)")
    print("=" * 60)

    # data.go.kr 미분양주택현황 API
    base_url = "http://apis.data.go.kr/1613000/OpenAPI_NSDI_UnslHouseService/getUnslHouseList"

    all_rows = []
    start_year = int(start_ym[:4])
    start_month = int(start_ym[4:6])
    end_year = int(end_ym[:4])
    end_month = int(end_ym[4:6])

    # 월별로 순회
    year, month = start_year, start_month
    total_months = 0
    success_months = 0

    while (year < end_year) or (year == end_year and month <= end_month):
        ym_str = f"{year}{month:02d}"
        params = {
            "serviceKey": DATA_GO_KR_KEY,
            "pblntfYm": ym_str,
            "numOfRows": "1000",
            "pageNo": "1",
        }

        try:
            resp = _api_get(base_url, params=params)
            root = ET.fromstring(resp.content)

            # 에러 체크
            result_code = root.findtext(".//resultCode")
            if result_code and result_code != "00":
                result_msg = root.findtext(".//resultMsg")
                if total_months == 0:
                    print(f"  API 오류 ({ym_str}): {result_msg}")

            items = root.findall(".//item")
            if items:
                for item in items:
                    sido = item.findtext("ctprvnNm", "").strip()
                    unsold = item.findtext("unslQy", "0").strip()
                    try:
                        unsold_cnt = int(unsold.replace(",", ""))
                    except ValueError:
                        unsold_cnt = 0

                    all_rows.append({
                        "연월": f"{year}-{month:02d}",
                        "시도_raw": sido,
                        "미분양_호수": unsold_cnt,
                    })
                success_months += 1

        except Exception as e:
            if total_months == 0:
                print(f"  API 요청 실패 ({ym_str}): {e}")

        total_months += 1
        # 다음 월
        month += 1
        if month > 12:
            month = 1
            year += 1

        # API 호출 간격
        time.sleep(0.3)

        # 진행률 표시 (12개월마다)
        if total_months % 12 == 0:
            print(f"  진행: {total_months}개월 처리, {success_months}개월 성공")

    print(f"  총 {total_months}개월 요청, {success_months}개월 데이터 수신")

    if not all_rows:
        print("  데이터 없음 — 대체 API 시도")
        return _fetch_unsold_housing_alt(start_ym, end_ym)

    df = pd.DataFrame(all_rows)
    df["시도"] = df["시도_raw"].apply(_normalize_sido)
    df = df.dropna(subset=["시도"])

    # 시도 레벨 집계 (중복 제거)
    result = df.groupby(["연월", "시도"])["미분양_호수"].sum().reset_index()
    result["연도"] = result["연월"].str[:4].astype(int)
    result["월"] = result["연월"].str[5:7].astype(int)
    result = result.sort_values(["연월", "시도"]).reset_index(drop=True)

    # 저장
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "unsold_housing_sido_monthly.csv")
    result.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  저장: {out_path}")
    print(f"    행 수: {len(result):,}, 시도 수: {result['시도'].nunique()}, "
          f"기간: {result['연월'].min()} ~ {result['연월'].max()}")

    return result


def _fetch_unsold_housing_alt(start_ym="200801", end_ym="202602"):
    """대체 API: KOSIS 미분양주택현황 통계"""
    print("  KOSIS 미분양 통계로 대체 시도 중...")

    # BOK ECOS 미분양주택현황 (901Y074)
    url = (
        f"{BOK_BASE_URL}/StatisticSearch/{BOK_API_KEY}/json/kr/"
        f"1/99999/901Y074/M/{start_ym}/{end_ym}/"
    )

    try:
        resp = _api_get(url)
        data = resp.json()
    except Exception as e:
        print(f"  BOK API 요청 실패: {e}")
        return None

    if "StatisticSearch" not in data:
        err_msg = data.get("RESULT", {}).get("MESSAGE", "알 수 없는 오류")
        print(f"  BOK API 오류: {err_msg}")
        return None

    rows = data["StatisticSearch"]["row"]
    print(f"  BOK에서 {len(rows)}행 수신")

    all_rows = []
    for row in rows:
        time_str = row.get("TIME", "")
        item_name = row.get("ITEM_NAME1", "").strip()
        value_str = row.get("DATA_VALUE", "")

        if len(time_str) < 6:
            continue

        ym = f"{time_str[:4]}-{time_str[4:6]}"
        sido = _normalize_sido(item_name)

        try:
            val = float(value_str.replace(",", ""))
        except (ValueError, AttributeError):
            continue

        all_rows.append({
            "연월": ym,
            "시도": sido,
            "미분양_호수": int(val),
        })

    if not all_rows:
        print("  BOK에서도 데이터 없음")
        return None

    result = pd.DataFrame(all_rows)
    result = result.dropna(subset=["시도"])
    result["연도"] = result["연월"].str[:4].astype(int)
    result["월"] = result["연월"].str[5:7].astype(int)
    result = result.sort_values(["연월", "시도"]).reset_index(drop=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "unsold_housing_sido_monthly.csv")
    result.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  저장: {out_path}")
    print(f"    행 수: {len(result):,}, 시도 수: {result['시도'].nunique()}")

    return result


# ═══════════════════════════════════════════════════════
# 2. 인구이동 (KOSIS API → 시도간 전입/전출)
# ═══════════════════════════════════════════════════════

def fetch_population_migration(start_year=2010, end_year=2025):
    """
    BOK ECOS API / data.go.kr → 시도별 월별 전입/전출 인구
    BOK 통계코드: 101Y008 (국내인구이동)
    주의: 이전에 901Y064를 사용했으나, 해당 코드는 지가변동률임 (버그 수정)
    출력: {OUTPUT_DIR}/population_migration_sido_monthly.csv
    """
    print("=" * 60)
    print("[2] 인구이동 수집 (BOK/data.go.kr)")
    print("=" * 60)

    start_ym = f"{start_year}01"
    end_ym = f"{end_year}12"

    # BOK ECOS에서 인구이동 통계 조회
    # 101Y008: 시도별 이동자수, 101Y009: 시도별 순이동
    # 여러 통계코드를 시도하여 인구이동 데이터를 가져옴
    bok_migration_codes = ["101Y008", "101Y009", "101Y003"]

    for stat_code in bok_migration_codes:
        url = (
            f"{BOK_BASE_URL}/StatisticSearch/{BOK_API_KEY}/json/kr/"
            f"1/99999/{stat_code}/M/{start_ym}/{end_ym}/"
        )
        print(f"  BOK 통계코드 {stat_code} 시도 중...")

        try:
            resp = _api_get(url)
            data = resp.json()
        except Exception as e:
            print(f"  BOK API 요청 실패: {e}")
            continue

        if "StatisticSearch" not in data:
            err_msg = data.get("RESULT", {}).get("MESSAGE", "알 수 없는 오류")
            print(f"  BOK API 오류 ({stat_code}): {err_msg}")
            continue

        rows = data["StatisticSearch"]["row"]
        print(f"  BOK {stat_code}에서 {len(rows)}행 수신")

        all_rows = []
        for row in rows:
            time_str = row.get("TIME", "") or ""
            item1 = (row.get("ITEM_NAME1") or "").strip()
            item2 = (row.get("ITEM_NAME2") or "").strip()
            value_str = row.get("DATA_VALUE", "") or ""

            if len(time_str) < 6:
                continue

            ym = f"{time_str[:4]}-{time_str[4:6]}"

            try:
                val = float(value_str.replace(",", ""))
            except (ValueError, AttributeError):
                continue

            sido = _normalize_sido(item1)
            if sido is None:
                sido = _normalize_sido(item2)

            category = item2 if sido == _normalize_sido(item1) else item1

            all_rows.append({
                "연월": ym,
                "시도": sido,
                "구분": category,
                "인구수": val,
            })

        if not all_rows:
            continue

        df = pd.DataFrame(all_rows)
        df = df.dropna(subset=["시도"])

        # 전입/전출 피벗
        pivot = df.pivot_table(
            index=["연월", "시도"],
            columns="구분",
            values="인구수",
            aggfunc="first",
        ).reset_index()
        pivot.columns.name = None

        # 컬럼명 정리 - 전입/전출 키워드 찾기
        result = pivot[["연월", "시도"]].copy()
        for col in pivot.columns:
            if "전입" in str(col):
                result["전입"] = pivot[col]
            elif "전출" in str(col):
                result["전출"] = pivot[col]

        if "전입" in result.columns and "전출" in result.columns:
            result["순이동"] = result["전입"] - result["전출"]
        elif len(pivot.columns) > 2:
            # 컬럼명이 다른 경우 모든 숫자 컬럼 포함
            for col in pivot.columns:
                if col not in ["연월", "시도"] and pivot[col].dtype in [np.float64, np.int64]:
                    result[col] = pivot[col]

        result["연도"] = result["연월"].str[:4].astype(int)
        result["월"] = result["연월"].str[5:7].astype(int)
        result = result.sort_values(["연월", "시도"]).reset_index(drop=True)

        # 저장
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out_path = os.path.join(OUTPUT_DIR, "population_migration_sido_monthly.csv")
        result.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"  저장: {out_path}")
        print(f"    행 수: {len(result):,}, 시도 수: {result['시도'].nunique()}")

        return result

    # BOK에서 인구이동 데이터를 못 찾으면 data.go.kr 대체
    print("  BOK에서 인구이동 데이터 없음 → data.go.kr 대체")
    return _fetch_migration_from_kosis(start_year, end_year)


def _fetch_migration_from_kosis(start_year=2010, end_year=2025):
    """대체: data.go.kr 인구이동통계 API"""
    print("  data.go.kr 인구이동 API로 대체 시도...")

    base_url = "http://apis.data.go.kr/1240000/CtpvPopMoveService/getCtpvPopMoveInfo"

    all_rows = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            params = {
                "serviceKey": DATA_GO_KR_KEY,
                "numOfRows": "1000",
                "pageNo": "1",
                "statsYm": f"{year}{month:02d}",
            }

            try:
                resp = _api_get(base_url, params=params)
                root = ET.fromstring(resp.content)
                items = root.findall(".//item")

                for item in items:
                    sido = item.findtext("ctprvnNm", "").strip()
                    move_in = item.findtext("moveInCnt", "0").strip()
                    move_out = item.findtext("moveOutCnt", "0").strip()

                    try:
                        in_cnt = int(move_in.replace(",", ""))
                        out_cnt = int(move_out.replace(",", ""))
                    except ValueError:
                        continue

                    all_rows.append({
                        "연월": f"{year}-{month:02d}",
                        "시도_raw": sido,
                        "전입": in_cnt,
                        "전출": out_cnt,
                    })

            except Exception:
                pass

            time.sleep(0.2)

        print(f"  {year}년 완료")

    if not all_rows:
        print("  data.go.kr에서도 데이터 없음")
        return None

    df = pd.DataFrame(all_rows)
    df["시도"] = df["시도_raw"].apply(_normalize_sido)
    df = df.dropna(subset=["시도"])
    df["순이동"] = df["전입"] - df["전출"]
    df["연도"] = df["연월"].str[:4].astype(int)
    df["월"] = df["연월"].str[5:7].astype(int)

    result = df[["연월", "시도", "전입", "전출", "순이동", "연도", "월"]].copy()
    result = result.sort_values(["연월", "시도"]).reset_index(drop=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "population_migration_sido_monthly.csv")
    result.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  저장: {out_path}")

    return result


# ═══════════════════════════════════════════════════════
# 3. 기준금리 (BOK ECOS API)
# ═══════════════════════════════════════════════════════

def fetch_base_rate(start_ym="200001", end_ym="202612"):
    """
    BOK ECOS API → 기준금리 + CD금리 + 국고채금리 시계열
    출력: {OUTPUT_DIR}/base_rate_monthly.csv
    """
    print("=" * 60)
    print("[3] 기준금리/시장금리 수집 (BOK)")
    print("=" * 60)

    # 통계코드: 722Y001 (한국은행 기준금리)
    # 항목: 0101000 (한국은행 기준금리)
    rate_items = [
        ("722Y001", "0101000", "기준금리"),      # 한국은행 기준금리
        ("721Y001", "2010000", "CD_91일"),       # CD 91일물 (월별)
        ("721Y001", "5020000", "국고채_3년"),     # 국고채 3년 (월별)
        ("721Y001", "5040000", "국고채_5년"),     # 국고채 5년 (월별)
        ("721Y001", "5050000", "국고채_10년"),    # 국고채 10년 (월별)
    ]

    all_rows = []
    for stat_code, item_code, label in rate_items:
        url = (
            f"{BOK_BASE_URL}/StatisticSearch/{BOK_API_KEY}/json/kr/"
            f"1/99999/{stat_code}/M/{start_ym}/{end_ym}/{item_code}/"
        )
        print(f"  요청: {label} ({stat_code}/{item_code})")

        try:
            resp = _api_get(url)
            data = resp.json()
        except Exception as e:
            print(f"  API 요청 실패: {e}")
            continue

        if "StatisticSearch" not in data:
            err_msg = data.get("RESULT", {}).get("MESSAGE", "알 수 없는 오류")
            print(f"  API 오류: {err_msg}")
            continue

        rows = data["StatisticSearch"]["row"]
        print(f"    {len(rows)}행 수신")

        for row in rows:
            time_str = row.get("TIME", "")
            value_str = row.get("DATA_VALUE", "")

            if len(time_str) < 6:
                continue

            ym = f"{time_str[:4]}-{time_str[4:6]}"

            try:
                val = float(value_str.replace(",", ""))
            except (ValueError, AttributeError):
                continue

            all_rows.append({
                "연월": ym,
                "지표": label,
                "금리": val,
            })

    if not all_rows:
        print("  데이터 없음")
        return None

    df = pd.DataFrame(all_rows)

    # 피벗: 지표별 → 컬럼으로
    pivot = df.pivot_table(
        index="연월",
        columns="지표",
        values="금리",
        aggfunc="first",
    ).reset_index()
    pivot.columns.name = None

    pivot["연도"] = pivot["연월"].str[:4].astype(int)
    pivot["월"] = pivot["연월"].str[5:7].astype(int)
    pivot = pivot.sort_values("연월").reset_index(drop=True)

    # 저장
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "base_rate_monthly.csv")
    pivot.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  저장: {out_path}")
    print(f"    행 수: {len(pivot):,}, 기간: {pivot['연월'].min()} ~ {pivot['연월'].max()}")
    print(f"    컬럼: {[c for c in pivot.columns if c not in ['연월', '연도', '월']]}")

    return pivot


# ═══════════════════════════════════════════════════════
# 4. 전월세전환율 (BOK ECOS API)
# ═══════════════════════════════════════════════════════

def fetch_jeonwolse_rate(start_ym="201201", end_ym="202602"):
    """
    BOK ECOS API → 시도별 전월세전환율
    통계표: 901Y093 (주택유형별/지역별 전월세전환율)
    출력: {OUTPUT_DIR}/jeonwolse_conversion_rate_sido_monthly.csv
    """
    print("=" * 60)
    print("[4] 전월세전환율 수집 (BOK)")
    print("=" * 60)

    url = (
        f"{BOK_BASE_URL}/StatisticSearch/{BOK_API_KEY}/json/kr/"
        f"1/99999/901Y093/M/{start_ym}/{end_ym}/"
    )

    try:
        resp = _api_get(url)
        data = resp.json()
    except Exception as e:
        print(f"  API 요청 실패: {e}")
        return None

    if "StatisticSearch" not in data:
        err_msg = data.get("RESULT", {}).get("MESSAGE", "알 수 없는 오류")
        print(f"  API 오류: {err_msg}")
        # 대체 통계코드 시도
        return _fetch_jeonwolse_rate_alt(start_ym, end_ym)

    rows = data["StatisticSearch"]["row"]
    print(f"  {len(rows)}행 수신")

    all_rows = []
    for row in rows:
        time_str = row.get("TIME", "") or ""
        value_str = row.get("DATA_VALUE", "") or ""
        item1 = (row.get("ITEM_NAME1") or "").strip()
        item2 = (row.get("ITEM_NAME2") or "").strip()
        item3 = (row.get("ITEM_NAME3") or "").strip()

        if len(time_str) < 6:
            continue

        ym = f"{time_str[:4]}-{time_str[4:6]}"

        try:
            val = float(value_str.replace(",", ""))
        except (ValueError, AttributeError):
            continue

        # 지역명 / 주택유형 구분
        sido = None
        housing_type = "전체"
        for item in [item1, item2, item3]:
            normalized = _normalize_sido(item)
            if normalized in SIDO_NORM.values():
                sido = normalized
            elif item in ("아파트", "단독주택", "연립다세대", "전체"):
                housing_type = item

        if sido is None:
            # 전국 포함
            for item in [item1, item2, item3]:
                if "전국" in item:
                    sido = "전국"
                    break

        all_rows.append({
            "연월": ym,
            "시도": sido,
            "주택유형": housing_type,
            "전월세전환율": val,
        })

    if not all_rows:
        print("  파싱 실패")
        return None

    df = pd.DataFrame(all_rows)
    df = df.dropna(subset=["시도"])

    # 아파트만 필터 (또는 전체)
    apt_df = df[df["주택유형"] == "아파트"]
    if apt_df.empty:
        apt_df = df[df["주택유형"] == "전체"]
    if apt_df.empty:
        apt_df = df

    result = apt_df[["연월", "시도", "전월세전환율"]].copy()
    result = result[result["시도"] != "전국"]
    result["연도"] = result["연월"].str[:4].astype(int)
    result["월"] = result["연월"].str[5:7].astype(int)
    result = result.sort_values(["연월", "시도"]).reset_index(drop=True)

    # 저장
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "jeonwolse_conversion_rate_sido_monthly.csv")
    result.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  저장: {out_path}")
    print(f"    행 수: {len(result):,}, 시도 수: {result['시도'].nunique()}")

    return result


def _fetch_jeonwolse_rate_alt(start_ym, end_ym):
    """대체: 다른 BOK 통계코드로 전월세전환율 시도"""
    alt_codes = ["901Y093", "901Y067"]
    for code in alt_codes:
        url = (
            f"{BOK_BASE_URL}/StatisticSearch/{BOK_API_KEY}/json/kr/"
            f"1/99999/{code}/M/{start_ym}/{end_ym}/"
        )
        try:
            resp = _api_get(url)
            data = resp.json()
            if "StatisticSearch" in data:
                rows = data["StatisticSearch"]["row"]
                print(f"  대체 코드 {code}: {len(rows)}행 수신")
                # 간단히 저장
                all_rows = []
                for row in rows:
                    time_str = row.get("TIME", "")
                    val_str = row.get("DATA_VALUE", "")
                    item1 = row.get("ITEM_NAME1", "").strip()
                    if len(time_str) >= 6:
                        ym = f"{time_str[:4]}-{time_str[4:6]}"
                        try:
                            val = float(val_str.replace(",", ""))
                        except (ValueError, AttributeError):
                            continue
                        all_rows.append({
                            "연월": ym,
                            "시도": _normalize_sido(item1),
                            "전월세전환율": val,
                        })

                if all_rows:
                    result = pd.DataFrame(all_rows)
                    result = result.dropna(subset=["시도"])
                    result["연도"] = result["연월"].str[:4].astype(int)
                    result["월"] = result["연월"].str[5:7].astype(int)

                    os.makedirs(OUTPUT_DIR, exist_ok=True)
                    out_path = os.path.join(OUTPUT_DIR, "jeonwolse_conversion_rate_sido_monthly.csv")
                    result.to_csv(out_path, index=False, encoding="utf-8-sig")
                    print(f"  저장: {out_path}")
                    return result
        except Exception:
            continue

    print("  전월세전환율 수집 실패")
    return None


# ═══════════════════════════════════════════════════════
# 5. 주택가격지수 (BOK ECOS API)
# ═══════════════════════════════════════════════════════

def fetch_housing_price_index(start_ym="200601", end_ym="202602"):
    """
    BOK ECOS API → 시도별 아파트매매가격지수/전세가격지수
    통계표: 901Y062 (주택가격동향 → 아파트매매가격지수)
    출력: {OUTPUT_DIR}/housing_price_index_sido_monthly.csv
    """
    print("=" * 60)
    print("[5] 주택가격지수 수집 (BOK)")
    print("=" * 60)

    # 주택가격지수 관련 통계표
    stat_items = [
        ("901Y062", "", "아파트매매가격지수"),
        ("901Y063", "", "아파트전세가격지수"),
    ]

    all_rows = []
    for stat_code, item_code, label in stat_items:
        url_path = f"{stat_code}/M/{start_ym}/{end_ym}/"
        if item_code:
            url_path += f"{item_code}/"

        url = (
            f"{BOK_BASE_URL}/StatisticSearch/{BOK_API_KEY}/json/kr/"
            f"1/99999/{url_path}"
        )
        print(f"  요청: {label} ({stat_code})")

        try:
            resp = _api_get(url)
            data = resp.json()
        except Exception as e:
            print(f"  API 요청 실패: {e}")
            continue

        if "StatisticSearch" not in data:
            err_msg = data.get("RESULT", {}).get("MESSAGE", "알 수 없는 오류")
            print(f"  API 오류: {err_msg}")
            continue

        rows = data["StatisticSearch"]["row"]
        print(f"    {len(rows)}행 수신")

        for row in rows:
            time_str = row.get("TIME", "")
            value_str = row.get("DATA_VALUE", "")
            item1 = row.get("ITEM_NAME1", "").strip()

            if len(time_str) < 6:
                continue

            ym = f"{time_str[:4]}-{time_str[4:6]}"
            sido = _normalize_sido(item1)

            try:
                val = float(value_str.replace(",", ""))
            except (ValueError, AttributeError):
                continue

            all_rows.append({
                "연월": ym,
                "시도": sido,
                "지표": label,
                "지수": val,
            })

    if not all_rows:
        print("  데이터 없음")
        return None

    df = pd.DataFrame(all_rows)
    df = df.dropna(subset=["시도"])

    # 주택유형 구분 (시도가 아닌 주택유형명: 아파트, 단독주택 등)
    housing_types = {"총지수", "단독주택", "연립주택", "아파트", "아파트(서울)", "총지수(서울)"}
    is_housing_type = df["시도"].isin(housing_types)

    if is_housing_type.all():
        # 전국 단위 주택유형별 지수 → 아파트만 추출
        df_apt = df[df["시도"] == "아파트"].copy()
        df_apt["시도"] = "전국"  # 전국 데이터로 표시

        # 서울 아파트도 추가
        df_seoul = df[df["시도"] == "아파트(서울)"].copy()
        df_seoul["시도"] = "서울"

        df = pd.concat([df_apt, df_seoul], ignore_index=True)

    # 피벗
    pivot = df.pivot_table(
        index=["연월", "시도"],
        columns="지표",
        values="지수",
        aggfunc="first",
    ).reset_index()
    pivot.columns.name = None

    pivot["연도"] = pivot["연월"].str[:4].astype(int)
    pivot["월"] = pivot["연월"].str[5:7].astype(int)
    pivot = pivot.sort_values(["연월", "시도"]).reset_index(drop=True)

    # 저장
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "housing_price_index_sido_monthly.csv")
    pivot.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  저장: {out_path}")
    print(f"    행 수: {len(pivot):,}, 시도: {sorted(pivot['시도'].unique())}")

    return pivot


# ═══════════════════════════════════════════════════════
# 6. 지가변동률 (BOK ECOS API — 901Y064)
# ═══════════════════════════════════════════════════════

def fetch_land_price_change(start_ym="201201", end_ym="202602"):
    """
    BOK ECOS API → 시도별 월별 지가변동률
    통계코드: 901Y064 (지가변동률)
    주의: 이전에 fetch_population_migration에서 잘못 사용되던 코드
    출력: {OUTPUT_DIR}/land_price_change_sido_monthly.csv
    """
    print("=" * 60)
    print("[6] 지가변동률 수집 (BOK 901Y064)")
    print("=" * 60)

    url = (
        f"{BOK_BASE_URL}/StatisticSearch/{BOK_API_KEY}/json/kr/"
        f"1/99999/901Y064/M/{start_ym}/{end_ym}/"
    )

    try:
        resp = _api_get(url)
        data = resp.json()
    except Exception as e:
        print(f"  BOK API 요청 실패: {e}")
        return None

    if "StatisticSearch" not in data:
        err_msg = data.get("RESULT", {}).get("MESSAGE", "알 수 없는 오류")
        print(f"  BOK API 오류: {err_msg}")
        return None

    rows = data["StatisticSearch"]["row"]
    print(f"  BOK에서 {len(rows)}행 수신")

    all_rows = []
    for row in rows:
        time_str = row.get("TIME", "") or ""
        item1 = (row.get("ITEM_NAME1") or "").strip()
        item2 = (row.get("ITEM_NAME2") or "").strip()
        value_str = row.get("DATA_VALUE", "") or ""

        if len(time_str) < 6:
            continue

        ym = f"{time_str[:4]}-{time_str[4:6]}"

        try:
            val = float(value_str.replace(",", ""))
        except (ValueError, AttributeError):
            continue

        # 시도명 추출
        sido = _normalize_sido(item1)
        if sido is None:
            sido = _normalize_sido(item2)

        # 구분 (용도별: 전체, 주거, 상업, 공업 등)
        category = item2 if sido == _normalize_sido(item1) else item1

        all_rows.append({
            "연월": ym,
            "시도": sido,
            "구분": category,
            "지가변동률": val,
        })

    if not all_rows:
        print("  데이터 없음")
        return None

    df = pd.DataFrame(all_rows)
    df = df.dropna(subset=["시도"])

    # 전체 용도 기준으로 필터 (또는 모든 용도 포함)
    # 구분 컬럼에 '전체' 또는 '전용도'가 있으면 그것만 사용
    if "전체" in df["구분"].unique():
        df = df[df["구분"] == "전체"]
    elif "계" in df["구분"].unique():
        df = df[df["구분"] == "계"]

    result = df[["연월", "시도", "지가변동률"]].copy()
    result["연도"] = result["연월"].str[:4].astype(int)
    result["월"] = result["연월"].str[5:7].astype(int)
    result = result.sort_values(["연월", "시도"]).reset_index(drop=True)

    # 저장
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "land_price_change_sido_monthly.csv")
    result.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  저장: {out_path}")
    print(f"    행 수: {len(result):,}, 시도 수: {result['시도'].nunique()}, "
          f"기간: {result['연월'].min()} ~ {result['연월'].max()}")

    return result


# ═══════════════════════════════════════════════════════
# 7. 주택건설실적 — 착공/준공 (BOK ECOS / data.go.kr)
# ═══════════════════════════════════════════════════════

def fetch_construction_pipeline(start_ym="201501", end_ym="202602"):
    """
    주택건설실적 (착공/준공) → 시도별 월별, 아파트/비아파트 구분
    KOSIS API 사용 (주택유형별 착공/준공 실적)
    출력: {OUTPUT_DIR}/construction_pipeline_sido_monthly.csv
    """
    print("=" * 60)
    print("[7] 주택건설실적(착공/준공) 수집 — KOSIS")
    print("=" * 60)

    result = _fetch_construction_from_kosis(start_ym, end_ym)
    if result is not None:
        return result

    print("  KOSIS 실패, BOK ECOS 시도...")
    result = _fetch_construction_from_bok(start_ym, end_ym)
    if result is not None:
        return result

    # BOK 실패 시 data.go.kr 대체
    print("  BOK에서 주택건설실적 없음 → data.go.kr 대체")
    return _fetch_construction_from_datagokr(start_ym, end_ym)


def _fetch_construction_from_kosis(start_ym, end_ym):
    """KOSIS에서 주택유형별 착공/준공 실적 조회 (아파트/비아파트 구분)"""
    import os as _os
    kosis_key = _os.environ.get("KOSIS_API_KEY", "")
    if not kosis_key:
        print("  KOSIS_API_KEY 환경변수 없음")
        return None

    kosis_url = "https://kosis.kr/openapi/Param/statisticsParameterData.do"

    # 착공: DT_MLTM_5387, 준공: DT_MLTM_5373
    tables = [
        ("DT_MLTM_5387", "착공"),
        ("DT_MLTM_5373", "준공"),
    ]

    all_rows = []

    # KOSIS 40,000셀 제한 → 2년 단위로 분할 요청
    start_y = int(start_ym[:4])
    end_y = int(end_ym[:4])
    end_m = end_ym[4:6]
    periods = []
    y = start_y
    while y <= end_y:
        p_start = f"{y}01" if y > start_y else start_ym
        p_end = f"{min(y + 1, end_y)}{end_m if min(y + 1, end_y) == end_y else '12'}"
        if int(p_start) <= int(p_end):
            periods.append((p_start, p_end))
        y += 2

    for tbl_id, category in tables:
        print(f"  [{category}] KOSIS {tbl_id} ({len(periods)}구간 분할)")
        for p_start, p_end in periods:
            params = {
                "method": "getList",
                "apiKey": kosis_key,
                "itmId": "ALL",
                "objL1": "ALL",
                "objL2": "ALL",
                "objL3": "ALL",
                "objL4": "ALL",
                "prdSe": "M",
                "startPrdDe": p_start,
                "endPrdDe": p_end,
                "orgId": "116",
                "tblId": tbl_id,
                "format": "json",
                "jsonVD": "Y",
            }

            try:
                resp = _api_get(kosis_url, params=params)
                data = resp.json()
            except Exception as e:
                print(f"    {p_start}~{p_end} 요청 실패: {e}")
                continue

            if not isinstance(data, list) or len(data) == 0:
                err_msg = data.get("errMsg", "") if isinstance(data, dict) else ""
                print(f"    {p_start}~{p_end} 응답 없음: {err_msg}")
                continue

            print(f"    {p_start}~{p_end}: {len(data)}행")

            for row in data:
            region = row.get("C1_NM", "").strip()
            housing_type = row.get("C2_NM", "").strip()
            prd = row.get("PRD_DE", "")
            value_str = row.get("DT", "")

            if not region or not prd or len(prd) < 6:
                continue

            # 유형 분류: 아파트 / 비아파트 / 전체
            if housing_type == "아파트":
                type_label = "아파트"
            elif "계" in housing_type:
                type_label = "전체"
            else:
                type_label = "비아파트"

            try:
                value = float(value_str.replace(",", ""))
            except (ValueError, AttributeError):
                value = 0

            ym = prd[:4] + "-" + prd[4:6]

            all_rows.append({
                "연월": ym,
                "시도_raw": region,
                "구분": category,
                "유형": type_label,
                "호수": value,
            })

    if not all_rows:
        print("  KOSIS 데이터 없음")
        return None

    df = pd.DataFrame(all_rows)
    df["시도"] = df["시도_raw"].map(SIDO_NORM)
    df = df.dropna(subset=["시도"])

    # 비아파트는 합산 (단독+연립+다세대)
    agg = df.groupby(["연월", "시도", "구분", "유형"])["호수"].sum().reset_index()

    # 피벗: 착공_아파트, 착공_비아파트, 착공_전체, 준공_아파트, ...
    agg["col_name"] = agg["구분"] + "_" + agg["유형"]
    pivot = agg.pivot_table(
        index=["연월", "시도"],
        columns="col_name",
        values="호수",
        aggfunc="sum",
    ).reset_index()
    pivot.columns.name = None

    # 기존 호환: 착공_호수 = 착공_전체, 준공_호수 = 준공_전체
    if "착공_전체" in pivot.columns:
        pivot["착공_호수"] = pivot["착공_전체"]
    if "준공_전체" in pivot.columns:
        pivot["준공_호수"] = pivot["준공_전체"]

    # 연도, 월 추가
    pivot["연도"] = pivot["연월"].str[:4].astype(int)
    pivot["월"] = pivot["연월"].str[5:7].astype(int)

    pivot = pivot.sort_values(["연월", "시도"]).reset_index(drop=True)

    # 저장
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "construction_pipeline_sido_monthly.csv")
    pivot.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  저장: {out_path}")
    print(f"    행 수: {len(pivot):,}, 시도 수: {pivot['시도'].nunique()}, "
          f"기간: {pivot['연월'].min()} ~ {pivot['연월'].max()}")

    cols = [c for c in pivot.columns if "착공" in c or "준공" in c]
    print(f"    컬럼: {cols}")

    return pivot


def _fetch_construction_from_bok(start_ym, end_ym):
    """BOK ECOS에서 주택건설실적(착공/준공) 조회 — fallback"""
    # 여러 통계코드를 시도 (주택건설실적 관련)
    bok_codes = ["901Y070", "901Y071", "104Y016"]

    for stat_code in bok_codes:
        url = (
            f"{BOK_BASE_URL}/StatisticSearch/{BOK_API_KEY}/json/kr/"
            f"1/99999/{stat_code}/M/{start_ym}/{end_ym}/"
        )
        print(f"  BOK 통계코드 {stat_code} 시도 중...")

        try:
            resp = _api_get(url)
            data = resp.json()
        except Exception as e:
            print(f"  API 요청 실패: {e}")
            continue

        if "StatisticSearch" not in data:
            err_msg = data.get("RESULT", {}).get("MESSAGE", "알 수 없는 오류")
            print(f"  API 오류 ({stat_code}): {err_msg}")
            continue

        rows = data["StatisticSearch"]["row"]
        print(f"  BOK {stat_code}에서 {len(rows)}행 수신")

        all_rows = []
        for row in rows:
            time_str = row.get("TIME", "") or ""
            item1 = (row.get("ITEM_NAME1") or "").strip()
            item2 = (row.get("ITEM_NAME2") or "").strip()
            item3 = (row.get("ITEM_NAME3") or "").strip()
            value_str = row.get("DATA_VALUE", "") or ""

            if len(time_str) < 6:
                continue

            ym = f"{time_str[:4]}-{time_str[4:6]}"

            try:
                val = float(value_str.replace(",", ""))
            except (ValueError, AttributeError):
                continue

            # 시도명, 착공/준공 구분 추출
            sido = None
            category = None  # 착공 또는 준공
            for item in [item1, item2, item3]:
                normalized = _normalize_sido(item)
                if normalized in SIDO_NORM.values():
                    sido = normalized
                elif "착공" in item:
                    category = "착공"
                elif "준공" in item or "완공" in item:
                    category = "준공"
                elif "전국" in item:
                    sido = "전국"

            all_rows.append({
                "연월": ym,
                "시도": sido,
                "구분": category,
                "호수": val,
                "item1": item1,
                "item2": item2,
                "item3": item3,
            })

        if not all_rows:
            continue

        df = pd.DataFrame(all_rows)
        df = df.dropna(subset=["시도"])

        if df.empty:
            print(f"  {stat_code}: 시도 매핑 후 데이터 없음, 다음 코드 시도")
            continue

        # 착공/준공 구분이 있는 경우 피벗
        if df["구분"].notna().any():
            df_filtered = df.dropna(subset=["구분"])
            if not df_filtered.empty:
                pivot = df_filtered.pivot_table(
                    index=["연월", "시도"],
                    columns="구분",
                    values="호수",
                    aggfunc="sum",
                ).reset_index()
                pivot.columns.name = None

                result = pivot.rename(columns={
                    "착공": "착공_호수",
                    "준공": "준공_호수",
                })
            else:
                # 구분이 없으면 전체 호수로 저장
                result = df.groupby(["연월", "시도"])["호수"].sum().reset_index()
                result = result.rename(columns={"호수": "착공_호수"})
        else:
            result = df.groupby(["연월", "시도"])["호수"].sum().reset_index()
            result = result.rename(columns={"호수": "착공_호수"})

        # 컬럼 정리
        if "착공_호수" not in result.columns:
            result["착공_호수"] = np.nan
        if "준공_호수" not in result.columns:
            result["준공_호수"] = np.nan

        result["연도"] = result["연월"].str[:4].astype(int)
        result["월"] = result["연월"].str[5:7].astype(int)
        result = result[["연월", "시도", "착공_호수", "준공_호수", "연도", "월"]]
        result = result.sort_values(["연월", "시도"]).reset_index(drop=True)

        # 저장
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out_path = os.path.join(OUTPUT_DIR, "construction_pipeline_sido_monthly.csv")
        result.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"  저장: {out_path}")
        print(f"    행 수: {len(result):,}, 시도 수: {result['시도'].nunique()}, "
              f"기간: {result['연월'].min()} ~ {result['연월'].max()}")

        return result

    return None


def _fetch_construction_from_datagokr(start_ym, end_ym):
    """data.go.kr 주택건설실적 API (착공/준공)"""
    print("  data.go.kr 주택건설실적 API 시도 중...")

    # 착공/준공 각각 수집
    endpoints = {
        "착공": "http://apis.data.go.kr/1613000/HWSMS/getHouseStartInfo",
        "준공": "http://apis.data.go.kr/1613000/HWSMS/getHouseComplInfo",
    }

    start_year = int(start_ym[:4])
    start_month = int(start_ym[4:6])
    end_year = int(end_ym[:4])
    end_month = int(end_ym[4:6])

    all_rows = []

    for category, base_url in endpoints.items():
        print(f"  [{category}] 수집 중...")
        year, month = start_year, start_month
        month_count = 0

        while (year < end_year) or (year == end_year and month <= end_month):
            ym_str = f"{year}{month:02d}"
            params = {
                "serviceKey": DATA_GO_KR_KEY,
                "numOfRows": "1000",
                "pageNo": "1",
                "pblntfYm": ym_str,
            }

            try:
                resp = _api_get(base_url, params=params)
                root = ET.fromstring(resp.content)
                items = root.findall(".//item")

                for item in items:
                    sido = item.findtext("ctprvnNm", "").strip()
                    # 호수 필드명은 API에 따라 다를 수 있음
                    cnt_str = (
                        item.findtext("hseCnt", "0").strip()
                        or item.findtext("hoCnt", "0").strip()
                        or item.findtext("totHoCnt", "0").strip()
                    )

                    try:
                        cnt = int(cnt_str.replace(",", ""))
                    except ValueError:
                        cnt = 0

                    all_rows.append({
                        "연월": f"{year}-{month:02d}",
                        "시도_raw": sido,
                        "구분": category,
                        "호수": cnt,
                    })

            except Exception:
                pass

            month_count += 1
            month += 1
            if month > 12:
                month = 1
                year += 1

            time.sleep(0.3)

            # 진행률 (12개월마다)
            if month_count % 12 == 0:
                print(f"    [{category}] {month_count}개월 처리")

        print(f"    [{category}] 총 {month_count}개월 요청 완료")

    if not all_rows:
        print("  data.go.kr에서도 데이터 없음")
        return None

    df = pd.DataFrame(all_rows)
    df["시도"] = df["시도_raw"].apply(_normalize_sido)
    df = df.dropna(subset=["시도"])

    # 착공/준공 피벗
    pivot = df.pivot_table(
        index=["연월", "시도"],
        columns="구분",
        values="호수",
        aggfunc="sum",
    ).reset_index()
    pivot.columns.name = None

    result = pivot.rename(columns={
        "착공": "착공_호수",
        "준공": "준공_호수",
    })

    if "착공_호수" not in result.columns:
        result["착공_호수"] = np.nan
    if "준공_호수" not in result.columns:
        result["준공_호수"] = np.nan

    result["연도"] = result["연월"].str[:4].astype(int)
    result["월"] = result["연월"].str[5:7].astype(int)
    result = result[["연월", "시도", "착공_호수", "준공_호수", "연도", "월"]]
    result = result.sort_values(["연월", "시도"]).reset_index(drop=True)

    # 저장
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "construction_pipeline_sido_monthly.csv")
    result.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  저장: {out_path}")
    print(f"    행 수: {len(result):,}, 시도 수: {result['시도'].nunique()}")

    return result


# ═══════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="공공데이터 API 수집: 미분양, 인구이동, 금리, 전월세전환율, 가격지수, 주택건설실적, 지가변동률"
    )
    parser.add_argument("--unsold", action="store_true", help="미분양주택만")
    parser.add_argument("--migration", action="store_true", help="인구이동만")
    parser.add_argument("--rate", action="store_true", help="기준금리만")
    parser.add_argument("--jeonwolse", action="store_true", help="전월세전환율만")
    parser.add_argument("--price-index", action="store_true", help="주택가격지수만")
    parser.add_argument("--land-price", action="store_true", help="지가변동률만")
    parser.add_argument("--construction", action="store_true", help="주택건설실적(착공/준공)만")
    args = parser.parse_args()

    run_all = not (args.unsold or args.migration or args.rate
                   or args.jeonwolse or args.price_index
                   or args.land_price or args.construction)

    print(f"\n데이터 출력 디렉토리: {OUTPUT_DIR}\n")

    results = {}

    if run_all or args.unsold:
        results["unsold"] = fetch_unsold_housing()

    if run_all or args.migration:
        results["migration"] = fetch_population_migration()

    if run_all or args.rate:
        results["rate"] = fetch_base_rate()

    if run_all or args.jeonwolse:
        results["jeonwolse"] = fetch_jeonwolse_rate()

    if run_all or args.price_index:
        results["price_index"] = fetch_housing_price_index()

    if run_all or args.land_price:
        results["land_price"] = fetch_land_price_change()

    if run_all or args.construction:
        results["construction"] = fetch_construction_pipeline()

    print("\n" + "=" * 60)
    print("완료 요약:")
    for key, val in results.items():
        if val is not None:
            status = f"{len(val):,}행"
        else:
            status = "실패/없음"
        print(f"  {key}: {status}")
    print("=" * 60)


if __name__ == "__main__":
    main()

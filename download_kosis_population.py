"""
KOSIS 시도/시군구별 5세별 주민등록인구 수집 스크립트
테이블: DT_1B04005N (행정구역/5세별 주민등록인구, 2011년~)

수집 항목:
  - 총인구, 남자인구, 여자인구
  - 5세 단위 연령대별 (0~4세, 5~9세, ..., 100세+)
  - → 10년 단위 집계: 20대, 30대, 40대, 50대이상

사용법:
    python download_kosis_population.py                     # 시도 전체 (2011~최신)
    python download_kosis_population.py --test              # 시도 2022~2023만 (빠른 확인)
    python download_kosis_population.py --sigungu           # 시군구 전체 (2011~최신)
    python download_kosis_population.py --sigungu --test    # 시군구 2022~2023만

출력 파일:
    cache/kosis_population_age_sido_yearly.csv      (--sido, 기본)
    cache/kosis_population_age_sigungu_yearly.csv   (--sigungu)

KOSIS 파라미터:
    orgId=101, tblId=DT_1B04005N
    C1=행정구역코드(시도 2자리 / 시군구 5자리), C2=5세연령코드
    itmId: T2=총인구, T3=남자, T4=여자

셀 수 제한 (40,000):
    시도:   17 × 22 × 3 × 10년 = 11,220  → 10년 청크
    시군구: 250 × 22 × 3 × 2년 = 33,000  →  2년 청크
"""

import os
import sys
import json
import time
import argparse
import subprocess
import pandas as pd
from dotenv import load_dotenv

# data_loader의 시군구 코드 맵 사용
sys.path.insert(0, os.path.dirname(__file__))
from data_loader import SIGUNGU_NAME_MAP

load_dotenv()

# ═══════════════════════════════════════════════════════
# 상수
# ═══════════════════════════════════════════════════════

KOSIS_API_URL = "https://kosis.kr/openapi/Param/statisticsParameterData.do"
CACHE_DIR     = os.path.join(os.path.dirname(__file__), "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

ORG_ID = "101"
TBL_ID = "DT_1B04005N"   # 행정구역/5세별 주민등록인구 (2011~)

# 17개 시도 코드 (KOSIS C1 코드)
SIDO_CODES = "11+21+22+23+24+25+26+29+31+32+33+34+35+36+37+38+39"

SIDO_NAME = {
    "11": "서울", "21": "부산", "22": "대구", "23": "인천",
    "24": "광주", "25": "대전", "26": "울산", "29": "세종",
    "31": "경기", "32": "강원", "33": "충북", "34": "충남",
    "35": "전북", "36": "전남", "37": "경북", "38": "경남",
    "39": "제주",
}

# C2: 5세 연령대 코드 (0=전체합계 + 5세구간 전체)
# 0(합), 5(0~4), 10(5~9), 15(10~14), 20(15~19), 25(20~24), 30(25~29),
# 35(30~34), 40(35~39), 45(40~44), 50(45~49), 55(50~54), 60(55~59),
# 65(60~64), 70(65~69), 75(70~74), 80(75~79), 85(80~84), 90(85~89),
# 95(90~94), 100(95~99), 105(100+)
AGE_CODES = "0+5+10+15+20+25+30+35+40+45+50+55+60+65+70+75+80+85+90+95+100+105"

# 5세 코드 → 연령대명 매핑
AGE_CODE_TO_LABEL = {
    "0":   "합계",
    "5":   "0~4세",   "10":  "5~9세",
    "15":  "10~14세", "20":  "15~19세",
    "25":  "20~24세", "30":  "25~29세",
    "35":  "30~34세", "40":  "35~39세",
    "45":  "40~44세", "50":  "45~49세",
    "55":  "50~54세", "60":  "55~59세",
    "65":  "60~64세", "70":  "65~69세",
    "75":  "70~74세", "80":  "75~79세",
    "85":  "80~84세", "90":  "85~89세",
    "95":  "90~94세", "100": "95~99세", "105": "100세이상",
}

# 10년 단위 집계에 사용할 5세 코드 목록
AGE_DECADE_MAP = {
    "20대":    ["25", "30"],           # 20~24 + 25~29
    "30대":    ["35", "40"],           # 30~34 + 35~39
    "40대":    ["45", "50"],           # 40~44 + 45~49
    "50대이상": ["55","60","65","70","75","80","85","90","95","100","105"],
}

# 수집 연도 범위 (KOSIS 제공: 2011~)
DEFAULT_START = 2011
DEFAULT_END   = 2024

# 1회 요청당 허용 셀 수: 40,000
# 시도:   17 × 22 × 3 × n년 = 1,122 × n  → 10년 청크
# 시군구: 250 × 22 × 3 × n년 = 16,500 × n → 2년 청크
MAX_YEAR_CHUNK_SIDO    = 10
MAX_YEAR_CHUNK_SIGUNGU =  2

# 시군구 코드: data_loader.SIGUNGU_NAME_MAP의 5자리 코드 전체
# "+" 구분자로 연결 (KOSIS objL1 파라미터)
SIGUNGU_CODES = "+".join(sorted(SIGUNGU_NAME_MAP.keys()))


# ═══════════════════════════════════════════════════════
# API 호출 (PowerShell 폴백 포함)
# ═══════════════════════════════════════════════════════

def _api_call_ps(params: dict, timeout: int = 60) -> list | None:
    """PowerShell을 통한 KOSIS API 호출 (WSL 네트워크 우회)"""
    query = "&".join(f"{k}={v}" for k, v in params.items())
    url   = f"{KOSIS_API_URL}?{query}"
    ps    = (
        f"$r = Invoke-RestMethod -Uri '{url}' -TimeoutSec {timeout}; "
        f"$r | ConvertTo-Json -Depth 5 -Compress"
    )
    try:
        result = subprocess.run(
            ["powershell.exe", "-Command", ps],
            capture_output=True, text=True,
            timeout=timeout + 30, encoding="utf-8", errors="replace",
        )
        raw = result.stdout.strip().lstrip("\ufeff")
        if not raw:
            raise ValueError(f"빈 응답. stderr: {result.stderr[:200]}")
        data = json.loads(raw)
        if isinstance(data, dict) and "err" in data:
            raise ValueError(f"KOSIS 오류: err={data['err']} {data.get('errMsg','')}")
        return data
    except subprocess.TimeoutExpired:
        raise TimeoutError("PowerShell 타임아웃")


def _fetch_chunk(start_yr: int, end_yr: int, region_codes: str = SIDO_CODES) -> list:
    """연도 범위와 지역코드를 지정하여 API 1회 호출"""
    params = {
        "method":     "getList",
        "apiKey":     os.getenv("KOSIS_API_KEY", ""),
        "orgId":      ORG_ID,
        "tblId":      TBL_ID,
        "itmId":      "T2+T3+T4",
        "objL1":      region_codes,
        "objL2":      AGE_CODES,
        "format":     "json",
        "jsonVD":     "Y",
        "prdSe":      "Y",
        "startPrdDe": str(start_yr),
        "endPrdDe":   str(end_yr),
    }
    return _api_call_ps(params)


# ═══════════════════════════════════════════════════════
# 데이터 수집
# ═══════════════════════════════════════════════════════

def fetch_all(start_year: int = DEFAULT_START, end_year: int = DEFAULT_END) -> pd.DataFrame:
    """시도 단위 전체 기간 수집 (10년 청크)"""
    all_rows = []
    years = list(range(start_year, end_year + 1))

    for i in range(0, len(years), MAX_YEAR_CHUNK_SIDO):
        chunk_years = years[i : i + MAX_YEAR_CHUNK_SIDO]
        yr_s, yr_e  = chunk_years[0], chunk_years[-1]
        print(f"  요청(시도): {yr_s}~{yr_e}", end=" ... ", flush=True)
        try:
            data = _fetch_chunk(yr_s, yr_e, SIDO_CODES)
            print(f"{len(data)}행")
            all_rows.extend(data)
        except Exception as e:
            print(f"실패: {e}")
        time.sleep(0.5)

    if not all_rows:
        return pd.DataFrame()

    return _parse(all_rows, level="sido")


def fetch_all_sigungu(start_year: int = DEFAULT_START, end_year: int = DEFAULT_END) -> pd.DataFrame:
    """시군구 단위 전체 기간 수집 (2년 청크)"""
    all_rows = []
    years = list(range(start_year, end_year + 1))

    for i in range(0, len(years), MAX_YEAR_CHUNK_SIGUNGU):
        chunk_years = years[i : i + MAX_YEAR_CHUNK_SIGUNGU]
        yr_s, yr_e  = chunk_years[0], chunk_years[-1]
        print(f"  요청(시군구): {yr_s}~{yr_e}", end=" ... ", flush=True)
        try:
            data = _fetch_chunk(yr_s, yr_e, SIGUNGU_CODES)
            print(f"{len(data)}행")
            all_rows.extend(data)
        except Exception as e:
            print(f"실패: {e}")
        time.sleep(0.8)   # 시군구 응답이 크므로 약간 긴 딜레이

    if not all_rows:
        return pd.DataFrame()

    return _parse(all_rows, level="sigungu")


# ═══════════════════════════════════════════════════════
# 파싱 및 집계
# ═══════════════════════════════════════════════════════

def _parse(raw: list, level: str = "sido") -> pd.DataFrame:
    """
    원시 API 응답 → 분석용 DataFrame 변환
    level: "sido" (시도) | "sigungu" (시군구)
    """
    rows = []
    for r in raw:
        region_cd = r.get("C1", "")
        age_cd    = str(r.get("C2", ""))
        itm_id    = r.get("ITM_ID", "")
        year      = int(r.get("PRD_DE", 0))
        val       = _to_int(r.get("DT"))

        gender = {"T2": "합계", "T3": "남", "T4": "여"}.get(itm_id)
        if gender is None:
            continue

        age_label = AGE_CODE_TO_LABEL.get(age_cd, f"C2={age_cd}")

        if level == "sigungu":
            region_nm = SIGUNGU_NAME_MAP.get(region_cd, region_cd)
            row = {
                "시군구코드": region_cd,
                "시군구":     region_nm,
                "시도코드":   region_cd[:2],
                "시도":       SIDO_NAME.get(region_cd[:2], region_cd[:2]),
                "연도":       year,
                "연령코드":   age_cd,
                "연령대":     age_label,
                "성별":       gender,
                "인구":       val,
            }
        else:
            row = {
                "시도코드": region_cd,
                "시도":     SIDO_NAME.get(region_cd, region_cd),
                "연도":     year,
                "연령코드": age_cd,
                "연령대":   age_label,
                "성별":     gender,
                "인구":     val,
            }
        rows.append(row)

    df_raw = pd.DataFrame(rows)
    if df_raw.empty:
        return df_raw

    return _build_decade_pivot(df_raw, level=level)


def _build_decade_pivot(df_raw: pd.DataFrame, level: str = "sido") -> pd.DataFrame:
    """
    원시 데이터(지역/연도/연령/성별/인구) →
    분석 편의용 wide 형식:
      [지역코드, 지역명, 연도, 총인구, 남자인구, 여자인구,
       인구_20대, 인구_30대, 인구_40대, 인구_50대이상,
       남_20대, ..., 여_20대, ...]
    """
    if level == "sigungu":
        base_key = ["시군구코드", "시군구", "시도코드", "시도", "연도"]
        sort_key = ["시도", "시군구", "연도"]
    else:
        base_key = ["시도코드", "시도", "연도"]
        sort_key = ["시도", "연도"]

    def _sum_decade(df, decade_name, gender, codes):
        mask = (
            df["연령코드"].isin(codes) &
            (df["성별"] == gender)
        )
        return (
            df[mask]
            .groupby(base_key)["인구"]
            .sum()
            .rename(f"{'' if gender=='합계' else gender+'_'}{decade_name}")
        )

    # 전체 합계
    tot = (
        df_raw[(df_raw["연령코드"] == "0") & (df_raw["성별"] == "합계")]
        .set_index(base_key)["인구"].rename("총인구")
    )
    male = (
        df_raw[(df_raw["연령코드"] == "0") & (df_raw["성별"] == "남")]
        .set_index(base_key)["인구"].rename("남자인구")
    )
    female = (
        df_raw[(df_raw["연령코드"] == "0") & (df_raw["성별"] == "여")]
        .set_index(base_key)["인구"].rename("여자인구")
    )

    parts = [tot, male, female]

    # 10년 단위 합계 × 성별
    for decade, codes in AGE_DECADE_MAP.items():
        for gender in ["합계", "남", "여"]:
            parts.append(_sum_decade(df_raw, decade, gender, codes))

    result = pd.concat(parts, axis=1).reset_index()
    result = result.sort_values(sort_key).reset_index(drop=True)
    return result


# ═══════════════════════════════════════════════════════
# 저장
# ═══════════════════════════════════════════════════════

def save(df: pd.DataFrame, filename: str):
    path = os.path.join(CACHE_DIR, filename)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"  ✅ 저장: {path}  ({len(df)}행, {len(df.columns)}컬럼)")
    print(f"  컬럼: {df.columns.tolist()}")


# ═══════════════════════════════════════════════════════
# 유틸
# ═══════════════════════════════════════════════════════

def _to_int(v) -> int | None:
    try:
        return int(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


# ═══════════════════════════════════════════════════════
# 메인
# ═══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="KOSIS 시도/시군구별 5세별 주민등록인구 수집 (DT_1B04005N)"
    )
    parser.add_argument("--sigungu", action="store_true", help="시군구 단위 수집 (기본: 시도)")
    parser.add_argument("--test",    action="store_true", help="2022~2023만 빠른 테스트")
    parser.add_argument("--start",   type=int, default=DEFAULT_START, help=f"시작연도 (기본: {DEFAULT_START})")
    parser.add_argument("--end",     type=int, default=DEFAULT_END,   help=f"종료연도 (기본: {DEFAULT_END})")
    args = parser.parse_args()

    if args.test:
        start_yr, end_yr = 2022, 2023
    else:
        start_yr, end_yr = args.start, args.end

    if args.sigungu:
        level_label = "시군구"
        out_file    = "kosis_population_age_sigungu_yearly.csv"
        n_regions   = len(SIGUNGU_NAME_MAP)
    else:
        level_label = "시도"
        out_file    = "kosis_population_age_sido_yearly.csv"
        n_regions   = len(SIDO_NAME)

    print("=" * 60)
    print(f"KOSIS {level_label}별 성/연령별 주민등록인구 수집")
    print(f"테이블: {TBL_ID} | 기간: {start_yr}~{end_yr} | 지역 {n_regions}개")
    print("=" * 60)

    if args.sigungu:
        df = fetch_all_sigungu(start_yr, end_yr)
    else:
        df = fetch_all(start_yr, end_yr)

    if df.empty:
        print("  ✗ 데이터 없음")
        sys.exit(1)

    print(f"\n수집 완료: {len(df)}행")
    print(df.head(5).to_string())

    save(df, out_file)


if __name__ == "__main__":
    main()

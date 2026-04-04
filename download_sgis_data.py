"""
SGIS 통계지리정보서비스 데이터 수집 스크립트

수집 데이터:
  - population: 총인구, 세대수, 주택수, 평균연령, 노령화지수, 종사자수
  - household:  세대수, 평균 세대원수
  - house:      주택수

사용법:
    python download_sgis_data.py             # 전체 실행
    python download_sgis_data.py --test      # 서울만 테스트

출력 파일 (cache/ 디렉토리):
    sgis_population_sigungu_yearly.csv
    sgis_household_sigungu_yearly.csv
    sgis_house_sigungu_yearly.csv

API 인증:
    .env 파일에 SGIS_CONSUMER_KEY, SGIS_CONSUMER_SECRET 설정 필요
    발급: https://sgis.mods.go.kr/developer/
"""

import os
import sys
import time
import argparse
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ═══════════════════════════════════════════════════════
# 상수 / 경로
# ═══════════════════════════════════════════════════════

BASE_URL = "https://sgisapi.mods.go.kr/OpenAPI3"
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

CONSUMER_KEY    = os.getenv("SGIS_CONSUMER_KEY", "")
CONSUMER_SECRET = os.getenv("SGIS_CONSUMER_SECRET", "")

# 시도코드 → 시도명 매핑 (SGIS 2자리 코드)
SIDO_CODE_MAP = {
    "11": "서울",  "21": "부산",  "22": "대구",  "23": "인천",
    "24": "광주",  "25": "대전",  "26": "울산",  "29": "세종",
    "31": "경기",  "32": "강원",  "33": "충북",  "34": "충남",
    "35": "전북",  "36": "전남",  "37": "경북",  "38": "경남",
    "39": "제주",
}

# 수집 연도 범위
YEARS = list(range(2015, 2025))  # SGIS 제공: 2015~2024

# ═══════════════════════════════════════════════════════
# 인증
# ═══════════════════════════════════════════════════════

_access_token: str = ""
_token_issued_at: float = 0.0
TOKEN_TTL = 3600  # 1시간


def get_access_token(force: bool = False) -> str:
    """SGIS accessToken 발급 (TTL 내 캐싱)"""
    global _access_token, _token_issued_at

    if not CONSUMER_KEY or not CONSUMER_SECRET:
        raise EnvironmentError(
            ".env에 SGIS_CONSUMER_KEY와 SGIS_CONSUMER_SECRET을 설정해주세요.\n"
            "발급: https://sgis.mods.go.kr/developer/"
        )

    now = time.time()
    if not force and _access_token and (now - _token_issued_at) < TOKEN_TTL:
        return _access_token

    resp = requests.get(
        f"{BASE_URL}/auth/authentication.json",
        params={"consumer_key": CONSUMER_KEY, "consumer_secret": CONSUMER_SECRET},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()

    if data.get("errCd") != 0:
        raise RuntimeError(f"SGIS 인증 실패: {data.get('errMsg')}")

    _access_token = data["result"]["accessToken"]
    _token_issued_at = now
    return _access_token


# ═══════════════════════════════════════════════════════
# API 호출 헬퍼
# ═══════════════════════════════════════════════════════

def _call(endpoint: str, params: dict, retry: int = 2) -> list:
    """SGIS API 호출, 에러 시 재시도"""
    token = get_access_token()
    params = {"accessToken": token, **params}

    for attempt in range(retry + 1):
        try:
            resp = requests.get(
                f"{BASE_URL}/stats/{endpoint}.json",
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            # 토큰 만료 시 재발급 후 1회 재시도
            if data.get("errCd") == -401:
                token = get_access_token(force=True)
                params["accessToken"] = token
                continue

            if data.get("errCd") != 0:
                print(f"  ⚠ {endpoint} 오류: {data.get('errMsg')}")
                return []

            return data.get("result", []) or []

        except requests.RequestException as e:
            if attempt == retry:
                print(f"  ✗ {endpoint} 요청 실패: {e}")
                return []
            time.sleep(1)

    return []


# ═══════════════════════════════════════════════════════
# 수집 함수
# ═══════════════════════════════════════════════════════

def collect_population(sido_codes: list[str] | None = None, years: list[int] | None = None) -> pd.DataFrame:
    """
    SGIS population 통계 수집
    반환: [시도코드, 시도, 연도, 총인구, 세대수, 주택수, 평균연령, 노령화지수, 노년부양비, 유소년부양비, 종사자수]
    """
    sido_codes = sido_codes or list(SIDO_CODE_MAP.keys())
    years = years or YEARS
    rows = []

    for sido_cd in sido_codes:
        sido_nm = SIDO_CODE_MAP.get(sido_cd, sido_cd)
        for year in years:
            print(f"  수집중: population {sido_nm} {year}", end="\r")
            result = _call("population", {"year": year, "adm_cd": sido_cd, "low_search": 0})
            for r in result:
                rows.append({
                    "시도코드": sido_cd,
                    "시도":     sido_nm,
                    "연도":     year,
                    "총인구":   _to_int(r.get("tot_ppltn")),
                    "세대수":   _to_int(r.get("tot_family")),
                    "주택수":   _to_int(r.get("tot_house")),
                    "평균연령": _to_float(r.get("avg_age")),
                    "노령화지수":   _to_float(r.get("aged_child_idx")),
                    "노년부양비":   _to_float(r.get("oldage_suprt_per")),
                    "유소년부양비": _to_float(r.get("juv_suprt_per")),
                    "종사자수":     _to_int(r.get("employee_cnt")),
                    "사업체수":     _to_int(r.get("corp_cnt")),
                    "인구밀도":     _to_float(r.get("ppltn_dnsty")),
                })
            time.sleep(0.1)

    print()
    return pd.DataFrame(rows)


def collect_household(sido_codes: list[str] | None = None, years: list[int] | None = None) -> pd.DataFrame:
    """
    SGIS household 가구 통계 수집
    반환: [시도코드, 시도, 연도, 가구수, 평균가구원수]
    """
    sido_codes = sido_codes or list(SIDO_CODE_MAP.keys())
    years = years or YEARS
    rows = []

    for sido_cd in sido_codes:
        sido_nm = SIDO_CODE_MAP.get(sido_cd, sido_cd)
        for year in years:
            print(f"  수집중: household {sido_nm} {year}", end="\r")
            result = _call("household", {"year": year, "adm_cd": sido_cd, "low_search": 0})
            for r in result:
                rows.append({
                    "시도코드":   sido_cd,
                    "시도":       sido_nm,
                    "연도":       year,
                    "가구수":     _to_int(r.get("household_cnt")),
                    "평균가구원수": _to_float(r.get("avg_family_member_cnt")),
                })
            time.sleep(0.1)

    print()
    return pd.DataFrame(rows)


def collect_house(sido_codes: list[str] | None = None, years: list[int] | None = None) -> pd.DataFrame:
    """
    SGIS house 주택 통계 수집
    반환: [시도코드, 시도, 연도, 주택수]
    """
    sido_codes = sido_codes or list(SIDO_CODE_MAP.keys())
    years = years or YEARS
    rows = []

    for sido_cd in sido_codes:
        sido_nm = SIDO_CODE_MAP.get(sido_cd, sido_cd)
        for year in years:
            print(f"  수집중: house {sido_nm} {year}", end="\r")
            result = _call("house", {"year": year, "adm_cd": sido_cd, "low_search": 0})
            for r in result:
                rows.append({
                    "시도코드": sido_cd,
                    "시도":     sido_nm,
                    "연도":     year,
                    "주택수":   _to_int(r.get("house_cnt")),
                })
            time.sleep(0.1)

    print()
    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════
# 유틸
# ═══════════════════════════════════════════════════════

def _to_int(v) -> int | None:
    try:
        return int(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _to_float(v) -> float | None:
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def save(df: pd.DataFrame, filename: str):
    path = os.path.join(CACHE_DIR, filename)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"  ✅ 저장: {path}  ({len(df)}행)")


# ═══════════════════════════════════════════════════════
# 메인
# ═══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="SGIS 통계지리정보 데이터 수집")
    parser.add_argument("--population", action="store_true", help="인구 통계만")
    parser.add_argument("--household",  action="store_true", help="가구 통계만")
    parser.add_argument("--house",      action="store_true", help="주택 통계만")
    parser.add_argument("--test",       action="store_true", help="서울만 테스트 (2022-2023)")
    args = parser.parse_args()

    test_sido  = ["11"] if args.test else None
    test_years = [2022, 2023] if args.test else None

    run_all = not any([args.population, args.household, args.house])

    print("=" * 50)
    print("SGIS 데이터 수집 시작")
    print("=" * 50)

    if run_all or args.population:
        print("\n[1/3] 인구 통계 수집 중...")
        df = collect_population(test_sido, test_years)
        if not df.empty:
            save(df, "sgis_population_sido_yearly.csv")
            print(df.head(3).to_string())

    if run_all or args.household:
        print("\n[2/3] 가구 통계 수집 중...")
        df = collect_household(test_sido, test_years)
        if not df.empty:
            save(df, "sgis_household_sido_yearly.csv")
            print(df.head(3).to_string())

    if run_all or args.house:
        print("\n[3/3] 주택 통계 수집 중...")
        df = collect_house(test_sido, test_years)
        if not df.empty:
            save(df, "sgis_house_sido_yearly.csv")
            print(df.head(3).to_string())

    print("\n완료!")


if __name__ == "__main__":
    main()

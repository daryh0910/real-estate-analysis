"""
부동산 실거래 데이터 업데이트 스크립트
- 기간: 2024.02 ~ 2026.02
- 대상: 아파트 매매 (getRTMSDataSvcAptTradeDev) + 임대차 (getRTMSDataSvcAptRent)
- 중단/재개 지원: update_progress.json에 진행상황 저장
"""

import requests
import xml.etree.ElementTree as ET
import csv
import os
import sys
import json
import time
import glob as _glob

from dotenv import load_dotenv
load_dotenv()

# ============================================================
# 설정
# ============================================================

# API Key — 환경변수에서 로드 (URL-encoded 상태로 설정)
API_KEY = os.environ.get("MOLIT_API_KEY", "")

# 날짜 범위
START_YM = 202402
END_YM = 202602

# API 호출 간 sleep (초)
SLEEP_SEC = 0.3

# 진행상황 저장 간격 (호출 수)
SAVE_EVERY = 50

# ============================================================
# 경로 설정
# ============================================================

def _detect_onedrive():
    pattern = "/mnt/c/Users/*/OneDrive - (주)에스티/101. 신사업폴더백업/부동산Tradingview"
    matches = _glob.glob(pattern)
    if matches:
        return matches[0]
    raise FileNotFoundError(f"OneDrive 경로를 찾을 수 없습니다. 패턴: {pattern}")

BASE = _detect_onedrive()
CODING_DIR = os.path.join(BASE, "5. coding")

APT_CSV = os.path.join(CODING_DIR, "#1. 매매_combine/apt/combined_files.csv")
RENT_CSV = os.path.join(CODING_DIR, "#2. 임대차_combine/apt/combined_files.csv")
BJD_CODE_FILE = os.path.join(CODING_DIR, "법정동코드.csv")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(SCRIPT_DIR, "cache")
PROGRESS_FILE = os.path.join(SCRIPT_DIR, "update_progress.json")

# API 엔드포인트 — HTTPS (2024~ openapi.molit.go.kr HTTP 차단으로 전환)
# 우선순위: 1) apis.data.go.kr (공공데이터포털 게이트웨이)
#          2) openapi.molit.go.kr HTTPS (폴백)
TRADE_URLS = [
    "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev?",
    "https://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev?",
]
RENT_URLS = [
    "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent?",
    "https://openapi.molit.go.kr:443/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptRent?",
]
# 하위호환: 기존 코드에서 TRADE_URL/RENT_URL 참조하는 부분 대응
TRADE_URL = TRADE_URLS[0]
RENT_URL = RENT_URLS[0]

# CSV 컬럼 정의 (기존 combined_files.csv 컬럼 순서)
TRADE_COLUMNS = [
    "년", "월", "일", "지역코드", "법정동", "지번", "아파트", "단지",
    "연립다세대", "연면적", "전용면적", "대지면적", "대지권면적", "층",
    "건축년도", "거래금액", "주택유형", "구분", "해제여부", "해제사유발생일",
    "거래유형", "등기일자",
]

RENT_COLUMNS = [
    "년", "월", "일", "지역코드", "법정동", "지번", "아파트", "연립다세대",
    "단지", "계약면적", "전용면적", "층", "건축년도", "보증금액", "월세금액",
    "보증금", "월세", "종전계약보증금", "종전계약월세", "갱신요구권사용",
    "계약기간", "계약구분",
]

# ============================================================
# 유틸리티
# ============================================================

def load_region_codes():
    """법정동코드.csv에서 5자리 시군구 코드 추출 (기존 코드 패턴)"""
    import csv as _csv

    codes = set()
    with open(BJD_CODE_FILE, encoding="cp949") as f:
        reader = _csv.reader(f)
        header = next(reader)  # skip header
        for row in reader:
            if len(row) < 3:
                continue
            if row[2].strip() != "존재":
                continue
            full_code = row[0].strip()
            if len(full_code) >= 5:
                five = str(int(full_code) // 100000)
                codes.add(five)

    codes = sorted(codes)
    print(f"[INFO] 시군구 코드 {len(codes)}개 로드")
    return codes


def generate_date_range(start_ym, end_ym):
    """YYYYMM 범위 생성"""
    dates = []
    y, m = start_ym // 100, start_ym % 100
    ey, em = end_ym // 100, end_ym % 100
    while (y, m) <= (ey, em):
        dates.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    print(f"[INFO] 날짜 범위: {dates[0]} ~ {dates[-1]} ({len(dates)}개월)")
    return dates


def load_progress():
    """진행상황 로드"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"trade_done": [], "rent_done": []}


def save_progress(progress):
    """진행상황 저장"""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, ensure_ascii=False)


# ============================================================
# API 호출 / XML 파싱
# ============================================================

def call_api(url_base, region_code, deal_ymd):
    """API 호출 — 여러 엔드포인트를 순차 시도 (HTTPS 우선)"""
    payload = (
        f"LAWD_CD={region_code}"
        f"&DEAL_YMD={deal_ymd}"
        f"&serviceKey={API_KEY}"
        f"&numOfRows=9999"
    )

    # url_base가 단일 URL이면 리스트로 변환
    if isinstance(url_base, str):
        # TRADE_URLS / RENT_URLS에서 매칭되는 폴백 리스트 찾기
        if "Trade" in url_base or "trade" in url_base:
            urls = TRADE_URLS
        elif "Rent" in url_base or "rent" in url_base:
            urls = RENT_URLS
        else:
            urls = [url_base]
    else:
        urls = url_base

    last_err = None
    for url in urls:
        try:
            full_url = url + payload
            resp = requests.get(full_url, timeout=30)
            resp.raise_for_status()
            return resp
        except (requests.ConnectionError, requests.Timeout) as e:
            last_err = e
            continue
        except requests.HTTPError as e:
            # 4xx/5xx는 다음 URL 시도
            last_err = e
            continue

    # 모든 URL 실패
    raise last_err


def parse_xml(response):
    """XML 응답 파싱 → dict 리스트"""
    root = ET.fromstring(response.content)

    # 에러 체크 — data.go.kr은 "000"(3자리), molit.go.kr은 "00"(2자리)
    result_code = root.findtext(".//resultCode")
    if result_code:
        code_stripped = result_code.strip().lstrip("0") or "0"
        if code_stripped != "0":
            result_msg = root.findtext(".//resultMsg", "")
            print(f"  [API ERROR] code={result_code} msg={result_msg}")
            return None  # 에러 시 None 반환

    # body/items 탐색 — data.go.kr과 molit.go.kr 모두 대응
    body = root.find("body")
    if body is None:
        body = root.find(".//body")  # 깊은 탐색 폴백
    if body is None:
        return []

    items = body.find("items")
    if items is None:
        items = body.find(".//items")
    if items is None:
        return []

    item_list = []
    for child in items:
        data = {}
        for element in child.findall("*"):
            tag = element.tag.strip()
            text = element.text.strip() if element.text is not None else ""
            data[tag] = text
        item_list.append(data)
    return item_list


# ============================================================
# 행 변환
# ============================================================

def map_trade_row(api_row):
    """API 응답 dict → 매매 CSV 행 (컬럼 순서 맞춤)"""
    row = {}
    for col in TRADE_COLUMNS:
        row[col] = api_row.get(col, "")
    return row


def map_rent_row(api_row):
    """API 응답 dict → 임대차 CSV 행 (컬럼 순서 맞춤 + 컬럼명 변경 대응)"""
    row = {}
    for col in RENT_COLUMNS:
        row[col] = api_row.get(col, "")

    # API가 '보증금'으로 반환하는 경우 → '보증금액'에도 복사
    if not row["보증금액"] and api_row.get("보증금", ""):
        row["보증금액"] = api_row["보증금"]
    # API가 '보증금액'으로 반환하는 경우 → '보증금'에도 복사
    if not row["보증금"] and row["보증금액"]:
        row["보증금"] = row["보증금액"]

    # 월세도 동일 처리
    if not row["월세금액"] and api_row.get("월세", ""):
        row["월세금액"] = api_row["월세"]
    if not row["월세"] and row["월세금액"]:
        row["월세"] = row["월세금액"]

    return row


# ============================================================
# 다운로드 루프
# ============================================================

def download_data(data_type, region_codes, date_range, progress):
    """
    data_type: 'trade' 또는 'rent'
    region_codes: 5자리 코드 리스트
    date_range: YYYYMM 문자열 리스트
    progress: 진행상황 dict
    """
    if data_type == "trade":
        url_base = TRADE_URL
        csv_path = APT_CSV
        columns = TRADE_COLUMNS
        map_fn = map_trade_row
        done_key = "trade_done"
    else:
        url_base = RENT_URL
        csv_path = RENT_CSV
        columns = RENT_COLUMNS
        map_fn = map_rent_row
        done_key = "rent_done"

    done_set = set(tuple(x) for x in progress[done_key])
    total_pairs = len(region_codes) * len(date_range)
    skip_count = sum(1 for rc in region_codes for d in date_range if (rc, d) in done_set)
    remaining = total_pairs - skip_count

    print(f"\n{'='*60}")
    print(f"[{data_type.upper()}] 다운로드 시작")
    print(f"  총 {total_pairs} 호출 중 {skip_count} 완료, {remaining} 남음")
    print(f"  대상 CSV: {csv_path}")
    print(f"{'='*60}")

    if remaining == 0:
        print(f"[{data_type.upper()}] 이미 모두 완료됨. 건너뜀.")
        return

    # CSV 파일 열기 (append 모드)
    csv_file = open(csv_path, "a", newline="\n", encoding="cp949")
    writer = csv.DictWriter(csv_file, fieldnames=columns, extrasaction="ignore")

    call_count = 0
    new_rows_total = 0
    errors = 0

    try:
        for code in region_codes:
            for ym in date_range:
                if (code, ym) in done_set:
                    continue

                call_count += 1
                sys.stdout.write(
                    f"\r  [{data_type}] {call_count}/{remaining} | "
                    f"code={code} date={ym} | 누적 {new_rows_total}건"
                )
                sys.stdout.flush()

                try:
                    resp = call_api(url_base, code, ym)
                    items = parse_xml(resp)

                    if items is None:
                        # API 에러 (호출 제한 등)
                        errors += 1
                        if errors >= 3:
                            print(f"\n[WARN] 연속 에러 {errors}회. 진행상황 저장 후 중단.")
                            save_progress(progress)
                            csv_file.flush()
                            print(f"[INFO] 다음 실행 시 이어서 진행됩니다.")
                            return
                        time.sleep(2)
                        continue

                    errors = 0  # 성공 시 에러 카운트 리셋

                    if items:
                        for item in items:
                            row = map_fn(item)
                            writer.writerow(row)
                        new_rows_total += len(items)

                    # 완료 기록
                    done_set.add((code, ym))
                    progress[done_key].append([code, ym])

                    # 주기적 저장
                    if call_count % SAVE_EVERY == 0:
                        save_progress(progress)
                        csv_file.flush()

                    time.sleep(SLEEP_SEC)

                except requests.exceptions.RequestException as e:
                    print(f"\n  [NET ERROR] code={code} date={ym}: {e}")
                    errors += 1
                    if errors >= 5:
                        print(f"\n[WARN] 네트워크 에러 {errors}회. 진행상황 저장 후 중단.")
                        save_progress(progress)
                        csv_file.flush()
                        return
                    time.sleep(3)
                    continue

    finally:
        csv_file.close()
        save_progress(progress)

    print(f"\n[{data_type.upper()}] 완료: {new_rows_total}건 추가")


# ============================================================
# 캐시 삭제
# ============================================================

def delete_cache():
    """parquet 캐시 파일 삭제"""
    cache_files = [
        os.path.join(CACHE_DIR, "apt_sigungu_monthly.parquet"),
        os.path.join(CACHE_DIR, "jeonse_sigungu_monthly.parquet"),
        os.path.join(CACHE_DIR, "wolse_sigungu_monthly.parquet"),
        os.path.join(CACHE_DIR, "rent_all_sigungu_monthly.parquet"),
    ]
    deleted = 0
    for f in cache_files:
        if os.path.exists(f):
            os.remove(f)
            deleted += 1
            print(f"  삭제: {f}")
    print(f"[CACHE] {deleted}개 파일 삭제 완료")


# ============================================================
# 메인
# ============================================================

def main():
    print("=" * 60)
    print(" 부동산 실거래 데이터 업데이트")
    print(f" 기간: {START_YM} ~ {END_YM}")
    print("=" * 60)

    # 1. 지역코드 로드
    region_codes = load_region_codes()

    # 2. 날짜 범위 생성
    date_range = generate_date_range(START_YM, END_YM)

    # 3. 진행상황 로드
    progress = load_progress()

    # 4. 매매 데이터 다운로드
    download_data("trade", region_codes, date_range, progress)

    # 5. 임대차 데이터 다운로드
    download_data("rent", region_codes, date_range, progress)

    # 6. parquet 캐시 삭제
    print(f"\n[CACHE] 캐시 파일 삭제 중...")
    delete_cache()

    # 7. 진행상황 파일 정리 (완전 완료 시)
    total_trade = len(region_codes) * len(date_range)
    total_rent = total_trade
    done_trade = len(set(tuple(x) for x in progress.get("trade_done", [])))
    done_rent = len(set(tuple(x) for x in progress.get("rent_done", [])))

    print(f"\n{'='*60}")
    print(f" 결과 요약")
    print(f"  매매: {done_trade}/{total_trade} 완료")
    print(f"  임대차: {done_rent}/{total_rent} 완료")

    if done_trade >= total_trade and done_rent >= total_rent:
        print(f"\n  모든 데이터 업데이트 완료!")
        print(f"  진행상황 파일 삭제: {PROGRESS_FILE}")
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
    else:
        print(f"\n  미완료 항목이 있습니다. 스크립트를 다시 실행하면 이어서 진행됩니다.")

    print(f"{'='*60}")


if __name__ == "__main__":
    main()

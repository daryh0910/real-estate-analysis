# D7. 주소/지역코드 정규화 및 Geocoding 전략

> 작성일: 2026-05-12  
> 목적: 입주예정 단지 주소를 `시도/시군구/지역코드/좌표`로 정규화하여 ASIL형 입주·공급맵 v1에 사용

---

## 1. 핵심 결론

입주물량 보강에서 주소 정규화는 P0임. 단지명·입주예정월·세대수를 확보해도 주소가 시군구/좌표로 안정적으로 매핑되지 않으면 지도, 지역 집계, Superchart 연결이 불가능함.

권장 전략은 다음 3단계임.

1. **1차: 주소 문자열 파싱**
   - 주소에서 시도/시군구/읍면동을 우선 추출
   - `data_loader.py`의 `SIDO_SHORT`, `SIGUNGU_NAME_MAP`, `_normalize_sido` 관행과 맞춤

2. **2차: 법정동코드 매핑**
   - 행정표준코드/법정동코드 파일 또는 API를 별도 마스터로 유지
   - 주소의 시도/시군구/읍면동을 법정동코드 10자리 또는 시군구코드 5자리로 매핑

3. **3차: Geocoding**
   - 좌표가 필요한 경우만 VWorld/카카오/네이버/SGIS 중 하나를 호출
   - 운영 초기에는 좌표 없는 데이터도 시군구 집계에는 허용
   - 좌표 실패는 error가 아니라 warn으로 처리

---

## 2. 표준 컬럼

`movein_plan_complex_monthly.csv`에 아래 컬럼을 둔다.

| 컬럼 | 설명 | 필수도 |
|---|---|---|
| 주소 | 원천 주소 | P0 |
| 주소_정규화 | 공백/괄호/특수문자 정리 주소 | P0 |
| 시도 | 짧은 시도명: 서울/경기 등 | P0 |
| 시군구 | 시군구명 | P0 |
| 읍면동 | 가능하면 법정동/행정동 | P1 |
| 지역코드 | 시군구 5자리 코드 | P0 |
| 법정동코드 | 10자리 법정동코드 | P1 |
| 위도 | WGS84 위도 | P1 |
| 경도 | WGS84 경도 | P1 |
| geocode_source | vworld/kakao/naver/sgis/manual 등 | P1 |
| geocode_status | ok/warn/error/not_attempted | P1 |
| geocode_message | 실패 사유/매칭 신뢰도 | P1 |
| region_match_score | 0~100 매칭 점수 | P1 |

---

## 3. 코드 설계

신규 모듈 후보:

```text
services/region_normalization_service.py
```

핵심 함수:

```python
normalize_address_text(address: str) -> str
extract_region_from_address(address: str) -> dict
normalize_sido_name(sido: str) -> str
lookup_sigungu_code(sido: str, sigungu: str) -> str
lookup_bjd_code(sido: str, sigungu: str, eupmyeondong: str) -> str
geocode_address(address: str, provider: str = "vworld") -> dict
normalize_complex_region(row: dict) -> dict
validate_region_mapping(row: dict) -> tuple[str, str]
```

---

## 4. 주소 파싱 규칙

1. 공백, 괄호, 특수문자 정리
2. 도로명주소와 지번주소를 모두 허용
3. 시도명 정규화
   - 서울특별시 → 서울
   - 경기도 → 경기
   - 강원특별자치도/강원도 → 강원
   - 전북특별자치도/전라북도 → 전북
4. 시군구명 정규화
   - `수원시 영통구`처럼 복합 시군구는 프로젝트 기존 `SIGUNGU_NAME_MAP` 표기와 맞춤
   - 예: 수원영통구, 성남분당구 등
5. 주소에서 시도/시군구를 추출하지 못하면 원천 컬럼 `시도`, `시군구`를 우선 사용

---

## 5. Geocoding provider 후보

| Provider | 장점 | 한계 | 권장도 |
|---|---|---|---|
| VWorld | 공공 성격, 주소/좌표 변환 적합 | API key 필요, 사용량 제한 확인 필요 | P0 |
| 카카오 Local API | 주소 검색 품질 좋음 | 상업/재배포 조건 확인 필요 | P1 |
| 네이버 Geocoding | 품질 좋음 | key 필요, 정책 확인 필요 | P1 |
| SGIS | 통계지리와 연계 가능 | 인증/좌표계/쿼터 관리 필요 | P1 |
| 수동 좌표 입력 | 소량 검수에 정확 | 대량 자동화 불가 | P2 |

초기 권장:

- 지역 집계만 할 때는 geocoding 없이 시군구코드만 사용
- 지도 marker가 필요할 때 VWorld를 1순위로 사용
- 실패 건은 수동 검수 CSV로 분리

---

## 6. Validation rule

### error

- 주소/시도/시군구가 모두 없음
- 시도 정규화 실패
- 시군구코드 5자리 매핑 실패
- 동일 단지인데 시군구가 상충

### warn

- 법정동코드 없음
- 위도/경도 없음
- geocoding 실패
- 주소는 있으나 시군구와 주소 파싱 결과 불일치
- 좌표가 해당 시군구 bounding box 밖에 있음

### ok

- 시도/시군구/지역코드 매핑 성공
- 좌표가 있으면 WGS84 범위 정상

---

## 7. Python 코드 초안

```python
import re
import os
import requests
import pandas as pd

SIDO_ALIASES = {
    "서울특별시": "서울", "서울시": "서울", "서울": "서울",
    "경기도": "경기", "경기": "경기",
    "인천광역시": "인천", "인천": "인천",
    "부산광역시": "부산", "부산": "부산",
    "대구광역시": "대구", "대구": "대구",
    "광주광역시": "광주", "광주": "광주",
    "대전광역시": "대전", "대전": "대전",
    "울산광역시": "울산", "울산": "울산",
    "세종특별자치시": "세종", "세종시": "세종", "세종": "세종",
    "강원특별자치도": "강원", "강원도": "강원", "강원": "강원",
    "충청북도": "충북", "충북": "충북",
    "충청남도": "충남", "충남": "충남",
    "전북특별자치도": "전북", "전라북도": "전북", "전북": "전북",
    "전라남도": "전남", "전남": "전남",
    "경상북도": "경북", "경북": "경북",
    "경상남도": "경남", "경남": "경남",
    "제주특별자치도": "제주", "제주도": "제주", "제주": "제주",
}

SIDO_PATTERN = re.compile("|".join(sorted(map(re.escape, SIDO_ALIASES), key=len, reverse=True)))


def normalize_address_text(address):
    if pd.isna(address):
        return ""
    s = str(address).strip()
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"[,;]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def normalize_sido_name(sido):
    if not sido:
        return ""
    s = str(sido).strip()
    return SIDO_ALIASES.get(s, s)


def extract_region_from_address(address):
    address = normalize_address_text(address)
    result = {"시도": "", "시군구": "", "읍면동": ""}
    if not address:
        return result

    parts = address.split()
    if not parts:
        return result

    # 시도
    m = SIDO_PATTERN.search(address)
    if m:
        result["시도"] = normalize_sido_name(m.group(0))

    # 시군구: 단순 초안. 실제 구현에서는 법정동코드 마스터와 결합 권장.
    for i, p in enumerate(parts):
        if p.endswith(("시", "군", "구")):
            if i + 1 < len(parts) and parts[i + 1].endswith("구") and p.endswith("시"):
                result["시군구"] = p + " " + parts[i + 1]
            else:
                result["시군구"] = p
            break

    for p in parts:
        if p.endswith(("동", "읍", "면", "리")):
            result["읍면동"] = p
            break
    return result


def lookup_sigungu_code(region_master, sido, sigungu):
    if not sido or not sigungu:
        return ""
    m = region_master[
        (region_master["시도"] == sido) &
        (region_master["시군구"].astype(str).str.replace(" ", "") == str(sigungu).replace(" ", ""))
    ]
    if m.empty:
        return ""
    return str(m.iloc[0]["지역코드"]).zfill(5)


def geocode_vworld(address, api_key=None):
    api_key = api_key or os.environ.get("VWORLD_API_KEY")
    if not api_key:
        return {"status": "not_attempted", "message": "VWORLD_API_KEY 없음"}
    url = "https://api.vworld.kr/req/address"
    params = {
        "service": "address",
        "request": "getcoord",
        "version": "2.0",
        "crs": "epsg:4326",
        "address": address,
        "refine": "true",
        "simple": "false",
        "format": "json",
        "type": "road",
        "key": api_key,
    }
    r = requests.get(url, params=params, timeout=10)
    data = r.json()
    if data.get("response", {}).get("status") != "OK":
        return {"status": "warn", "message": "geocode 실패", "raw": data}
    point = data["response"]["result"]["point"]
    return {
        "status": "ok",
        "위도": float(point["y"]),
        "경도": float(point["x"]),
        "source": "vworld",
        "message": "",
    }
```

---

## 8. 권장 구현 순서

1. `data/region/법정동코드_마스터.csv` 또는 유사 마스터 확보
2. `services/region_normalization_service.py` 작성
3. 사용자 입주예정 파일 ingest 시 주소/지역코드 정규화 먼저 적용
4. geocoding은 별도 command로 분리
5. 실패 건은 `data/supply/validation/geocode_failed_YYYYMMDD.csv`로 저장
6. 수동 보정 후 재반영

---

## 9. 결론

입주물량 보강의 핵심은 데이터 원천 확보와 동일하게 주소/지역코드 정규화임. 초기에는 좌표보다 시군구코드 매핑을 우선해야 함. 좌표는 지도 marker용 후속 보강으로 두고, ASIL형 v1의 최소 성공 기준은 `단지명 + 입주예정연월 + 세대수 + 시군구코드`로 잡는 것이 현실적임.

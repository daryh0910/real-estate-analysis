# D5 KOSIS/국토부 공식 공급집계 보강 설계

작성일: 2026-05-12
범위: 웹 조회 없이 프로젝트 파일과 일반 지식 기준으로 실행 가능한 설계만 정리
참고 파일: `data/construction_pipeline_sido_monthly.csv`, `docs/supply_data_source_design.md`, `download_public_data.py`, `update_all.py`, `data_loader.py`

## 1. 결론

현재 `construction_pipeline_sido_monthly.csv`는 KOSIS 기반 시도·월별 착공/준공 집계로 가격 분석의 공급압력 변수로는 사용 가능하다. 그러나 공급 데이터셋으로는 범위가 좁다. v0에서는 기존 착공/준공에 `인허가`, `분양승인`, `미분양`을 시도 단위로 붙여 공식 공급 대시보드의 골격을 만든다. v1에서는 KOSIS/국토부 원천을 시군구 단위까지 확장하고, 단지 단위 입주예정은 청약홈·LH·사용자 보유 파일 등 별도 원천으로 분리한다.

핵심 방향은 다음과 같다.

| 구분 | v0 역할 | v1 역할 |
|---|---|---|
| 착공 | 공급 파이프라인 선행지표 | 시군구별 착공 압력, 준공 예상 선행 변수 |
| 준공 | 실제 공급 반영지표 | 가격·전세·미분양 반응 검증 기준 |
| 인허가 | 2~4년 선행 공급 의향 | 공급 과잉/부족 사이클의 최선행 변수 |
| 분양승인/분양실적 | 분양시장 공급 확정 흐름 | 청약홈 단지 자료의 커버리지 검증 |
| 미분양 | 공급-수요 불균형 결과지표 | 가격 하방 리스크와 지역 재고 압력 지표 |
| 주택건설실적 전체 | 공식 통계 백본 | 모든 단지 원천의 지역 합계 검증 기준 |

## 2. 현재 construction_pipeline의 한계

파일 확인 결과:

- 파일: `data/construction_pipeline_sido_monthly.csv`
- 크기: 2,261행 × 12컬럼
- 기간: 2015-01 ~ 2026-01
- 지역: 17개 시도
- 컬럼: `연월`, `시도`, `준공_아파트`, `준공_전체`, `착공_아파트`, `착공_전체`, `착공_호수`, `준공_호수`, `착공_비아파트`, `준공_비아파트`, `연도`, `월`
- 결측: `준공_아파트` 222건, `착공_아파트` 308건, 비아파트 파생 컬럼도 동일 결측 존재

한계는 명확하다.

1. 착공/준공만 있음: 인허가, 분양승인, 미분양이 없어 공급 파이프라인의 앞단과 재고 부담을 설명하지 못한다.
2. 시도 단위만 있음: 프로젝트의 핵심 실거래·NPS·소득 데이터는 시군구 분석이 가능하나 공급은 시도 단위에 머문다.
3. 단지 단위 정보 없음: 단지명, 주소, 세대수, 입주예정월이 없어 ASIL형 입주맵 대체재가 아니다.
4. 아파트 결측 처리 필요: 전체 호수는 있으나 아파트/비아파트 세부가 일부 결측이므로 분석 시 `전체`와 `아파트` 중 기준 컬럼을 명시해야 한다.
5. 저장 경로 이원화 가능성: `download_public_data.py`는 OneDrive 수요 집계 폴더에 저장하고, `data_loader.py`는 우선 `data/` 폴더를 읽는다. 동기화/복사 규칙이 필요하다.

## 3. 보강해야 할 공식 지표

우선순위는 “분석 설명력”과 “현재 코드 연결 난이도” 기준이다.

| 우선 | 지표 | 원천 후보 | 최소 지역 단위 | 월별 여부 | 표준 컬럼 | 용도 |
|---:|---|---|---|---|---|---|
| 1 | 착공실적 | KOSIS 국토부 주택건설실적, 현재 `DT_MLTM_5387` 사용 | 시도, 가능 시 시군구 | 월 | `착공_전체`, `착공_아파트`, `착공_비아파트` | 공급 현실화 1~3년 선행 |
| 2 | 준공실적 | KOSIS 국토부 주택건설실적, 현재 `DT_MLTM_5373` 사용 | 시도, 가능 시 시군구 | 월 | `준공_전체`, `준공_아파트`, `준공_비아파트` | 실제 입주/재고 증가 |
| 3 | 인허가실적 | KOSIS 국토부 주택건설 인허가실적 또는 기존 `permit.csv` | 시도 우선, 시군구 확장 | 월 | `인허가_전체`, `인허가_아파트` | 최선행 공급 의향 |
| 4 | 분양승인/분양실적 | KOSIS 국토부 분양실적, 문서상 `DT_MLTM_5557` 후보 | 시도 우선 | 월 | `분양승인_전체`, `분양승인_아파트` | 착공 이후 시장 공급 확정 |
| 5 | 미분양 | 국토부 미분양 API 또는 BOK/KOSIS fallback, 기존 `fetch_unsold_housing()` 존재 | 시도 우선, 가능 시 시군구 | 월 | `미분양_호수` | 수요 대비 공급 부담 |
| 6 | 주택건설실적 총괄 | KOSIS 국토부 총괄표 | 시도 | 월 | `공급단계`, `주택유형`, `호수` | 원천 검증/감사 trail |

## 4. 시도 vs 시군구 우선순위

v0는 시도 단위가 맞다. 이유는 현재 `construction_pipeline_sido_monthly.csv`, `unsold_housing_sido_monthly.csv`, BOK·KOSIS 계열 거시 데이터와 바로 병합 가능하고, 17개 시도 월별 패널은 결측 관리가 쉽다.

v1은 시군구 확장이 필요하다. 이유는 실거래 가격, NPS, 국세청 소득 등 프로젝트의 차별화 데이터가 시군구 단위이며, 공급 충격은 시도보다 시군구·생활권 단위에서 가격에 더 직접적으로 작동하기 때문이다.

실행 우선순위:

1. v0: 시도·월별 공식 공급 패널 완성
   - 산출물: `data/supply/supply_pipeline_sido_monthly.csv`
   - 지표: 인허가/착공/분양승인/준공/미분양
2. v0.5: 기존 `construction_pipeline_sido_monthly.csv`에 신규 컬럼 병합
   - 하위 호환: 기존 `load_construction_data()`는 깨지지 않게 유지
3. v1: 시군구·월별 패널 신설
   - 산출물: `data/supply/supply_pipeline_sigungu_monthly.csv`
   - 원천이 시군구를 제공하지 않는 지표는 시도만 유지하고 `지역단위='sido'`로 표시
4. v1 이후: 단지 단위 입주예정/사용승인 데이터 별도 구축
   - 공식 집계와 단지 데이터는 혼합하지 말고 검증 관계로 사용

## 5. 표준 컬럼 매핑

### 5.1 원천 long 포맷

KOSIS/국토부 원천은 먼저 long 포맷으로 저장한다.

| 표준 컬럼 | 설명 | 예시 |
|---|---|---|
| `source` | 원천 | `kosis`, `molit_api`, `bok_fallback` |
| `org_id` | KOSIS 기관 ID 또는 API 기관 코드 | `116` |
| `tbl_id` | 통계표 ID 또는 API 서비스 ID | `DT_MLTM_5387` |
| `indicator` | 지표명 | `착공`, `준공`, `인허가`, `분양승인`, `미분양` |
| `housing_type` | 주택유형 | `전체`, `아파트`, `비아파트`, `공동주택` |
| `region_level` | 지역 단위 | `sido`, `sigungu` |
| `region_code` | 행정구역 코드 | 시도 2자리 또는 시군구 5자리 |
| `시도` | 표준 시도명 | `경기` |
| `시군구` | 표준 시군구명 | `화성시` |
| `연월` | `YYYY-MM` | `2026-01` |
| `value` | 호수 | `1234` |
| `unit` | 단위 | `호` |
| `collected_at` | 수집일시 | ISO datetime |
| `source_updated_at` | 원천 갱신일 | KOSIS `LST_CHN_DE` 등 |

### 5.2 분석 wide 포맷

대시보드와 병합용은 wide 포맷으로 제공한다.

필수 컬럼:

- `연월`, `연도`, `월`
- `region_level`, `지역코드`, `시도`, `시군구`
- `인허가_전체`, `인허가_아파트`, `인허가_비아파트`
- `착공_전체`, `착공_아파트`, `착공_비아파트`
- `분양승인_전체`, `분양승인_아파트`
- `준공_전체`, `준공_아파트`, `준공_비아파트`
- `미분양_호수`
- `공급압력_12m`: 최근 12개월 준공 또는 입주예정 합계
- `선행공급_36m`: 최근 36개월 인허가/착공 합계
- `source_coverage`: 사용 원천 요약
- `quality_flag`: 결측/추정/시도-only 여부

기존 호환 컬럼:

- `착공_호수 = 착공_전체`
- `준공_호수 = 준공_전체`

## 6. 수집 코드 설계 초안

현재 `download_public_data.py`는 `fetch_construction_pipeline()` 안에서 KOSIS 착공/준공을 수집한다. 이를 “공급 공식 집계 수집기”로 일반화한다.

### 6.1 권장 파일 구조

1안: 기존 파일 확장

- `download_public_data.py`
  - 기존 `fetch_construction_pipeline()` 유지
  - `fetch_supply_pipeline()` 신규 추가
  - 내부 공통 함수 `_fetch_kosis_table_long()` 추가

2안: 전용 파일 신설

- `download_supply_official_data.py`
  - 공급 집계 전용. 장기적으로 더 권장
  - `update_all.py`에서 5번째 수집 스크립트로 호출

### 6.2 함수 초안

```python
def fetch_supply_pipeline(start_ym: str, end_ym: str, region_level: str = "sido") -> pd.DataFrame:
    """KOSIS/국토부 공식 공급지표를 월별 지역 패널로 수집한다."""
    specs = [
        {"indicator": "착공", "tbl_id": "DT_MLTM_5387", "types": ["전체", "아파트"]},
        {"indicator": "준공", "tbl_id": "DT_MLTM_5373", "types": ["전체", "아파트"]},
        {"indicator": "분양승인", "tbl_id": "DT_MLTM_5557", "types": ["전체", "아파트"]},
        {"indicator": "인허가", "tbl_id": "DT_MLTM_1948", "types": ["전체", "아파트"]},
    ]
    raw_frames = []
    for spec in specs:
        raw_frames.append(fetch_kosis_supply_table(spec, start_ym, end_ym, region_level))
    official = normalize_supply_long(pd.concat(raw_frames, ignore_index=True))
    unsold = fetch_unsold_housing(start_ym, end_ym)  # 기존 함수 재사용
    wide = build_supply_pipeline_wide(official, unsold, region_level)
    save_supply_outputs(official, wide, region_level)
    return wide
```

### 6.3 KOSIS 호출 설계

- endpoint: `https://kosis.kr/openapi/Param/statisticsParameterData.do`
- 공통 파라미터: `method=getList`, `apiKey`, `orgId=116`, `tblId`, `prdSe=M`, `startPrdDe`, `endPrdDe`, `format=json`, `jsonVD=Y`
- 40,000셀 제한 회피:
  1. 지표별 호출 분리
  2. 주택유형별 호출 분리
  3. 12~24개월 단위 기간 분할
  4. 시도/시군구 코드 그룹 분할
- KOSIS 코드값은 하드코딩 최소화:
  - 현재 착공/준공처럼 검증된 코드만 상수로 둔다.
  - 신규 인허가/분양승인은 최초 1회 샘플 호출 후 `source_registry.csv`에 `objL*` 코드와 명칭을 저장한다.

### 6.4 저장 경로

- raw long: `data/supply/raw/kosis_supply_official_long.csv`
- 시도 wide: `data/supply/supply_pipeline_sido_monthly.csv`
- 시군구 wide: `data/supply/supply_pipeline_sigungu_monthly.csv`
- 기존 호환본: `data/construction_pipeline_sido_monthly.csv`
- 메타데이터: `data/supply/metadata/source_registry.csv`

### 6.5 update_all.py 연결

`MONTHLY_CSV_MAP`에 다음을 추가한다.

```python
"supply_pipeline_sido_monthly.csv": {
    "col": "연월",
    "func_module": "public",
    "func_name": "fetch_supply_pipeline",
    "param_type": "ym",
    "label": "공식 공급집계",
}
```

기존 `construction_pipeline_sido_monthly.csv` 항목은 최소 1~2회 릴리스 동안 유지한다.

### 6.6 data_loader.py 연결

신규 로더:

```python
def load_supply_pipeline_data(region_level: str = "sido") -> pd.DataFrame:
    """공식 공급집계 패널 로드. 시도/시군구 단위 선택."""
```

기존 로더 유지:

```python
def load_construction_data():
    # 기존 파일이 있으면 기존처럼 반환
    # 신규 supply_pipeline_sido_monthly.csv가 있으면 착공/준공 컬럼만 추출해 fallback 가능
```

## 7. v0/v1 역할 분담

### v0: 공식 공급집계 최소 완성

목표: 웹 없이도 현재 코드 기반에서 바로 구현 가능한 공식 공급 패널.

- 단위: 시도·월별
- 지표: 착공, 준공, 인허가, 분양승인, 미분양
- 산출물: `data/supply/supply_pipeline_sido_monthly.csv`
- 대시보드 역할:
  - 지역별 공급압력 비교
  - 가격/거래량/전세와 공급지표 상관·lag 분석
  - 미분양과 준공 증가의 조합으로 공급과잉 경고
- 제외: 단지명, 입주예정월 지도, 정확한 생활권 공급량

### v1: 시군구 확장 및 검증 체계

목표: 프로젝트의 시군구 가격·수요 데이터와 결합 가능한 공급 패널.

- 단위: 시군구·월별 우선, 원천 제약 지표는 시도 유지
- 지표: v0 전부 + 시군구 가능한 원천 확장
- 산출물: `data/supply/supply_pipeline_sigungu_monthly.csv`
- 역할:
  - 시군구별 공급 충격과 가격 반응 분석
  - 단지 단위 입주예정 데이터의 지역 합계 검증
  - 시도-only 지표와 시군구 지표의 커버리지 플래그 관리

### v1 이후: 단지 단위 별도 체계

- 청약홈/LH/사용자 보유 파일/K-apt/건축물대장은 단지 단위 테이블로 분리
- 공식 공급집계는 단지 데이터의 정합성 검증용 합계 기준으로 사용
- ASIL 무단 복제 회피: 공식 API와 사용자 제공 파일만 허용

## 8. 구현 순서

1. `data/supply/` 폴더 구조 생성
2. `fetch_supply_pipeline()` 신규 작성
3. 현재 KOSIS 착공/준공 로직을 공통 함수화
4. 미분양은 기존 `fetch_unsold_housing()` 결과를 병합
5. 인허가는 기존 `permit.csv` 로더와 KOSIS 신규 수집 중 더 안정적인 쪽을 v0에 채택
6. 분양승인은 `DT_MLTM_5557` 후보를 샘플 호출로 코드 확정 후 병합
7. `supply_pipeline_sido_monthly.csv` 생성
8. `data_loader.py`에 `load_supply_pipeline_data()` 추가
9. `update_all.py` 증분 업데이트 등록
10. 대시보드에는 기존 `load_construction_data()`와 신규 로더를 병행 적용

## 9. 리스크와 처리

| 리스크 | 처리 |
|---|---|
| KOSIS objL 코드가 통계표마다 다름 | `source_registry.csv`에 코드-명칭 저장, 신규 표는 샘플 호출 후 고정 |
| 40,000셀 제한 | 기간·지역·유형 분할 호출 |
| 시군구 미제공 지표 존재 | `region_level`, `quality_flag`로 표시하고 시도 분석에만 사용 |
| 기존 파일 경로와 산출 경로 불일치 | `data/`를 분석 기준 경로로 통일하고 OneDrive 산출물은 동기화 대상으로 분리 |
| 단지 단위와 집계 단위 혼합 | 단지 테이블과 공식 집계 테이블을 분리, 합계 검증으로만 연결 |
| 아파트 세부 결측 | v0 기본 분석은 `전체` 기준, 아파트 분석은 결측률 표시 후 제한 사용 |

## 10. 최종 제안

바로 구현할 v0 범위는 `공식 공급집계 시도 월별 패널`이다. 기존 착공/준공을 유지하면서 `인허가`, `분양승인`, `미분양`만 추가하면 공급 파이프라인은 완성된다. v1에서 시군구 확장과 단지 단위 입주예정 데이터를 붙인다. 공식 집계는 “분석용 공급압력 백본”, 단지 데이터는 “지도/입주 예정 상세”, 미분양은 “공급 부담 결과지표”로 역할을 분리하는 것이 가장 안전하다.

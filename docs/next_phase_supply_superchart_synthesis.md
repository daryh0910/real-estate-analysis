# 다음 목표 진입을 위한 종합 개선안: ASIL형 입주물량 + TradingView Superchart형 분석 플랫폼

> 작성일: 2026-05-12  
> 범위: D1~D8 데이터 원천 조사 결과 종합 및 다음 개발 단계 제안  
> 프로젝트: real_estate_analysis

---

## 1. 한 줄 결론

현재 프로젝트는 가격·수요·거시 지표 기반의 분석 대시보드로는 이미 골격이 있으나, 다음 단계로 가려면 `공급 데이터의 단지 단위화`, `사용자별 분석 데이터셋 저장`, `차트 설정 저장·공유`, `agent 기반 데이터 수집/검증 파이프라인`을 추가해야 함.

가장 빠른 성공 경로는 다음임.

1. 청약홈 + 사용자 보유 파일로 `movein_plan_complex_monthly.csv`를 먼저 만든다.
2. K-apt/건축HUB로 사용승인일·세대수·주소를 보정한다.
3. KOSIS/국토부 공식 공급집계로 시도·시군구 총량을 검증한다.
4. 앱에는 `입주물량 지도/월별 차트/지역 비교`를 먼저 붙인다.
5. 그 다음 TradingView Superchart형 `개인 분석 캔버스 + 저장 + 공유`로 확장한다.

---

## 2. D1~D8 조사 결과 요약

| 구분 | 결론 | 우선도 | 역할 |
|---|---|---:|---|
| D1 청약홈 ApplyHome | 단지명, 주소, 입주예정월, 공급세대수, 공고일, 시행/시공사 확보 가능 | P0 | ASIL형 입주예정 단지 데이터의 1차 원천 |
| D2 공공데이터 후보 | 주택인허가, 건축물대장, 통계누리/KOSIS, 지자체 미분양, LH 공고가 후보 | P0~P2 | 공공 보강 원천 풀 |
| D3 K-apt | 단지명, 주소, 세대수, 사용승인일, kaptCode 확보 가능 | P0 | 준공 후 단지 마스터/세대수 보정 |
| D4 건축물대장/건축HUB | 사용승인일, 착공일, 허가일, 세대수, 주소키 확보 가능 | P0 | 실제 준공/입주 확정 검증 |
| D5 KOSIS/국토부 집계 | 기존 착공/준공에 인허가·분양승인·미분양 보강 필요 | P0 | 지역 총량/공급압력 백본 |
| D6 사용자 파일 ingest | 사용자 엑셀/CSV를 표준 스키마로 변환 가능 | P0 | 가장 빠른 단지 데이터 확보 경로 |
| D7 주소/지역코드/geocoding | 시군구코드 매핑이 좌표보다 우선 | P0 | 지도/지역집계/차트 연결 키 |
| D8 라이선스 | ASIL/상용 DB 직접 복제 금지, source metadata 필수 | P0 | 서비스화 리스크 통제 |

---

## 3. 현재 프로젝트에서 가장 부족한 점

## 3.1 데이터 측면

현재 강점:
- 실거래 가격, 전세/월세, NPS, 소득, 미분양, 금리, 심리, 인허가, 착공/준공 등 지표 카탈로그가 이미 있음.
- `INDICATOR_CATALOG`가 있어 Superchart형 지표 선택 UI로 확장하기 좋음.
- `board.py`와 저장 기능이 있어 차트 설정 저장의 기반이 있음.

현재 약점:
- ASIL처럼 단지별 `입주예정월 + 세대수 + 위치`를 볼 수 있는 데이터가 없음.
- 공급은 대부분 시도 단위 집계이며 시군구·단지 단위가 약함.
- 사용자가 직접 보유한 입주예정 엑셀을 앱 데이터로 넣는 ingest 경로가 없음.
- 데이터별 출처·라이선스·수집일시가 차트에 따라다니는 구조가 약함.

## 3.2 제품 측면

현재 강점:
- Streamlit 기반으로 빠른 실험 가능.
- Plotly 기반이므로 저장/공유할 수 있는 JSON chart spec로 확장 가능.
- 대시보드 탭이 이미 여러 분석 흐름을 갖고 있음.

현재 약점:
- TradingView Superchart처럼 사용자가 원하는 지표를 자유롭게 축/패널/오버레이로 조합하는 기능은 제한적임.
- 저장된 차트를 남에게 보여주는 기능은 초기 형태이나, 데이터 snapshot/권한/출처까지 묶인 공유 모델은 아직 부족함.
- 개인별 watchlist, condition set, chart template은 더 강화해야 함.

---

## 4. 목표 1: ASIL보다 simple한 입주물량 분석 목표

## 4.1 v0 목표 정의

ASIL 전체를 따라가는 것이 아니라 다음 4개만 구현하면 충분히 가치 있음.

1. 지역별 입주예정 세대수 지도
2. 지역별 월별 입주예정 세대수 차트
3. 단지 리스트: 단지명, 주소, 입주예정월, 세대수, 시행/시공사, source
4. 공급압력 비교: 입주예정 + 착공/준공 + 미분양 + 가격/전세 흐름

## 4.2 v0 표준 산출물

```text
data/supply/movein_plan_complex_monthly.csv
```

필수 컬럼:

| 컬럼 | 설명 |
|---|---|
| source | applyhome/user_file/lh/kapt/building_hub |
| source_id | 원천 ID |
| complex_id | 내부 단지 ID |
| 단지명 | 단지명 |
| 주소 | 원천 주소 |
| 시도 | 짧은 시도명 |
| 시군구 | 시군구명 |
| 지역코드 | 시군구 5자리 |
| 법정동코드 | 가능하면 10자리 |
| 입주예정연월 | YYYY-MM |
| 준공연월 | YYYY-MM |
| 사용승인일 | YYYY-MM-DD |
| 세대수 | 총세대수 |
| 공급세대수 | 공급 기준 세대수 |
| 사업주체 | 시행사/사업주체 |
| 시공사 | 시공사 |
| 위도 | WGS84 |
| 경도 | WGS84 |
| source_url | 원천 URL |
| license_note | 라이선스/출처 메모 |
| 수집일시 | 수집일시 |
| validation_status | ok/warn/error/hold |

## 4.3 데이터 우선순위

1순위: 사용자 보유 엑셀/CSV
- 이유: 가장 빠름.
- 단, source/license_note/allowed_scope 필수.

2순위: 청약홈 ApplyHome
- 이유: 단지명·주소·입주예정월·공급세대수가 직접 있음.
- 한계: 공고 기준 예정치라 실제 입주는 변동 가능.

3순위: K-apt
- 이유: 사용승인 후 단지명·세대수·사용승인일 보정 가능.
- 한계: 사용승인 전 신규 단지는 누락.

4순위: 건축HUB/건축물대장/주택인허가
- 이유: 실제 사용승인일, 허가일, 착공일, 총세대수 검증 가능.
- 한계: 주소키/지번 매칭 설계 필요.

5순위: KOSIS/국토부 집계
- 이유: 단지 데이터의 지역 총량 검증과 공급압력 분석.
- 한계: 단지명/주소 없음.

---

## 5. 목표 2: TradingView Superchart형 개인 분석 플랫폼

## 5.1 필요한 제품 구성

TradingView Superchart와 같은 방향으로 가려면 앱을 다음 구조로 바꾸는 것이 좋음.

1. 데이터셋 레이어
   - 지표 카탈로그: 가격, 전세, 거래, 수요, 공급, 심리, 금리, 파생
   - 사용자 업로드 데이터셋: CSV/XLSX ingest
   - 데이터셋 metadata: 출처, 기준일, 공개범위

2. 차트 빌더 레이어
   - X축: 연월/연도/지역/면적/가격구간
   - Y축: 지표 1~N개
   - 패널: 가격, 공급, 수요, 전세, 미분양 등 복수 패널
   - 오버레이: 이동평균, 전년동월비, 누적합, z-score, percentile
   - 지역 필터: 시도/시군구/watchlist

3. 저장 레이어
   - chart_template: 차트 구조 저장
   - dataset_snapshot: 사용 데이터 버전 저장
   - condition_set: 필터/조건 저장
   - watchlist: 관심지역/단지 저장

4. 공유 레이어
   - 공개 링크 또는 게시판 글
   - data_metadata_snapshot 포함
   - 외부 공개 가능한 데이터만 표시

5. agent 레이어
   - 데이터 수집 agent
   - 원천 검증 agent
   - 주소/좌표 보정 agent
   - 차트 추천 agent
   - 이상치 탐지 agent
   - 설명문 생성 agent

## 5.2 먼저 만들 화면

우선순위는 다음 3개 화면임.

### 화면 A. 입주물량 맵

- 지도: 시군구별 입주예정 세대수 choropleth 또는 bubble
- 슬라이더: 입주예정연월 범위
- 필터: 시도, 시군구, source, validation_status
- 클릭 시: 단지 리스트 표시

### 화면 B. 지역 Superchart

- 선택 지역: 시도/시군구/watchlist
- 지표 선택: 평균가격, 전세가율, 미분양, 인허가, 착공, 준공, 입주예정
- 표시 방식: line/bar/area, YoY, MoM, 이동평균
- 저장 버튼: chart_template + data_snapshot 저장

### 화면 C. 단지 공급 리스트

- 표: 단지명, 주소, 입주예정월, 세대수, source, 사용승인일, validation
- 검색: 단지명/주소
- 정렬: 입주월, 세대수, 지역
- 검수: warn/error/hold 필터

---

## 6. 20개 agent까지 쓴다면 역할 분담

20개를 모두 코딩 agent로 쓰기보다는, 조사/수집/검증/구현/리뷰를 분리하는 것이 좋음.

| Agent | 역할 | 산출물 |
|---:|---|---|
| 1 | 청약홈 API 구현 | applyhome raw/detail/model downloader |
| 2 | 사용자 파일 ingest 구현 | user_movein_ingest.py |
| 3 | K-apt 수집 구현 | kapt master downloader |
| 4 | 건축HUB PoC | building_hub matcher |
| 5 | KOSIS 공급지표 보강 | supply_pipeline_sido_monthly.csv |
| 6 | 주소 정규화 구현 | region_normalization_service.py |
| 7 | geocoding 구현 | geocode cache/failed report |
| 8 | 라이선스 metadata 구현 | source_registry, policy check |
| 9 | data_loader 통합 | load_movein_plan_data, aggregate functions |
| 10 | 입주물량 맵 UI | Streamlit tab/component |
| 11 | 지역 Superchart UI | chart builder MVP |
| 12 | chart template 저장 | board.py 확장 |
| 13 | 공유 snapshot 구현 | data_metadata_snapshot |
| 14 | 테스트 작성 | ingest/load/validation tests |
| 15 | 성능 최적화 | parquet/cache/build_cache |
| 16 | UX 문구/도움말 | 데이터 주의문구 |
| 17 | 품질검증 | 중복/오탐/결측 report |
| 18 | 문서화 | README/운영가이드 |
| 19 | 보안점검 | API key/secret 누출 검사 |
| 20 | 최종 통합 리뷰 | release checklist |

---

## 7. 구현 순서 제안

## Phase 0. 준비

- `.env.example`에 필요한 키 정리
  - DATA_GO_KR_KEY
  - KOSIS_API_KEY
  - VWORLD_API_KEY 또는 geocoding provider key
- `data/supply/` 폴더 구조 생성

```text
data/supply/
  raw/
    applyhome/
    user_file/
    kapt/
    building_hub/
  metadata/
    source_registry.csv
  validation/
  movein_plan_complex_monthly.csv
  supply_pipeline_sido_monthly.csv
  supply_pipeline_sigungu_monthly.csv
```

## Phase 1. 사용자 파일 ingest MVP

가장 먼저 할 일:

1. `services/user_movein_ingest.py` 작성
2. 표준 템플릿 CSV 생성
3. 사용자가 가진 입주예정 파일 1개를 표준화
4. `movein_plan_complex_monthly.csv` 생성
5. validation report 생성

성공 기준:
- 단지명/입주예정연월/세대수/시도/시군구가 채워진 표준 CSV 생성
- error 행과 warn 행이 구분됨

## Phase 2. 앱 로더 + 입주물량 탭

1. `data_loader.py`에 `load_movein_plan_data()` 추가
2. `aggregate_movein_sigungu_monthly()` 추가
3. `INDICATOR_CATALOG`에 `입주예정_세대수`, `입주예정_단지수` 추가
4. 앱에 `입주물량` 또는 `공급맵` 탭 추가

성공 기준:
- 월별/지역별 입주예정 세대수 차트가 보임
- 단지 리스트가 필터링됨

## Phase 3. 청약홈 API 수집

1. 공공데이터포털 활용신청
2. raw downloader 작성
3. detail/model 조인
4. 사용자 파일 표준 스키마와 동일한 `movein_plan_complex_monthly.csv`에 append

성공 기준:
- 최근 공고 100건 이상에서 입주예정월/세대수 추출
- 사용자 파일과 동일 UI에서 조회 가능

## Phase 4. K-apt/건축HUB 보정

1. K-apt 단지 목록/기본정보 수집
2. 주소+단지명 fuzzy matching
3. 사용승인일/세대수 보정
4. 건축HUB는 후보 단지 기반 점조회 PoC부터 시작

성공 기준:
- 사용승인 완료 단지의 입주예정월과 사용승인월 차이를 확인 가능
- match_score와 validation_status가 표시됨

## Phase 5. Superchart MVP

1. 기존 `INDICATOR_CATALOG`를 UI 지표 선택기로 사용
2. 지역 + 지표 + 변환식을 선택하는 차트 빌더 제작
3. chart_template 저장
4. 게시판 공유 시 data_metadata_snapshot 저장

성공 기준:
- 사용자가 `경기 화성시 + 입주예정_세대수 + 평균가격 + 미분양_호수` 같은 조합을 직접 저장 가능

---

## 8. 다음 액션: 바로 착수할 개발 티켓

### P0-1. 사용자 파일 ingest MVP

- 파일: `services/user_movein_ingest.py`
- 추가 산출물:
  - `data/supply/templates/movein_user_upload_template.csv`
  - `data/supply/metadata/source_registry.csv`
- 기능:
  - CSV/XLSX 읽기
  - 컬럼 alias 매핑
  - 연월/숫자/지역 정규화
  - validation_status 생성

### P0-2. movein data_loader 연결

- 파일: `data_loader.py`
- 함수:
  - `load_movein_plan_data()`
  - `aggregate_movein_sigungu_monthly()`
  - `aggregate_movein_sido_monthly()`

### P0-3. 앱 입주물량 탭

- 파일: `app.py`
- 기능:
  - 지역/연월/source 필터
  - 월별 입주예정 세대수 bar chart
  - 단지 리스트 table
  - 데이터 출처/주의문구 표시

### P0-4. 라이선스 체크

- 파일: `services/source_policy.py`
- 기능:
  - allowed_scope 검사
  - forbidden/hold 데이터 앱 노출 차단
  - chart 공유 전 metadata snapshot 생성

### P1-1. 청약홈 downloader

- 파일: `services/applyhome_client.py`
- 조건:
  - DATA_GO_KR_KEY 필요
  - API 활용신청 후 endpoint/필드명 최종 확인

### P1-2. 주소 정규화/geocoding

- 파일: `services/region_normalization_service.py`
- 기능:
  - 주소 파싱
  - 시군구코드 매핑
  - 좌표 보정은 선택 실행

---

## 9. 리스크와 대응

| 리스크 | 영향 | 대응 |
|---|---|---|
| 청약홈 입주예정월이 실제와 다름 | 분석 신뢰도 저하 | K-apt/건축HUB 사용승인일로 보정 |
| 사용자 파일 출처 불명 | 외부 공유 리스크 | license_note/allowed_scope 필수, hold 처리 |
| 주소 매칭 실패 | 지도/집계 누락 | 시군구 수동 컬럼 허용, 실패 report 생성 |
| 단지/주택형 중복 집계 | 세대수 과대계상 | complex_id 기준 dedupe, 주택형 파일은 합산 규칙 분리 |
| 공공 API 키/쿼터 | 수집 실패 | raw cache, 증분 수집, retry/backoff |
| Streamlit 성능 | 앱 느림 | parquet cache, pre-aggregation, build_cache 확장 |

---

## 10. 최종 권고

다음 목표는 한 번에 TradingView급 전체 플랫폼을 만들기보다, `입주물량 데이터 파이프라인 + Superchart MVP`를 붙이는 방식이 맞음.

우선 구현 순서는 다음으로 확정하는 것을 권장함.

1. 사용자 파일 ingest MVP
2. movein_plan 표준 CSV 생성
3. data_loader 연결
4. 입주물량 탭 추가
5. chart 저장/공유에 metadata snapshot 추가
6. 청약홈 API 자동 수집
7. K-apt/건축HUB 보정
8. Superchart형 차트 빌더 확장

이 순서면 ASIL 수준 전체 복제 없이도, 합법적이고 빠르게 `simple한 입주물량 분석`을 먼저 만들고, 이후 개인별 분석·저장·공유 플랫폼으로 확장할 수 있음.

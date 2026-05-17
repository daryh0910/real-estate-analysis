# 입주·공급 데이터 원천 조사 및 다운로드 설계

작성일: 2026-05-12
목적: ASIL형 입주·공급맵에 필요한 `입주 예정/준공/단지/세대수/연월` 데이터를 ASIL 무단 복제 없이 공공데이터·공식 API·사용자 보유 파일 중심으로 확보하기 위한 원천 조사와 안전한 다운로드 설계.

## 1. 현재 프로젝트 내 확인 결과

읽은 파일:
- `data_loader.py`
- `update_all.py`
- `.env.example`
- `README.md`
- `data/`, `cache/` 목록
- 보조 확인: `download_public_data.py`, `data/construction_pipeline_sido_monthly.csv`, `data/permit.csv`

현재 공급 관련 보유/연동 상태:
- `data/construction_pipeline_sido_monthly.csv` 존재
  - 컬럼: `연월, 시도, 준공_아파트, 준공_전체, 착공_아파트, 착공_전체, 착공_호수, 준공_호수, 착공_비아파트, 준공_비아파트, 연도, 월`
  - 범위 예시: 2015-01부터 수록, 시도 월별 집계
  - 단점: 단지명·주소·개별 입주예정월 없음. ASIL형 지도에는 보조 지표로만 적합.
- `data/permit.csv` 존재
  - 주택 인허가 wide CSV. `data_loader.load_permit_data()`가 아파트 가구수만 필터링해 `시도/연도/월/인허가_호수`로 변환.
  - 단점: 인허가→실입주까지의 시차가 크고 단지 단위가 아님.
- `download_public_data.py`에 KOSIS 기반 착공/준공 수집 함수 존재
  - KOSIS `DT_MLTM_5387`: 주택유형별 주택건설 착공실적(월계)
  - KOSIS `DT_MLTM_5373`: 주택유형별 주택건설 준공실적(월계)
  - 출력: `construction_pipeline_sido_monthly.csv`
- `.env.example` 환경변수
  - `BOK_API_KEY`, `DATA_GO_KR_KEY`, `MOLIT_API_KEY`, `KOSIS_API_KEY`, `SGIS_CONSUMER_KEY`, `SGIS_CONSUMER_SECRET`

## 2. 후보 원천별 평가

### A. 한국부동산원 청약홈 분양정보 조회 서비스 - 최우선

- 원천: 공공데이터포털 / 한국부동산원 청약홈
- 검색어:
  - `한국부동산원 청약홈 분양정보 조회 서비스`
  - `ApplyhomeInfoDetailSvc getAPTLttotPblancDetail`
  - `청약홈 아파트 분양정보 API 입주예정월`
- 예상 API endpoint:
  - `https://api.odcloud.kr/api/ApplyhomeInfoDetailSvc/v1/getAPTLttotPblancDetail`
  - `https://api.odcloud.kr/api/ApplyhomeInfoDetailSvc/v1/getAPTLttotPblancMdl`
- 키 필요 여부: 필요. `DATA_GO_KR_KEY` 또는 별도 공공데이터포털 활용신청 필요.
- 라이선스/이용조건: 공공데이터포털 개별 서비스 이용허락 조건 확인 필요. 일반적으로 출처표시 및 상업적 이용 가능 여부가 서비스별로 다름.
- 주요 필드 후보:
  - 공고 상세: `HOUSE_MANAGE_NO`, `PBLANC_NO`, `HOUSE_NM`, `HOUSE_SECD_NM`, `HSSPLY_ADRES`, `TOT_SUPLY_HSHLDCO`, `RCRIT_PBLANC_DE`, `PRZWNER_PRESNATN_DE`, `CNSTRCT_ENTRPS_NM`, `BSNS_MBY_NM`, `MDHS_TELNO`, `HMPG_ADRES`, `SUBSCRPT_AREA_CODE_NM`, `MVN_PREARNGE_YM` 또는 유사 입주예정월 필드
  - 주택형: `HOUSE_MANAGE_NO`, `PBLANC_NO`, `MODEL_NO`, `HOUSE_TY`, `SUPLY_AR`, `SUPLY_HSHLDCO`, 특별공급/일반공급 세대수 관련 필드
- 다운로드 난이도: 중
  - ODcloud 방식이라 페이지네이션은 단순하나, 과거 전체 보존 범위와 필드명이 수시로 다를 수 있음.
  - 공공데이터포털에서 해당 API 활용신청이 별도로 필요할 수 있음.
- 프로젝트 매핑:
  - 원천 테이블 1: `data/supply/applyhome_apt_detail_raw.csv`
  - 원천 테이블 2: `data/supply/applyhome_apt_model_raw.csv`
  - 정규화 산출물: `data/supply/movein_plan_complex_monthly.csv`
  - 표준 컬럼:
    - `source='applyhome'`
    - `complex_id=HOUSE_MANAGE_NO + '_' + PBLANC_NO`
    - `단지명=HOUSE_NM`
    - `주소=HSSPLY_ADRES`
    - `시도/시군구=주소 파싱 또는 법정동코드 매핑`
    - `입주예정연월=MVN_PREARNGE_YM 정규화(YYYY-MM)`
    - `공급세대수=TOT_SUPLY_HSHLDCO`
    - `주택형별세대수=sum(SUPLY_HSHLDCO)`
  - 장점: ASIL형 “입주 예정 단지명/세대수/월”에 가장 가까움.
  - 한계: 공고 이후 변경·연기된 실제 입주월 반영은 제한될 수 있음. 준공/사용승인으로 사후 보정 필요.

### B. KOSIS 국토교통부 주택건설실적통계 - 현재 코드와 호환되는 공식 집계

- 원천: KOSIS OpenAPI / 국토교통부
- 검색어:
  - `KOSIS 주택건설실적통계 주택유형별 주택건설 준공실적 월계`
  - `DT_MLTM_5373`
  - `DT_MLTM_5387`
  - `DT_MLTM_5557 주택건설 분양실적 공동주택`
- API endpoint:
  - `https://kosis.kr/openapi/Param/statisticsParameterData.do`
- 키 필요 여부: 필요. `KOSIS_API_KEY`
- 확인된 통계표:
  - `orgId=116, tblId=DT_MLTM_5373`: 주택유형별 주택건설 준공실적(월계)
  - `orgId=116, tblId=DT_MLTM_5387`: 주택유형별 주택건설 착공실적(월계)
  - `orgId=116, tblId=DT_MLTM_5372`: 주택건설 준공실적(월계)
  - `orgId=116, tblId=DT_MLTM_5386`: 주택건설 착공실적(월계)
  - `orgId=116, tblId=DT_MLTM_5374`: 주택규모별 주택건설 준공실적(월계)
  - `orgId=116, tblId=DT_MLTM_5388`: 주택규모별 주택건설 착공실적(월계)
  - `orgId=116, tblId=DT_MLTM_5557`: 주택건설 분양실적(공동주택)
  - `orgId=116, tblId=DT_MLTM_1948`: 주택유형별 주택건설 인허가실적(월별 누계)
  - `orgId=116, tblId=DT_MLTM_1952`: 주택규모별 주택건설 인허가실적(월별 누계)
- 현재 코드 예시 파라미터:
  - `method=getList`
  - `apiKey={KOSIS_API_KEY}`
  - `itmId=ALL`
  - `objL1=ALL`
  - `objL2/objL3/objL4=아파트 또는 전체 코드`
  - `prdSe=M`
  - `startPrdDe=YYYYMM`
  - `endPrdDe=YYYYMM`
  - `orgId=116`
  - `tblId=DT_MLTM_5373 또는 DT_MLTM_5387`
  - `format=json`, `jsonVD=Y`
- 주요 필드:
  - `PRD_DE`, `C1_NM`(지역), `C2_NM/C3_NM/C4_NM`(유형), `DT`(호수), `UNIT_NM`, `TBL_NM`, `LST_CHN_DE`
- 다운로드 난이도: 낮음~중
  - 이미 코드 구현이 있음.
  - 단, 4만 셀 제한 회피를 위해 유형/지역/기간 분할 필요.
- 프로젝트 매핑:
  - 기존 `construction_pipeline_sido_monthly.csv` 유지
  - 추가로 `DT_MLTM_5557`를 붙여 `분양승인_호수` 컬럼 생성 권장
  - ASIL형 지도에는 “지역별 공급 압력/검증용”으로 사용. 단지 단위 입주예정 데이터는 아님.

### C. 공동주택관리정보시스템 K-apt / 공동주택 단지 기본정보 - 준공·단지 마스터 보강

- 원천: 국토교통부/공동주택관리정보시스템 또는 공공데이터포털 연계 API
- 검색어:
  - `국토교통부 공동주택 단지 목록제공 서비스`
  - `K-apt 공동주택 단지 기본정보 API`
  - `공동주택관리정보시스템 단지정보 서비스 사용승인일 세대수`
- API endpoint 후보:
  - 서비스 문서에서 `getKaptList`, `getKaptInfo_detail`류 endpoint 확인 필요.
  - 공공데이터포털 서비스명/운영 URL이 개편될 수 있어 문서 재확인 필수.
- 키 필요 여부: 대체로 필요. `DATA_GO_KR_KEY` 또는 K-apt 별도 키.
- 주요 필드 후보:
  - `kaptCode`, `kaptName`, `kaptAddr`, `bjdCode`, `as1/as2/as3`, `hoCnt`, `totDongCnt`, `useAprvYmd`, `kaptdaCnt`, `privArea`, 관리방식/난방방식 등
- 라이선스/이용조건: 공공데이터포털/K-apt 이용조건 확인 필요. ASIL 복제와 무관한 공식 원천.
- 다운로드 난이도: 중~상
  - 법정동 단위 또는 지역 단위 호출이 필요할 수 있음.
  - 이미 입주 완료된 관리대상 공동주택 중심이라 `입주예정`에는 부적합, `준공/사용승인 단지 마스터`에는 적합.
- 프로젝트 매핑:
  - `data/supply/kapt_complex_master_raw.csv`
  - `complex_master.csv` 표준 컬럼으로 변환:
    - `source='kapt'`, `complex_id=kaptCode`, `단지명`, `주소`, `법정동코드`, `세대수=hoCnt`, `사용승인일=useAprvYmd`, `준공연월=YYYY-MM`
  - 청약홈 입주예정 데이터와 주소/단지명 fuzzy join하여 실제 준공·세대수 검증.

### D. 국토교통부 건축물대장정보 서비스 - 사용승인일 기반 실제 준공 보정

- 원천: 공공데이터포털 / 국토교통부 건축물대장정보 서비스
- 검색어:
  - `국토교통부 건축물대장정보 서비스 getBrTitleInfo`
  - `건축물대장 표제부 공동주택 사용승인일 세대수 API`
- API endpoint 후보:
  - `https://apis.data.go.kr/1613000/BldRgstService_v2/getBrTitleInfo`
- 키 필요 여부: 필요. `DATA_GO_KR_KEY`
- 주요 파라미터 후보:
  - `sigunguCd`, `bjdongCd`, `bun`, `ji`, `startDate`, `endDate`, `numOfRows`, `pageNo`, `_type=json`
- 주요 필드 후보:
  - `platPlc`, `newPlatPlc`, `sigunguCd`, `bjdongCd`, `bun`, `ji`, `mainPurpsCdNm`, `hhldCnt`, `fmlyCnt`, `useAprDay`, `totArea`, `dongNm`, `bldNm`
- 라이선스/이용조건: 공공데이터포털 조건 확인.
- 다운로드 난이도: 상
  - 전국 필지별 호출은 매우 크고 API 제한·시간 비용 큼.
  - 추천 방식: 청약홈/K-apt 후보 단지 주소를 먼저 확보한 뒤 해당 주소·법정동·번지에 대해서만 보강 호출.
- 프로젝트 매핑:
  - `building_register_completion_raw.csv`는 원천 캐시만 저장.
  - `movein_plan_complex_monthly`의 `actual_completion_ym`, `actual_households` 보정 컬럼으로 사용.

### E. LH 분양·임대 공고 API - 공공분양/공공임대 공급 보강

- 원천: 공공데이터포털 / 한국토지주택공사
- 검색어:
  - `한국토지주택공사 분양임대공고문 조회 서비스`
  - `LH 분양임대공고문 API`
  - `LH 청약센터 분양주택 공급정보 API`
- endpoint 후보:
  - 공공데이터포털 서비스 문서 확인 필요. 통상 `apis.data.go.kr` 기반 LH 서비스와 공고목록/상세 endpoint 제공.
- 키 필요 여부: 필요. `DATA_GO_KR_KEY` 및 서비스 활용신청.
- 주요 필드 후보:
  - 공고ID, 공고명, 공고유형, 지역, 주소, 단지명, 공급호수, 접수일, 당첨자발표일, 입주예정월, 상세URL
- 라이선스/이용조건: 공공데이터포털/LH 조건 확인.
- 다운로드 난이도: 중
  - 공공분양/임대 커버리지에는 좋으나 민간분양 전체에는 부족.
- 프로젝트 매핑:
  - `source='lh'`로 청약홈 데이터와 union.
  - 공공임대/분양 구분 컬럼 `supply_type` 유지.

### F. HUG/주택도시보증공사 분양보증·보증통계 - 선행 공급 지표

- 원천: KOSIS/공공데이터포털 / 주택도시보증공사
- 검색어:
  - `주택도시보증공사 주택보증통계 분양보증 KOSIS`
  - `HUG 민간아파트 분양가격 동향 분양보증`
- 키 필요 여부: KOSIS 또는 공공데이터 키
- 주요 필드 후보:
  - 보증건수/금액, 지역, 월/분기, 주택유형
- 다운로드 난이도: 중
- 프로젝트 매핑:
  - 직접 입주맵 데이터가 아니라 분양/착공 전단계 공급심리·공급확정 보조지표.

### G. 사용자 보유 파일 - 가장 현실적인 단지 단위 보강 경로

- 원천 후보:
  - 내부 보유 입주예정 단지 엑셀/CSV
  - 분양캘린더, 입주캘린더, 자체 수집 단지 마스터
- 라이선스/주의:
  - ASIL, 부동산지인, 닥터아파트 등 유료/상용 서비스 화면·DB를 무단 복제하면 안 됨.
  - 사용자가 직접 작성했거나 재배포 가능한 파일만 사용.
- 프로젝트 매핑 표준 템플릿:
  - `source`, `complex_name`, `address`, `sido`, `sigungu`, `eupmyeondong`, `movein_ym`, `completion_ym`, `households`, `construction_company`, `developer`, `latitude`, `longitude`, `source_url`, `license_note`, `updated_at`
- 다운로드 난이도: 낮음. 품질관리/중복제거가 핵심.

## 3. 추천 데이터 아키텍처

### 표준 산출물 1: 단지 단위 입주예정/준공

파일: `data/supply/movein_plan_complex_monthly.csv`

필수 컬럼:
- `source`: applyhome/lh/user/kapt/building_register 등
- `source_id`: 원천 고유 ID
- `complex_id`: 프로젝트 내부 고유 ID
- `단지명`
- `주소`
- `시도`
- `시군구`
- `법정동코드`
- `입주예정연월`
- `준공연월`
- `사용승인일`
- `세대수`
- `공급세대수`
- `일반공급세대수`
- `특별공급세대수`
- `주택형`
- `전용면적`
- `사업주체`
- `시공사`
- `위도`, `경도`
- `source_url`
- `license_note`
- `수집일시`

### 표준 산출물 2: 지역 월별 집계

파일: `data/supply/supply_pipeline_sigungu_monthly.csv`

필수 컬럼:
- `연월`, `시도`, `시군구`, `지역코드`
- `입주예정_세대수`
- `준공_세대수`
- `분양승인_세대수`
- `착공_세대수`
- `인허가_세대수`
- `단지수`
- `source_coverage`: applyhome/lh/user/kosis 등

### 표준 산출물 3: 현재 기존 집계와 호환

기존 `data/construction_pipeline_sido_monthly.csv`는 유지하되, 단지 단위 산출물에서 시도 집계한 `입주예정_세대수`를 병합하여 `data_loader.py`에 `load_movein_supply_data()`를 추가하는 방향 권장.

## 4. 안전한 다운로드 agent 설계

### 원칙

1. ASIL 또는 상용 서비스 데이터는 크롤링/복제하지 않는다.
2. 공식 API/공공데이터포털/KOSIS/LH/청약홈/사용자 제공 파일만 허용한다.
3. 최초 실행은 `--dry-run`과 `--sample`만 수행한다.
4. 대량 다운로드 금지: 기간·지역·페이지 단위로 rate limit을 둔다.
5. 원천 raw와 표준화 output을 분리 저장한다.
6. 원천별 이용조건·활용신청 상태·수집일시를 metadata에 남긴다.
7. API 키는 `.env`에서만 읽고 로그에 출력하지 않는다.

### 제안 스크립트

파일명: `download_supply_data.py`

CLI:
- `python download_supply_data.py --source applyhome --start 202001 --end 202612 --sample 10 --dry-run`
- `python download_supply_data.py --source applyhome --incremental`
- `python download_supply_data.py --source kosis --tables construction,completion,presale --incremental`
- `python download_supply_data.py --source user-file --path <csv/xlsx>`
- `python download_supply_data.py --build-aggregate`

모듈 구조:
- `fetch_applyhome_detail(start_ym, end_ym, page_size=100, sample=None)`
- `fetch_applyhome_model(start_ym, end_ym, page_size=100, sample=None)`
- `fetch_kosis_supply_pipeline(start_ym, end_ym)`
- `fetch_kapt_complex_master(region_codes, sample=None)`
- `fetch_building_register_for_candidates(candidate_df, sample=None)`
- `normalize_movein_complex(raw_frames)`
- `aggregate_supply_sigungu_monthly(complex_df)`
- `validate_supply_data(complex_df, aggregate_df)`

저장 경로:
- raw: `data/supply/raw/{source}/{endpoint}_{YYYYMMDD}.csv`
- normalized: `data/supply/movein_plan_complex_monthly.csv`
- aggregate: `data/supply/supply_pipeline_sigungu_monthly.csv`
- metadata: `data/supply/metadata/source_registry.csv`
- logs: `cache/supply_download_log.jsonl`

### 검증 규칙

- `입주예정연월`, `준공연월`은 `YYYY-MM` 형식 강제.
- `세대수`, `공급세대수`는 0 이상 정수.
- `주소` 또는 `시도+시군구`가 없으면 지도 집계에서 제외하고 오류 로그 기록.
- 동일 단지 중복 기준:
  - 1차: 원천 ID 동일
  - 2차: `단지명+시군구+입주예정연월`
  - 3차: 주소 정규화 후 fuzzy match
- 청약홈 입주예정월과 K-apt/건축물대장 사용승인월 차이가 6개월 이상이면 `date_conflict_flag=True`.
- 지역 월별 집계와 KOSIS 시도 준공실적의 차이를 월/시도별 비교해 커버리지 지표 생성.

## 5. 우선순위 제안

1. `청약홈 분양정보 조회 서비스` 활용신청/샘플 수집
   - 목표: 단지명, 주소, 총공급세대수, 입주예정월 확보
2. KOSIS `DT_MLTM_5557` 분양실적(공동주택) 추가
   - 목표: 청약홈 커버리지 검증 및 지역별 공급 지표 확장
3. 기존 KOSIS 착공/준공(`DT_MLTM_5387`, `DT_MLTM_5373`) 유지·증분화
   - 목표: 지역별 실제 준공 흐름 유지
4. K-apt 단지 기본정보 또는 사용자 보유 파일로 준공·세대수 보정
   - 목표: 실제 준공/사용승인일과 단지 마스터 보강
5. 건축물대장 API는 후보 단지에 대해서만 선택 호출
   - 목표: 주소 기반 사용승인일 보정. 전국 대량 수집은 비추천

## 6. 결론

ASIL형 입주·공급맵의 핵심 단지 단위 데이터는 `청약홈 분양정보 API`가 1순위다. 현재 프로젝트가 이미 보유한 KOSIS 착공/준공 집계는 지역별 공급 압력과 검증용으로 계속 사용하되, 단지명·주소·입주예정월은 청약홈/LH/사용자 보유 파일에서 확보해야 한다. 실제 준공/사용승인 보정은 K-apt 또는 건축물대장 API를 후보 단지에 한정해 연결하는 방식이 안전하다.

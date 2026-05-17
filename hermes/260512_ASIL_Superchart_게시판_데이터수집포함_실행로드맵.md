# 260512_ASIL_Superchart_게시판_데이터수집포함_실행로드맵

> 작성일: 2026-05-12  
> 대상 프로젝트: real_estate_analysis  
> 목적: ASIL형 입주·공급맵 + TradingView식 Superchart + 차트 아이디어 게시판 + 데이터 다운로드 agents 실행 방향 통합

---

## 1. 핵심 결론

이 프로젝트의 목표점은 다음 한 문장으로 고정한다.

> 사용자가 1분 안에 특정 지역의 입주·공급 부담과 가격·전세·거래 반응을 이해하고, 그 판단 근거를 재현 가능한 차트 아이디어로 저장·공유할 수 있게 만든다.

제품 wedge는 다음이다.

1. **ASIL처럼 단순하게 공급을 본다**  
   - 지역/기간 선택
   - 총 공급량/입주량
   - 최대 공급월
   - 월별 bar chart
   - 단지 또는 지역별 공급 리스트
   - 출처·기준일·주의문구

2. **TradingView처럼 깊게 검증한다**  
   - 입주/준공 bar + 매매가격 line + 전세가격 line + 거래량/미분양 보조축
   - Index=100, YoY, MoM
   - 대규모 공급월 marker/shading
   - 사용자 지표/저장 차트

3. **게시판은 잡담판이 아니라 차트 아이디어 저장소로 만든다**  
   - 게시글 = 차트 이미지 + settings JSON + 해석 + 태그 + 데이터 기준 snapshot
   - 게시글 상세에서 “이 차트 불러오기”
   - 공유는 PNG가 아니라 재현 가능한 분석 세트

4. **데이터 agents가 제품 성공의 선행조건이다**  
   - 현재 가격·전세·거래 데이터는 충분히 강하다.
   - 그러나 ASIL형 핵심인 **단지 단위 입주예정 데이터**가 없다.
   - 데이터 다운로드 agents는 단순 수집이 아니라 원천 탐색 → API 수집 → 검증 → 표준화 → manifest → cache 반영까지 책임져야 한다.

---

## 2. 에이전트 진단 총괄

이번 라운드에서 사용한 진단/리뷰/데이터 에이전트 역할은 다음과 같다.

### 2.1 10개 진단 에이전트

| # | 관점 | 핵심 결론 |
|---|---|---|
| 1 | 게시판 구조 | board.py 기반은 있으나 Superchart 게시글에서 “이 차트 불러오기”, chart_type, tags, interpretation, schema_version이 부족 |
| 2 | Streamlit UI | 홈/입주·공급맵/Superchart/지역검색기/전략검증/게시판/관심지역으로 재정렬 필요 |
| 3 | Plotly 구현 | make_subplots 기반 입주 bar + 가격/전세 line + 이벤트 marker/shading 권장. add_vline 대신 add_shape 사용 |
| 4 | 데이터 신뢰도 | 기준월, 출처, 누락률, 품질등급, source_metadata, data_run_manifest 필요 |
| 5 | TDD | 입주물량 normalize/filter/summarize/aggregate, settings roundtrip, board CRUD/복원 테스트 필요 |
| 6 | 제품전략 | ASIL 단순함으로 시작하고 TradingView 확장성은 고급/공유 구조로 열기 |
| 7 | 경쟁분석 | wedge는 입주공급 이벤트 → 가격/전세/거래 반응 → 재현 가능한 차트 공유 |
| 8 | 아키텍처 | app.py 4천라인 모놀리스. config/core/services/components/views 점진 분리 필요 |
| 9 | 투자지표 | 공급부담, 가격조정, 전세방어, 거래회복, 종합시그널 설계 가능 |
| 10 | 운영 | update_all.py, cache/board.db/board_images 백업, kaleido/SQLite/문서불일치 리스크 관리 필요 |

### 2.2 5개 리뷰 에이전트

| # | 리뷰 관점 | 핵심 판단 |
|---|---|---|
| 1 | CEO/제품전략 | 첫 질문을 좁혀야 함: “이 지역의 공급 부담과 가격·전세 반응은?” |
| 2 | 기술 아키텍처 | app.py 대규모 리팩토링은 미루고 테스트/metadata/저장복원 contract부터 |
| 3 | 데이터/퀀트 | 종합 0~100 점수는 과신 리스크. MVP는 룰 기반 진단 배지 + 품질등급 우선 |
| 4 | UX/커뮤니티 | 게시판 명칭은 “차트 아이디어”. 차트 없는 자유 글쓰기는 P0에서 막는 것이 좋음 |
| 5 | PM/QA/운영 | acceptance criteria는 “같은 분석을 저장·공유·재현할 수 있는가”가 최상위 |

### 2.3 4개 데이터 에이전트

| # | 데이터 관점 | 핵심 판단 |
|---|---|---|
| 1 | 데이터 inventory/gap | 가격·전세·거래는 충분. ASIL형 핵심인 단지 단위 입주예정 데이터 없음 |
| 2 | 입주·공급 원천 | 청약홈 분양정보 API, KOSIS 착공/준공/인허가, K-apt/건축물대장 보강 후보 |
| 3 | 가격·전세·거시 | update_all.py 누락 지표 존재: CSI, KB, M2, 예대금리차, 가계신용, KRIHS 등 |
| 4 | 다운로드 pipeline | staging → validation → standardize → manifest → atomic publish 구조 필요 |

---

## 3. 현재 데이터 상태 판단

### 3.1 바로 쓸 수 있는 강점 데이터

| 데이터 | 상태 | 용도 |
|---|---|---|
| 아파트 매매 실거래 월별 집계 | 2006-01~2026-04, 시군구 | 평균가격, 평균단가, 거래량, 가격조정, 거래회복 |
| 전세 실거래 월별 집계 | 2011-01~2026-04, 시군구 | 전세가격, 전세거래량, 전세방어, 전세가율 |
| 월세 실거래 월별 집계 | 2011-01~2026-04, 시군구 | 월세화, 임대수익률 보조 |
| 미분양 | 2008-01~2026-01, 시도 | 공급부담, 미분양소화기간 |
| 착공/준공 | 2015-01~2026-01, 시도 | 입주/공급 proxy, Superchart bar |
| 기준금리/시장금리 | 2000-01~2026-02, 전국 | 금리 축, valuation 보조 |
| 주담대 | 2006-12~2026-01, 시도 | 금융환경 보조 |
| NPS | 2015-11~2026-02, 시군구 | 고용/소득 proxy |

### 3.2 가장 큰 결손

| Gap | 중요도 | 이유 |
|---|---:|---|
| 단지 단위 입주예정 데이터 | P0 | ASIL형 “위치/단지명/입주년월/세대수” 리스트의 핵심 |
| 단지 주소/좌표/시군구 매핑 | P0 | 지도 marker, 단지→지역 집계, 생활권 분석에 필요 |
| 데이터 기준일/출처/품질 metadata | P0 | 게시판 공유와 투자판단 신뢰도의 전제 |
| 주택가격지수 17개 시도 커버리지 | P1 | 현재 일부 지역만 확인되어 지역 비교 지수로 부족 |
| 인구이동 데이터 오류/누락 | P1 | 수요 변화 해석에 중요. 기존 파일 오염 가능성 있음 |
| KB/CSI/KRIHS/M2/예대금리차/가계신용 | P1 | Superchart/시장심리/거시 보강에 유용하나 update_all.py 통합 누락 |
| SGIS 인구/가구/주택 | P2 | 현재 cache가 불완전. 공급부담 분모 고도화용 |

### 3.3 ASIL형 MVP 가능 수준

현재 보유 데이터만으로 가능한 수준:

- 시도 단위 착공/준공/미분양 기반 “공급압력 v0”
- 시군구 가격·전세·거래량과 연결한 Superchart
- 단지 리스트 없는 공급 bar 중심 MVP

진짜 ASIL형에 필요한 수준:

- 단지명
- 주소
- 입주예정연월
- 총세대수
- 시군구
- 위도/경도 또는 geocoding 가능한 주소
- 출처
- 데이터 기준일

따라서 제품 문구는 단계별로 구분해야 한다.

- v0: “준공/착공 기반 공급압력 분석”
- v1: “입주예정 단지 기반 공급맵”

---

## 4. 필요한 데이터 다운로드 agents

데이터 다운로드는 하나의 스크립트가 아니라 agent 역할을 나눠야 한다.

### 4.1 Agent A — Source Discovery Agent

역할:

- 필요한 데이터셋 registry 관리
- API key 필요 여부 확인
- 원천 URL/API endpoint 관리
- data_loader.py가 읽는 파일과 update_all.py가 수집하는 파일의 gap 확인

출력:

- DatasetSpec
- 수집 가능/불가 판단
- dry-run 계획
- 키 누락 리포트

핵심 DatasetSpec 필드:

```json
{
  "dataset_id": "movein_plan_complex",
  "source": "ApplyHome / 공공데이터포털",
  "frequency": "monthly",
  "grain": "complex",
  "target_file": "data/supply/movein_plan_complex_monthly.csv",
  "time_col": "입주예정연월",
  "key_columns": ["HOUSE_MANAGE_NO", "PBLANC_NO"],
  "required_columns": ["단지명", "주소", "입주예정연월", "세대수"],
  "env_keys": ["DATA_GO_KR_KEY"]
}
```

### 4.2 Agent B — API Collect Agent

역할:

- 실제 API 호출
- 기존 fetch 함수 adapter화
- 원천 raw 응답과 staging CSV 저장
- retry/backoff/rate limit 처리

주의:

- 최종 data/cache에 바로 쓰지 않는다.
- `.download_staging/{run_id}/`에 먼저 저장한다.
- API key는 로그/manifest에 원문 저장 금지.

### 4.3 Agent C — File Validate Agent

역할:

- 필수 컬럼 검증
- row count 검증
- 기간 min/max 검증
- 중복 key 검증
- 결측률/음수/이상치 검증
- 지역단위/시도명 정규화 검증

예: 입주예정 단지 데이터 검증

- 단지명 not null
- 주소 not null
- 입주예정연월 YYYY-MM 형식
- 세대수 > 0
- 시군구 매핑 성공률 95% 이상
- 중복 HOUSE_MANAGE_NO/PBLANC_NO 처리

### 4.4 Agent D — Standardize Agent

역할:

- 컬럼명을 앱 표준으로 변환
- dtype 정리
- 연월/연도/월 생성
- 시도/시군구 표준화
- 주소 geocoding 전처리
- utf-8-sig 저장

권장 표준 컬럼:

| 컬럼 | 설명 |
|---|---|
| source_name | 원천명 |
| source_dataset_id | 원천 dataset id |
| data_as_of | 데이터 기준일 |
| collected_at | 수집일시 |
| 시도 | 표준 시도명 |
| 시군구 | 표준 시군구명 |
| 지역코드 | 가능하면 5자리 |
| 주소 | 원천 주소 |
| 단지명 | 단지명 |
| 입주예정연월 | YYYY-MM |
| 세대수 | 정수 |
| 위도/경도 | 있으면 사용, 없으면 추후 geocoding |

### 4.5 Agent E — Manifest Agent

역할:

- 매 실행 결과를 json으로 남긴다.
- 데이터 재현성과 rollback의 기준이 된다.

저장 위치:

```text
manifests/download_runs/YYYYMMDD_HHMMSS_manifest.json
```

필수 필드:

- run_id
- dataset_id
- source
- mode: dry_run/incremental/full
- target_file
- staging_file
- previous_hash
- new_hash
- previous_rows
- new_rows
- min_period
- max_period
- validation_result
- publish_status
- errors/warnings

### 4.6 Agent F — Cache Publish Agent

역할:

- 검증 통과 파일만 최종 경로로 반영
- 기존 파일 백업
- os.replace 기반 원자적 교체
- 필요한 cache만 rebuild

반영 순서:

1. staging 저장
2. 검증 통과
3. 기존 target hash 계산
4. backup 생성
5. os.replace로 target 반영
6. manifest 기록
7. cache rebuild 여부 판단

---

## 5. 데이터 원천 우선순위

### 5.1 P0 — 입주·공급맵 필수

| 데이터 | 후보 원천 | 키 | 난이도 | 비고 |
|---|---|---|---:|---|
| 단지 단위 분양/입주예정 | 청약홈 ApplyhomeInfoDetailSvc | DATA_GO_KR_KEY | 중 | `MVN_PREARNGE_YM`류 입주예정월 필드 확인 필요 |
| 착공/준공 월별 | KOSIS/국토부 통계표 | KOSIS_API_KEY | 낮음~중 | 현재 시도 단위 보유. 시군구 가능 여부 추가 확인 |
| 인허가 | KOSIS/국토부 | KOSIS_API_KEY | 중 | 현재 permit.csv 최신성 낮음 |
| 미분양 | 국토부/공공데이터/BOK 대체 | DATA_GO_KR_KEY/BOK_API_KEY | 낮음 | 현재 보유 양호. 시군구/준공후 미분양 보강 검토 |
| 단지 좌표/주소 | K-apt/건축물대장/geocoding | DATA_GO_KR_KEY 등 | 중~상 | 전체 대량보다 후보 단지 보강 방식 권장 |

### 5.2 P1 — Superchart/투자판단 보강

| 데이터 | 후보 원천 | 키 | 필요성 |
|---|---|---|---|
| 주택가격지수 17개 시도 | BOK/한국부동산원/KOSIS | BOK/KOSIS | 지역별 index 비교 보강 |
| 인구이동 | BOK/KOSIS | BOK/KOSIS | 수요 변화 해석 |
| CSI/주택가격전망 | BOK ECOS | BOK | 시장심리 |
| KB 매수우위/거래/전세수급 | KB/PublicDataReader | 보통 키 없음 | 투자심리/수급 |
| KRIHS 부동산소비심리 | 국토연구원 | 확인 필요 | 지역 심리 |
| M2/예대금리차/가계신용 | BOK ECOS | BOK | 거시 환경 |
| 주택보급률 | KOSIS/국토부 | KOSIS | 공급부담 분모 |
| 국세청 근로소득 | KOSIS | KOSIS | 구매력/PIR 보강 |

### 5.3 P2 — 고급 기능

| 데이터 | 용도 |
|---|---|
| 청약 경쟁률/분양가 | 공급의 질·가격 매력도 판단 |
| 준공후 미분양 | 공급 리스크 고도화 |
| 학군/교통/POI | 지역검색기 고도화 |
| 단지 브랜드/시공사/세대구성 | 단지 상세 분석 |
| 게시판 사용 로그 | 인기 지표/프리셋 추천 |

---

## 6. 1주 / 3주 / 2개월 실행 로드맵

### 6.1 1주차 — 기반 안정화와 데이터 결손 확정

목표:

> 화면을 크게 만들기 전에, 저장/복원/metadata/데이터 수집 안전장치를 고정한다.

#### P0 작업

| ID | 작업 | 완료 기준 |
|---|---|---|
| W1-001 | pytest 실행 표준화 | `PYTHONPATH=. pytest -q` 또는 pytest.ini로 기존 8개 테스트 통과 |
| W1-002 | chart_settings schema v1 정의 | region/period/metrics/transform/axes/data_metadata_snapshot/schema_version 포함 |
| W1-003 | board 저장/복원 roundtrip 테스트 | saved_chart/post settings JSON이 동일하게 저장·복원 |
| W1-004 | data source metadata 최소 schema 정의 | source_name, data_as_of, collected_at, frequency, region_level, caution |
| W1-005 | 현재 보유 데이터 inventory 고정 | P0/P1/P2 gap 표 작성 및 README/roadmap 반영 |
| W1-006 | 입주·공급맵 v0 순수함수 설계 | 준공/착공 기반 filter/aggregate/summarize 테스트 |
| W1-007 | 데이터 다운로드 registry 초안 | data_loader.py가 읽는 파일 전체와 update_all.py 수집 파일 차이 표시 |
| W1-008 | 다운로드 pipeline dry-run 설계 | 실제 다운로드 없이 수집 계획/side effect 범위 출력 |

#### 1주차에서 하지 말 것

- app.py 전체 탭 분해
- 대량 다운로드 실행
- ASIL/상용 데이터 scraping
- 0~100 종합 투자점수 확정
- 게시판 커뮤니티 기능 확장

### 6.2 3주차 — MVP 동선 연결

목표:

> 입주·공급맵 → Superchart → 차트 아이디어 공유/복원 흐름을 실제로 연결한다.

#### P0 작업

| ID | 작업 | 완료 기준 |
|---|---|---|
| W3-001 | 입주·공급맵 v0 UI | 지역/기간/KPI/월별 bar/표/출처 표시 |
| W3-002 | Superchart context 전달 | 공급맵 선택 지역·기간·지표가 Superchart에 자동 반영 |
| W3-003 | TradingView형 차트 v1 | 입주/준공 bar + 매매/전세 line + 거래량/미분양 보조축 |
| W3-004 | 차트 아이디어 게시판 v1 | 차트 이미지 + settings JSON + 해석 + 태그 저장 |
| W3-005 | “이 차트 불러오기” | 게시글 settings를 Superchart로 복원 |
| W3-006 | data_metadata_snapshot 저장 | 게시글 작성 당시 기준일/출처/품질 snapshot 고정 |
| W3-007 | 다운로드 agent dry-run 구현 | registry 기반으로 수집 계획, 키 누락, target 경로 출력 |
| W3-008 | P0 데이터 소스 sample 수집 승인안 | 청약홈/KOSIS 등 어떤 API를 먼저 호출할지 사용자 승인 요청 |

#### 3주차 acceptance criteria

- 사용자가 입주·공급맵에서 지역/기간 선택 후 10초 안에 공급 부담을 볼 수 있다.
- Superchart에서 입주/준공, 매매, 전세, 거래량이 한 화면에 표시된다.
- 게시글 하나가 동일 차트를 복원할 수 있다.
- 출처/기준일 없는 지표는 공유 시 경고된다.
- 기존 8개 테스트 + 신규 저장/복원/공급집계 테스트가 통과한다.

### 6.3 2개월 — 내부 베타와 데이터 자동화

목표:

> 반복 사용 가능한 분석 플랫폼으로 확장한다.

#### 주요 작업

| 영역 | 작업 |
|---|---|
| 데이터 | 청약홈/공공데이터 기반 단지 단위 입주예정 v1 확보 |
| 데이터 | update_all.py를 pipeline v2 registry/orchestrator와 연결 |
| 데이터 | data_run_manifest 자동 생성 |
| 데이터 | cache/board.db/board_images 백업 루틴 문서화 |
| 제품 | 관심지역별 공급/가격/전세/거래 요약 |
| 제품 | 차트 아이디어 목록 필터: 지역/분석유형/태그/인기순 |
| 제품 | 룰 기반 진단 배지: 공급부담/가격조정/전세방어/거래회복 |
| 운영 | 월간 데이터 갱신 runbook |
| QA | 게시글 복원 실패율, kaleido 실패율, SQLite lock 기록 |

---

## 7. 투자판단 지표 방향

MVP에서는 0~100 종합점수보다 진단 배지를 우선한다.

### 7.1 추천 P0 진단 배지

| 진단 | 최소 산식 | 표시 방식 |
|---|---|---|
| 공급부담 | 향후/최근 준공 또는 입주물량 ÷ 최근 12개월 거래량 | 낮음/보통/높음/매우높음 |
| 가격조정 | 현재 평균단가 ÷ 최근 36개월 고점 - 1 | 조정중/바닥탐색/횡보/상승 |
| 전세방어 | 전세가율 변화 + 전세가격 YoY | 약함/보통/강함 |
| 거래회복 | 최근 3개월 거래량 ÷ 직전 12개월 평균 | 미확인/초기/확인 |
| 종합시그널 | 룰 기반 조건 충족 | 관망/주의/후보/강한후보 |

### 7.2 표시 원칙

- 반영 지표 수 n/m 표시
- 최저 기준월 표시
- 최저 품질등급 표시
- 시도/시군구 혼합 시 명시
- 데이터 품질 C 이하이면 “판단보류” 또는 강한 경고

---

## 8. 기술 아키텍처 방향

### 8.1 app.py 리팩토링 원칙

바로 큰 리팩토링을 하지 않는다.

우선순위:

1. tests 추가
2. metadata 모듈 분리
3. board settings contract 안정화
4. 입주·공급 순수함수 분리
5. Superchart service 분리
6. views/superchart.py부터 점진 분리

### 8.2 추천 폴더 구조

```text
real_estate_analysis/
  app.py
  analysis.py
  board.py
  data_loader.py
  update_all.py
  config/
    indicators.py
    data_sources.py
  services/
    supply_service.py
    superchart_service.py
    chart_settings_service.py
    data_quality_service.py
  download_pipeline/
    registry.py
    orchestrator.py
    agents/
      source_discovery.py
      api_collect.py
      file_validate.py
      standardize.py
      manifest.py
      cache_publish.py
  tests/
    test_supply_service.py
    test_superchart_service.py
    test_chart_settings.py
    test_data_quality.py
    test_download_pipeline.py
```

---

## 9. QA / 운영 체크리스트

### 9.1 자동 테스트

```bash
PYTHONPATH=. pytest -q
python -m py_compile app.py analysis.py data_loader.py board.py update_all.py
```

추가할 테스트:

- chart_settings roundtrip
- post settings 복원
- create_post image fallback
- 공급 데이터 filter/aggregate/summarize
- data_metadata quality grade
- download registry 필수 필드
- manifest에서 API key 미노출
- staging publish 실패 시 final 파일 미변경

### 9.2 수동 검증

입주·공급맵:

- 지역/기간 선택 가능
- KPI 표시
- 월별 bar 표시
- 데이터 없음 처리
- 기준일/출처/주의문구 표시

Superchart:

- 공급맵 context 유지
- 입주/가격/전세/거래량 동시 표시
- 보조축 단위 명확
- 저장 후 복원

차트 아이디어:

- 공유 버튼 표시
- 제목/해석/태그 입력
- 이미지 또는 fallback 표시
- settings JSON 저장
- “이 차트 불러오기” 동작
- metadata snapshot 유지

운영:

- board.db 백업/복구
- board_images 누락 처리
- cache 누락 경고
- kaleido 실패 처리
- SQLite lock 재시도 안내

---

## 10. 사용자 승인 필요 작업

다음은 반드시 사용자 승인 후 실행한다.

| 작업 | 승인 필요 이유 |
|---|---|
| 외부 API 호출 | API quota/로그/키 사용 발생 |
| 청약홈/공공데이터 대량 호출 | 호출량·활용신청·라이선스 확인 필요 |
| 최종 data/ 또는 OneDrive 수요_집계 CSV 덮어쓰기 | 원본/운영 데이터 변경 |
| cache/*.parquet 삭제/재생성 | 앱 수치 영향 |
| board.db/board_images 변경 | 사용자 게시글/이미지 영향 |
| PowerShell fallback 실행 | Windows 환경 네트워크 호출 발생 |
| 파일 이동/정리 | 원본/산출물 경로 변경 |

권장 실행 방식:

1. dry-run
2. sample download
3. staging validation
4. 사용자 확인
5. publish
6. cache rebuild
7. smoke test

---

## 11. 즉시 다음 액션

### 11.1 의사결정

1. 입주·공급맵 v0를 “준공/착공 기반 공급압력”으로 먼저 낼지, 단지 입주예정 데이터 확보 후 v1로 낼지 결정
2. 청약홈 API를 P0 수집 대상으로 승인할지 결정
3. 게시판 명칭을 “차트 아이디어”로 고정할지 결정
4. chart_settings schema v1 필수 필드를 확정
5. 데이터 품질등급 A/B/C/D 기준을 확정

### 11.2 구현 착수 후보

P0 순서:

1. `tests/test_chart_settings.py`
2. `services/chart_settings_service.py`
3. `services/supply_service.py`
4. `tests/test_supply_service.py`
5. `config/data_sources.py`
6. `services/data_quality_service.py`
7. `download_pipeline/registry.py`
8. `download_pipeline/orchestrator.py --dry-run`

---

## 12. 최종 권고

현재 프로젝트는 이미 가격·전세·거래 분석 기반이 강하다. 따라서 새로 만들어야 할 핵심은 “분석 기능” 자체가 아니라 다음 네 가지다.

1. **단순한 입구**  
   - ASIL처럼 공급을 빠르게 보여주는 화면

2. **깊은 검증 캔버스**  
   - Superchart에서 공급 이벤트와 가격·전세·거래를 겹쳐 보는 구조

3. **재현 가능한 공유**  
   - 게시판을 차트 아이디어 저장소로 만들고 settings JSON으로 복원

4. **신뢰 가능한 데이터 파이프라인**  
   - 필요한 데이터 판단, 안전한 다운로드, 검증, manifest, cache 반영

특히 “필요한 데이터가 무엇인가”는 다음처럼 결론낸다.

- 이미 충분: 매매/전세/월세 실거래, 거래량, 미분양, 금리, 주담대, NPS
- 즉시 필요: 단지 단위 입주예정, 단지 주소/좌표, 데이터 기준일/출처 metadata
- 보강 필요: 인허가 최신화, 주택가격지수 17개 시도, 인구이동 정상화, KB/CSI/KRIHS/M2/예대금리차/가계신용
- 후순위: 청약경쟁률, 분양가, POI, 단지 브랜드, 고급 커뮤니티 로그

이 기준으로 데이터 agents를 추가하는 것은 맞다. 단, 처음부터 대량 다운로드를 시키기보다 **dry-run registry agent → sample collector → validator → manifest publisher** 순서로 안전하게 붙여야 한다.

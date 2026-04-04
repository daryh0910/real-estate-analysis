# 부동산 가격분석 플랫폼 (Real Estate Analysis Dashboard)

## 프로젝트 개요
부동산 실거래 데이터 + 거시경제 지표를 결합한 **Streamlit 기반 분석 대시보드**.

## 기술 스택
- **런타임**: Python 3.12+
- **프레임워크**: Streamlit
- **시각화**: Plotly
- **통계/ML**: scipy, statsmodels, scikit-learn
- **데이터**: pandas, numpy, pyarrow (Parquet 캐시)
- **API**: BOK ECOS, data.go.kr, KOSIS

## 핵심 파일 구조
```
real_estate_analysis/
├── app.py                    # 메인 Streamlit 대시보드 (9개 탭)
├── data_loader.py            # 데이터 로딩/전처리/병합 엔진
├── analysis.py               # 분석 함수 (상관관계, 회귀, 이상치, 클러스터, Granger)
├── download_public_data.py   # 공공데이터 API 수집 (BOK ECOS 7종)
├── download_demand_data.py   # 수요 데이터 수집 (NPS/BOK/KOSIS)
├── update_data.py            # 실거래 데이터 업데이트
├── update_all.py             # 통합 업데이트 (4개 스크립트 순차 실행, 증분 지원)
├── build_cache.py            # Parquet 캐시 빌더
├── requirements.txt          # Python 의존성
├── .env.example              # 환경변수 템플릿
├── cache/                    # Parquet 캐시 파일
└── .streamlit/               # Streamlit 설정
```

## 대시보드 탭 구성 (9개)
1. **Overview** — KPI 카드 + 가격/거래량 추이
2. **상관관계 분석** — 히트맵 + 산점도 + 시도별/연도별 상관계수
3. **시계열 비교** — 듀얼 Y축 + 정규화 비교
4. **지역별 분석** — 시군구 순위 + 요약 통계
5. **수식 계산기** — 사용자 정의 수식 (최대 4개)
6. **회귀분석** — 다중회귀 OLS + 변수 중요도
7. **이상치 탐지** — Z-score/IQR + 산점도
8. **클러스터링** — K-Means + 레이더 차트
9. **Granger 인과성** — 시도별 lag별 p값 히트맵

## 데이터 소스 (19종)
| 상태 | 데이터 | 출처 |
|------|--------|------|
| ✅ | 아파트 매매/전세/월세 실거래 | 부동산거래현황 CSV |
| ✅ | 인구(연령별), GRDP, 인허가 | 통계청/지역경제통계 CSV |
| ✅ | 국민연금(NPS) | 국민연금공단 CSV |
| ✅ | 주담대, 가계자산 | BOK ECOS / KOSIS API |
| ✅ | 미분양, 금리, 전월세전환율, 주택가격지수, 지가변동률 | BOK ECOS API |
| ✅ | 국세청 근로소득 (시군구별, 2016~) | KOSIS API (orgId=133) |
| ✅ | 소득5분위별 가계자산/부채/소득 | KOSIS API (DT_1HDAAA10) |
| ✅ | KB 매수우위지수/매매거래/전세수급 | KB부동산 (PublicDataReader) |
| ✅ | 소비자심리지수(CSI) | BOK ECOS API |
| ✅ | 정책 이벤트 DB (64건) | 수작업 CSV |
| 🔧 | 인구이동(전입/전출) | 수집함수 작성완료 |
| 🔧 | 착공/준공 실적 | 수집함수 작성완료 |

## 환경변수 설정
`.env.example`을 `.env`로 복사 후 API 키를 입력하세요.

## 코딩 규칙
- 한글 변수명 사용 가능 (데이터프레임 컬럼)
- Streamlit 캐싱: @st.cache_data 사용
- 데이터 로딩: data_loader.py의 load_all_data() 엔트리포인트
- 새 분석 함수: analysis.py에 추가
- 새 탭: app.py에 추가 (탭 번호 순서 유지)

## 데이터 소스 탐색 프로토콜 (필수)

### 세션 시작 시 자동 실행
데이터 수집/지표 관련 작업이 있을 때마다 **별도 에이전트**를 백그라운드로 실행하여 더 적합한 지표나 신규 데이터를 탐색한다.

```python
# 에이전트 실행 트리거 조건
- 새 지표 추가 논의 시
- 데이터 품질 문제 발생 시
- 분석 한계 지적 시
- 명시적 요청 시
```

### 탐색 대상 데이터 소스
| 소스 | URL | 특징 | API 키 |
|------|-----|------|--------|
| **SGIS** | sgisapi.mods.go.kr | 연령대별/성별/1인가구/주택통계, 시군구 단위 | consumer_key + consumer_secret |
| **KOSIS** | kosis.kr/openapi | 통계청 전체, 400만+ 통계표 | KOSIS_API_KEY |
| **BOK ECOS** | ecos.bok.or.kr/api | 금융/경제 지표, 월별 시계열 | BOK_API_KEY |
| **data.go.kr** | data.go.kr | 국토부/행안부/통계청 공공데이터 | DATA_GO_KR_KEY |

### 탐색 에이전트 프롬프트 템플릿
새 지표 탐색이 필요하면 아래 프롬프트로 Explore 에이전트를 백그라운드 실행:

```
[탐색 목적]: [수요/공급/심리/인구 등 카테고리]에서 더 나은 지표 탐색
[현재 지표]: [현재 사용 중인 관련 지표명]
[탐색 소스]: SGIS, KOSIS, BOK ECOS, data.go.kr
[확인 항목]:
  1. 지역 단위 (시도/시군구/읍면동)
  2. 시계열 범위 및 업데이트 주기
  3. API 파라미터 (연령대별, 성별 등 세분화 가능 여부)
  4. 기존 지표 대비 장점
  5. Python 수집 코드 초안
```

### 기존 데이터 소스 현황 (탐색 시 중복 방지)
- ✅ BOK ECOS: 금리, 주담대, CSI, 인구이동(101Y008/009), 주택건설실적, 지가변동률
- ✅ KOSIS: 가구자산(DT_1HDAAA10), 국세청 소득(orgId=133, DT_133001N_4215)
- ✅ data.go.kr: 미분양(1613000/KMTL_003)
- ✅ PublicDataReader: KB 매수우위/매매거래/전세수급
- ✅ NPS: 국민연금 가입자/고지금액
- 🔧 SGIS: API 키 발급 완료, consumer_secret 추가 필요

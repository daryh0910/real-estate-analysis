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

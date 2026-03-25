# 부동산 가격분석 대시보드

Streamlit 기반 부동산 실거래 데이터 + 거시경제 지표 통합 분석 플랫폼.

## Quick Start

```bash
cd real_estate_analysis
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 데이터 수집
python download_public_data.py --all
python download_demand_data.py
python update_data.py

# 대시보드 실행
streamlit run app.py
```

## 주요 기능

| 탭 | 기능 | 활용 |
|----|------|------|
| Overview | KPI 카드 + 가격/거래량 추이 | 시장 전체 조망 |
| 상관관계 | 히트맵 + 산점도 | 변수 간 관계 파악 |
| 시계열 | 듀얼 Y축 비교 | 선행/후행 지표 확인 |
| 지역별 | 시군구 순위 | 투자 유망 지역 탐색 |
| 수식 계산기 | 사용자 정의 수식 | 커스텀 지표 생성 |
| 회귀분석 | 다중회귀 OLS | 가격 결정 요인 분석 |
| 이상치 탐지 | Z-score/IQR | 급등/급락 지역 감지 |
| 클러스터링 | K-Means | 유사 지역 군집화 |
| Granger 인과성 | lag별 F-test | 선행지표 식별 |

## 데이터 소스

공공데이터 17종 (실거래, 인구, GRDP, 인허가, 국민연금, 주담대, 가계자산, 미분양, 금리, 전월세전환율, 주택가격지수, 지가변동률, 인구이동, 착공/준공)

## 프로젝트 구조

```
├── app.py                    # 메인 대시보드
├── data_loader.py            # 데이터 로딩/전처리 엔진
├── analysis.py               # 통계 분석 함수
├── download_public_data.py   # 공공데이터 API 수집
├── download_demand_data.py   # 수요 데이터 수집
├── update_data.py            # 실거래 데이터 갱신
├── requirements.txt          # Python 의존성
├── CLAUDE.md                 # Claude Code 프로젝트 컨텍스트
├── SPRINT_2_PLAN.md          # 현재 스프린트 계획
└── cache/                    # Parquet 캐시
```

## 환경변수 (권장)

```bash
export DATA_GO_KR_API_KEY="your_api_key"
export BOK_API_KEY="your_bok_key"
```

## 개발 현황

- **Sprint 1** (2025-02 ~ 2026-03-13): 데이터 14종 + 분석 9탭 구축 완료
- **Sprint 2** (2026-03-19 ~ 03-25): Phase C — 자동화/리포트/알림 인프라

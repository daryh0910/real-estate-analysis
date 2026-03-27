# 부동산 가격분석 대시보드

Streamlit 기반 부동산 실거래 데이터 + 거시경제 지표 통합 분석 플랫폼.

## Quick Start

```bash
git clone https://github.com/daryh0910/real-estate-analysis.git
cd real-estate-analysis
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 환경변수 설정
cp .env.example .env
# .env 파일을 열어 API 키를 입력하세요
```

## API 키 발급

이 프로젝트는 한국 공공데이터 API를 사용합니다. 아래 사이트에서 무료로 발급받을 수 있습니다:

| 환경변수 | 발급처 | 용도 |
|---------|--------|------|
| `BOK_API_KEY` | [한국은행 ECOS](https://ecos.bok.or.kr/api/#/) | 금리, 대출, 주택가격지수 등 |
| `DATA_GO_KR_KEY` | [공공데이터포털](https://www.data.go.kr/) | 미분양, 인구이동 등 |
| `MOLIT_API_KEY` | [공공데이터포털](https://www.data.go.kr/) | 아파트 실거래가 |
| `KOSIS_API_KEY` | [KOSIS](https://kosis.kr/openapi/) | 가계금융복지조사 |

## 데이터 수집 & 실행

```bash
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

공공데이터 17종: 실거래(매매/전세/월세), 인구, GRDP, 인허가, 국민연금, 주담대, 가계자산, 미분양, 금리, 전월세전환율, 주택가격지수, 지가변동률, 인구이동, 착공/준공

## 프로젝트 구조

```
├── app.py                    # 메인 대시보드
├── data_loader.py            # 데이터 로딩/전처리 엔진
├── analysis.py               # 통계 분석 함수
├── download_public_data.py   # 공공데이터 API 수집
├── download_demand_data.py   # 수요 데이터 수집
├── update_data.py            # 실거래 데이터 갱신
├── requirements.txt          # Python 의존성
├── .env.example              # 환경변수 템플릿
└── cache/                    # Parquet 캐시
```

## License

MIT

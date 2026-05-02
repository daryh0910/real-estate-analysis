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

## 배포

이 앱은 `cache/`, `data/`, `geo_data/`에 포함된 파일만으로 읽기 전용 Streamlit 대시보드 배포가 가능합니다. 일반 배포에는 API 키가 필요하지 않습니다.

```bash
python -m py_compile app.py
streamlit run app.py --server.headless true --server.port 8503
```

Streamlit Community Cloud에서는 main file을 `app.py`로 지정하고, `runtime.txt`, `requirements.txt`, `packages.txt`를 함께 배포하세요. 자세한 절차는 `DEPLOYMENT.md`를 참고하세요.

## 주요 기능

| 탭 | 기능 | 활용 |
|----|------|------|
| Overview | 시장 온도계 + 역사 흐름 + 핵심 트렌드 | 반복 국면 조망 |
| 수요공급분석 | 수요, 공급, 2차가공 지표 | 구조적 변화 확인 |
| 거래현황 | 실거래 + 네이버 매물 업로드 | 현재 거래/호가 비교 |
| 매물찾기 | 구매력, 급지, 매물 필터 | 후보 지역 탐색 |
| 적정값가상계산 | 과거분석, 통계검증, 예측, 투자 계산 | 가설 검증과 시나리오 계산 |

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

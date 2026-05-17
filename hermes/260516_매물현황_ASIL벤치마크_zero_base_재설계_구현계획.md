# Plan: 매물현황 ASIL 벤치마크 기반 zero-base 재설계
> 작성일: 2026-05-16

## 1. 목표
- **최종 목표:** 기존 `매물찾기` 탭의 단순 네이버 매물 후보 표를 ASIL식 “지역/기간/공급량/리스트” 흐름의 `매물현황` 대시보드로 재설계하고, Streamlit 앱에 구현한다.
- **성공 기준:** 업로드된 매물 데이터가 있을 때 KPI, 필터, 호가 분포, 지역별/단지별 요약, 원본 목록이 표시되고, 데이터가 없거나 필터 결과가 없을 때 명확한 빈 상태가 표시된다. 기존 테스트와 Streamlit 렌더 검증을 통과한다.

## 2. 작업 분해
| # | 작업 | 의존성 | 복잡도 | 비고 |
|---|------|--------|--------|------|
| 1 | 10개 에이전트 벤치마크/AS-IS/TO-BE 의견 수집 | - | 중간 | ASIL, 스크리너, Streamlit, 데이터품질, 테스트 관점 |
| 2 | app.py 매물찾기 AS-IS 위치와 데이터 흐름 확인 | #1 | 낮음 | `normalize_naver_listings`, `listing_tab` |
| 3 | 매물현황 helper 추가 | #2 | 중간 | 가격/문자 정리, KPI, 필터, 단지 요약 |
| 4 | 기존 네이버 매물 후보 블록을 매물현황 대시보드로 교체 | #3 | 중간 | 최소 변경: app.py 내부만 수정 |
| 5 | 정적 테스트 추가/보강 | #4 | 낮음 | helper/문구 존재 확인 |
| 6 | py_compile, pytest, Streamlit AppTest 검증 | #5 | 중간 | 브라우저 자동화 없이 검증 |

## 3. 에이전트 전략
- **사용자 지정:** 10개 에이전트
- **실행 방식:** max concurrent 제한으로 3+3+3+1 배치 실행
- **역할:** ASIL 벤치마크, zero-base IA, TradingView 스크리너, AS-IS 코드 점검, Streamlit UI 패턴, 데이터품질, UX 카피, AS-IS/TO-BE 비교, 테스트전략, 최소변경 구현스펙
- [x] 사용자 확인 완료: “agent10명 구성” 명시

## 4. 리스크 & 의존성
- **리스크:** app.py가 4천 라인 이상 모놀리스라 대규모 구조 변경은 회귀 위험이 큼 → 기존 `4030~4072` 네이버 매물 후보 블록 중심으로 최소 변경.
- **리스크:** 현재 가격 파싱이 `억/만원` 혼합 표현에 약함 → 이번 구현에서 `_parse_korean_price_to_manwon`를 추가해 대표 케이스 보강.
- **리스크:** 네이버 매물 데이터는 session_state 업로드 기반이라 영속 저장은 없음 → 데이터 없음 상태와 업로드 안내를 명확히 표시.
- **외부 의존성:** pandas, plotly, streamlit, pytest.
- **병목:** Streamlit 렌더 전체 실행 시간이 길 수 있음.

## 5. 검증 기준
- [x] `python -m py_compile app.py`
- [x] `python -m pytest tests/test_app_static.py -q`
- [x] `python -m pytest -q`
- [x] `streamlit.testing.v1.AppTest`로 app.py 렌더 예외 0건 확인
- [x] 매물현황 관련 필수 UI 문구/함수 정적 테스트 통과
- [x] Streamlit 서버 health check: `/_stcore/health` → `ok`, HTTP 200

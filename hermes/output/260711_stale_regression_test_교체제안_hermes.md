# 원본 regression test 교체 제안
> 작성일: 2026-07-11
> 대상: `tests/test_app_buy_decision_static.py`
> 상태: 제안만 작성, 원본 미적용

## 문제

기존 테스트는 소스에서 가장 먼저 나타나는 `st.tabs([...])`를 top-level navigation으로 간주한다. 현재 앱은 top-level navigation을 `_PAGES` + `st.segmented_control()`로 구현하고, `st.tabs()`는 페이지 내부 subtab에만 사용한다.

따라서 현재 실패는 앱 기능 회귀가 아니라 테스트가 이전 UI 구조에 고정된 결과다.

## 권장 교체 기준

1. AST로 `_PAGES`의 문자열 목록을 읽는다.
2. `st.segmented_control(..., _PAGES)`가 실제 사용되는지 확인한다.
3. 첫 페이지에 `Overview`가 포함되는지 확인한다.
4. 전체 목록에 `매수판단`이 포함되는지 확인한다.
5. `st.tabs()` subtab 순서는 top-level page 검증에서 제외한다.

## 이미 구현된 재사용 코드

- `hermes/scripts/260711_real_estate_quality_gate_hermes.py`
  - `extract_segmented_navigation()`
  - `extract_primary_tabs()` fallback
  - `audit_app_source()`

## 적용 후 예상 결과

```plain text
현재 원본 suite: 72 passed, 1 failed, 4 warnings
stale test 교체 후 예상: 73 passed, 4 warnings
```

예상 결과는 원본 테스트를 실제 수정·실행하기 전에는 성공으로 간주하지 않는다.

## 별도 후속

`data_loader.py`의 pandas `replace` downcasting FutureWarning 4건은 동작 실패는 아니지만 향후 pandas upgrade 전에 명시적 dtype 처리가 필요하다.

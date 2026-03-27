# Plan: GitHub Public 공개를 위한 API 키 제거 & 히스토리 정리
> 작성일: 2026-03-28

## 1. 목표
- **최종 목표:** 부동산 분석 프로젝트를 GitHub public으로 안전하게 공개
- **성공 기준:**
  - 모든 .py 파일에서 하드코딩된 API 키 제거
  - Git 히스토리에서 API 키 흔적 완전 삭제
  - `.env.example` + 환경변수 기반 키 로딩으로 전환
  - README에 설치/설정 가이드 반영

## 2. 작업 분해
| # | 작업 | 의존성 | 복잡도 | 비고 |
|---|------|--------|--------|------|
| 1 | 3개 .py 파일에서 API 키 → `os.environ.get()` 전환 | - | 낮음 | download_public_data.py, download_demand_data.py, update_data.py |
| 2 | `.env.example` 생성 (플레이스홀더 키) | - | 낮음 | |
| 3 | `app.py`/`data_loader.py`에서 `st.secrets` 사용 확인 | - | 낮음 | 이미 st.secrets 사용 중이면 변경 불필요 |
| 4 | `.gitignore`에 `.env` 확인 | - | 낮음 | 이미 있음 |
| 5 | OneDrive 경로 하드코딩 범용화 | - | 중간 | _detect_onedrive() → 환경변수 또는 상대경로 |
| 6 | Git 히스토리 rewrite (`git filter-branch` 또는 `git-filter-repo`) | #1~#5 | 높음 | force push 필요 |
| 7 | README.md 업데이트 (설치/환경변수 가이드) | #1~#5 | 낮음 | |
| 8 | CLAUDE.md에서 API 키 제거 | - | 낮음 | 커밋 히스토리에 포함됨 |
| 9 | GitHub force push + 확인 | #6 | 중간 | |

## 3. 에이전트 전략
- **추천:** 단일 에이전트 (순차 처리)
- **근거:** 작업 간 의존성이 높고(#1~#5 → #6 → #9), 파괴적 git 작업 포함
- [ ] 사용자 확인 완료

## 4. 리스크 & 의존성
- **리스크:** force push 후 기존 히스토리 복구 불가 → 사전 백업 필수
- **리스크:** API 키가 이미 노출됨 → 작업 후 키 재발급 권장
- **외부 의존성:** `git-filter-repo` 설치 필요 (pip install git-filter-repo)
- **병목:** Git 히스토리 rewrite 단계

## 5. 검증 기준
- [ ] `grep -r "4AZC0DPNJE" .` → 결과 없음
- [ ] `grep -r "EAocop7EB0O8" .` → 결과 없음
- [ ] `grep -r "901c4f2792" .` → 결과 없음
- [ ] `git log --all -p | grep "API_KEY"` → 하드코딩 키 없음
- [ ] `.env.example` 존재, `.env` 는 .gitignore에 포함
- [ ] `streamlit run app.py` 정상 동작 (`.env` 또는 `.streamlit/secrets.toml` 기반)

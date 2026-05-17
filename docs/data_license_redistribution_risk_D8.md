# D8. 입주물량 데이터 라이선스·출처·재배포 리스크 검토

> 작성일: 2026-05-12  
> 목적: ASIL보다 약한 공급데이터를 보강하되, 원천·라이선스·재배포 리스크를 통제

---

## 1. 핵심 결론

입주물량 보강은 필수이나, 데이터 출처 리스크를 통제하지 않으면 외부 공유/게시판/서비스화 단계에서 문제가 발생할 수 있음.

운영 원칙은 다음으로 고정한다.

1. ASIL은 UX 벤치마크로만 사용한다.
2. ASIL 또는 유료/상용 서비스의 데이터베이스를 무단 복제하지 않는다.
3. 공공데이터는 이용조건, 출처표시, API key 관리, 재배포 가능성을 metadata에 기록한다.
4. 사용자 보유 파일은 `license_note`, `source`, `수집일시`, `원천파일명`, `허용범위`를 필수로 기록한다.
5. 게시판 공유 시 차트와 함께 `data_metadata_snapshot`을 저장해 데이터 출처와 기준일을 노출한다.

---

## 2. 데이터 분류 정책

| 분류 | 설명 | 앱 사용 | 외부 공유 | 예시 |
|---|---|---|---|---|
| 허용 | 공공데이터 이용조건 충족, 자체 작성, 재배포 가능 또는 내부 사용 명확 | 가능 | 조건부 가능 | 청약홈 API, K-apt API, KOSIS, 직접 작성 파일 |
| 조건부 허용 | 내부 업무용, 외부 재배포 제한, 출처표시 필요 | 내부 가능 | 제한 | 사용자 수집 엑셀, 지자체 파일 재가공 |
| 보류 | 출처 불명확, 상용 서비스 유래 가능성, 라이선스 확인 전 | 앱 반영 보류 | 금지 | 출처 없는 입주물량 대량 파일 |
| 금지 | 상용 서비스 DB/화면 무단 복제, 약관 위반 가능성 높음 | 금지 | 금지 | ASIL/부동산지인/닥터아파트 등에서 무단 추출한 데이터 |

---

## 3. 원천별 정책

## 3.1 청약홈 ApplyHome API

- 사용 목적: 분양 공고 기반 단지명, 공급위치, 입주예정월, 공급세대수 확보
- 정책:
  - 공공데이터포털 이용조건 확인
  - API key 원문 저장 금지
  - 출처: 한국부동산원/청약홈 명시
  - 공고 기반 예정치이므로 실제 입주월과 다를 수 있음을 표시
- metadata 필수:
  - source_name: 한국부동산원 청약홈
  - source_url
  - data_as_of
  - collected_at
  - license_note
  - caution: 공고 기준 예정 데이터

## 3.2 K-apt 공동주택 정보

- 사용 목적: 사용승인일, 세대수, 단지명/주소 보정
- 정책:
  - 공공데이터포털 공식 API 우선
  - K-apt 웹 내부 API에 운영 의존하지 않음
  - 신규 분양 예정 단지 누락 가능성 표시
- metadata 필수:
  - source_name: K-apt/국토교통부 공동주택관리정보
  - quality_grade: 보정용 마스터

## 3.3 건축물대장/건축HUB

- 사용 목적: 실제 사용승인일, 건축/주택 인허가 정보 보정
- 정책:
  - 후보 단지에 한정 조회 권장
  - 대량 수집 시 API 이용량/약관 준수
  - 개인정보/민감정보는 수집하지 않음
- metadata 필수:
  - source_name: 국토교통부 건축HUB
  - source_dataset_id
  - matched_by: 주소/법정동코드/지번 등
  - match_score

## 3.4 KOSIS/국토부 공식 공급집계

- 사용 목적: 시도/시군구 공급 총량, v0 공급압력, v1 단지 데이터 총량 검증
- 정책:
  - 집계 통계로 앱 공개에 상대적으로 안전
  - 출처와 통계표명을 표시
  - 단지 단위 입주물량 대체가 아니라 검증/보조로 사용

## 3.5 사용자 보유 파일

- 사용 목적: 가장 빠른 단지 단위 입주예정 데이터 보강
- 정책:
  - 원천파일명, 작성자, 수집일시, license_note 필수
  - 출처 불명확/상용 의심 자료는 `status=hold`로 분리
  - 외부 공개 전 허용범위 재확인
- metadata 필수:
  - source=user_file
  - source_batch_id
  - 원천파일명
  - 파일해시_sha256
  - license_note
  - 허용범위: internal_only / share_allowed / public_allowed / hold

## 3.6 ASIL 및 상용 서비스

- 정책:
  - ASIL은 UX 벤치마크만 허용
  - 화면 구조, 질문, 사용자 흐름은 참고 가능
  - 데이터 직접 수집/복제/스크래핑은 금지
  - 상용 서비스 데이터로 의심되는 사용자 파일은 보류

---

## 4. source_metadata 필수 필드

`data/source_metadata.json` 또는 `data/supply/metadata/source_registry.csv`에 다음을 포함한다.

```json
{
  "dataset_id": "movein_plan_complex",
  "source_name": "user_file / applyhome / kapt / building_hub",
  "source_url": "",
  "license_note": "",
  "allowed_scope": "internal_only/share_allowed/public_allowed/hold/forbidden",
  "data_as_of": "YYYY-MM-DD",
  "collected_at": "YYYY-MM-DD HH:MM:SS",
  "update_frequency": "ad_hoc/monthly/realtime",
  "region_granularity": "complex/sigungu/sido",
  "time_granularity": "monthly",
  "quality_grade": "A/B/C/D",
  "caution": "",
  "redistribution_policy": ""
}
```

---

## 5. 게시판 공유 시 data_snapshot 정책

차트 아이디어 게시판에는 다음을 반드시 저장한다.

```json
{
  "data_metadata_snapshot": {
    "created_at": "",
    "manifest_run_id": "",
    "sources": [
      {
        "metric": "입주예정_세대수",
        "source_name": "청약홈/user_file/K-apt",
        "data_as_of": "",
        "quality_grade": "",
        "allowed_scope": "",
        "license_note": "",
        "caution": ""
      }
    ]
  }
}
```

외부 공개 모드에서는 `allowed_scope`가 `public_allowed` 또는 `share_allowed`가 아닌 데이터는 게시판 공유를 제한한다.

---

## 6. 공개 범위별 운영 정책

| 모드 | 데이터 허용 | 게시판 공유 | 권장 저장소 |
|---|---|---|---|
| LOCAL_DEV | 대부분 허용, 금지 데이터 제외 | 로컬만 | SQLite |
| INTERNAL | internal_only 허용 | 사내만 | SQLite/Postgres |
| PUBLIC_READONLY | public_allowed만 | 읽기 전용 | 정적 cache |
| COMMUNITY | public/share_allowed만 | 가능 | 외부 DB + storage |

환경변수 예시:

```text
APP_DATA_SCOPE=local_dev/internal/public_readonly/community
STRICT_LICENSE_CHECK=true
```

---

## 7. Validation checklist

입주물량 파일 ingest 전 체크:

- [ ] source_name 존재
- [ ] source_url 또는 원천파일명 존재
- [ ] license_note 존재
- [ ] allowed_scope 존재
- [ ] 수집일시 존재
- [ ] data_as_of 존재
- [ ] 상용 서비스 무단 복제 의심 여부 확인
- [ ] 외부 공개 가능 여부 확인
- [ ] API key/secret이 파일에 포함되지 않았는지 확인

차트 공유 전 체크:

- [ ] data_metadata_snapshot 포함
- [ ] 모든 source의 allowed_scope가 현재 앱 모드와 호환
- [ ] 출처/기준일/주의문구 표시
- [ ] 사용자 파일의 원천명이 민감정보를 포함하지 않음

---

## 8. 금지 사항

- API key, serviceKey, client secret을 소스/문서/manifest에 원문 저장
- ASIL 등 상용 서비스 화면/DB를 무단 수집하여 앱 데이터로 사용
- 출처 불명 파일을 public/community 모드로 노출
- 공공데이터 원천명을 제거하고 자체 데이터처럼 표시
- 품질등급 D 또는 hold 데이터를 투자판단 지표로 확정 표시

---

## 9. 결론

입주물량 보강은 P0이지만, 데이터 출처 통제도 P0임. 가장 안전한 실행 순서는 다음임.

1. 청약홈/K-apt/건축HUB/KOSIS 등 공공 원천 우선
2. 사용자 보유 파일은 `internal_only`로 먼저 ingest
3. source_registry와 data_metadata_snapshot 필수화
4. 상용 서비스 유래 의심 데이터는 hold 또는 forbidden 처리
5. 외부 공개 전 allowed_scope를 기준으로 필터링

이 정책을 적용하면 ASIL보다 약한 공급데이터를 보강하면서도 서비스화 리스크를 줄일 수 있음.

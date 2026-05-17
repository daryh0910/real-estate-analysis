# 입주예정 데이터 실제 확보 결과

작성일: 2026-05-12

## 1. 결론

실제 입주예정/준공예정 데이터를 찾아 프로젝트 표준 CSV로 저장하였음.

표준 산출물:

```text
data/supply/movein_plan_complex_monthly.csv
```

총 확보 행수:

```text
1,079행
```

비어 있지 않은 입주예정연월 범위:

```text
2017-12 ~ 2032-10
```

단, 2017~2025년은 일부 지자체 과거 준공예정 데이터가 포함된 값이고, 실제 앱의 기본 분석 범위는 2026년 이후로 필터링하는 것을 권장함.

---

## 2. 확보한 실제 원천

| source | 원천명 | 행수 | 세대수 합계 | 기간 | 성격 |
|---|---|---:|---:|---|---|
| reb_movein_plan | 한국부동산원_주택공급정보_입주예정물량정보 | 675 | 414,906 | 2026-01 ~ 2027-12 | 입주예정월 직접 제공 |
| lh_public_completion | 한국토지주택공사_공공주택 준공예정현황 | 351 | 151,590 | 2026-01 ~ 2032-10 | 준공예정일 제공, 입주 proxy |
| local_construction | 지자체/기관 공동주택 시공·준공계획 파일 | 53 | 28,903 | 2017-12 ~ 2029-02 | 준공예정/입주시기 제공 |

총 세대수 합계:

```text
595,399세대
```

---

## 3. raw 파일 저장 위치

```text
data/supply/raw/public_files/15111714.csv
한국부동산원_주택공급정보_입주예정물량정보_20251231

데이터 URL:
https://www.data.go.kr/data/15111714/fileData.do
```

```text
data/supply/raw/public_files/15141761.csv
한국토지주택공사_공공주택 준공예정현황_20260127

데이터 URL:
https://www.data.go.kr/data/15141761/fileData.do
```

```text
data/supply/raw/public_files/3079705.csv
부산광역시_동래구_주택건설사업계획 승인 현황_20251112

데이터 URL:
https://www.data.go.kr/data/3079705/fileData.do
```

```text
data/supply/raw/public_files/15032289.csv
대구광역시 동구_공동주택(아파트)시공현황_20251117

데이터 URL:
https://www.data.go.kr/data/15032289/fileData.do
```

```text
data/supply/raw/public_files/15064155.csv
행정중심복합도시건설청_행복도시 공동주택 준공계획_20250821

데이터 URL:
https://www.data.go.kr/data/15064155/fileData.do
```

참고로 추가 확인한 원천:

```text
data/supply/raw/public_files/15108016.csv
경상북도 포항시_미분양현황_20260228
```

이 파일은 `준공(예정)일`은 있으나 미분양 재고 데이터 성격이 강하여 이번 표준 입주예정 산출물에는 직접 병합하지 않았음. 후속 미분양/공급부담 분석에는 활용 가능함.

---

## 4. 표준 CSV 컬럼

주요 컬럼:

- source
- source_name
- source_id
- complex_id
- 단지명
- 주소
- 시도
- 시군구
- 지역코드
- 법정동코드
- 입주예정연월
- 준공연월
- 사용승인일
- 세대수
- 공급세대수
- 사업유형
- 사업주체
- 시공사
- 위도
- 경도
- source_url
- license_note
- 수집일시
- allowed_scope
- validation_status
- validation_message

---

## 5. 품질 상태

```text
ok   1,074행
warn     5행
```

warn 사유:

- 일부 준공예정 파일은 입주예정월이 아니라 준공예정일을 입주예정 proxy로 사용함.
- 일부 행은 주소/시군구 추출이 불완전할 수 있음.

---

## 6. 바로 앱에 붙이는 방법

1. `data_loader.py`에 `load_movein_plan_data()` 추가
2. `aggregate_movein_sigungu_monthly()` 추가
3. `app.py`에 입주물량 탭 추가
4. 기본 필터는 `입주예정연월 >= 2026-01`, `validation_status in ['ok', 'warn']` 권장
5. 차트 기본 지표:
   - 월별 입주예정 세대수
   - 시군구별 입주예정 세대수
   - source별 세대수
   - 단지 리스트

---

## 7. 한계와 후속 보강

1. 한국부동산원 입주예정물량정보가 가장 직접적인 원천임.
   - 2026~2027년 전국 단지 입주예정 데이터로 바로 활용 가능함.

2. LH 준공예정현황은 공공주택 중심임.
   - 2032년까지 장기 준공예정이 있으나, 민간 공급 전체를 대체하지는 못함.

3. 지자체 파일은 지역별 스키마가 다름.
   - 단기적으로는 보조 데이터, 장기적으로는 지자체별 adapter가 필요함.

4. 좌표/지역코드는 아직 보강 전임.
   - 현재는 주소 문자열에서 시도/시군구를 추출했음.
   - 후속으로 법정동코드와 VWorld geocoding을 붙여야 지도 품질이 올라감.

5. 청약홈 API는 이번 실행에서 인증/활용신청 문제로 직접 수집하지 못했음.
   - 그러나 한국부동산원 파일데이터가 이미 입주예정월/주소/아파트명/세대수를 제공하므로 MVP에는 충분함.

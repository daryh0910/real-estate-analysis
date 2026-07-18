# real_estate_analysis Quality Gate

> 생성시각(UTC): 2026-07-10T21:45:28.067498+00:00
> 앱 구조 판정: **PASS**

## Primary navigation (segmented_control)

🧭 Overview → 🧭 수요공급분석 → 🧭 거래현황 → 🧭 매물현황 → 🔬 매수판단 → 🔬 적정가·구매력 → 🔬 자유차트

## 구조 체크

- PASS: `primary_navigation_found`
- PASS: `overview_first`
- PASS: `buy_present`

## 데이터 파일 인벤토리

- 파일 수: 48
- 총 크기(bytes): 27266480

| 경로 | 형식 | 크기(bytes) | 수정시각(UTC) |
|---|---|---:|---|
| cache/apt_sigungu_monthly.parquet | .parquet | 1226842 | 2026-06-18T13:13:11.343845+00:00 |
| cache/apt_sigungu_monthly_detail.parquet | .parquet | 17180452 | 2026-05-01T03:28:01.883861+00:00 |
| cache/jeonse_sigungu_monthly.parquet | .parquet | 836028 | 2026-06-18T13:14:33.882480+00:00 |
| cache/kosis_population_age_sido_yearly.csv | .csv | 28823 | 2026-05-03T01:48:44.580123+00:00 |
| cache/kosis_population_age_sigungu_yearly.csv | .csv | 411630 | 2026-04-04T09:03:04.204327+00:00 |
| cache/rent_all_sigungu_monthly.parquet | .parquet | 1223906 | 2026-06-18T13:17:15.316328+00:00 |
| cache/sgis_house_sido_yearly.csv | .csv | 86 | 2026-04-04T08:20:38.581059+00:00 |
| cache/sgis_household_sido_yearly.csv | .csv | 113 | 2026-04-04T08:20:38.153379+00:00 |
| cache/sgis_population_sido_yearly.csv | .csv | 331 | 2026-04-04T08:20:37.790265+00:00 |
| cache/wolse_sigungu_monthly.parquet | .parquet | 1114615 | 2026-06-18T13:15:44.676220+00:00 |
| data/base_rate_monthly.csv | .csv | 12806 | 2026-03-25T23:06:56.504155+00:00 |
| data/bok_housing_loan_sido_monthly.csv | .csv | 284930 | 2026-03-25T23:06:56.691191+00:00 |
| data/cluster.csv | .csv | 438 | 2026-03-25T23:07:44.431803+00:00 |
| data/construction_pipeline_sido_monthly.csv | .csv | 159720 | 2026-06-17T13:55:56+00:00 |
| data/csi_monthly.csv | .csv | 3573 | 2026-06-17T13:55:47+00:00 |
| data/grdp.csv | .csv | 44673 | 2026-03-25T23:07:44.359174+00:00 |
| data/housing_price_index_sido_monthly.csv | .csv | 17553 | 2026-03-25T23:06:56.574079+00:00 |
| data/jeonwolse_conversion_rate_sido_monthly.csv | .csv | 74157 | 2026-03-25T23:06:56.612214+00:00 |
| data/kb_indicators_national_monthly.csv | .csv | 4191 | 2026-06-17T13:55:59+00:00 |
| data/kb_indicators_regional_monthly.csv | .csv | 83736 | 2026-06-17T13:55:59+00:00 |
| data/kb_market_supply_demand_monthly.csv | .csv | 59288 | 2026-06-17T13:55:58+00:00 |
| data/kosis_household_asset_quintile_yearly.csv | .csv | 8226 | 2026-06-17T13:47:01.332930+00:00 |
| data/kosis_household_asset_sido_yearly.csv | .csv | 3417 | 2026-03-28T05:13:38.393040+00:00 |
| data/land_price_change_sido_monthly.csv | .csv | 67057 | 2026-06-17T13:55:56+00:00 |
| data/nps_sigungu_monthly.csv | .csv | 206549 | 2026-03-26T05:15:28+00:00 |
| data/nts_income_sigungu_yearly.csv | .csv | 234014 | 2026-06-17T13:47:01.347785+00:00 |
| data/permit.csv | .csv | 265998 | 2026-03-25T23:07:44.405381+00:00 |
| data/policy_events.csv | .csv | 8022 | 2026-03-30T13:56:26.153203+00:00 |
| data/policy_research/korea_mortgage_ltv_dsr_timeline.csv | .csv | 11897 | 2026-06-17T22:02:30.246474+00:00 |
| data/pop/201312_201512_연령별인구현황_연간.csv | .csv | 854461 | 2026-03-25T23:07:44.487243+00:00 |
| data/pop/201612_201812_연령별인구현황_연간.csv | .csv | 851619 | 2026-03-25T23:07:44.529040+00:00 |
| data/pop/201912_202112_연령별인구현황_연간.csv | .csv | 840210 | 2026-03-25T23:07:44.566242+00:00 |
| data/population_migration_sido_monthly.csv | .csv | 87966 | 2026-06-17T14:04:46+00:00 |
| data/supply/metadata/source_registry.csv | .csv | 848 | 2026-05-12T08:21:55+00:00 |
| data/supply/movein_plan_complex_monthly.csv | .csv | 505534 | 2026-05-12T08:21:55+00:00 |
| data/supply/raw/public_files/15032289.csv | .csv | 2385 | 2026-05-12T08:18:10+00:00 |
| data/supply/raw/public_files/15033976.csv | .csv | 53623 | 2026-05-12T08:17:14+00:00 |
| data/supply/raw/public_files/15040743.csv | .csv | 124 | 2026-05-12T08:17:12+00:00 |
| data/supply/raw/public_files/15055106.csv | .csv | 32345 | 2026-05-12T08:18:09+00:00 |
| data/supply/raw/public_files/15063216.csv | .csv | 205319 | 2026-05-12T08:18:08+00:00 |
| data/supply/raw/public_files/15064155.csv | .csv | 246 | 2026-05-12T08:18:12+00:00 |
| data/supply/raw/public_files/15108016.csv | .csv | 32589 | 2026-05-12T08:17:13+00:00 |
| data/supply/raw/public_files/15111714.csv | .csv | 72600 | 2026-05-12T08:18:08+00:00 |
| data/supply/raw/public_files/15116841.csv | .csv | 20822 | 2026-05-12T08:18:11+00:00 |
| data/supply/raw/public_files/15141761.csv | .csv | 29279 | 2026-05-12T08:17:11+00:00 |
| data/supply/raw/public_files/3079705.csv | .csv | 3808 | 2026-05-12T08:17:11+00:00 |
| data/supply/validation/movein_seed_validation_summary.csv | .csv | 158 | 2026-05-12T08:21:55+00:00 |
| data/unsold_housing_sido_monthly.csv | .csv | 99473 | 2026-03-25T23:06:56.648417+00:00 |

## 해석 제한

- metadata inventory는 데이터 내용의 정확성이나 point-in-time 적합성을 보장하지 않는다.
- 기존 pytest 실패는 별도로 실행·기록해야 한다.
- 이 검사는 Linux 브라우저를 실행하지 않는 정적 quality gate다.

"""
캐시 빌드 스크립트 - 아파트 실거래 데이터를 Parquet으로 사전 집계
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

from data_loader import (
    load_apt_data, load_apt_data_detail, load_rent_data,
    APT_CACHE_PARQUET, APT_DETAIL_CACHE_PARQUET,
    JEONSE_CACHE_PARQUET, WOLSE_CACHE_PARQUET, RENT_ALL_CACHE_PARQUET,
)

print("=== 아파트 실거래 데이터 캐시 빌드 시작 ===")
start = time.time()

# 1. 기존 시군구 월별 집계 캐시
print("\n[1/5] 매매 시군구 월별 집계...")
df = load_apt_data(force_rebuild=True, chunksize=500_000)
elapsed = time.time() - start
print(f"  Shape: {df.shape} ({elapsed:.1f}초)")
if "시도" in df.columns:
    print(f"  시도별 거래량: {df.groupby('시도')['거래량'].sum().nlargest(5).to_dict()}")

# 2. 건축년도 포함 상세 캐시
print("\n[2/5] 매매 상세 캐시 (건축년도 포함)...")
t2 = time.time()
df_detail = load_apt_data_detail(force_rebuild=True, chunksize=500_000)
print(f"  Shape: {df_detail.shape} ({time.time()-t2:.1f}초)")
if "건축년도" in df_detail.columns and len(df_detail) > 0:
    print(f"  건축년도 범위: {df_detail['건축년도'].min():.0f} ~ {df_detail['건축년도'].max():.0f}")

# 3~5. 임대차 캐시
for i, rtype in enumerate(["jeonse", "wolse", "all"], 3):
    print(f"\n[{i}/5] {rtype} 캐시...")
    t = time.time()
    rdf = load_rent_data(rtype, force_rebuild=True, chunksize=500_000)
    print(f"  Shape: {rdf.shape} ({time.time()-t:.1f}초)")

total = time.time() - start
print(f"\n=== 전체 빌드 완료 ({total:.1f}초) ===")

for path, name in [
    (APT_CACHE_PARQUET, "매매 집계"),
    (APT_DETAIL_CACHE_PARQUET, "매매 상세(건축년도)"),
    (JEONSE_CACHE_PARQUET, "전세"),
    (WOLSE_CACHE_PARQUET, "월세"),
    (RENT_ALL_CACHE_PARQUET, "전체임대"),
]:
    if os.path.exists(path):
        size_mb = os.path.getsize(path) / (1024 * 1024)
        print(f"  ✅ {name}: {path} ({size_mb:.1f} MB)")
    else:
        print(f"  ❌ {name}: 미생성")

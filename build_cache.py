"""
캐시 빌드 스크립트 - 아파트 실거래 데이터를 Parquet으로 사전 집계

기본 원천은 프로젝트 data/ 디렉터리의 CSV를 사용한다. 다른 원천을
사용하려면 --apt-source와 --rent-source를 명시해야 한다.
"""

import argparse
import sys
import time
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_APT_SOURCE = PROJECT_ROOT / "data" / "apt_combined_files.csv"
DEFAULT_RENT_SOURCE = PROJECT_ROOT / "data" / "rent_combined_files.csv"
DEFAULT_CACHE_DIR = PROJECT_ROOT / "cache"

CACHE_FILENAMES = {
    "apt": "apt_sigungu_monthly.parquet",
    "apt_complex": "apt_complex_monthly.parquet",
    "apt_detail": "apt_sigungu_monthly_detail.parquet",
    "jeonse": "jeonse_sigungu_monthly.parquet",
    "wolse": "wolse_sigungu_monthly.parquet",
    "rent_all": "rent_all_sigungu_monthly.parquet",
}


def _format_ym(ym: int) -> str:
    return f"{ym // 100:04d}-{ym % 100:02d}"


def _detect_csv_encoding(path: Path) -> str:
    for encoding in ("utf-8-sig", "cp949", "euc-kr"):
        try:
            with path.open("r", encoding=encoding) as handle:
                handle.read(65_536)
            return encoding
        except UnicodeDecodeError:
            continue
    raise ValueError(f"CSV 인코딩을 확인할 수 없습니다: {path}")


def _latest_source_month(path: Path, chunksize: int = 500_000) -> int:
    """실거래 CSV의 최신 연월을 읽기 전용으로 계산한다."""
    if not path.is_file():
        raise FileNotFoundError(
            f"원천 CSV가 없습니다: {path}\n"
            "프로젝트 data/ 원천을 준비하거나 원천 경로 옵션을 명시하세요."
        )

    latest = None
    reader = pd.read_csv(
        path,
        encoding=_detect_csv_encoding(path),
        usecols=["년", "월"],
        dtype=str,
        chunksize=chunksize,
        on_bad_lines="skip",
    )
    for chunk in reader:
        years = pd.to_numeric(chunk["년"], errors="coerce")
        months = pd.to_numeric(chunk["월"], errors="coerce")
        valid = years.between(2000, 9999) & months.between(1, 12)
        if not valid.any():
            continue
        chunk_latest = int((years[valid] * 100 + months[valid]).max())
        latest = chunk_latest if latest is None else max(latest, chunk_latest)

    if latest is None:
        raise ValueError(f"원천 CSV에 유효한 년/월이 없습니다: {path}")
    return latest


def _latest_cache_month(path: Path) -> int:
    """Parquet 캐시의 최신 연월을 읽기 전용으로 계산한다."""
    try:
        month_frame = pd.read_parquet(path, columns=["연월"])
        month_parts = month_frame["연월"].astype(str).str.extract(
            r"(?P<year>\d{4})\D?(?P<month>\d{1,2})"
        )
        years = pd.to_numeric(month_parts["year"], errors="coerce")
        months = pd.to_numeric(month_parts["month"], errors="coerce")
    except (KeyError, ValueError):
        month_frame = pd.read_parquet(path, columns=["연도", "월"])
        years = pd.to_numeric(month_frame["연도"], errors="coerce")
        months = pd.to_numeric(month_frame["월"], errors="coerce")

    valid = years.between(2000, 9999) & months.between(1, 12)
    if not valid.any():
        raise ValueError(f"기존 캐시에 유효한 연월이 없습니다: {path}")
    return int((years[valid] * 100 + months[valid]).max())


def _cache_paths(cache_dir: Path) -> dict[str, Path]:
    return {name: cache_dir / filename for name, filename in CACHE_FILENAMES.items()}


def _guard_against_rollback(
    apt_source: Path,
    rent_source: Path,
    cache_dir: Path,
) -> dict[str, int]:
    """원천이 기존 캐시보다 과거이면 어떤 쓰기도 하기 전에 중단한다."""
    paths = _cache_paths(cache_dir)
    groups = (
        ("매매", apt_source, ("apt", "apt_complex", "apt_detail")),
        ("임대차", rent_source, ("jeonse", "wolse", "rent_all")),
    )
    latest_by_group: dict[str, int] = {}
    rollback_errors = []

    for label, source_path, cache_keys in groups:
        source_latest = _latest_source_month(source_path)
        latest_by_group[label] = source_latest
        print(f"[사전검증] {label} 원천 최신월: {_format_ym(source_latest)} ({source_path})")

        existing_cache_months = []
        for cache_key in cache_keys:
            cache_path = paths[cache_key]
            if not cache_path.is_file():
                print(f"[사전검증] {label} 캐시 없음: {cache_path}")
                continue
            cache_latest = _latest_cache_month(cache_path)
            existing_cache_months.append((cache_path, cache_latest))
            print(
                f"[사전검증] {label} 캐시 최신월: "
                f"{_format_ym(cache_latest)} ({cache_path})"
            )

        newer_caches = [
            (cache_path, cache_latest)
            for cache_path, cache_latest in existing_cache_months
            if source_latest < cache_latest
        ]
        if newer_caches:
            newest_cache_month = max(month for _, month in newer_caches)
            rollback_errors.append(
                f"{label} 원천 {_format_ym(source_latest)} < "
                f"기존 캐시 {_format_ym(newest_cache_month)}"
            )

    if rollback_errors:
        details = "; ".join(rollback_errors)
        raise RuntimeError(
            "역행 방지 가드: 원천이 기존 캐시보다 과거입니다. "
            f"캐시를 변경하지 않고 중단합니다. ({details})"
        )

    print("[사전검증] 역행 방지 가드 통과")
    return latest_by_group


def _configure_data_loader(apt_source: Path, rent_source: Path, cache_dir: Path):
    """사전 가드 통과 후 data_loader의 빌드 입·출력 경로를 설정한다."""
    import data_loader

    paths = _cache_paths(cache_dir)
    data_loader.APT_PATH = str(apt_source)
    data_loader.JEONSE_PATH = str(rent_source)
    data_loader.CACHE_DIR = str(cache_dir)
    data_loader.APT_CACHE_PARQUET = str(paths["apt"])
    data_loader.APT_COMPLEX_CACHE_PARQUET = str(paths["apt_complex"])
    data_loader.APT_DETAIL_CACHE_PARQUET = str(paths["apt_detail"])
    data_loader.JEONSE_CACHE_PARQUET = str(paths["jeonse"])
    data_loader.WOLSE_CACHE_PARQUET = str(paths["wolse"])
    data_loader.RENT_ALL_CACHE_PARQUET = str(paths["rent_all"])
    return data_loader


def _build(apt_source: Path, rent_source: Path, cache_dir: Path) -> None:
    loader = _configure_data_loader(apt_source, rent_source, cache_dir)
    print("=== 아파트 실거래 데이터 캐시 빌드 시작 ===")
    start = time.time()

    print("\n[1/6] 매매 시군구 월별 집계...")
    frame = loader.load_apt_data(force_rebuild=True, chunksize=500_000)
    elapsed = time.time() - start
    print(f"  Shape: {frame.shape} ({elapsed:.1f}초)")
    if "시도" in frame.columns:
        print(f"  시도별 거래량: {frame.groupby('시도')['거래량'].sum().nlargest(5).to_dict()}")

    print("\n[2/6] 매매 단지 월별 집계...")
    started = time.time()
    complex_frame = loader.load_apt_complex_data(force_rebuild=True, chunksize=500_000)
    print(f"  Shape: {complex_frame.shape} ({time.time() - started:.1f}초)")
    if not complex_frame.empty:
        count = complex_frame[["지역코드", "법정동", "아파트"]].drop_duplicates().shape[0]
        print(f"  단지 수: {count:,}")

    print("\n[3/6] 매매 상세 캐시 (건축년도 포함)...")
    started = time.time()
    detail_frame = loader.load_apt_data_detail(force_rebuild=True, chunksize=500_000)
    print(f"  Shape: {detail_frame.shape} ({time.time() - started:.1f}초)")
    if "건축년도" in detail_frame.columns and len(detail_frame) > 0:
        print(
            f"  건축년도 범위: {detail_frame['건축년도'].min():.0f} ~ "
            f"{detail_frame['건축년도'].max():.0f}"
        )

    for index, rent_type in enumerate(["jeonse", "wolse", "all"], 4):
        print(f"\n[{index}/6] {rent_type} 캐시...")
        started = time.time()
        rent_frame = loader.load_rent_data(rent_type, force_rebuild=True, chunksize=500_000)
        print(f"  Shape: {rent_frame.shape} ({time.time() - started:.1f}초)")

    total = time.time() - start
    print(f"\n=== 전체 빌드 완료 ({total:.1f}초) ===")
    labels = {
        "apt": "매매 집계",
        "apt_complex": "매매 단지 월별",
        "apt_detail": "매매 상세(건축년도)",
        "jeonse": "전세",
        "wolse": "월세",
        "rent_all": "전체임대",
    }
    for name, path in _cache_paths(cache_dir).items():
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            print(f"  ✅ {labels[name]}: {path} ({size_mb:.1f} MB)")
        else:
            print(f"  ❌ {labels[name]}: 미생성")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="아파트 실거래 Parquet 캐시 빌드",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--apt-source", type=Path, default=DEFAULT_APT_SOURCE, help="매매 원천 CSV 경로"
    )
    parser.add_argument(
        "--rent-source", type=Path, default=DEFAULT_RENT_SOURCE, help="임대차 원천 CSV 경로"
    )
    parser.add_argument(
        "--cache-dir", type=Path, default=DEFAULT_CACHE_DIR, help="Parquet 캐시 출력 디렉터리"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="원천·기존 캐시 최신월과 역행 가드만 검증",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    apt_source = args.apt_source.expanduser().resolve()
    rent_source = args.rent_source.expanduser().resolve()
    cache_dir = args.cache_dir.expanduser().resolve()

    try:
        _guard_against_rollback(apt_source, rent_source, cache_dir)
    except (FileNotFoundError, OSError, RuntimeError, ValueError) as error:
        print(f"❌ 사전 검증 실패: {error}", file=sys.stderr)
        return 2

    if args.dry_run:
        print("[DRY-RUN] 사전 검증만 완료했습니다. 캐시를 변경하지 않습니다.")
        return 0

    _build(apt_source, rent_source, cache_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

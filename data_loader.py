"""
부동산 가격분석 서비스 - 데이터 로딩/전처리 모듈
"""
import pandas as pd
import numpy as np
import os

# 기본 경로 설정 (WSL2에서 Windows 경로 접근 - 사용자명 자동 감지)
import glob as _glob

def _detect_onedrive():
    """Windows 사용자명을 자동 감지하여 OneDrive 경로를 반환.
    OneDrive 경로가 없는 환경(Cowork, CI 등)에서는 None 반환 → 캐시만 사용.
    """
    # 환경변수로 직접 지정 가능
    env_path = os.environ.get("ONEDRIVE_BASE_PATH")
    if env_path and os.path.isdir(env_path):
        return env_path
    pattern = "/mnt/c/Users/*/OneDrive - (주)에스티/101. 신사업폴더백업/부동산Tradingview"
    matches = _glob.glob(pattern)
    if matches:
        return matches[0]
    # OneDrive 없는 환경에서는 None → 캐시 전용 모드
    return None

BASE_ONEDRIVE = _detect_onedrive()
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
_FALLBACK_DIR = _PROJECT_ROOT  # 프로젝트 루트 폴백
_FALLBACK_DATA = os.path.join(_PROJECT_ROOT, "data")  # 클라우드용 데이터 폴더

def _safe_join(base, *parts):
    """base가 None이면 존재하지 않는 더미 경로 반환 (캐시 전용 모드)"""
    if base is None:
        return os.path.join(_FALLBACK_DIR, "_unavailable_", *parts)
    return os.path.join(base, *parts)

APT_PATH = _safe_join(BASE_ONEDRIVE, "5. coding/#1. 매매_combine/apt/combined_files.csv")
JEONSE_PATH = _safe_join(BASE_ONEDRIVE, "5. coding/#2. 임대차_combine/apt/combined_files.csv")
BACKDATA = _safe_join(BASE_ONEDRIVE, "실거래_데이터/BackData(거래이외 Table)")
POP_DIR = _safe_join(BACKDATA, "수요/#2. 수요_정책이외/1.수요/3. 인구/연령대별인구") if BASE_ONEDRIVE else os.path.join(_FALLBACK_DATA, "pop")
GRDP_PATH = _safe_join(BACKDATA, "수요/#2. 수요_정책이외/2.유효수요/GRDP/GRDP_시_군_구__2010_2022.csv") if BASE_ONEDRIVE else os.path.join(_FALLBACK_DATA, "grdp.csv")
PERMIT_PATH = _safe_join(BACKDATA, "주택공급/인허가/인허가/주택종류별인허가_200701_202311.csv") if BASE_ONEDRIVE else os.path.join(_FALLBACK_DATA, "permit.csv")
CLUSTER_PATH = _safe_join(BACKDATA, "클러스터시도연결.csv") if BASE_ONEDRIVE else os.path.join(_FALLBACK_DATA, "cluster.csv")

# 수요 데이터 경로 (download_demand_data.py 산출물)
DEMAND_DIR = os.path.join(BACKDATA, "수요/수요_집계") if BASE_ONEDRIVE else _FALLBACK_DATA
NPS_AGG_PATH = os.path.join(DEMAND_DIR, "nps_sigungu_monthly.csv")
BOK_LOAN_PATH = os.path.join(DEMAND_DIR, "bok_housing_loan_sido_monthly.csv")
KOSIS_ASSET_PATH = os.path.join(DEMAND_DIR, "kosis_household_asset_sido_yearly.csv")
KOSIS_QUINTILE_PATH = os.path.join(DEMAND_DIR, "kosis_household_asset_quintile_yearly.csv")
NTS_INCOME_PATH = os.path.join(DEMAND_DIR, "nts_income_sigungu_yearly.csv")

# 공공데이터 추가 수집 경로 (download_public_data.py 산출물)
UNSOLD_PATH = os.path.join(DEMAND_DIR, "unsold_housing_sido_monthly.csv")
LAND_PRICE_PATH = os.path.join(DEMAND_DIR, "land_price_change_sido_monthly.csv")
POP_MIGRATION_PATH = os.path.join(DEMAND_DIR, "population_migration_sido_monthly.csv")
BASE_RATE_PATH = os.path.join(DEMAND_DIR, "base_rate_monthly.csv")
JEONWOLSE_RATE_PATH = os.path.join(DEMAND_DIR, "jeonwolse_conversion_rate_sido_monthly.csv")
PRICE_INDEX_PATH = os.path.join(DEMAND_DIR, "housing_price_index_sido_monthly.csv")
CSI_PATH = os.path.join(DEMAND_DIR, "csi_monthly.csv")
KB_MARKET_PATH = os.path.join(DEMAND_DIR, "kb_market_supply_demand_monthly.csv")
POLICY_EVENTS_PATH = os.path.join(_PROJECT_ROOT, "data", "policy_events.csv")

# 착공/준공 파이프라인 경로 (data/ 디렉토리 우선, 없으면 DEMAND_DIR 탐색)
CONSTRUCTION_PATH = os.path.join(_PROJECT_ROOT, "data", "construction_pipeline_sido_monthly.csv")
if not os.path.exists(CONSTRUCTION_PATH):
    CONSTRUCTION_PATH = os.path.join(DEMAND_DIR, "construction_pipeline_sido_monthly.csv")

# 캐시 경로
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
APT_CACHE_PARQUET = os.path.join(CACHE_DIR, "apt_sigungu_monthly.parquet")
APT_DETAIL_CACHE_PARQUET = os.path.join(CACHE_DIR, "apt_sigungu_monthly_detail.parquet")  # 건축년도 포함
JEONSE_CACHE_PARQUET = os.path.join(CACHE_DIR, "jeonse_sigungu_monthly.parquet")
WOLSE_CACHE_PARQUET = os.path.join(CACHE_DIR, "wolse_sigungu_monthly.parquet")
RENT_ALL_CACHE_PARQUET = os.path.join(CACHE_DIR, "rent_all_sigungu_monthly.parquet")

# 지역코드 앞 2자리 → 시도명 매핑
SIDO_CODE_MAP = {
    11: "서울특별시", 26: "부산광역시", 27: "대구광역시", 28: "인천광역시",
    29: "광주광역시", 30: "대전광역시", 31: "울산광역시", 36: "세종특별자치시",
    41: "경기도", 42: "강원도", 43: "충청북도", 44: "충청남도",
    45: "전라북도", 46: "전라남도", 47: "경상북도", 48: "경상남도",
    50: "제주특별자치도",
    51: "강원특별자치도",  # 강원도 → 강원특별자치도 (2023~)
    52: "전북특별자치도",  # 전라북도 → 전북특별자치도 (2024~)
}

# 시도명 → 짧은 이름 매핑
SIDO_SHORT = {
    "서울특별시": "서울", "부산광역시": "부산", "대구광역시": "대구",
    "인천광역시": "인천", "광주광역시": "광주", "대전광역시": "대전",
    "울산광역시": "울산", "세종특별자치시": "세종", "경기도": "경기",
    "강원도": "강원", "충청북도": "충북", "충청남도": "충남",
    "전라북도": "전북", "전라남도": "전남", "경상북도": "경북",
    "경상남도": "경남", "제주특별자치도": "제주",
    "강원특별자치도": "강원", "전북특별자치도": "전북",
    "제주특별자치도 ": "제주",
}

# 지역코드(5자리) → 시군구명 매핑
SIGUNGU_NAME_MAP = {
    # 서울특별시
    "11110": "종로구", "11140": "중구", "11170": "용산구", "11200": "성동구",
    "11215": "광진구", "11230": "동대문구", "11260": "중랑구", "11290": "성북구",
    "11305": "강북구", "11320": "도봉구", "11350": "노원구", "11380": "은평구",
    "11410": "서대문구", "11440": "마포구", "11470": "양천구", "11500": "강서구",
    "11530": "구로구", "11545": "금천구", "11560": "영등포구", "11590": "동작구",
    "11620": "관악구", "11650": "서초구", "11680": "강남구", "11710": "송파구",
    "11740": "강동구",
    # 부산광역시
    "26110": "중구", "26140": "서구", "26170": "동구", "26200": "영도구",
    "26230": "부산진구", "26260": "동래구", "26290": "남구", "26320": "북구",
    "26350": "해운대구", "26380": "사하구", "26410": "금정구", "26440": "강서구",
    "26470": "연제구", "26500": "수영구", "26530": "사상구", "26710": "기장군",
    # 대구광역시
    "27110": "중구", "27140": "동구", "27170": "서구", "27200": "남구",
    "27230": "북구", "27260": "수성구", "27290": "달서구", "27710": "달성군",
    "27720": "군위군",
    # 인천광역시
    "28110": "중구", "28140": "동구", "28177": "미추홀구", "28185": "연수구",
    "28200": "남동구", "28237": "부평구", "28245": "계양구", "28260": "서구",
    "28710": "강화군",
    # 광주광역시
    "29110": "동구", "29140": "서구", "29155": "남구", "29170": "북구",
    "29200": "광산구",
    # 대전광역시
    "30110": "동구", "30140": "중구", "30170": "서구", "30200": "유성구",
    "30230": "대덕구",
    # 울산광역시
    "31110": "중구", "31140": "남구", "31170": "동구", "31200": "북구",
    "31710": "울주군",
    # 세종특별자치시
    "36110": "세종시",
    # 경기도
    "41111": "수원장안구", "41113": "수원권선구", "41115": "수원팔달구",
    "41117": "수원영통구", "41131": "성남수정구", "41133": "성남중원구",
    "41135": "성남분당구", "41150": "의정부시", "41171": "안양만안구",
    "41173": "안양동안구", "41192": "부천원미구", "41194": "부천소사구",
    "41196": "부천오정구", "41210": "광명시", "41220": "평택시",
    "41250": "동두천시", "41271": "안산상록구", "41273": "안산단원구",
    "41281": "고양덕양구", "41285": "고양일산동구", "41287": "고양일산서구",
    "41290": "과천시", "41310": "구리시", "41360": "남양주시",
    "41370": "오산시", "41390": "시흥시", "41410": "군포시",
    "41430": "의왕시", "41450": "하남시", "41461": "용인처인구",
    "41463": "용인기흥구", "41465": "용인수지구", "41480": "파주시",
    "41500": "이천시", "41550": "안성시", "41570": "김포시",
    "41590": "화성시", "41610": "광주시", "41630": "양주시",
    "41650": "포천시", "41670": "여주시", "41800": "연천군",
    "41820": "가평군", "41830": "양평군",
    # 충청북도
    "43111": "청주상당구", "43112": "청주서원구", "43113": "청주흥덕구",
    "43114": "청주청원구", "43130": "충주시", "43150": "제천시",
    "43720": "보은군", "43730": "옥천군", "43740": "영동군",
    "43745": "증평군", "43750": "진천군", "43760": "괴산군",
    "43770": "음성군", "43800": "단양군",
    # 충청남도
    "44131": "천안동남구", "44133": "천안서북구", "44150": "공주시",
    "44180": "보령시", "44200": "아산시", "44210": "서산시",
    "44230": "논산시", "44250": "계룡시", "44270": "당진시",
    "44710": "금산군", "44760": "부여군", "44770": "서천군",
    "44790": "청양군", "44800": "홍성군", "44810": "예산군",
    "44825": "태안군",
    # 전라남도
    "46110": "목포시", "46130": "여수시", "46150": "순천시",
    "46170": "나주시", "46230": "광양시", "46710": "담양군",
    "46720": "곡성군", "46730": "구례군", "46770": "고흥군",
    "46780": "보성군", "46790": "화순군", "46800": "장흥군",
    "46810": "강진군", "46820": "해남군", "46830": "영암군",
    "46840": "무안군", "46860": "함평군", "46870": "영광군",
    "46880": "장성군", "46890": "완도군", "46900": "진도군",
    "46910": "신안군",
    # 경상북도
    "47111": "포항남구", "47113": "포항북구", "47130": "경주시",
    "47150": "김천시", "47170": "안동시", "47190": "구미시",
    "47210": "영주시", "47230": "영천시", "47250": "상주시",
    "47280": "경산시", "47290": "문경시", "47730": "의성군",
    "47750": "청송군", "47760": "영양군", "47770": "영덕군",
    "47820": "청도군", "47830": "고령군", "47840": "성주군",
    "47850": "칠곡군", "47900": "예천군", "47920": "봉화군",
    "47930": "울진군", "47940": "울릉군",
    # 강원특별자치도 (51xxx)
    "51110": "춘천시", "51130": "원주시", "51150": "강릉시", "51170": "동해시",
    "51190": "태백시", "51210": "속초시", "51230": "삼척시", "51720": "홍천군",
    "51730": "횡성군", "51750": "영월군", "51760": "평창군", "51770": "정선군",
    "51780": "철원군", "51790": "화천군", "51800": "양구군", "51810": "인제군",
    "51820": "고성군", "51830": "양양군",
    # 전북특별자치도 (52xxx)
    "52111": "전주완산구", "52113": "전주덕진구", "52130": "군산시",
    "52140": "익산시", "52180": "정읍시", "52190": "남원시", "52210": "김제시",
    "52710": "완주군", "52720": "진안군", "52730": "무주군", "52740": "장수군",
    "52750": "임실군", "52770": "순창군", "52790": "고창군", "52800": "부안군",
    # 경상남도
    "48121": "창원의창구", "48123": "창원성산구", "48125": "창원마산합포구",
    "48127": "창원마산회원구", "48129": "창원진해구", "48170": "진주시",
    "48220": "통영시", "48240": "사천시", "48250": "김해시",
    "48270": "밀양시", "48310": "거제시", "48330": "양산시",
    "48720": "의령군", "48730": "함안군", "48740": "창녕군",
    "48820": "고성군", "48840": "남해군", "48850": "하동군",
    "48860": "산청군", "48870": "함양군", "48880": "거창군",
    "48890": "합천군",
    # 제주특별자치도
    "50110": "제주시", "50130": "서귀포시",
}


def get_sigungu_name(code):
    """지역코드 → 시군구명 반환 (매핑에 없으면 코드 그대로 반환)"""
    return SIGUNGU_NAME_MAP.get(str(code).strip(), str(code))


def read_csv_auto(path, **kwargs):
    """인코딩 자동 감지 CSV 로더 (utf-8 → cp949 → euc-kr)"""
    for enc in ("utf-8", "cp949", "euc-kr"):
        try:
            return pd.read_csv(path, encoding=enc, **kwargs)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Encoding detection failed for {path}")


def _clean_amount(s):
    """거래금액 문자열을 숫자(만원)로 변환"""
    if pd.isna(s):
        return np.nan
    s = str(s).replace(",", "").replace('"', "").strip()
    try:
        return int(s)
    except ValueError:
        return np.nan


def _sido_from_code(code):
    """지역코드(5자리)에서 시도 코드(앞 2자리) 추출 → 시도명"""
    try:
        return SIDO_CODE_MAP.get(int(str(code)[:2]))
    except (ValueError, TypeError):
        return None


def _normalize_sido(name):
    """시도명을 짧은 이름으로 정규화"""
    if pd.isna(name):
        return None
    name = str(name).strip()
    if name in SIDO_SHORT:
        return SIDO_SHORT[name]
    if name in SIDO_SHORT.values():
        return name
    for full, short in SIDO_SHORT.items():
        if short in name or name in full:
            return short
    return name


def load_apt_data(chunksize=500_000, keep_sido=True, force_rebuild=False):
    """
    아파트 실거래 CSV → 시군구+연월 단위 집계 (Parquet 캐시 활용)
    Returns: DataFrame [지역코드, (시도), 연도, 월, 연월, 평균가격, 거래량, 평균단가_per_m2]
    """
    os.makedirs(CACHE_DIR, exist_ok=True)

    if (not force_rebuild) and os.path.exists(APT_CACHE_PARQUET):
        return pd.read_parquet(APT_CACHE_PARQUET)

    # Cloud 환경: 캐시도 없고 원본 CSV도 없으면 빈 DataFrame 반환
    if not os.path.exists(APT_PATH):
        return pd.DataFrame()

    chunks = []
    reader = read_csv_auto(
        APT_PATH,
        chunksize=chunksize,
        dtype=str,  # 모든 컬럼을 str로 읽어서 NA 안전 처리
        usecols=["년", "월", "지역코드", "전용면적", "거래금액"],
        on_bad_lines="skip",
    )

    for chunk in reader:
        chunk["지역코드"] = chunk["지역코드"].astype(str).str.strip().str.zfill(5)
        chunk["년"] = pd.to_numeric(chunk["년"], errors="coerce")
        chunk["월"] = pd.to_numeric(chunk["월"], errors="coerce")
        chunk["거래금액_num"] = chunk["거래금액"].apply(_clean_amount)
        chunk["전용면적_num"] = pd.to_numeric(chunk["전용면적"], errors="coerce")

        chunk = chunk.dropna(subset=["거래금액_num", "전용면적_num", "지역코드", "년", "월"])
        chunk = chunk[(chunk["거래금액_num"] > 0) & (chunk["전용면적_num"] > 0)]

        chunk["단가_per_m2"] = chunk["거래금액_num"] / chunk["전용면적_num"]

        if keep_sido:
            chunk["시도"] = chunk["지역코드"].str[:2].apply(_sido_from_code).apply(_normalize_sido)

        group_keys = ["지역코드", "년", "월"]
        if keep_sido:
            group_keys = ["시도"] + group_keys

        agg = (
            chunk.groupby(group_keys)
            .agg(
                평균가격=("거래금액_num", "mean"),
                거래량=("거래금액_num", "count"),
                평균단가_per_m2=("단가_per_m2", "mean"),
            )
            .reset_index()
        )
        chunks.append(agg)

    if not chunks:
        df = pd.DataFrame()
        df.to_parquet(APT_CACHE_PARQUET, index=False)
        return df

    result = pd.concat(chunks, ignore_index=True)

    def _weighted_agg(g):
        w = g["거래량"].to_numpy()
        return pd.Series({
            "평균가격": np.average(g["평균가격"], weights=w),
            "거래량": g["거래량"].sum(),
            "평균단가_per_m2": np.average(g["평균단가_per_m2"], weights=w),
        })

    group_keys = ["지역코드", "년", "월"]
    if keep_sido:
        group_keys = ["시도"] + group_keys

    result = (
        result.groupby(group_keys, group_keys=False)
        .apply(_weighted_agg, include_groups=False)
        .reset_index()
    )

    result["연도"] = result["년"].astype(int)
    result["월"] = result["월"].astype(int)
    result["연월"] = result["연도"].astype(str) + "-" + result["월"].apply(lambda x: f"{x:02d}")
    result = result.drop(columns=["년"])

    # 캐시 저장
    result.to_parquet(APT_CACHE_PARQUET, index=False)
    return result


def load_apt_data_detail(chunksize=500_000, force_rebuild=False):
    """
    아파트 실거래 CSV → 시군구+연월+준공년차 단위 집계 (건축년도 포함 상세 캐시)
    Returns: DataFrame [시도, 지역코드, 연도, 월, 연월, 건축년도, 준공년차, 평균가격, 거래량, 평균단가_per_m2]
    """
    os.makedirs(CACHE_DIR, exist_ok=True)

    if (not force_rebuild) and os.path.exists(APT_DETAIL_CACHE_PARQUET):
        return pd.read_parquet(APT_DETAIL_CACHE_PARQUET)

    if not os.path.exists(APT_PATH):
        return pd.DataFrame()

    chunks = []
    reader = read_csv_auto(
        APT_PATH,
        chunksize=chunksize,
        dtype=str,  # 모든 컬럼을 str로 읽어서 NA 안전 처리
        usecols=["년", "월", "지역코드", "전용면적", "거래금액", "건축년도"],
        on_bad_lines="skip",
    )

    for chunk in reader:
        chunk["지역코드"] = chunk["지역코드"].astype(str).str.strip().str.zfill(5)
        chunk["년"] = pd.to_numeric(chunk["년"], errors="coerce")
        chunk["월"] = pd.to_numeric(chunk["월"], errors="coerce")
        chunk["거래금액_num"] = chunk["거래금액"].apply(_clean_amount)
        chunk["전용면적_num"] = pd.to_numeric(chunk["전용면적"], errors="coerce")
        chunk["건축년도_num"] = pd.to_numeric(chunk["건축년도"], errors="coerce")

        chunk = chunk.dropna(subset=["거래금액_num", "전용면적_num", "지역코드", "년", "월"])
        chunk = chunk[(chunk["거래금액_num"] > 0) & (chunk["전용면적_num"] > 0)]

        chunk["단가_per_m2"] = chunk["거래금액_num"] / chunk["전용면적_num"]
        chunk["시도"] = chunk["지역코드"].str[:2].apply(_sido_from_code).apply(_normalize_sido)
        # 준공년차 = 거래년도 - 건축년도
        chunk["준공년차"] = chunk["년"] - chunk["건축년도_num"]
        chunk.loc[chunk["준공년차"] < 0, "준공년차"] = np.nan

        group_keys = ["시도", "지역코드", "년", "월", "건축년도_num"]
        agg = (
            chunk.groupby(group_keys)
            .agg(
                평균가격=("거래금액_num", "mean"),
                거래량=("거래금액_num", "count"),
                평균단가_per_m2=("단가_per_m2", "mean"),
                평균준공년차=("준공년차", "mean"),
            )
            .reset_index()
        )
        agg = agg.rename(columns={"건축년도_num": "건축년도"})
        chunks.append(agg)

    if not chunks:
        df = pd.DataFrame()
        df.to_parquet(APT_DETAIL_CACHE_PARQUET, index=False)
        return df

    result = pd.concat(chunks, ignore_index=True)
    result["연도"] = result["년"].astype(int)
    result["월"] = result["월"].astype(int)
    result["연월"] = result["연도"].astype(str) + "-" + result["월"].apply(lambda x: f"{x:02d}")
    result = result.drop(columns=["년"])

    result.to_parquet(APT_DETAIL_CACHE_PARQUET, index=False)
    return result


def load_rent_data(rent_type="jeonse", chunksize=500_000, keep_sido=True, force_rebuild=False):
    """
    아파트 임대차 CSV → 시군구+연월 단위 집계 (Parquet 캐시 활용)
    rent_type: 'jeonse'(순수전세), 'wolse'(순수월세), 'all'(전세+월세)
    Returns: DataFrame [지역코드, (시도), 연도, 월, 연월, 보증금평균, 임대거래량, 보증금단가_per_m2, (월세평균)]
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_map = {"jeonse": JEONSE_CACHE_PARQUET, "wolse": WOLSE_CACHE_PARQUET, "all": RENT_ALL_CACHE_PARQUET}
    cache_path = cache_map[rent_type]

    if (not force_rebuild) and os.path.exists(cache_path):
        return pd.read_parquet(cache_path)

    # Cloud 환경: 캐시도 없고 원본 CSV도 없으면 빈 DataFrame 반환
    if not os.path.exists(JEONSE_PATH):
        return pd.DataFrame()

    chunks = []
    reader = read_csv_auto(
        JEONSE_PATH,
        chunksize=chunksize,
        dtype={"지역코드": str},
        usecols=["년", "월", "지역코드", "전용면적", "보증금액", "월세금액"],
        on_bad_lines="skip",
    )

    for chunk in reader:
        chunk["지역코드"] = chunk["지역코드"].astype(str).str.strip().str.zfill(5)
        chunk["보증금_num"] = chunk["보증금액"].apply(_clean_amount)
        chunk["월세금액_num"] = pd.to_numeric(chunk["월세금액"], errors="coerce").fillna(0)
        chunk["전용면적_num"] = pd.to_numeric(chunk["전용면적"], errors="coerce")
        chunk["년"] = pd.to_numeric(chunk["년"], errors="coerce")
        chunk["월"] = pd.to_numeric(chunk["월"], errors="coerce")

        chunk = chunk.dropna(subset=["보증금_num", "전용면적_num", "지역코드", "년", "월"])
        chunk = chunk[(chunk["보증금_num"] > 0) & (chunk["전용면적_num"] > 0)]

        # 유형별 필터
        if rent_type == "jeonse":
            chunk = chunk[chunk["월세금액_num"] == 0]
        elif rent_type == "wolse":
            chunk = chunk[chunk["월세금액_num"] > 0]
        # 'all'이면 필터 없음

        if chunk.empty:
            continue

        chunk["보증금단가_per_m2"] = chunk["보증금_num"] / chunk["전용면적_num"]

        if keep_sido:
            chunk["시도"] = chunk["지역코드"].str[:2].apply(_sido_from_code).apply(_normalize_sido)

        group_keys = ["지역코드", "년", "월"]
        if keep_sido:
            group_keys = ["시도"] + group_keys

        agg_dict = {
            "보증금평균": ("보증금_num", "mean"),
            "임대거래량": ("보증금_num", "count"),
            "보증금단가_per_m2": ("보증금단가_per_m2", "mean"),
        }
        if rent_type in ("wolse", "all"):
            agg_dict["월세평균"] = ("월세금액_num", "mean")

        agg = chunk.groupby(group_keys).agg(**agg_dict).reset_index()
        chunks.append(agg)

    if not chunks:
        df = pd.DataFrame()
        df.to_parquet(cache_path, index=False)
        return df

    result = pd.concat(chunks, ignore_index=True)

    # 청크 간 재집계 (가중평균)
    def _weighted_agg_rent(g):
        w = g["임대거래량"].to_numpy()
        d = {
            "보증금평균": np.average(g["보증금평균"], weights=w),
            "임대거래량": g["임대거래량"].sum(),
            "보증금단가_per_m2": np.average(g["보증금단가_per_m2"], weights=w),
        }
        if "월세평균" in g.columns:
            d["월세평균"] = np.average(g["월세평균"], weights=w)
        return pd.Series(d)

    group_keys = ["지역코드", "년", "월"]
    if keep_sido:
        group_keys = ["시도"] + group_keys

    result = (
        result.groupby(group_keys, group_keys=False)
        .apply(_weighted_agg_rent, include_groups=False)
        .reset_index()
    )

    result["연도"] = result["년"].astype(int)
    result["월"] = result["월"].astype(int)
    result["연월"] = result["연도"].astype(str) + "-" + result["월"].apply(lambda x: f"{x:02d}")
    result = result.drop(columns=["년"])

    result.to_parquet(cache_path, index=False)
    return result


def _parse_pop_int(val):
    """인구 문자열을 정수로 변환 (콤마/따옴표 제거, 실패 시 None 반환)"""
    s = str(val).replace(",", "").replace('"', "").strip()
    try:
        return int(s)
    except ValueError:
        return None


def load_population_data():
    """
    연령대별 인구 CSV 3개 파일 로드 → 시도별 연도별 인구 집계.

    원본 파일 컬럼 구조:
      행정구역 | YYYY년_계_총인구수 | YYYY년_계_연령구간인구수 | YYYY년_계_0~4세
               | YYYY년_남_총인구수 | YYYY년_남_연령구간인구수 | YYYY년_남_0~4세
               | YYYY년_여_총인구수 | YYYY년_여_연령구간인구수 | YYYY년_여_0~4세
      (연령대별 세부 컬럼은 0~4세 구간 하나만 존재)

    Returns:
        DataFrame [시도, 연도, 총인구, 인구_남성, 인구_여성]
        ※ 연령대별(20대/30대 등) 세부 컬럼은 원본 파일에 없어 추출 불가
    """
    pop_files = [
        "201312_201512_연령별인구현황_연간.csv",
        "201612_201812_연령별인구현황_연간.csv",
        "201912_202112_연령별인구현황_연간.csv",
    ]
    all_rows = []
    for fname in pop_files:
        fpath = os.path.join(POP_DIR, fname)
        if not os.path.exists(fpath):
            continue
        df = read_csv_auto(fpath)

        for _, row in df.iterrows():
            region = str(row.iloc[0])
            if "(" not in region:
                continue
            name_part = region.split("(")[0].strip()
            code_part = region.split("(")[1].replace(")", "").strip()
            # 시도 레벨 행 판별: 10자리 코드, 뒤 8자리가 모두 0
            if not (len(code_part) == 10 and code_part[2:] == "00000000"):
                continue

            sido = _normalize_sido(name_part)
            if sido is None:
                continue

            for col in df.columns:
                col_str = str(col)
                # 총인구 추출: YYYY년_계_총인구수
                if "계_총인구수" in col_str:
                    year_str = col_str.split("년")[0]
                    try:
                        year = int(year_str)
                    except ValueError:
                        continue
                    pop = _parse_pop_int(row[col])
                    if pop is None:
                        continue

                    # 동일 행에서 남성/여성 총인구 컬럼 탐색
                    male_col = col_str.replace("계_총인구수", "남_총인구수")
                    female_col = col_str.replace("계_총인구수", "여_총인구수")
                    male_pop = _parse_pop_int(row[male_col]) if male_col in df.columns else None
                    female_pop = _parse_pop_int(row[female_col]) if female_col in df.columns else None

                    entry = {"시도": sido, "연도": year, "총인구": pop}
                    if male_pop is not None:
                        entry["인구_남성"] = male_pop
                    if female_pop is not None:
                        entry["인구_여성"] = female_pop
                    all_rows.append(entry)

    if not all_rows:
        return pd.DataFrame(columns=["시도", "연도", "총인구"])

    result = pd.DataFrame(all_rows).drop_duplicates(subset=["시도", "연도"])

    # 반환 컬럼 안내 (연령대별 컬럼은 원본 파일 미지원으로 제외)
    # 현재 반환: 시도, 연도, 총인구, 인구_남성(있으면), 인구_여성(있으면)
    return result


def load_grdp_data():
    """
    GRDP CSV 로드 (wide → long 변환)
    Returns: DataFrame [시도, 연도, GRDP]
    """
    if not os.path.exists(GRDP_PATH):
        return pd.DataFrame(columns=["시도", "연도", "GRDP"])
    df = pd.read_csv(GRDP_PATH, encoding="utf-8", header=[0, 1])
    rows = []
    for idx in range(len(df)):
        sido_name = str(df.iloc[idx, 0]).strip().replace('"', '')
        sub_name = str(df.iloc[idx, 1]).strip().replace('"', '')
        if sub_name != "소계":
            continue
        sido = _normalize_sido(sido_name)
        if sido is None:
            continue
        col_pairs = list(df.columns)
        for i in range(2, len(col_pairs), 2):
            year_str = str(col_pairs[i][0]).strip()
            try:
                year = int(year_str)
            except ValueError:
                continue
            val = str(df.iloc[idx, i]).replace(",", "").replace('"', "").replace("-", "").strip()
            try:
                grdp = int(val)
            except ValueError:
                continue
            rows.append({"시도": sido, "연도": year, "GRDP": grdp})

    if not rows:
        return pd.DataFrame(columns=["시도", "연도", "GRDP"])
    return pd.DataFrame(rows)


def load_permit_data():
    """
    인허가 CSV 로드 (아파트 행만 필터, wide → long 변환)
    Returns: DataFrame [시도, 연도, 월, 인허가_호수]
    """
    if not os.path.exists(PERMIT_PATH):
        return pd.DataFrame(columns=["시도", "연도", "월", "인허가_호수"])
    df = read_csv_auto(PERMIT_PATH)

    apt_df = df[
        (df["대분류"].astype(str).str.strip() == "아파트") &
        (df["소분류"].astype(str).str.strip() == "가구수")
    ].copy()

    if apt_df.empty:
        apt_df = df[
            (df["대분류"].astype(str).str.strip() == "아파트") &
            (df["소분류"].astype(str).str.strip() == "아파트")
        ].copy()

    month_cols = [c for c in df.columns if "월" in str(c) and "." in str(c)]
    rows = []
    for _, row in apt_df.iterrows():
        raw_sido = str(row["시도명"]).strip()
        if raw_sido in ("전국", "수도권", "지방소계", "기타광역시", "기타지방"):
            continue
        sido = _normalize_sido(raw_sido)
        if sido is None:
            continue
        for col in month_cols:
            parts = str(col).replace("월", "").strip().split(".")
            try:
                year = int(parts[0])
                month = int(parts[1].strip())
            except (ValueError, IndexError):
                continue
            val = str(row[col]).replace(",", "").replace('"', "").strip()
            try:
                permits = int(float(val))
            except ValueError:
                continue
            rows.append({"시도": sido, "연도": year, "월": month, "인허가_호수": permits})

    if not rows:
        return pd.DataFrame(columns=["시도", "연도", "월", "인허가_호수"])
    result = pd.DataFrame(rows)
    result["연월"] = result["연도"].astype(str) + "-" + result["월"].apply(lambda x: f"{x:02d}")
    return result


def load_nps_data():
    """
    국민연금 집계 데이터 로드 (시군구/시도)
    Returns: DataFrame [지역코드, 시도, 연도, 월, 연월, NPS_가입자수, NPS_1인당고지금액,
                        NPS_고지금액합계, NPS_사업장수, NPS_고용증감]
    """
    if not os.path.exists(NPS_AGG_PATH):
        return pd.DataFrame()

    df = pd.read_csv(NPS_AGG_PATH, dtype={"지역코드": str})
    df["지역코드"] = df["지역코드"].astype(str).str.zfill(5)
    if "시도" not in df.columns:
        df["시도"] = df["지역코드"].str[:2].apply(_sido_from_code).apply(_normalize_sido)
    else:
        df["시도"] = df["시도"].apply(_normalize_sido)

    return df


def load_housing_loan_data():
    """
    BOK 주담대 데이터 로드 (시도, 월별)
    Returns: DataFrame [시도, 연도, 월, 연월, 주담대_잔액, 주담대_증감률, 주담대_비중, 기타대출_잔액]
    """
    if not os.path.exists(BOK_LOAN_PATH):
        return pd.DataFrame()

    df = pd.read_csv(BOK_LOAN_PATH)
    df["시도"] = df["시도"].apply(_normalize_sido)
    return df


def load_household_asset_data():
    """
    KOSIS 가계자산 데이터 로드 (시도, 연간)
    Returns: DataFrame [시도, 연도, 가구_자산평균, 가구_부채평균, 가구_순자산, 가구_소득평균, DSR]
    """
    if not os.path.exists(KOSIS_ASSET_PATH):
        return pd.DataFrame()

    df = pd.read_csv(KOSIS_ASSET_PATH)
    df["시도"] = df["시도"].apply(_normalize_sido)
    return df


def load_household_asset_quintile_data():
    """
    KOSIS 소득5분위별 가계자산 데이터 로드 (전국, 연간)
    Returns: DataFrame [연도, 소득분위, 가구_자산평균, 가구_부채평균, 가구_순자산,
                        가구_소득평균, 가구_금융자산, 가구_실물자산, ...]
    """
    if not os.path.exists(KOSIS_QUINTILE_PATH):
        return pd.DataFrame()

    df = pd.read_csv(KOSIS_QUINTILE_PATH)
    return df


def load_nts_income_data():
    """
    국세청 근로소득 데이터 로드 (시군구, 연간)
    Returns: DataFrame [지역코드, 시도, 시군구, 연도, 총급여_인원, 총급여_금액,
                        1인당총급여_백만원, 1인당결정세액_백만원, ...]
    """
    if not os.path.exists(NTS_INCOME_PATH):
        return pd.DataFrame()

    df = pd.read_csv(NTS_INCOME_PATH)
    if "시도" in df.columns:
        df["시도"] = df["시도"].apply(_normalize_sido)
    return df


def load_unsold_housing_data():
    """
    미분양주택현황 로드 (시도, 월별)
    Returns: DataFrame [연월, 시도, 미분양_호수, 연도, 월]
    """
    if not os.path.exists(UNSOLD_PATH):
        return pd.DataFrame()
    df = pd.read_csv(UNSOLD_PATH)
    df["시도"] = df["시도"].apply(_normalize_sido)
    return df


def load_land_price_data():
    """
    지가변동률 데이터 로드 (시도, 월별) — BOK 901Y064
    Returns: DataFrame [연월, 시도, 지가변동률, 연도, 월]
    """
    if not os.path.exists(LAND_PRICE_PATH):
        return pd.DataFrame()
    df = pd.read_csv(LAND_PRICE_PATH)
    df["시도"] = df["시도"].apply(_normalize_sido)
    # 전국 제외
    df = df[df["시도"] != "전국"]
    return df


def load_migration_data():
    """
    인구이동(전입/전출/순이동) 데이터 로딩 — 공공데이터 수집 파일 사용.

    download_public_data.py 실행 후 생성되는
    population_migration_sido_monthly.csv 파일을 읽는다.
    파일이 없거나 필수 컬럼(전입/전출/순이동)이 없으면 빈 DataFrame을 반환한다.

    Returns:
        DataFrame [연월, 시도, 전입, 전출, 순이동, 연도, 월]
        또는 빈 DataFrame (파일 없음 / 컬럼 불일치)
    """
    if not os.path.exists(POP_MIGRATION_PATH):
        return pd.DataFrame(columns=["연월", "시도", "전입", "전출", "순이동", "연도", "월"])

    df = pd.read_csv(POP_MIGRATION_PATH)
    required_cols = {"시도", "전입", "전출", "순이동"}
    if not required_cols.issubset(set(df.columns)):
        return pd.DataFrame(columns=["연월", "시도", "전입", "전출", "순이동", "연도", "월"])

    df["시도"] = df["시도"].apply(_normalize_sido)
    # 전국 집계 행 제외
    df = df[df["시도"] != "전국"]
    return df


# 하위호환: 기존 코드에서 load_population_migration_data()를 호출하는 경우 대응
load_population_migration_data = load_migration_data


def load_base_rate_data():
    """
    기준금리/시장금리 로드 (전국, 월별)
    Returns: DataFrame [연월, 기준금리, CD_91일, 국고채_3년, 국고채_5년, 국고채_10년, 연도, 월]
    """
    if not os.path.exists(BASE_RATE_PATH):
        return pd.DataFrame()
    return pd.read_csv(BASE_RATE_PATH)


def load_jeonwolse_rate_data():
    """
    전월세전환율 로드 (시도, 월별)
    Returns: DataFrame [연월, 시도, 전월세전환율, 연도, 월]
    """
    if not os.path.exists(JEONWOLSE_RATE_PATH):
        return pd.DataFrame()
    df = pd.read_csv(JEONWOLSE_RATE_PATH)
    df["시도"] = df["시도"].apply(_normalize_sido)
    return df


def load_price_index_data():
    """
    주택가격지수 로드 (전국/서울, 월별)
    Returns: DataFrame [연월, 시도, 아파트매매가격지수, 아파트전세가격지수, 연도, 월]
    """
    if not os.path.exists(PRICE_INDEX_PATH):
        return pd.DataFrame()
    return pd.read_csv(PRICE_INDEX_PATH)


def load_kb_market_data():
    """
    KB부동산 수급 데이터 로드 (시도, 월별)
    Returns: DataFrame [시도, 연월, KB_매수우위지수, KB_매매거래지수, KB_전세수급지수, 연도, 월]
    """
    if not os.path.exists(KB_MARKET_PATH):
        return pd.DataFrame()
    df = pd.read_csv(KB_MARKET_PATH)
    if "시도" in df.columns:
        df["시도"] = df["시도"].apply(_normalize_sido)
    return df


def load_csi_data():
    """
    소비자심리지수(CSI) 로드 (전국, 월별)
    Returns: DataFrame [연월, 소비자심리지수, 주택가격전망CSI, 연도, 월]
    """
    if not os.path.exists(CSI_PATH):
        return pd.DataFrame()
    return pd.read_csv(CSI_PATH)


def load_construction_data():
    """
    착공/준공 파이프라인 데이터 로드 (시도, 월별).

    data/construction_pipeline_sido_monthly.csv 파일을 읽는다.
    파일이 없으면 빈 DataFrame을 반환한다.

    Returns:
        DataFrame [연월, 시도, 착공_호수, 준공_호수, 연도, 월, ...]
        또는 빈 DataFrame (파일 없음)
    """
    if not os.path.exists(CONSTRUCTION_PATH):
        return pd.DataFrame(columns=["연월", "시도", "착공_호수", "준공_호수", "연도", "월"])

    df = pd.read_csv(CONSTRUCTION_PATH)
    if "시도" in df.columns:
        df["시도"] = df["시도"].apply(_normalize_sido)
        # 전국 집계 행 제외
        df = df[df["시도"] != "전국"]
    return df


def load_policy_events():
    """
    정책 이벤트 DB 로드
    Returns: DataFrame [날짜, 카테고리, 이벤트명, 방향, 영향지역, 상세]
    """
    if not os.path.exists(POLICY_EVENTS_PATH):
        return pd.DataFrame()
    df = pd.read_csv(POLICY_EVENTS_PATH)
    if "날짜" in df.columns:
        df["날짜"] = pd.to_datetime(df["날짜"])
    return df


def _agg_nps_sido(nps_df, freq="yearly"):
    """국민연금 데이터를 시도 레벨로 집계 (yearly 또는 monthly)"""
    if nps_df.empty or "시도" not in nps_df.columns:
        return pd.DataFrame()

    def _w_agg(g):
        w = g["NPS_가입자수"].to_numpy()
        total_w = w.sum()
        d = {
            "NPS_가입자수": total_w,
            "NPS_사업장수": g["NPS_사업장수"].sum(),
            "NPS_고용증감": g["NPS_고용증감"].sum(),
        }
        if total_w > 0 and "NPS_고지금액합계" in g.columns:
            d["NPS_1인당고지금액"] = g["NPS_고지금액합계"].sum() / total_w
        elif total_w > 0 and "NPS_1인당고지금액" in g.columns:
            d["NPS_1인당고지금액"] = np.average(g["NPS_1인당고지금액"].fillna(0), weights=w)
        else:
            d["NPS_1인당고지금액"] = np.nan
        return pd.Series(d)

    if freq == "yearly":
        keys = ["시도", "연도"]
    else:
        keys = ["시도", "연도", "월"]

    agg = nps_df.groupby(keys, group_keys=False).apply(_w_agg, include_groups=False).reset_index()
    if freq == "monthly":
        agg["연월"] = agg["연도"].astype(str) + "-" + agg["월"].apply(lambda x: f"{x:02d}")
    return agg


def _agg_rent_sido(rent_df, freq="yearly"):
    """임대차 데이터를 시도 레벨로 집계 (yearly 또는 monthly)"""
    if rent_df.empty or "시도" not in rent_df.columns:
        return pd.DataFrame()

    has_wolse = "월세평균" in rent_df.columns

    def _w_agg(g):
        w = g["임대거래량"].to_numpy()
        d = {
            "보증금평균": np.average(g["보증금평균"], weights=w),
            "임대거래량": g["임대거래량"].sum(),
            "보증금단가_per_m2": np.average(g["보증금단가_per_m2"], weights=w),
        }
        if has_wolse:
            d["월세평균"] = np.average(g["월세평균"], weights=w)
        return pd.Series(d)

    if freq == "yearly":
        keys = ["시도", "연도"]
    else:
        keys = ["시도", "연도", "월"]

    agg = rent_df.groupby(keys, group_keys=False).apply(_w_agg, include_groups=False).reset_index()
    if freq == "monthly":
        agg["연월"] = agg["연도"].astype(str) + "-" + agg["월"].apply(lambda x: f"{x:02d}")
    return agg


def merge_all(apt_df, pop_df, grdp_df, permit_df, freq="yearly",
              jeonse_df=None, wolse_df=None, rent_all_df=None,
              nps_df=None, loan_df=None, asset_df=None,
              nts_df=None,
              unsold_df=None, land_price_df=None, pop_migration_df=None,
              migration_df=None, rate_df=None,
              jeonwolse_df=None, price_index_df=None,
              csi_df=None, kb_df=None,
              construction_df=None):
    """
    전체 데이터 병합 (시도 레벨)
    freq: 'yearly' → 연도+시도 기준 병합, 'monthly' → 연월+시도 기준 병합
    construction_df: 착공/준공 파이프라인 데이터 (선택)
    """
    # apt_df를 시도 레벨로 집계
    if "시도" not in apt_df.columns:
        return apt_df

    def _weighted_agg(g):
        w = g["거래량"].to_numpy()
        return pd.Series({
            "평균가격": np.average(g["평균가격"], weights=w),
            "거래량": g["거래량"].sum(),
            "평균단가_per_m2": np.average(g["평균단가_per_m2"], weights=w),
        })

    if freq == "yearly":
        apt_agg = (
            apt_df.groupby(["시도", "연도"], group_keys=False)
            .apply(_weighted_agg, include_groups=False)
            .reset_index()
        )
        merged = apt_agg.copy()
        if not pop_df.empty:
            merged = merged.merge(pop_df, on=["시도", "연도"], how="left")
        if not grdp_df.empty:
            merged = merged.merge(grdp_df, on=["시도", "연도"], how="left")
        if not permit_df.empty:
            permit_yearly = (
                permit_df.groupby(["시도", "연도"])["인허가_호수"]
                .sum()
                .reset_index()
            )
            merged = merged.merge(permit_yearly, on=["시도", "연도"], how="left")
        merge_keys = ["시도", "연도"]

    else:  # monthly
        apt_agg = (
            apt_df.groupby(["시도", "연도", "월"], group_keys=False)
            .apply(_weighted_agg, include_groups=False)
            .reset_index()
        )
        apt_agg["연월"] = apt_agg["연도"].astype(str) + "-" + apt_agg["월"].apply(lambda x: f"{x:02d}")
        merged = apt_agg.copy()
        if not pop_df.empty:
            merged = merged.merge(pop_df, on=["시도", "연도"], how="left")
        if not grdp_df.empty:
            merged = merged.merge(grdp_df, on=["시도", "연도"], how="left")
        if not permit_df.empty:
            permit_monthly = permit_df[["시도", "연도", "월", "인허가_호수"]].copy()
            merged = merged.merge(permit_monthly, on=["시도", "연도", "월"], how="left")
        merge_keys = ["시도", "연도", "월"]

    # 임대차 데이터 병합
    for rent_src, prefix in [(jeonse_df, "전세"), (wolse_df, "월세"), (rent_all_df, "임대전체")]:
        if rent_src is not None and not rent_src.empty:
            rent_agg = _agg_rent_sido(rent_src, freq=freq)
            if not rent_agg.empty:
                rename_map = {
                    "보증금평균": f"{prefix}_보증금평균",
                    "임대거래량": f"{prefix}_거래량",
                    "보증금단가_per_m2": f"{prefix}_보증금단가",
                }
                if "월세평균" in rent_agg.columns:
                    rename_map["월세평균"] = f"{prefix}_월세평균"
                rent_agg = rent_agg.rename(columns=rename_map)
                cols_to_merge = merge_keys + [c for c in rent_agg.columns if c.startswith(prefix)]
                merged = merged.merge(rent_agg[cols_to_merge], on=merge_keys, how="left")

    # 국민연금 데이터 병합 (시도 레벨 집계)
    if nps_df is not None and not nps_df.empty:
        nps_agg = _agg_nps_sido(nps_df, freq=freq)
        if not nps_agg.empty:
            nps_cols = ["NPS_가입자수", "NPS_1인당고지금액", "NPS_사업장수", "NPS_고용증감"]
            nps_merge_cols = merge_keys + [c for c in nps_cols if c in nps_agg.columns]
            merged = merged.merge(
                nps_agg[nps_merge_cols], on=merge_keys, how="left"
            )

    # BOK 주담대 데이터 병합 (시도 레벨, 월별)
    if loan_df is not None and not loan_df.empty:
        loan_cols = ["주담대_잔액", "주담대_증감률", "주담대_비중"]
        if freq == "yearly":
            # 연도별: 12월(연말) 잔액 사용
            loan_yearly = loan_df[loan_df["월"] == 12].copy()
            loan_merge_cols = ["시도", "연도"] + [c for c in loan_cols if c in loan_yearly.columns]
            merged = merged.merge(
                loan_yearly[loan_merge_cols], on=["시도", "연도"], how="left"
            )
        else:
            loan_merge_cols = ["시도", "연도", "월"] + [c for c in loan_cols if c in loan_df.columns]
            merged = merged.merge(
                loan_df[loan_merge_cols], on=["시도", "연도", "월"], how="left"
            )

    # KOSIS 가계자산 데이터 병합 (시도 레벨, 연간)
    if asset_df is not None and not asset_df.empty:
        asset_cols = ["가구_자산평균", "가구_부채평균", "가구_순자산", "가구_소득평균", "DSR"]
        asset_merge_cols = ["시도", "연도"] + [c for c in asset_cols if c in asset_df.columns]
        merged = merged.merge(
            asset_df[asset_merge_cols], on=["시도", "연도"], how="left"
        )

    # 국세청 근로소득 데이터 병합 (시군구→시도 집계, 연간)
    if nts_df is not None and not nts_df.empty:
        nts_cols = ["총급여_인원", "총급여_금액", "1인당총급여_백만원", "1인당결정세액_백만원"]
        nts_cols = [c for c in nts_cols if c in nts_df.columns]
        if nts_cols and "시도" in nts_df.columns:
            # 시군구→시도 집계: 인원/금액은 합산, 1인당은 가중평균
            def _agg_nts_sido(df):
                rows = []
                for (sido, yr), g in df.groupby(["시도", "연도"]):
                    row = {"시도": sido, "연도": yr}
                    if "총급여_인원" in g.columns:
                        row["총급여_인원"] = g["총급여_인원"].sum()
                    if "총급여_금액" in g.columns:
                        row["총급여_금액"] = g["총급여_금액"].sum()
                    # 1인당 = 총금액/총인원
                    if row.get("총급여_인원", 0) > 0 and row.get("총급여_금액", 0) > 0:
                        row["1인당총급여_백만원"] = row["총급여_금액"] / row["총급여_인원"]
                    if "결정세액_인원" in g.columns and "결정세액_금액" in g.columns:
                        total_tax_pop = g["결정세액_인원"].sum()
                        total_tax_amt = g["결정세액_금액"].sum()
                        if total_tax_pop > 0:
                            row["1인당결정세액_백만원"] = total_tax_amt / total_tax_pop
                    rows.append(row)
                return pd.DataFrame(rows)

            nts_sido = _agg_nts_sido(nts_df)
            nts_merge_cols = ["시도", "연도"] + [c for c in nts_cols if c in nts_sido.columns]
            merged = merged.merge(
                nts_sido[nts_merge_cols], on=["시도", "연도"], how="left"
            )

    # 미분양주택 데이터 병합 (시도, 월별)
    if unsold_df is not None and not unsold_df.empty:
        if freq == "yearly":
            unsold_yearly = unsold_df.groupby(["시도", "연도"])["미분양_호수"].mean().reset_index()
            unsold_yearly = unsold_yearly.rename(columns={"미분양_호수": "미분양_평균"})
            merged = merged.merge(unsold_yearly, on=["시도", "연도"], how="left")
        else:
            unsold_cols = ["시도", "연도", "월", "미분양_호수"]
            merged = merged.merge(
                unsold_df[unsold_cols], on=["시도", "연도", "월"], how="left"
            )

    # 착공/준공 파이프라인 데이터 병합 (시도, 월별)
    if construction_df is not None and not construction_df.empty:
        constr_cols = ["착공_호수", "준공_호수"]
        constr_cols = [c for c in constr_cols if c in construction_df.columns]
        if constr_cols:
            if freq == "yearly":
                # 연도별: 착공/준공 호수 합산
                constr_yearly = (
                    construction_df.groupby(["시도", "연도"])[constr_cols]
                    .sum()
                    .reset_index()
                )
                merged = merged.merge(constr_yearly, on=["시도", "연도"], how="left")
            else:
                constr_merge_cols = ["시도", "연도", "월"] + constr_cols
                avail_cols = [c for c in constr_merge_cols if c in construction_df.columns]
                merged = merged.merge(
                    construction_df[avail_cols], on=["시도", "연도", "월"], how="left"
                )

    # 하위호환: migration_df가 전달되면 land_price_df로 사용
    if land_price_df is None and migration_df is not None:
        land_price_df = migration_df

    # 지가변동률 데이터 병합 (시도, 월별)
    if land_price_df is not None and not land_price_df.empty:
        lp_num_cols = [c for c in land_price_df.columns
                       if c not in ["연월", "시도", "연도", "월"] and land_price_df[c].dtype in [np.float64, np.int64, float, int]]
        if lp_num_cols:
            if freq == "yearly":
                lp_yearly = land_price_df.groupby(["시도", "연도"])[lp_num_cols].sum().reset_index()
                merged = merged.merge(lp_yearly, on=["시도", "연도"], how="left")
            else:
                lp_merge_cols = ["시도", "연도", "월"] + lp_num_cols
                merged = merged.merge(
                    land_price_df[lp_merge_cols], on=["시도", "연도", "월"], how="left"
                )

    # 인구이동(전입/전출/순이동) 데이터 병합 (시도, 월별)
    if pop_migration_df is not None and not pop_migration_df.empty:
        pop_mig_cols = ["전입", "전출", "순이동"]
        pop_mig_cols = [c for c in pop_mig_cols if c in pop_migration_df.columns]
        if pop_mig_cols:
            if freq == "yearly":
                pop_mig_yearly = pop_migration_df.groupby(["시도", "연도"])[pop_mig_cols].sum().reset_index()
                merged = merged.merge(pop_mig_yearly, on=["시도", "연도"], how="left")
            else:
                pop_mig_merge_cols = ["시도", "연도", "월"] + pop_mig_cols
                merged = merged.merge(
                    pop_migration_df[pop_mig_merge_cols], on=["시도", "연도", "월"], how="left"
                )

    # 기준금리 데이터 병합 (전국, 월별 → 모든 시도에 동일 적용)
    if rate_df is not None and not rate_df.empty:
        rate_cols = [c for c in rate_df.columns if c not in ["연월", "연도", "월"]]
        if rate_cols:
            if freq == "yearly":
                rate_yearly = rate_df[rate_df["월"] == 12][["연도"] + rate_cols].copy()
                merged = merged.merge(rate_yearly, on=["연도"], how="left")
            else:
                merged = merged.merge(
                    rate_df[["연도", "월"] + rate_cols], on=["연도", "월"], how="left"
                )

    # 전월세전환율 병합 (시도, 월별)
    if jeonwolse_df is not None and not jeonwolse_df.empty:
        if freq == "yearly":
            jw_yearly = jeonwolse_df.groupby(["시도", "연도"])["전월세전환율"].mean().reset_index()
            merged = merged.merge(jw_yearly, on=["시도", "연도"], how="left")
        else:
            merged = merged.merge(
                jeonwolse_df[["시도", "연도", "월", "전월세전환율"]], on=["시도", "연도", "월"], how="left"
            )

    # 주택가격지수 병합 (전국/서울, 월별)
    if price_index_df is not None and not price_index_df.empty:
        pi_cols = [c for c in price_index_df.columns if "지수" in c]
        if pi_cols:
            if freq == "yearly":
                pi_yearly = price_index_df[price_index_df["월"] == 12][["시도", "연도"] + pi_cols].copy()
                merged = merged.merge(pi_yearly, on=["시도", "연도"], how="left")
            else:
                merged = merged.merge(
                    price_index_df[["시도", "연도", "월"] + pi_cols], on=["시도", "연도", "월"], how="left"
                )

    # 소비자심리지수(CSI) 병합 (전국, 월별 → 모든 시도에 동일 적용)
    if csi_df is not None and not csi_df.empty:
        csi_cols = [c for c in csi_df.columns if c not in ["연월", "연도", "월"]]
        if csi_cols:
            if freq == "yearly":
                csi_yearly = csi_df[csi_df["월"] == 12][["연도"] + csi_cols].copy()
                merged = merged.merge(csi_yearly, on=["연도"], how="left")
            else:
                merged = merged.merge(
                    csi_df[["연도", "월"] + csi_cols], on=["연도", "월"], how="left"
                )

    # KB부동산 수급 데이터 병합 (시도, 월별)
    if kb_df is not None and not kb_df.empty:
        kb_cols = [c for c in kb_df.columns if c.startswith("KB_")]
        if kb_cols:
            if freq == "yearly":
                kb_yearly = kb_df[kb_df["월"] == 12][["시도", "연도"] + kb_cols].copy()
                merged = merged.merge(kb_yearly, on=["시도", "연도"], how="left")
            else:
                kb_merge = kb_df[["시도", "연도", "월"] + kb_cols].copy()
                merged = merged.merge(kb_merge, on=["시도", "연도", "월"], how="left")

    # ── 파생지표 계산 ────────────────────────────────────────────
    _safe = lambda col: merged[col].replace(0, np.nan)

    # 전세가율 (%)
    if "전세_보증금평균" in merged.columns and "평균가격" in merged.columns:
        merged["전세가율"] = merged["전세_보증금평균"] / _safe("평균가격") * 100

    # PIR — 가계자산 소득 기반 (배)
    if "가구_소득평균" in merged.columns and "평균가격" in merged.columns:
        merged["PIR"] = merged["평균가격"] / _safe("가구_소득평균")

    # PIR — NPS 기반 (배): NPS 1인당고지금액(원) → 연소득(만원) = 고지금액×12/10000
    if "NPS_1인당고지금액" in merged.columns and "평균가격" in merged.columns:
        nps_annual_income = merged["NPS_1인당고지금액"] * 12 / 10000
        merged["PIR_NPS"] = merged["평균가격"] / nps_annual_income.replace(0, np.nan)

    # 매매 거래회전율 (‰)
    if "거래량" in merged.columns and "총인구" in merged.columns:
        merged["매매_거래회전율"] = merged["거래량"] / _safe("총인구") * 1000

    # 전세 거래회전율 (‰)
    if "전세_거래량" in merged.columns and "총인구" in merged.columns:
        merged["전세_거래회전율"] = merged["전세_거래량"] / _safe("총인구") * 1000

    # 가격변화율 YoY (%) — 시도별 전년대비
    if freq == "yearly" and "평균가격" in merged.columns:
        merged = merged.sort_values(["시도", "연도"])
        merged["가격변화율_YoY"] = merged.groupby("시도")["평균가격"].pct_change() * 100

    # 소득대비대출 (배): 주담대_잔액(십억원)→만원 / 가구_소득평균(만원)
    if "주담대_잔액" in merged.columns and "가구_소득평균" in merged.columns:
        merged["소득대비대출"] = (merged["주담대_잔액"] * 100000) / _safe("가구_소득평균")

    return merged


def load_all_data(force_rebuild=False):
    """모든 데이터를 로드하고 병합하여 반환"""
    import streamlit as st

    with st.spinner("아파트 실거래 데이터 로딩 중..."):
        apt_df = load_apt_data(force_rebuild=force_rebuild)
    with st.spinner("전세 데이터 로딩 중..."):
        jeonse_df = load_rent_data("jeonse", force_rebuild=force_rebuild)
    with st.spinner("월세 데이터 로딩 중..."):
        wolse_df = load_rent_data("wolse", force_rebuild=force_rebuild)
    with st.spinner("전체 임대차 데이터 로딩 중..."):
        rent_all_df = load_rent_data("all", force_rebuild=force_rebuild)
    with st.spinner("인구 데이터 로딩 중..."):
        pop_df = load_population_data()
    with st.spinner("GRDP 데이터 로딩 중..."):
        grdp_df = load_grdp_data()
    with st.spinner("인허가 데이터 로딩 중..."):
        permit_df = load_permit_data()
    with st.spinner("국민연금 데이터 로딩 중..."):
        nps_df = load_nps_data()
    with st.spinner("주담대 데이터 로딩 중..."):
        loan_df = load_housing_loan_data()
    with st.spinner("가계자산 데이터 로딩 중..."):
        asset_df = load_household_asset_data()
    with st.spinner("국세청 근로소득 데이터 로딩 중..."):
        nts_df = load_nts_income_data()
    with st.spinner("미분양 데이터 로딩 중..."):
        unsold_df = load_unsold_housing_data()
    with st.spinner("지가변동률 데이터 로딩 중..."):
        land_price_df = load_land_price_data()
    with st.spinner("인구이동 데이터 로딩 중..."):
        pop_migration_df = load_population_migration_data()
    with st.spinner("기준금리 데이터 로딩 중..."):
        rate_df = load_base_rate_data()
    with st.spinner("전월세전환율 데이터 로딩 중..."):
        jeonwolse_df = load_jeonwolse_rate_data()
    with st.spinner("주택가격지수 데이터 로딩 중..."):
        price_index_df = load_price_index_data()
    with st.spinner("소비자심리지수 데이터 로딩 중..."):
        csi_df = load_csi_data()
    with st.spinner("KB부동산 수급 데이터 로딩 중..."):
        kb_df = load_kb_market_data()
    with st.spinner("착공/준공 파이프라인 데이터 로딩 중..."):
        construction_df = load_construction_data()

    merge_kwargs = dict(
        jeonse_df=jeonse_df, wolse_df=wolse_df, rent_all_df=rent_all_df,
        nps_df=nps_df, loan_df=loan_df, asset_df=asset_df,
        nts_df=nts_df,
        unsold_df=unsold_df, land_price_df=land_price_df,
        pop_migration_df=pop_migration_df, rate_df=rate_df,
        jeonwolse_df=jeonwolse_df, price_index_df=price_index_df,
        csi_df=csi_df, kb_df=kb_df,
        construction_df=construction_df,
    )

    yearly = merge_all(apt_df, pop_df, grdp_df, permit_df, freq="yearly", **merge_kwargs)
    monthly = merge_all(apt_df, pop_df, grdp_df, permit_df, freq="monthly", **merge_kwargs)

    return {
        "apt": apt_df,
        "jeonse": jeonse_df,
        "wolse": wolse_df,
        "rent_all": rent_all_df,
        "pop": pop_df,
        "grdp": grdp_df,
        "permit": permit_df,
        "nps": nps_df,
        "loan": loan_df,
        "asset": asset_df,
        "nts": nts_df,
        "unsold": unsold_df,
        "land_price": land_price_df,
        "pop_migration": pop_migration_df,
        "migration": land_price_df,  # 하위호환: 기존 코드에서 "migration" 키 참조 시
        "rate": rate_df,
        "jeonwolse": jeonwolse_df,
        "price_index": price_index_df,
        "csi": csi_df,
        "kb": kb_df,
        "policy_events": load_policy_events(),
        "yearly": yearly,
        "monthly": monthly,
    }

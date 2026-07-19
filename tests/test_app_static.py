import ast
from pathlib import Path


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"


def _leader_apartment_block() -> str:
    source = APP_PATH.read_text(encoding="utf-8")
    start = source.find("# Page: 대장아파트")
    assert start != -1, "대장아파트 페이지 블록 주석이 필요합니다"
    body_start = source.find("if leader_apt_tab:", start)
    assert body_start != -1, "대장아파트 페이지 본문이 필요합니다"
    next_marker = source.find("# ============================", body_start)
    return source[start:] if next_marker == -1 else source[start:next_marker]


def _indicator_catalog_columns():
    tree = ast.parse(APP_PATH.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "INDICATOR_CATALOG":
                    return {item["column"] for item in ast.literal_eval(node.value)}
    raise AssertionError("INDICATOR_CATALOG not found")


def test_movein_indicators_are_available_in_superchart_catalog():
    columns = _indicator_catalog_columns()

    assert "입주예정_세대수" in columns
    assert "입주예정_단지수" in columns


def test_saved_chart_share_link_uses_selected_chart_token_not_global_stale_token():
    source = APP_PATH.read_text(encoding="utf-8")

    assert 'st.session_state.get("last_chart_share_token") or selected_saved_row.get("share_token")' not in source
    assert 'last_chart_share_id' in source
    assert 'key=f"saved_chart_share_link_{selected_chart_id}"' in source


def test_listing_status_redesign_contract_is_present():
    source = APP_PATH.read_text(encoding="utf-8")

    assert '"매물현황"' in source
    assert 'def _parse_korean_price_to_manwon' in source
    assert 'def _build_complex_listing_summary' in source
    assert '호가 분포' in source
    assert '지역별 매물 수 Top 15' in source
    assert '단지별 매물현황' in source
    assert 'listing_price_range_v2' in source
    assert 'listing_complex_download_v2' in source


def test_listing_price_parser_replaces_old_first_number_extraction():
    source = APP_PATH.read_text(encoding="utf-8")

    assert 'normalized["매물가격"] = normalized["매물가격"].apply(_parse_korean_price_to_manwon)' in source
    assert '.str.extract(r"([\\d.]+)")[0]\n        .astype(float)' not in source


def test_top_level_tab_usage_guides_are_visible_to_users():
    source = APP_PATH.read_text(encoding="utf-8")

    for tab_name in ["Overview", "매수판단", "수요공급분석", "거래현황", "대장아파트", "매물현황", "자유차트"]:
        assert f'"{tab_name}"' in source
        assert f'render_tab_usage_guide("{tab_name}")' in source

    assert "처음 보는 분을 위한 사용법" in source
    assert "**예시**" in source


def test_leader_apartment_page_contract_is_present():
    source = APP_PATH.read_text(encoding="utf-8")

    for text in [
        '"🧭 대장아파트"',
        'leader_apt_tab',
        'get_apt_complex_data()',
        'select_leader_apartments(',
        'get_leader_apartment_flow(',
        'get_region_market_flow(',
        '"대장아파트_지역단지흐름"',
        '"평균평당가격"',
        '"거래량"',
        '"선정 기준과 주의사항"',
    ]:
        assert text in source


def test_leader_apartment_loader_is_streamlit_hot_reload_safe():
    source = APP_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)

    imported_from_data_loader = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "data_loader"
        for alias in node.names
    }
    assert "load_apt_complex_data" not in imported_from_data_loader
    assert 'getattr(_data_loader, "load_apt_complex_data", None)' in source
    assert '"apt_complex_monthly.parquet"' in source


def test_leader_apartment_helpers_are_streamlit_hot_reload_safe():
    source = APP_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)

    imported_from_leader = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "leader_apartment"
        for alias in node.names
    }
    assert not imported_from_leader
    assert "import leader_apartment as _leader_apartment" in source
    assert "importlib.reload(_leader_apartment)" in source
    lazy_helper_names = {
        node.args[1].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "getattr"
        and len(node.args) >= 3
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id == "_leader_apartment"
        and isinstance(node.args[1], ast.Constant)
    }
    for helper in {
        "extract_selected_region_code",
        "get_leader_apartment_flow",
        "get_region_market_flow",
        "map_region_code",
        "select_leader_apartments",
    }:
        assert helper in lazy_helper_names
    assert "if not LEADER_HELPERS_READY:" in source


def test_leader_apartment_uses_one_month_range_for_selection_and_both_flows():
    block = _leader_apartment_block()

    assert 'leader_start_period, leader_end_period = st.select_slider(' in block
    assert 'leader_period_options[-min(24, len(leader_period_options))]' in block
    assert 'key="leader_period_range"' in block
    assert block.count('start_period=leader_start_period') == 3
    assert block.count('end_period=leader_end_period') == 3
    assert 'select_leader_apartments(' in block
    assert 'get_leader_apartment_flow(' in block
    assert 'get_region_market_flow(' in block


def test_leader_apartment_map_is_selectable_and_keeps_original_region_code():
    block = _leader_apartment_block()

    for text in [
        '"geo_data", "sigungu.geojson"',
        'px.choropleth_map(',
        'featureidkey="properties.SIG_CD"',
        'color="평균평당가격"',
        'custom_data=["지역코드"]',
        'leader_rank["지도지역코드"] = leader_rank["지역코드"].apply(map_region_code)',
        'key="leader_apartment_map"',
        'on_select=_sync_leader_map_selection',
        'selection_mode="points"',
        'extract_selected_region_code(',
        'st.session_state["leader_region_code"] = str(selected_code)',
        '지도에서 제외된 지역',
        '실제 좌표가 아닌 시군구 행정구역',
    ]:
        assert text in block

    assert 'leader_map_views = {' in block
    assert 'width="stretch"' in block


def test_leader_apartment_flow_compares_region_and_complex_on_two_axes():
    block = _leader_apartment_block()

    for trace_name in [
        'name="지역 전체 거래량"',
        'name="대장단지 거래량"',
        'name="지역 전체 평당가"',
        'name="대장단지 평당가"',
    ]:
        assert trace_name in block

    assert 'make_subplots(specs=[[{"secondary_y": True}]])' in block
    assert 'register_fig("대장아파트_지역단지흐름"' in block
    assert 'st.plotly_chart(leader_fig, width="stretch")' in block

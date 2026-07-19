import ast
from pathlib import Path


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"


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
        '"대장아파트_가격거래량"',
        '"평균 평당가격"',
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

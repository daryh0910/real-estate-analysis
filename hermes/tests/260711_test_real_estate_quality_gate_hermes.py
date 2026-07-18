from __future__ import annotations

import importlib.util
import json
from pathlib import Path

MODULE_PATH = Path(__file__).parents[1] / "scripts" / "260711_real_estate_quality_gate_hermes.py"


def load_module():
    spec = importlib.util.spec_from_file_location("real_estate_quality_gate", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_extract_primary_tabs_ignores_earlier_nested_tabs():
    mod = load_module()
    source = '''
import streamlit as st
inner_a, inner_b = st.tabs(["시계열 비교", "가격비교"])
tab_overview, tab_buy, tab_data = st.tabs(["Overview", "매수판단", "데이터"])
'''
    assert mod.extract_primary_tabs(source) == ["Overview", "매수판단", "데이터"]


def test_extract_primary_tabs_handles_multiline_labels():
    mod = load_module()
    source = '''
a, b = st.tabs([
    "Overview",
    "매수판단",
])
'''
    assert mod.extract_primary_tabs(source) == ["Overview", "매수판단"]


def test_extract_primary_navigation_prefers_segmented_page_constant():
    mod = load_module()
    source = '''
_PAGES = ["🧭 Overview", "🧭 수요공급분석", "🔬 매수판단"]
_sel = st.segmented_control("분석 화면", _PAGES)
a, b, c = st.tabs(["시계열 비교", "가격비교", "갭분석"])
'''
    result = mod.audit_app_source(source)
    assert result["navigation_mode"] == "segmented_control"
    assert result["primary_navigation"] == ["🧭 Overview", "🧭 수요공급분석", "🔬 매수판단"]


def test_audit_reports_required_navigation_items():
    mod = load_module()
    result = mod.audit_app_source('a,b=st.tabs(["Overview","매수판단"])')
    assert result["primary_navigation"][:2] == ["Overview", "매수판단"]
    assert result["checks"]["buy_present"] is True


def test_inventory_data_files_reports_metadata_without_loading_payload(tmp_path: Path):
    mod = load_module()
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "sample.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    inventory = mod.inventory_data_files(tmp_path)
    assert inventory[0]["path"] == "cache/sample.csv"
    assert inventory[0]["size_bytes"] > 0
    assert "modified_at" in inventory[0]


def test_cli_writes_strict_json_and_markdown(tmp_path: Path):
    mod = load_module()
    project = tmp_path / "project"
    project.mkdir()
    (project / "app.py").write_text('a,b=st.tabs(["Overview","매수판단"])', encoding="utf-8")
    out = tmp_path / "out"
    rc = mod.main(["--project-root", str(project), "--output-dir", str(out)])
    assert rc == 0
    json_path = out / "260711_real_estate_quality_gate_hermes.json"
    md_path = out / "260711_real_estate_quality_gate_hermes.md"
    assert json_path.exists() and md_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["app"]["checks"]["buy_present"] is True

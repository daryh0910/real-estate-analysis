import ast
import re
from pathlib import Path


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"


def _source() -> str:
    return APP_PATH.read_text(encoding="utf-8")


def _buy_decision_block() -> str:
    source = _source()
    start = source.find("# Tab: 매수판단")
    assert start != -1, "매수판단 탭 블록 주석이 필요합니다"
    next_marker = source.find("# ============================", start + 1)
    return source[start:] if next_marker == -1 else source[start:next_marker]


def test_buy_decision_and_leader_are_in_segmented_navigation():
    tree = ast.parse(_source())
    pages = None
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "_PAGES" for target in node.targets
        ):
            pages = ast.literal_eval(node.value)
            break

    assert pages is not None, "상위 segmented navigation 목록을 찾을 수 없습니다"
    assert pages[0] == "🧭 Overview"
    assert "🔬 매수판단" in pages
    assert pages.index("🧭 대장아파트") == pages.index("🧭 거래현황") + 1


def test_app_imports_buy_decision_view_model_or_region_compare():
    tree = ast.parse(_source())
    imported_modules = []
    imported_names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported_modules.append(node.module or "")
            imported_names.extend(alias.name for alias in node.names)

    assert (
        "buy_decision.view_model" in imported_modules
        or "buy_decision.region_compare" in imported_modules
        or "build_buy_decision_view_model" in imported_names
        or "compare_regions" in imported_names
    )


def test_app_does_not_define_buy_decision_analysis_functions():
    tree = ast.parse(_source())
    forbidden_defs = {"calculate_buy_score", "compare_regions", "calculate_buy_decision", "score_region_for_purpose"}
    defined_functions = {node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}

    assert forbidden_defs.isdisjoint(defined_functions)


def test_buy_decision_block_has_required_ui_sections_and_module_call():
    block = _buy_decision_block()

    for text in [
        "어디를 살까?",
        "지역 A",
        "지역 B",
        "목적",
        "종합 판단",
        "핵심 근거",
        "다음 확인사항",
        "강점",
        "주의축",
        "비교표",
        "데이터 품질/주의사항",
    ]:
        assert text in block

    assert "build_buy_decision_view_model(" in block or "compare_regions(" in block
    assert block.count("st.selectbox") >= 2
    assert "st.radio" in block
    assert block.count("st.plotly_chart") >= 1


def test_buy_decision_block_keeps_analysis_logic_out_of_app_py():
    block = _buy_decision_block()

    forbidden_patterns = [
        r"PURPOSE_WEIGHTS",
        r"threshold\s*=",
        r"thresholds\s*=",
        r"rolling\s*\(",
        r"quantile\s*\(",
        r"rank\s*\(\s*pct\s*=\s*True",
        r"\*\s*0\.\d+\s*\+",
        r"사용자\s*가중치",
        r"가중치\s*커스터마이징",
        r"가격예측",
        r"ML",
        r"머신러닝",
        r"매수 적기",
        r"투자 유망",
        r"추천합니다",
        r"확실",
        r"보장",
    ]
    for pattern in forbidden_patterns:
        assert not re.search(pattern, block), f"매수판단 UI 블록에 금지 패턴이 있습니다: {pattern}"

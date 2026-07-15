#!/usr/bin/env python3
"""부동산 대시보드의 tab 구조와 데이터 파일 metadata를 검사한다."""
from __future__ import annotations

import argparse
import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

OUTPUT_STEM = "260711_real_estate_quality_gate_hermes"
DATA_SUFFIXES = {".csv", ".parquet", ".json", ".xlsx"}
DATA_DIRS = ("cache", "data", "geo_data")


def _string_list(node: ast.AST) -> list[str] | None:
    if not isinstance(node, (ast.List, ast.Tuple)):
        return None
    labels: list[str] = []
    for item in node.elts:
        if not isinstance(item, ast.Constant) or not isinstance(item.value, str):
            return None
        labels.append(item.value)
    return labels


def extract_primary_tabs(source: str) -> list[str]:
    tree = ast.parse(source)
    candidates: list[list[str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "tabs"):
            continue
        labels = _string_list(node.args[0])
        if labels:
            candidates.append(labels)
    if not candidates:
        return []
    return max(candidates, key=lambda labels: (len(labels), labels[:1] == ["Overview"]))


def extract_segmented_navigation(source: str) -> list[str]:
    tree = ast.parse(source)
    constants: dict[str, list[str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            labels = _string_list(node.value)
            if labels:
                constants[node.targets[0].id] = labels

    candidates: list[list[str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or len(node.args) < 2:
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "segmented_control"):
            continue
        labels = _string_list(node.args[1])
        if labels:
            candidates.append(labels)
        elif isinstance(node.args[1], ast.Name) and node.args[1].id in constants:
            candidates.append(constants[node.args[1].id])
    return max(candidates, key=len) if candidates else []


def _has_label(labels: list[str], expected: str) -> bool:
    return any(expected in label for label in labels)


def audit_app_source(source: str) -> dict[str, object]:
    segmented = extract_segmented_navigation(source)
    navigation = segmented or extract_primary_tabs(source)
    mode = "segmented_control" if segmented else "tabs"
    checks = {
        "primary_navigation_found": bool(navigation),
        "overview_first": bool(navigation) and "Overview" in navigation[0],
        "buy_present": _has_label(navigation, "매수판단"),
    }
    return {
        "navigation_mode": mode,
        "primary_navigation": navigation,
        "checks": checks,
        "status": "pass" if all(checks.values()) else "fail",
    }


def inventory_data_files(project_root: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for dirname in DATA_DIRS:
        directory = project_root / dirname
        if not directory.exists():
            continue
        for path in directory.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in DATA_SUFFIXES:
                continue
            stat = path.stat()
            records.append({
                "path": path.relative_to(project_root).as_posix(),
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                "suffix": path.suffix.lower(),
            })
    return sorted(records, key=lambda item: str(item["path"]))


def render_markdown(payload: dict[str, Any]) -> str:
    app = payload["app"]
    inv = payload["data_inventory"]
    lines = [
        "# real_estate_analysis Quality Gate",
        "",
        f"> 생성시각(UTC): {payload['generated_at']}",
        f"> 앱 구조 판정: **{app['status'].upper()}**",
        "",
        f"## Primary navigation ({app['navigation_mode']})",
        "",
        " → ".join(app["primary_navigation"]) if app["primary_navigation"] else "N/A",
        "",
        "## 구조 체크",
        "",
    ]
    for key, value in app["checks"].items():
        lines.append(f"- {'PASS' if value else 'FAIL'}: `{key}`")
    lines += ["", "## 데이터 파일 인벤토리", "", f"- 파일 수: {len(inv)}", f"- 총 크기(bytes): {sum(item['size_bytes'] for item in inv)}", "", "| 경로 | 형식 | 크기(bytes) | 수정시각(UTC) |", "|---|---|---:|---|"]
    for item in inv:
        lines.append(f"| {item['path']} | {item['suffix']} | {item['size_bytes']} | {item['modified_at']} |")
    lines += ["", "## 해석 제한", "", "- metadata inventory는 데이터 내용의 정확성이나 point-in-time 적합성을 보장하지 않는다.", "- 기존 pytest 실패는 별도로 실행·기록해야 한다.", "- 이 검사는 Linux 브라우저를 실행하지 않는 정적 quality gate다."]
    return "\n".join(lines) + "\n"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    root = args.project_root.resolve()
    app = audit_app_source((root / "app.py").read_text(encoding="utf-8"))
    inventory = inventory_data_files(root)
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "project_root": str(root), "app": app, "data_inventory": inventory}
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / f"{OUTPUT_STEM}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    (args.output_dir / f"{OUTPUT_STEM}.md").write_text(render_markdown(payload), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

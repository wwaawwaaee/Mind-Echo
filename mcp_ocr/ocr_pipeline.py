import base64
import json
import os
import re
from typing import Dict, List, Tuple

import requests

from mcp_ocr.paddleocr_config import API_URL, OPTIONAL_PAYLOAD, TOKEN


META_PATTERNS = {
    "姓名": [
        r"(?:姓名|患者姓名)\s*[:：]?\s*([^\n\r]+)",
    ],
    "性别": [
        r"(?:性别|性别/性)\s*[:：]?\s*([^\n\r]+)",
        r"\b(男|女|男性|女性)\b",
    ],
    "年龄": [
        r"(?:年龄|岁数)\s*[:：]?\s*([^\n\r]+)",
        r"(\d{1,3}\s*岁)",
    ],
    "记录日期": [
        r"(?:记录日期|就诊日期|检查日期|日期)\s*[:：]?\s*([^\n\r]+)",
        r"(\d{4}[-/.年]\d{1,2}[-/.月]\d{1,2}日?)",
    ],
}

CASE_LABELS: List[Tuple[str, List[str]]] = [
    ("主诉", ["主诉"]),
    ("现病史", ["现病史", "现病史摘要"]),
    ("既往史", ["既往史", "既往病史"]),
    ("体格检查", ["体格检查", "体检", "查体"]),
    ("初步判断", ["初步判断", "初步诊断", "诊断", "初诊判断"]),
    ("处理", ["处理", "处置", "处理意见", "治疗", "治疗方案"]),
]


def _normalize_line(line: str) -> str:
    line = line.replace("：", ":")
    return re.sub(r"\s+", " ", line).strip()


def _find_first_match(text: str, patterns: List[str]) -> str:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            value = match.group(1) if match.lastindex else match.group(0)
            return value.strip()
    return ""


def _parse_case_sections(lines: List[str]) -> Dict[str, str]:
    result = {key: "" for key, _ in CASE_LABELS}
    current_key = None

    for raw_line in lines:
        line = _normalize_line(raw_line)
        if not line:
            continue

        matched = False
        for key, labels in CASE_LABELS:
            for label in labels:
                pattern = rf"^{re.escape(label)}\s*[:：]?\s*(.*)$"
                match = re.match(pattern, line)
                if match:
                    current_key = key
                    content = match.group(1).strip()
                    if content:
                        result[key] = _append_text(result[key], content)
                    matched = True
                    break
            if matched:
                break

        if not matched and current_key:
            result[current_key] = _append_text(result[current_key], line)

    return result


def _append_text(existing: str, new_text: str) -> str:
    if not existing:
        return new_text
    return f"{existing}\n{new_text}"


def extract_structured_fields(lines: List[str]) -> Dict[str, Dict[str, str]]:
    text = "\n".join(lines)
    metadata = {key: _find_first_match(text, patterns) for key, patterns in META_PATTERNS.items()}
    case_data = _parse_case_sections(lines)
    return {"元数据": metadata, "病例数据": case_data}


def _file_to_base64(file_path: str) -> str:
    with open(file_path, "rb") as file:
        file_bytes = file.read()
    return base64.b64encode(file_bytes).decode("ascii")


def _detect_file_type(file_path: str) -> int:
    ext = os.path.splitext(file_path)[1].lower()
    return 0 if ext == ".pdf" else 1


def call_layout_api(file_path: str) -> Dict:
    print(f"[DEBUG] call_layout_api called with file: {file_path}")
    print(f"[DEBUG] TOKEN length: {len(TOKEN)}, API_URL: {API_URL}")
    if not TOKEN:
        raise RuntimeError("TOKEN is empty. Please set it in mcp_ocr/paddleocr_config.py.")

    file_data = _file_to_base64(file_path)
    headers = {
        "Authorization": f"token {TOKEN}",
        "Content-Type": "application/json",
    }

    required_payload = {
        "file": file_data,
        "fileType": _detect_file_type(file_path),
    }

    payload = {**required_payload, **(OPTIONAL_PAYLOAD or {})}
    response = requests.post(API_URL, json=payload, headers=headers, timeout=60)
    response.raise_for_status()
    return response.json()


def extract_markdown_lines(api_result: Dict) -> List[List[str]]:
    results = api_result.get("result", {}).get("layoutParsingResults", [])
    markdown_groups: List[List[str]] = []

    for res in results:
        md_text = res.get("markdown", {}).get("text", "")
        lines = md_text.splitlines()
        markdown_groups.append(lines)

    return markdown_groups


def write_json(data: Dict, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

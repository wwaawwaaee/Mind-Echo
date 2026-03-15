import argparse
import os
from typing import List

import requests

from mcp_ocr.ocr_pipeline import (
    call_layout_api,
    extract_markdown_lines,
    extract_structured_fields,
    write_json,
)


IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff", ".webp"}
DOC_EXTS = IMAGE_EXTS | {".pdf"}


def _collect_files(input_path: str, recursive: bool) -> List[str]:
    if os.path.isfile(input_path):
        return [input_path]

    files = []
    if recursive:
        for root, _, files in os.walk(input_path):
            for name in files:
                if os.path.splitext(name)[1].lower() in DOC_EXTS:
                    files.append(os.path.join(root, name))
    else:
        for name in os.listdir(input_path):
            full_path = os.path.join(input_path, name)
            if os.path.isfile(full_path) and os.path.splitext(name)[1].lower() in DOC_EXTS:
                files.append(full_path)

    return files


def _resolve_output_path(input_path: str, output_path: str, input_file: str, suffix: str) -> str:
    input_is_dir = os.path.isdir(input_path)
    base_name = os.path.splitext(os.path.basename(input_file))[0] + suffix

    if not output_path:
        if input_is_dir:
            output_dir = os.path.join(input_path, "ocr_json")
        else:
            output_dir = os.path.dirname(input_path) or "."
        os.makedirs(output_dir, exist_ok=True)
        return os.path.join(output_dir, base_name)

    if input_is_dir or os.path.isdir(output_path):
        os.makedirs(output_path, exist_ok=True)
        return os.path.join(output_path, base_name)

    return output_path


def _save_markdown_assets(output_dir: str, res: dict, index: int) -> None:
    md_filename = os.path.join(output_dir, f"doc_{index}.md")
    with open(md_filename, "w", encoding="utf-8") as md_file:
        md_file.write(res.get("markdown", {}).get("text", ""))

    for img_path, img in (res.get("markdown", {}).get("images") or {}).items():
        full_img_path = os.path.join(output_dir, img_path)
        os.makedirs(os.path.dirname(full_img_path), exist_ok=True)
        img_bytes = requests.get(img, timeout=30).content
        with open(full_img_path, "wb") as img_file:
            img_file.write(img_bytes)

    for img_name, img in (res.get("outputImages") or {}).items():
        img_response = requests.get(img, timeout=30)
        if img_response.status_code == 200:
            filename = os.path.join(output_dir, f"{img_name}_{index}.jpg")
            with open(filename, "wb") as f:
                f.write(img_response.content)


def main() -> int:
    parser = argparse.ArgumentParser(description="Call layout-parsing API and output structured JSON.")
    parser.add_argument("--input", required=True, help="Image/PDF file or directory path.")
    parser.add_argument("--output", help="Output JSON file or directory path.")
    parser.add_argument("--assets-dir", help="Directory to store markdown/images.")
    parser.add_argument("--recursive", action="store_true", help="Scan directories recursively.")
    args = parser.parse_args()

    files = _collect_files(args.input, args.recursive)
    if not files:
        raise SystemExit("No images or PDFs found in input path.")

    for file_path in files:
        api_result = call_layout_api(file_path)
        markdown_groups = extract_markdown_lines(api_result)
        layout_results = api_result.get("result", {}).get("layoutParsingResults", [])

        for index, lines in enumerate(markdown_groups):
            data = extract_structured_fields(lines)
            out_path = _resolve_output_path(args.input, args.output, file_path, f"_{index}.json")
            write_json(data, out_path)

            if args.assets_dir and index < len(layout_results):
                os.makedirs(args.assets_dir, exist_ok=True)
                _save_markdown_assets(args.assets_dir, layout_results[index], index)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

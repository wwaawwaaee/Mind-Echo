import json
import re
from pathlib import Path

import pandas as pd


def _split_visits(content: str):
    lines = content.splitlines()
    segments = []
    current = []
    for line in lines:
        stripped = line.strip()
        if (stripped.startswith("（") and stripped.endswith("）")) or (
            stripped.startswith("(") and stripped.endswith(")")
        ):
            if current:
                segments.append("\n".join(current).strip())
                current = []
            continue
        current.append(line)
    if current:
        segments.append("\n".join(current).strip())
    return [s for s in segments if s]


def _normalize_role(raw_role: str):
    if raw_role.startswith("D") or "医生" in raw_role:
        return "doctor"
    if "家属" in raw_role or "家长" in raw_role:
        return "caregiver"
    if raw_role.startswith("P") or "患者" in raw_role:
        return "patient"
    return "other"


def _parse_turns(content: str):
    turns = []
    current_role = None
    current_note = None
    buffer = []

    def flush():
        nonlocal buffer, current_role, current_note
        if current_role and buffer:
            text = "".join(buffer).strip()
            if text:
                turn = {"role": current_role, "text": text}
                if current_note:
                    turn["speaker_note"] = current_note
                turns.append(turn)
        buffer = []
        current_role = None
        current_note = None

    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        match = re.match(r"^[\[【]([^\]】]+)[\]】[:：]?\s*(.*)$", line)
        if match:
            flush()
            raw_role = match.group(1)
            text = match.group(2).strip()
            note = None
            role = _normalize_role(raw_role)
            if "（" in raw_role and "）" in raw_role:
                note = raw_role.split("（", 1)[1].split("）", 1)[0]
            current_role = role
            current_note = note
            if text:
                buffer.append(text)
            continue
        if current_role:
            buffer.append(line)
    flush()
    return turns


def _extract_keywords_and_body(text: str):
    keywords = None
    body = text
    match = re.search(r"关键词[:：]?\s*\n(.+?)(?:\n\s*\n|\n(?:文字记录|场景)[:：])", text, flags=re.S)
    if match:
        kw_line = match.group(1).strip()
        parsed = [k.strip(" ，、") for k in re.split(r"[、，]", kw_line) if k.strip()]
        if parsed:
            keywords = parsed
        start = text.find(match.group(0))
        if start != -1:
            body = (text[:start] + "\n" + text[start + len(match.group(0)) :]).strip()
    body = re.sub(r"^(文字记录|场景)[:：]\s*", "", body.strip())
    return keywords, body


def parse_dialogue(path: Path):
    text = path.read_text(encoding="utf-8")
    keywords, body = _extract_keywords_and_body(text)

    start = body.find("【D】")
    if start == -1:
        start = body.find("[医生]")
    if start == -1:
        start = body.find("【P】")

    content = body[start:].strip() if start != -1 else body.strip()
    visit_contents = _split_visits(content)
    visit_turns = [_parse_turns(vc) for vc in visit_contents]

    return {
        "keywords": keywords,
        "visit_contents": visit_contents,
        "visit_turns": visit_turns,
    }


def parse_scales(df: pd.DataFrame, ids):
    rows = df[df["序号"].isin(ids)]

    results = []
    for _, row in rows.iterrows():
        g_items = []
        for i in range(1, 8):
            col = next(c for c in df.columns if c.startswith(f"G{i}. 在过去2个星期"))
            g_items.append(int(row[col]))
        g_total = sum(g_items)

        p_items = []
        for i in range(1, 10):
            col = next(c for c in df.columns if c.startswith(f"P{i}. 在过去2个星期"))
            p_items.append(int(row[col]))
        p_total = sum(p_items)

        results.append(
            {
                "respondent_role": "self",
                "GAD-7": {"items": g_items, "total": g_total},
                "PHQ-9": {"items": p_items, "total": p_total},
            }
        )

    return results


def _parse_file_title(stem: str):
    match = re.match(r"^(\d+(?:，\d+)*)\s*(.+)$", stem)
    if not match:
        raise ValueError(f"文件名不符合约定格式: {stem}")

    ids = [int(x) for x in match.group(1).split("，")]
    tail = match.group(2).strip()

    meta = {"title_raw": tail}
    match_fixed = re.match(r"^(?P<name>.+?)\s+(?P<gender>男|女)\s+(?P<age>\d+)\s*岁?$", tail)
    if match_fixed:
        meta["name"] = match_fixed.group("name").strip()
        meta["gender"] = match_fixed.group("gender")
        meta["age"] = int(match_fixed.group("age"))
        return ids, meta

    match_gender = re.match(r"^(?P<name>.+?)\s+(?P<gender>男|女)\s*$", tail)
    if match_gender:
        meta["name"] = match_gender.group("name").strip()
        meta["gender"] = match_gender.group("gender")
        return ids, meta

    meta["name"] = tail
    return ids, meta


def build_patient(dialogue_path: Path, score_df: pd.DataFrame): #返回患者类
    ids, meta = _parse_file_title(dialogue_path.stem)
    dialogue = parse_dialogue(dialogue_path)
    scales = parse_scales(score_df, ids)

    visits = []
    for i, visit_content in enumerate(dialogue["visit_contents"]):
        visits.append(
            {
                "visit_id": f"V-{ids[0]:06d}-{i+1}",
                "dialogue": {
                    "source_file": dialogue_path.name,
                    "content": visit_content,
                    "turns": dialogue["visit_turns"][i] if i < len(dialogue["visit_turns"]) else [],
                },
            }
        )

    patient = {
        "patient_id": f"P-{ids[0]:06d}",
        "name": meta.get("name"),
        "scales": scales,
        "visits": visits,
    }
    if "gender" in meta:
        patient["gender"] = meta["gender"]
    if "age" in meta:
        patient["age"] = meta["age"]
    if dialogue["keywords"]:
        patient["keywords"] = dialogue["keywords"]

    return patient


def build_dataset(dialogue_root: Path, score_path: Path): #处理全部.txt文本
    score_df = pd.read_excel(score_path)
    stats = {
        "total_files": 0,
        "converted_files": 0,
        "failed_files": 0,
        "patients_with_keywords": 0,
        "patients_with_gender": 0,
        "patients_with_age": 0,
        "patients_with_scales": 0,
        "total_visits": 0,
        "errors": [],
    }
    patients = []

    for path in sorted(dialogue_root.rglob("*.txt")):
        stats["total_files"] += 1
        try:
            patient = build_patient(path, score_df)
            patients.append(patient)
            stats["converted_files"] += 1
            stats["total_visits"] += len(patient.get("visits", []))
            if patient.get("keywords"):
                stats["patients_with_keywords"] += 1
            if patient.get("gender"):
                stats["patients_with_gender"] += 1
            if patient.get("age") is not None:
                stats["patients_with_age"] += 1
            if patient.get("scales"):
                stats["patients_with_scales"] += 1
        except Exception as exc:
            stats["failed_files"] += 1
            stats["errors"].append({"file": str(path), "error": str(exc)})

    return {
        "dataset_meta": {
            "schema_version": "0.2",
            "patient_centered": True,
            "source_dir": str(dialogue_root),
        },
        "stats": stats,
        "patients": patients,
    }


def main():
    dialogue_root = Path("processed_data/anonymized_dialogues")
    score_path = Path("raw_data/diagram/score.csv")
    output_path = Path("processed_data/output/anonymized_dataset.json")

    dataset = build_dataset(dialogue_root, score_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(dataset, ensure_ascii=False, indent=2), encoding="utf-8")

    stats = dataset["stats"]
    print(f"Wrote dataset: {output_path}")
    print(
        "Converted: "
        f"{stats['converted_files']}/{stats['total_files']}, "
        f"failed: {stats['failed_files']}, "
        f"visits: {stats['total_visits']}"
    )


if __name__ == "__main__":
    main()

"""Window-specific visit reconstruction from raw turn boundaries."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .manifest import ordered_manifest_keys
from .paths import RAW_WITH_CAREGIVER_PATH


Row = dict[str, Any]


def extract_early_role_text(turns: list[Row], target_role: str, max_role_turns: int, max_chars: int = 1000) -> str:
    """Match core/01_prepare_data.py early target-role text semantics."""
    text_parts = []
    char_count = 0
    role_turn_count = 0
    for turn in turns:
        if turn.get("role") != target_role:
            continue
        text = str(turn.get("text", "")).strip()
        if not text:
            continue
        if char_count + len(text) > max_chars:
            break
        text_parts.append(f"{target_role}: {text}")
        char_count += len(text)
        role_turn_count += 1
        if role_turn_count >= max_role_turns:
            break
    return "\n".join(text_parts)


def extract_role_text(turns: list[Row], target_role: str) -> str:
    texts = []
    for turn in turns:
        if turn.get("role") != target_role:
            continue
        text = str(turn.get("text", "")).strip()
        if text:
            texts.append(text)
    return " ".join(texts)


def load_raw_with_caregiver(path: Path = RAW_WITH_CAREGIVER_PATH) -> Row:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def reconstruct_window_rows(manifest_data: list[Row], window: str, raw_path: Path = RAW_WITH_CAREGIVER_PATH) -> list[Row]:
    raw = load_raw_with_caregiver(raw_path)
    source_by_key = build_source_visit_index(raw)
    rows = []
    missing = []
    for source_row in manifest_data:
        key = (str(source_row["patient_id"]), str(source_row["visit_id"]))
        source = source_by_key.get(key)
        if source is None:
            missing.append(key)
            continue
        validate_manifest_alignment(source_row, source)
        turns = source["turns"]
        if window == "full_text":
            text = extract_role_text(turns, target_role="caregiver")
            text_field = "full_text"
            if text != str(source_row.get("full_text", "")):
                raise ValueError(f"Reconstructed full_text mismatch for manifest visit {key}")
        else:
            text = extract_early_role_text(turns, target_role="caregiver", max_role_turns=int(window), max_chars=1000)
            text_field = f"early_{window}_turns"
        if window != "full_text" and len(text) < 30:
            raise ValueError(f"Reconstructed text shorter than min_chars=30 for {key} at window={window}")
        row = dict(source_row)
        row["revision_window_text"] = text
        row["revision_window"] = window
        row["revision_text_field"] = text_field
        rows.append(row)
    if missing:
        raise ValueError(f"Manifest visits could not be reconstructed from raw caregiver data: {missing[:10]}")
    if ordered_manifest_keys(rows) != ordered_manifest_keys(manifest_data):
        raise ValueError("Reconstructed window rows do not preserve fixed manifest order")
    return rows


def validate_manifest_alignment(manifest_row: Row, source_visit: Row) -> None:
    gad_score = float(source_visit["gad_score"])
    phq_score = float(source_visit["phq_score"])
    if float(manifest_row["gad_score"]) != gad_score or float(manifest_row["phq_score"]) != phq_score:
        key = (str(manifest_row["patient_id"]), str(manifest_row["visit_id"]))
        raise ValueError(f"Raw caregiver visit scores do not match manifest for {key}")

    expected_anxiety = 1 if gad_score >= 10 else 0
    expected_depression = 1 if phq_score >= 10 else 0
    if int(manifest_row["anxiety_label"]) != expected_anxiety or int(manifest_row["depression_label"]) != expected_depression:
        key = (str(manifest_row["patient_id"]), str(manifest_row["visit_id"]))
        raise ValueError(f"Raw caregiver visit labels do not match manifest for {key}")


def build_source_visit_index(raw: Row) -> dict[tuple[str, str], Row]:
    result = {}
    for patient in raw.get("patients", []):
        patient_id = str(patient.get("patient_id"))
        visits = patient.get("visits", [])
        scales = patient.get("scales", [])
        paired_count = min(len(visits), len(scales))
        for visit, scale in zip(visits[:paired_count], scales[:paired_count]):
            gad = scale.get("GAD-7", {}).get("total")
            phq = scale.get("PHQ-9", {}).get("total")
            if not isinstance(gad, (int, float)) or not isinstance(phq, (int, float)):
                continue
            visit_id = str(visit.get("visit_id"))
            turns = visit.get("dialogue", {}).get("turns", [])
            result[(patient_id, visit_id)] = {"turns": turns, "gad_score": float(gad), "phq_score": float(phq)}
    return result

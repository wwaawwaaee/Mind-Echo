"""Fixed visit manifest and patient-fold maps for revision experiments."""

from __future__ import annotations

import csv
import json
from collections import Counter
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.model_selection import GroupKFold

from .paths import EXPECTED_PATIENTS, EXPECTED_VISITS, MANIFEST_DIR, N_SPLITS, SEED, SOURCE_DATA_PATH


MANIFEST_FIELDS = [
    "row_index",
    "patient_id",
    "visit_id",
    "dataset",
    "role",
    "gad_score",
    "phq_score",
    "anxiety_label",
    "depression_label",
    "target_role_turns",
    "dialogue_turns",
]


Row = dict[str, Any]


def load_source_data(path: Path = SOURCE_DATA_PATH) -> list[Row]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    validate_source_data(data)
    return data


def validate_source_data(data: list[Row]) -> None:
    visit_count = len(data)
    patient_count = len({row["patient_id"] for row in data})
    if visit_count != EXPECTED_VISITS or patient_count != EXPECTED_PATIENTS:
        raise ValueError(
            f"Expected {EXPECTED_VISITS} visits and {EXPECTED_PATIENTS} patients; "
            f"found {visit_count} visits and {patient_count} patients."
        )
    missing = [key for key in ("patient_id", "visit_id") if any(key not in row for row in data)]
    if missing:
        raise ValueError(f"Source rows missing required keys: {missing}")
    visit_ids = [row["visit_id"] for row in data]
    duplicates = [visit_id for visit_id, count in Counter(visit_ids).items() if count > 1]
    if duplicates:
        raise ValueError(f"visit_id values must be unique in the fixed manifest: {duplicates[:5]}")


def ensure_manifest_files(data: list[Row], manifest_dir: Path = MANIFEST_DIR, seed: int = SEED) -> dict[str, Path]:
    validate_source_data(data)
    manifest_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "visit_manifest": manifest_dir / "visit_manifest.csv",
        "patient_fold_map": manifest_dir / "patient_fold_map.csv",
        "visit_fold_map": manifest_dir / "visit_fold_map.csv",
        "metadata": manifest_dir / "manifest_metadata.json",
    }

    fold_by_patient = build_patient_fold_map(data)
    write_visit_manifest(data, paths["visit_manifest"])
    write_patient_fold_map(fold_by_patient, paths["patient_fold_map"])
    write_visit_fold_map(data, fold_by_patient, paths["visit_fold_map"])
    write_manifest_metadata(data, fold_by_patient, paths["metadata"], seed)
    return paths


def build_patient_fold_map(data: list[Row], n_splits: int = N_SPLITS) -> dict[str, int]:
    groups = np.array([row["patient_id"] for row in data])
    y_placeholder = np.array([row["anxiety_label"] for row in data], dtype=int)
    fold_by_patient: dict[str, int] = {}
    splitter = GroupKFold(n_splits=n_splits)
    for fold, (_, test_idx) in enumerate(splitter.split(np.zeros(len(data)), y_placeholder, groups)):
        for idx in test_idx:
            patient_id = str(data[int(idx)]["patient_id"])
            previous = fold_by_patient.setdefault(patient_id, fold)
            if previous != fold:
                raise ValueError(f"Patient {patient_id} assigned to multiple folds: {previous}, {fold}")
    expected_patients = {str(row["patient_id"]) for row in data}
    if set(fold_by_patient) != expected_patients:
        raise ValueError("Not all patients were assigned to folds")
    return dict(sorted(fold_by_patient.items()))


def load_patient_fold_map(path: Path = MANIFEST_DIR / "patient_fold_map.csv") -> dict[str, int]:
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return {row["patient_id"]: int(row["fold"] ) for row in reader}


def load_visit_manifest(path: Path = MANIFEST_DIR / "visit_manifest.csv") -> list[dict[str, str]]:
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def validate_no_group_leakage(data: list[Row], fold_by_patient: dict[str, int]) -> None:
    seen: dict[str, int] = {}
    for row in data:
        patient_id = str(row["patient_id"])
        if patient_id not in fold_by_patient:
            raise ValueError(f"Patient {patient_id} missing from patient_fold_map.csv")
        fold = fold_by_patient[patient_id]
        previous = seen.setdefault(patient_id, fold)
        if previous != fold:
            raise ValueError(f"Patient {patient_id} leaks across folds")


def row_fold(row: Row, fold_by_patient: dict[str, int]) -> int:
    return int(fold_by_patient[str(row["patient_id"])])


def ordered_manifest_keys(rows: Iterable[Row]) -> list[tuple[str, str]]:
    return [(str(row["patient_id"]), str(row["visit_id"])) for row in rows]


def write_visit_manifest(data: list[Row], path: Path) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        for i, row in enumerate(data):
            writer.writerow({
                "row_index": i,
                "patient_id": row["patient_id"],
                "visit_id": row["visit_id"],
                "dataset": row.get("dataset", ""),
                "role": row.get("role", ""),
                "gad_score": row["gad_score"],
                "phq_score": row["phq_score"],
                "anxiety_label": row["anxiety_label"],
                "depression_label": row["depression_label"],
                "target_role_turns": row.get("target_role_turns", ""),
                "dialogue_turns": row.get("dialogue_turns", ""),
            })


def write_patient_fold_map(fold_by_patient: dict[str, int], path: Path) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["patient_id", "fold"])
        writer.writeheader()
        for patient_id, fold in sorted(fold_by_patient.items()):
            writer.writerow({"patient_id": patient_id, "fold": fold})


def write_visit_fold_map(data: list[Row], fold_by_patient: dict[str, int], path: Path) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["row_index", "patient_id", "visit_id", "fold"])
        writer.writeheader()
        for i, row in enumerate(data):
            writer.writerow({
                "row_index": i,
                "patient_id": row["patient_id"],
                "visit_id": row["visit_id"],
                "fold": fold_by_patient[str(row["patient_id"])],
            })


def write_manifest_metadata(data: list[Row], fold_by_patient: dict[str, int], path: Path, seed: int) -> None:
    counts_by_fold = Counter(fold_by_patient[str(row["patient_id"])] for row in data)
    patients_by_fold = Counter(fold_by_patient[patient_id] for patient_id in fold_by_patient)
    metadata = {
        "source_data": str(SOURCE_DATA_PATH),
        "sample_count": len(data),
        "patient_count": len(fold_by_patient),
        "grouping_field": "patient_id",
        "grouping_limitation": "patient_id is the only available grouping field; no structured caregiver_id or family_id is present in the source rows.",
        "fold_strategy": "sklearn.model_selection.GroupKFold(n_splits=5) on patient_id, using source row order",
        "seed": seed,
        "counts_by_fold": {str(k): int(v) for k, v in sorted(counts_by_fold.items())},
        "patients_by_fold": {str(k): int(v) for k, v in sorted(patients_by_fold.items())},
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

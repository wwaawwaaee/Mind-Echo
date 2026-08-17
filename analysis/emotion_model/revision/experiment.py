"""Shared OOF experiment execution for revision runners."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.linear_model import LogisticRegression

from .features import build_fold_features, create_liwc_analyzer
from .manifest import row_fold, validate_no_group_leakage
from .metrics import bootstrap_patient_ci, compute_binary_metrics, majority_baseline_score, prevalence_random_scores
from .paths import DEFAULT_LIWC_DICT, N_SPLITS, SEED


OOF_FIELDS = ["row_index", "patient_id", "visit_id", "fold", "target", "source", "y_true", "y_score", "y_pred"]
Row = dict[str, Any]


def run_oof_experiment(
    data: list[Row],
    feature_mode: str,
    target: str,
    output_dir: Path,
    fold_by_patient: dict[str, int],
    text_field: str = "full_text",
    liwc_dict: str = DEFAULT_LIWC_DICT,
    seed: int = SEED,
    n_bootstrap: int = 2000,
) -> dict[str, Any]:
    validate_no_group_leakage(data, fold_by_patient)
    output_dir.mkdir(parents=True, exist_ok=True)
    liwc_analyzer = create_liwc_analyzer(liwc_dict)
    y = np.array([int(row[f"{target}_label"]) for row in data], dtype=int)
    folds = np.array([row_fold(row, fold_by_patient) for row in data], dtype=int)
    model_rows: list[Row] = []
    majority_rows: list[Row] = []
    random_rows: list[Row] = []
    fold_summaries = []

    for fold in range(N_SPLITS):
        train_idx = np.where(folds != fold)[0]
        test_idx = np.where(folds == fold)[0]
        if len(test_idx) == 0:
            raise ValueError(f"Fold {fold} has no held-out rows")
        train_texts = [str(data[int(i)][text_field]) for i in train_idx]
        test_texts = [str(data[int(i)][text_field]) for i in test_idx]
        x_train, x_test = build_fold_features(train_texts, test_texts, feature_mode, liwc_analyzer, svd_seed=42)
        model = LogisticRegression(max_iter=2000, class_weight="balanced", random_state=42)
        model.fit(x_train, y[train_idx])
        model_score = model.predict_proba(x_test)[:, 1]
        y_train = [int(value) for value in y[train_idx].tolist()]
        majority_score, majority_label, train_prevalence = majority_baseline_score(y_train, len(test_idx))
        random_score, random_prevalence = prevalence_random_scores(y_train, len(test_idx), seed=seed, fold=fold, target=target)

        for local_pos, row_index in enumerate(test_idx):
            row = data[int(row_index)]
            base: Row = {
                "row_index": int(row_index),
                "patient_id": row["patient_id"],
                "visit_id": row["visit_id"],
                "fold": fold,
                "target": target,
                "y_true": int(y[int(row_index)]),
            }
            model_rows.append(make_oof_row(base, "model", model_score[local_pos]))
            majority_rows.append(make_oof_row(base, "majority", majority_score[local_pos]))
            random_rows.append(make_oof_row(base, "prevalence_random", random_score[local_pos]))

        fold_summaries.append({
            "fold": fold,
            "n_train": int(len(train_idx)),
            "n_test": int(len(test_idx)),
            "train_patients": int(len({data[int(i)]["patient_id"] for i in train_idx})),
            "test_patients": int(len({data[int(i)]["patient_id"] for i in test_idx})),
            "train_positive_rate": train_prevalence,
            "random_prevalence": random_prevalence,
            "majority_label": majority_label,
        })

    model_rows = sorted(model_rows, key=lambda row: row["row_index"])
    majority_rows = sorted(majority_rows, key=lambda row: row["row_index"])
    random_rows = sorted(random_rows, key=lambda row: row["row_index"])
    combined_baseline_rows = sorted(majority_rows + random_rows, key=lambda row: (row["source"], row["row_index"]))
    write_oof_csv(output_dir / "oof_predictions.csv", model_rows)
    write_oof_csv(output_dir / "baseline_oof_predictions.csv", combined_baseline_rows)

    summary = {
        "feature_mode": feature_mode,
        "target": target,
        "text_field": text_field,
        "classifier": "LogisticRegression(max_iter=2000, class_weight='balanced', random_state=42)",
        "preprocessing": "TF-IDF, SVD, LIWC/domain scalers are fit on each training fold only, matching V2 behavior.",
        "baseline_definitions": {
            "majority": "For each fold, predict the deterministic majority class from training-fold labels only; ties choose class 0. y_score is 0.0 or 1.0.",
            "prevalence_random": f"For each fold, draw Bernoulli labels with p equal to the training-fold positive prevalence using seed {seed} plus deterministic fold/target offsets. y_score is the sampled 0.0/1.0 label.",
        },
        "folds": fold_summaries,
        "metrics": {
            "model": metrics_with_ci(model_rows, seed, n_bootstrap),
            "majority": metrics_with_ci(majority_rows, seed + 17, n_bootstrap),
            "prevalence_random": metrics_with_ci(random_rows, seed + 29, n_bootstrap),
        },
    }
    with open(output_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    return summary


def make_oof_row(base: Row, source: str, y_score: float) -> Row:
    y_pred = int(float(y_score) >= 0.5)
    row = dict(base)
    row.update({"source": source, "y_score": float(y_score), "y_pred": y_pred})
    return row


def metrics_with_ci(rows: list[Row], seed: int, n_bootstrap: int) -> dict[str, Any]:
    y_true = [int(row["y_true"]) for row in rows]
    y_score = [float(row["y_score"]) for row in rows]
    return {
        "pooled_oof": compute_binary_metrics(y_true, y_score),
        "patient_bootstrap_95ci": bootstrap_patient_ci(rows, seed=seed, n_bootstrap=n_bootstrap),
    }


def write_oof_csv(path: Path, rows: list[Row]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OOF_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row[field] for field in OOF_FIELDS})

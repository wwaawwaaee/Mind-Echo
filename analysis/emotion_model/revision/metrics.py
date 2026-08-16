"""OOF metrics, baselines, and patient-level bootstrap CIs."""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from typing import Any

import numpy as np
from numpy.typing import NDArray
from sklearn.metrics import average_precision_score, confusion_matrix, roc_auc_score


METRIC_KEYS = ("bacc", "f1", "recall", "specificity", "roc_auc", "pr_auc")


def positive_scores_to_labels(y_score: Sequence[float] | NDArray[np.float64], threshold: float = 0.5) -> NDArray[np.int_]:
    return (np.asarray(y_score, dtype=float) >= threshold).astype(int)


Row = dict[str, Any]


def compute_binary_metrics(y_true: Sequence[int], y_score: Sequence[float], threshold: float = 0.5) -> dict[str, Any]:
    y_true_arr = np.asarray(y_true, dtype=int)
    y_score_arr = np.asarray(y_score, dtype=float)
    y_pred = positive_scores_to_labels(y_score, threshold=threshold)
    cm = confusion_matrix(y_true_arr, y_pred, labels=[0, 1])
    tn, fp, fn, tp = [int(x) for x in cm.ravel()]
    specificity = tn / (tn + fp) if (tn + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    f1 = (2.0 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    bacc = (recall + specificity) / 2.0
    return {
        "bacc": float(bacc),
        "f1": float(f1),
        "recall": float(recall),
        "specificity": float(specificity),
        "roc_auc": safe_roc_auc(y_true_arr, y_score_arr),
        "pr_auc": safe_pr_auc(y_true_arr, y_score_arr),
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "tp": tp,
        "confusion_matrix": [[tn, fp], [fn, tp]],
        "n": int(len(y_true_arr)),
        "positive_rate": float(np.mean(y_true_arr)) if len(y_true_arr) else float("nan"),
    }


def safe_roc_auc(y_true: NDArray[np.int_], y_score: NDArray[np.float64]) -> float:
    if len(np.unique(y_true)) < 2:
        return float("nan")
    return float(roc_auc_score(y_true, y_score))


def safe_pr_auc(y_true: NDArray[np.int_], y_score: NDArray[np.float64]) -> float:
    if len(np.unique(y_true)) < 2:
        return float("nan")
    return float(average_precision_score(y_true, y_score))


def majority_baseline_score(y_train: Sequence[int], n_test: int) -> tuple[NDArray[np.float64], int, float]:
    y_train_arr = np.asarray(y_train, dtype=int)
    counts = Counter(int(x) for x in y_train_arr)
    majority_label = 1 if counts[1] > counts[0] else 0
    prevalence = float(np.mean(y_train_arr)) if len(y_train_arr) else 0.0
    score = np.ones(n_test, dtype=float) if majority_label == 1 else np.zeros(n_test, dtype=float)
    return score, majority_label, prevalence


def prevalence_random_scores(y_train: Sequence[int], n_test: int, seed: int, fold: int, target: str) -> tuple[NDArray[np.float64], float]:
    y_train_arr = np.asarray(y_train, dtype=int)
    prevalence = float(np.mean(y_train_arr)) if len(y_train_arr) else 0.0
    target_offset = sum(ord(ch) for ch in target)
    rng = np.random.default_rng(seed + fold * 1009 + target_offset)
    return rng.binomial(1, prevalence, size=n_test).astype(float), prevalence


def bootstrap_patient_ci(rows: list[Row], seed: int, n_bootstrap: int = 2000) -> dict[str, Any]:
    if not rows:
        raise ValueError("Cannot bootstrap an empty OOF table")
    by_patient: dict[str, list[Row]] = {}
    for row in rows:
        by_patient.setdefault(str(row["patient_id"]), []).append(row)
    patients = np.array(sorted(by_patient), dtype=object)
    rng = np.random.default_rng(seed)
    draws = {key: [] for key in METRIC_KEYS}

    for _ in range(n_bootstrap):
        sampled_patients = rng.choice(patients, size=len(patients), replace=True)
        sampled_rows = [row for patient_id in sampled_patients for row in by_patient[str(patient_id)]]
        y_true = [int(row["y_true"]) for row in sampled_rows]
        y_score = [float(row["y_score"]) for row in sampled_rows]
        metrics = compute_binary_metrics(y_true, y_score)
        for key in METRIC_KEYS:
            draws[key].append(metrics[key])

    ci: dict[str, dict[str, float | int]] = {}
    for key, values in draws.items():
        arr = np.asarray(values, dtype=float)
        valid = arr[~np.isnan(arr)]
        if len(valid) == 0:
            ci[key] = {"low": float("nan"), "high": float("nan"), "n_valid_bootstrap": 0}
        else:
            low, high = np.percentile(valid, [2.5, 97.5])
            ci[key] = {"low": float(low), "high": float(high), "n_valid_bootstrap": int(len(valid))}
    return ci

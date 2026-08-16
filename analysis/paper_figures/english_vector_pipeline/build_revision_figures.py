#!/usr/bin/env python3
# pyright: reportImplicitRelativeImport=false, reportMissingTypeStubs=false, reportUnknownMemberType=false
"""Build additive Mind-Echo revision-guide outputs for Figures 1, 3, and 4.

This script is intentionally separate from generate_academic_figures.py. It reads
completed revision experiment summaries and writes only outputs_revision_guide/.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import matplotlib.pyplot as plt
import numpy as np

from plot_style import COLORS, apply_academic_style, export_figure, panel_label, style_axis


PIPELINE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = PIPELINE_DIR / "outputs_revision_guide"
MAIN_REVISION_SUMMARY_PATH = (
    PROJECT_ROOT
    / "analysis"
    / "emotion_model"
    / "experiments"
    / "mind_echo_revision_minimal"
    / "main_v2_oof"
    / "summary.json"
)
WINDOW_REVISION_SUMMARY_PATH = (
    PROJECT_ROOT
    / "analysis"
    / "emotion_model"
    / "experiments"
    / "mind_echo_revision_minimal"
    / "window_oof"
    / "summary.json"
)

FEATURE_MODE_LABELS = {
    "tfidf_svd": "TF-IDF + SVD",
    "liwc_only": "LIWC only",
    "domain_only": "Domain rules",
    "hybrid_v2": "Hybrid v2",
}

TARGET_LABELS = {"anxiety": "Anxiety", "depression": "Depression"}
MODEL_ORDER = ["tfidf_svd", "liwc_only", "domain_only", "hybrid_v2"]
WINDOW_ORDER = ["4", "6", "8", "10", "12", "15", "full_text"]
BASELINE_KEYS = ["majority", "prevalence_random"]
BASELINE_LABELS = {
    "majority": "Majority baseline",
    "prevalence_random": "Seeded prevalence random",
}
SELECTED_MAIN_MODELS = {"anxiety": "hybrid_v2", "depression": "liwc_only"}
METRIC_LABELS = {
    "bacc": "Balanced accuracy",
    "f1": "F1",
    "recall": "Recall",
    "specificity": "Specificity",
    "roc_auc": "ROC-AUC",
    "pr_auc": "PR-AUC",
}
CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\u3040-\u30ff\uac00-\ud7af]")


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return data


def require_mapping(value: Any, context: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{context} must be an object")
    return value


def require_list(value: Any, context: str) -> List[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{context} must be a list")
    return value


def require_number(value: Any, context: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{context} must be numeric")
    return float(value)


def require_equal(name: str, actual: Any, expected: Any) -> None:
    if actual != expected:
        raise ValueError(f"{name} mismatch: expected {expected!r}, found {actual!r}")


def format_float(value: Any, digits: int = 6) -> str:
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def write_csv(path: Path, rows: Sequence[Dict[str, Any]], fieldnames: Sequence[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: format_float(row.get(key, "")) for key in fieldnames})
    return path


def markdown_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def metric_payload(entry: Dict[str, Any], variant: str, metric: str, context: str) -> Dict[str, float]:
    metrics = require_mapping(entry.get("metrics"), f"{context}.metrics")
    variant_block = require_mapping(metrics.get(variant), f"{context}.metrics.{variant}")
    pooled = require_mapping(variant_block.get("pooled_oof"), f"{context}.metrics.{variant}.pooled_oof")
    ci_root = require_mapping(
        variant_block.get("patient_bootstrap_95ci"),
        f"{context}.metrics.{variant}.patient_bootstrap_95ci",
    )
    ci = require_mapping(ci_root.get(metric), f"{context}.metrics.{variant}.patient_bootstrap_95ci.{metric}")
    return {
        "value": require_number(pooled.get(metric), f"{context}.{variant}.{metric}"),
        "low": require_number(ci.get("low"), f"{context}.{variant}.{metric}.low"),
        "high": require_number(ci.get("high"), f"{context}.{variant}.{metric}.high"),
        "n_valid_bootstrap": require_number(
            ci.get("n_valid_bootstrap"), f"{context}.{variant}.{metric}.n_valid_bootstrap"
        ),
    }


def pooled_counts(entry: Dict[str, Any], variant: str, context: str) -> Dict[str, Any]:
    metrics = require_mapping(entry.get("metrics"), f"{context}.metrics")
    variant_block = require_mapping(metrics.get(variant), f"{context}.metrics.{variant}")
    pooled = require_mapping(variant_block.get("pooled_oof"), f"{context}.metrics.{variant}.pooled_oof")
    return {
        "tn": int(require_number(pooled.get("tn"), f"{context}.{variant}.tn")),
        "fp": int(require_number(pooled.get("fp"), f"{context}.{variant}.fp")),
        "fn": int(require_number(pooled.get("fn"), f"{context}.{variant}.fn")),
        "tp": int(require_number(pooled.get("tp"), f"{context}.{variant}.tp")),
        "n": int(require_number(pooled.get("n"), f"{context}.{variant}.n")),
        "positive_rate": require_number(pooled.get("positive_rate"), f"{context}.{variant}.positive_rate"),
    }


def validate_revision_summary(summary: Dict[str, Any], expected_experiment: str) -> None:
    require_equal("experiment", summary.get("experiment"), expected_experiment)
    counts = require_mapping(summary.get("validated_counts"), "validated_counts")
    require_equal("revision visit count", int(require_number(counts.get("visits"), "validated_counts.visits")), 78)
    require_equal("revision patient count", int(require_number(counts.get("patients"), "validated_counts.patients")), 63)
    require_equal("grouping field", summary.get("grouping_field"), "patient_id")
    require_equal("seed", int(require_number(summary.get("seed"), "seed")), 2026)


def target_entries(summary: Dict[str, Any], target: str) -> List[Dict[str, Any]]:
    targets = require_mapping(summary.get("targets"), "targets")
    entries = [require_mapping(row, f"targets.{target} row") for row in require_list(targets.get(target), f"targets.{target}")]
    if not entries:
        raise ValueError(f"targets.{target} must not be empty")
    return entries


def entry_by_feature_mode(entries: Sequence[Dict[str, Any]], feature_mode: str, target: str) -> Dict[str, Any]:
    for entry in entries:
        if entry.get("feature_mode") == feature_mode:
            return entry
    raise ValueError(f"Missing {target}/{feature_mode} entry")


def entry_by_window(entries: Sequence[Dict[str, Any]], window: str, target: str) -> Dict[str, Any]:
    for entry in entries:
        if str(entry.get("window")) == window:
            return entry
    raise ValueError(f"Missing {target}/window {window} entry")


def append_metrics(row: Dict[str, Any], entry: Dict[str, Any], variant: str, context: str) -> None:
    for metric in ["bacc", "f1", "recall", "specificity", "roc_auc", "pr_auc"]:
        payload = metric_payload(entry, variant, metric, context)
        row[metric] = payload["value"]
        row[f"{metric}_low"] = payload["low"]
        row[f"{metric}_high"] = payload["high"]
        row[f"{metric}_bootstrap_draws"] = int(payload["n_valid_bootstrap"])
    row.update(pooled_counts(entry, variant, context))


def main_revision_rows(summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for target in ["anxiety", "depression"]:
        entries = target_entries(summary, target)
        modes = [str(entry.get("feature_mode")) for entry in entries]
        require_equal(f"{target} feature modes", modes, MODEL_ORDER)
        for feature_mode in MODEL_ORDER:
            entry = entry_by_feature_mode(entries, feature_mode, target)
            row: Dict[str, Any] = {
                "figure_id": "figure3_1" if target == "anxiety" else "figure3_2",
                "target": target,
                "target_label": TARGET_LABELS[target],
                "source_kind": "model",
                "feature_mode": feature_mode,
                "method_label": FEATURE_MODE_LABELS[feature_mode],
                "selected_main_model": feature_mode == SELECTED_MAIN_MODELS[target],
                "visits": 78,
                "patients": 63,
                "cv_grouping": "patient_id",
                "folds": 5,
                "seed": 2026,
                "bootstrap_draws": 2000,
            }
            append_metrics(row, entry, "model", f"main.{target}.{feature_mode}")
            rows.append(row)

        baseline_source = entries[0]
        for baseline_key in BASELINE_KEYS:
            row = {
                "figure_id": "figure3_1" if target == "anxiety" else "figure3_2",
                "target": target,
                "target_label": TARGET_LABELS[target],
                "source_kind": "baseline",
                "feature_mode": baseline_key,
                "method_label": BASELINE_LABELS[baseline_key],
                "selected_main_model": False,
                "visits": 78,
                "patients": 63,
                "cv_grouping": "patient_id",
                "folds": 5,
                "seed": 2026,
                "bootstrap_draws": 2000,
            }
            append_metrics(row, baseline_source, baseline_key, f"main.{target}.{baseline_key}")
            rows.append(row)
    return rows


def window_revision_rows(summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    windows = [str(window) for window in require_list(summary.get("windows"), "windows")]
    require_equal("window order", windows, WINDOW_ORDER)
    rows: List[Dict[str, Any]] = []
    for target in ["anxiety", "depression"]:
        entries = target_entries(summary, target)
        target_windows = [str(entry.get("window")) for entry in entries]
        require_equal(f"{target} window order", target_windows, WINDOW_ORDER)
        for window in WINDOW_ORDER:
            entry = entry_by_window(entries, window, target)
            label = "Full text" if window == "full_text" else window
            row: Dict[str, Any] = {
                "figure_id": "figure4_1" if target == "anxiety" else "figure4_2",
                "target": target,
                "target_label": TARGET_LABELS[target],
                "window": window,
                "window_label": label,
                "feature_mode": str(entry.get("feature_mode")),
                "visits": 78,
                "patients": 63,
                "cv_grouping": "patient_id",
                "folds": 5,
                "seed": 2026,
                "bootstrap_draws": 2000,
            }
            append_metrics(row, entry, "model", f"window.{target}.{window}")
            rows.append(row)
    return rows


def rows_for_target(rows: Sequence[Dict[str, Any]], target: str) -> List[Dict[str, Any]]:
    return [row for row in rows if row["target"] == target]


def main_row(rows: Sequence[Dict[str, Any]], target: str, feature_mode: str) -> Dict[str, Any]:
    for row in rows:
        if row["target"] == target and row["feature_mode"] == feature_mode:
            return row
    raise ValueError(f"Missing main row for {target}/{feature_mode}")


def metric_values(rows: Sequence[Dict[str, Any]], metric: str) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    values = np.asarray([float(row[metric]) for row in rows], dtype=float)
    lows = np.asarray([float(row[f"{metric}_low"]) for row in rows], dtype=float)
    highs = np.asarray([float(row[f"{metric}_high"]) for row in rows], dtype=float)
    return values, values - lows, highs - values


def add_value_labels(ax: Any, bars: Iterable[Any], values: Iterable[float]) -> None:
    for bar, value in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            min(0.98, float(value) + 0.035),
            f"{float(value):.2f}",
            ha="center",
            va="bottom",
            fontsize=7.8,
            rotation=0,
        )


def plot_main_metric_bars(rows: Sequence[Dict[str, Any]], target: str, output_dir: Path, stem: str) -> List[Path]:
    target_rows = rows_for_target(rows, target)
    labels = [row["method_label"] for row in target_rows]
    x = np.arange(len(target_rows))
    width = 0.34
    fig, ax = plt.subplots(figsize=(9.4, 4.8))

    metric_specs = [
        ("bacc", "Balanced accuracy", COLORS["caregiver"], ""),
        ("f1", "F1", COLORS["accent"], "//"),
    ]
    for idx, (metric, label, color, hatch) in enumerate(metric_specs):
        values, lower, upper = metric_values(target_rows, metric)
        bars = ax.bar(
            x + (idx - 0.5) * width,
            values,
            width,
            yerr=np.vstack([lower, upper]),
            capsize=3,
            label=label,
            color=color,
            edgecolor=COLORS["text"],
            linewidth=0.55,
            hatch=hatch,
            alpha=0.92,
            error_kw={"elinewidth": 0.9, "capthick": 0.9, "ecolor": COLORS["text"]},
        )
        add_value_labels(ax, bars, values)

    ax.axvline(3.5, color=COLORS["muted"], linewidth=1.0, linestyle="--", alpha=0.75)
    ax.text(0.82, 0.965, "Feature modes", transform=ax.transAxes, ha="right", va="top", fontsize=8.6)
    ax.text(0.98, 0.965, "Baselines", transform=ax.transAxes, ha="right", va="top", fontsize=8.6)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=18, ha="right")
    ax.set_ylim(0, 1.04)
    ax.set_ylabel("Metric value")
    ax.set_title(f"{TARGET_LABELS[target]} target main-model OOF performance")
    ax.legend(frameon=False, loc="upper left", ncols=2)
    ax.text(
        0.01,
        0.02,
        "Pooled OOF with 95% patient-bootstrap CI",
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=8.3,
        color=COLORS["muted"],
    )
    style_axis(ax)
    fig.subplots_adjust(left=0.085, right=0.985, top=0.86, bottom=0.28)
    generated = export_figure(fig, output_dir, stem)
    plt.close(fig)
    return generated


def plot_confusion_matrices(rows: Sequence[Dict[str, Any]], output_dir: Path) -> List[Path]:
    fig, axes = plt.subplots(1, 2, figsize=(8.8, 4.1))
    targets = ["anxiety", "depression"]
    cmaps = ["Blues", "Reds"]
    for idx, (ax, target, cmap_name) in enumerate(zip(axes, targets, cmaps)):
        row = main_row(rows, target, SELECTED_MAIN_MODELS[target])
        matrix = np.asarray([[row["tn"], row["fp"]], [row["fn"], row["tp"]]], dtype=int)
        image = ax.imshow(matrix, cmap=cmap_name, vmin=0, vmax=max(1, int(matrix.max())))
        for i in range(2):
            for j in range(2):
                value = int(matrix[i, j])
                color = "white" if value > matrix.max() * 0.52 else COLORS["text"]
                ax.text(j, i, str(value), ha="center", va="center", fontsize=12, fontweight="bold", color=color)
        ax.set_xticks([0, 1])
        ax.set_xticklabels(["Negative", "Positive"])
        ax.set_yticks([0, 1])
        ax.set_yticklabels(["Negative", "Positive"])
        ax.set_xlabel("Predicted class")
        ax.set_ylabel("Reference class" if idx == 0 else "")
        ax.set_title(
            f"{TARGET_LABELS[target]}: {FEATURE_MODE_LABELS[SELECTED_MAIN_MODELS[target]]}\n"
            f"BACC={row['bacc']:.2f}, F1={row['f1']:.2f}"
        )
        panel_label(ax, f"3{chr(ord('a') + idx)}")
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.tick_params(length=0)
        fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04).set_label("Visits")
    fig.suptitle("Selected main-model confusion matrices", y=0.99)
    fig.subplots_adjust(left=0.08, right=0.96, top=0.78, bottom=0.16, wspace=0.38)
    generated = export_figure(fig, output_dir, "figure3_3")
    plt.close(fig)
    return generated


def plot_window_metric_lines(rows: Sequence[Dict[str, Any]], target: str, output_dir: Path, stem: str) -> List[Path]:
    target_rows = rows_for_target(rows, target)
    x = np.arange(len(target_rows))
    labels = [row["window_label"] for row in target_rows]
    fig, ax = plt.subplots(figsize=(8.2, 4.4))
    specs = [
        ("bacc", "Balanced accuracy", COLORS["caregiver"], "s", "-"),
        ("f1", "F1", COLORS["accent"], "o", "--"),
    ]
    for metric, label, color, marker, linestyle in specs:
        values, lower, upper = metric_values(target_rows, metric)
        ax.errorbar(
            x,
            values,
            yerr=np.vstack([lower, upper]),
            marker=marker,
            linestyle=linestyle,
            color=color,
            markerfacecolor="white",
            markeredgewidth=1.5,
            capsize=3,
            linewidth=1.8,
            label=label,
        )
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylim(0, 1.04)
    ax.set_xlabel("Context window")
    ax.set_ylabel("Metric value")
    ax.set_title(f"{TARGET_LABELS[target]} target by context window")
    ax.legend(frameon=False, loc="upper left")
    ax.text(
        0.01,
        0.02,
        "Pooled OOF with 95% patient-bootstrap CI; n=78 visits / 63 patients",
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=8.2,
        color=COLORS["muted"],
    )
    style_axis(ax, grid_axis="both")
    fig.subplots_adjust(left=0.095, right=0.985, top=0.86, bottom=0.18)
    generated = export_figure(fig, output_dir, stem)
    plt.close(fig)
    return generated


def plot_recall_specificity(rows: Sequence[Dict[str, Any]], output_dir: Path) -> List[Path]:
    fig, axes = plt.subplots(1, 2, figsize=(12.0, 4.6), sharey=True)
    specs = [
        ("recall", "Recall", COLORS["gad"], "o", "-"),
        ("specificity", "Specificity", COLORS["phq"], "s", "--"),
    ]
    for idx, (ax, target) in enumerate(zip(axes, ["anxiety", "depression"])):
        target_rows = rows_for_target(rows, target)
        x = np.arange(len(target_rows))
        labels = [row["window_label"] for row in target_rows]
        for metric, label, color, marker, linestyle in specs:
            values, lower, upper = metric_values(target_rows, metric)
            ax.errorbar(
                x,
                values,
                yerr=np.vstack([lower, upper]),
                marker=marker,
                linestyle=linestyle,
                color=color,
                markerfacecolor="white",
                markeredgewidth=1.5,
                capsize=3,
                linewidth=1.8,
                label=label,
            )
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.set_ylim(0, 1.04)
        ax.set_xlabel("Context window")
        ax.set_ylabel("Metric value" if idx == 0 else "")
        ax.set_title(f"{TARGET_LABELS[target]} target")
        panel_label(ax, f"4{chr(ord('a') + idx)}")
        style_axis(ax, grid_axis="both")
    axes[0].legend(frameon=False, loc="upper left")
    fig.suptitle("Recall and specificity by context window", y=0.98)
    fig.text(
        0.5,
        0.02,
        "Patient-grouped five-fold OOF estimates with 95% patient-bootstrap CI.",
        ha="center",
        fontsize=9,
    )
    fig.subplots_adjust(left=0.075, right=0.985, top=0.80, bottom=0.18, wspace=0.18)
    generated = export_figure(fig, output_dir, "figure4_3")
    plt.close(fig)
    return generated


def write_figure1_outputs(output_dir: Path) -> List[Path]:
    rows = [
        {
            "section": "Dialogue source",
            "measure": "Patient records from dialogue source files",
            "count": 84,
            "denominator": 84,
            "note": "One patient record per dialogue source file.",
        },
        {
            "section": "Dialogue source",
            "measure": "Visit segments",
            "count": 131,
            "denominator": "",
            "note": "Visit-level dialogue segments across patient records.",
        },
        {
            "section": "Clinical scales",
            "measure": "Paired GAD-7/PHQ-9 scale records",
            "count": 109,
            "denominator": "",
            "note": "Scale respondent identity cannot be independently verified.",
        },
        {
            "section": "Caregiver turns",
            "measure": "Patient records with at least one caregiver-labeled turn",
            "count": 63,
            "denominator": 84,
            "note": "Caregiver status is role-derived from dialogue turns.",
        },
        {
            "section": "Caregiver turns",
            "measure": "Patient records without caregiver-labeled turns",
            "count": 21,
            "denominator": 84,
            "note": "No caregiver-labeled dialogue turn was present.",
        },
        {
            "section": "Age availability",
            "measure": "Patient records with any recorded age",
            "count": 35,
            "denominator": 84,
            "note": "Age availability is incomplete.",
        },
        {
            "section": "Age availability",
            "measure": "Patient records with pediatric age 0-17",
            "count": 21,
            "denominator": 84,
            "note": "Pediatric age uses recorded ages from 0 through 17 years.",
        },
        {
            "section": "Revision ML cohort",
            "measure": "Visit-level records used in revision ML analyses",
            "count": 78,
            "denominator": "",
            "note": "Patient-grouped five-fold OOF analyses.",
        },
        {
            "section": "Revision ML cohort",
            "measure": "Patients used in revision ML analyses",
            "count": 63,
            "denominator": "",
            "note": "Patient identifier is the grouping field.",
        },
    ]
    csv_path = write_csv(output_dir / "figure1_2.csv", rows, ["section", "measure", "count", "denominator", "note"])

    md_rows = [[str(row["section"]), str(row["measure"]), str(row["count"]), str(row["denominator"]), str(row["note"])] for row in rows]
    text = "# Figure 1. Mind-Echo revision sample structure\n\n"
    text += (
        "Figure 1 is represented as text and tables only. The data structure contains "
        "84 patient records from 84 dialogue source files, 131 visit segments, and "
        "109 paired GAD-7/PHQ-9 scale records. Among the 84 patient records, 63 have "
        "at least one caregiver-labeled dialogue turn and 21 do not. Recorded age is "
        "available for 35 patient records, including 21 with pediatric ages from 0 "
        "through 17 years. The revision machine-learning analyses use 78 visit-level "
        "records from 63 patients with patient-grouped five-fold OOF predictions.\n\n"
    )
    text += (
        "The source structure does not contain caregiver_id, family_id, recording_id, "
        "an EMR/MRN field, or a standalone scale_id. Caregiver status is derived from "
        "dialogue turn roles, and scale respondent identity cannot be independently verified.\n\n"
    )
    text += markdown_table(["Section", "Measure", "Count", "Denominator", "Note"], md_rows)
    text += "\n"
    md_path = output_dir / "figure1_1.md"
    md_path.write_text(text, encoding="utf-8")
    return [md_path, csv_path]


def metric_ci_text(row: Dict[str, Any], metric: str) -> str:
    return f"{row[metric]:.3f} ({row[f'{metric}_low']:.3f}-{row[f'{metric}_high']:.3f})"


def write_figure3_caption_stats(output_dir: Path, rows: Sequence[Dict[str, Any]]) -> Path:
    text = "# Figure 3 caption statistics\n\n"
    text += (
        "Figure 3 reports patient-grouped five-fold pooled OOF performance for 78 visits "
        "from 63 patients. Error bars are patient-bootstrap 95% confidence intervals from "
        "2,000 draws with seed 2026. Majority and seeded prevalence-random baselines are "
        "shown to contextualize both balanced accuracy and F1.\n\n"
    )
    for target in ["anxiety", "depression"]:
        text += f"## {TARGET_LABELS[target]} target\n\n"
        table_rows = []
        for row in rows_for_target(rows, target):
            table_rows.append(
                [
                    row["method_label"],
                    metric_ci_text(row, "bacc"),
                    metric_ci_text(row, "f1"),
                    metric_ci_text(row, "roc_auc"),
                    metric_ci_text(row, "pr_auc"),
                ]
            )
        text += markdown_table(["Method", "BACC (95% CI)", "F1 (95% CI)", "ROC-AUC (95% CI)", "PR-AUC (95% CI)"], table_rows)
        text += "\n\n"
    selected = [main_row(rows, target, SELECTED_MAIN_MODELS[target]) for target in ["anxiety", "depression"]]
    text += "## Selected confusion-matrix models\n\n"
    text += markdown_table(
        ["Target", "Selected model", "TN", "FP", "FN", "TP", "BACC", "F1"],
        [
            [
                row["target_label"],
                row["method_label"],
                str(row["tn"]),
                str(row["fp"]),
                str(row["fn"]),
                str(row["tp"]),
                f"{row['bacc']:.3f}",
                f"{row['f1']:.3f}",
            ]
            for row in selected
        ],
    )
    text += "\n"
    path = output_dir / "figure3_caption_stats.md"
    path.write_text(text, encoding="utf-8")
    return path


def write_figure4_caption_stats(output_dir: Path, rows: Sequence[Dict[str, Any]]) -> Path:
    text = "# Figure 4 caption statistics\n\n"
    text += (
        "Figure 4 reports fixed hybrid v2 window analyses using the same 78 visits and "
        "63 patients, patient-grouped five-fold OOF predictions, seed 2026, and 2,000 "
        "patient-bootstrap draws. The optimal context window remains uncertain because "
        "the confidence intervals overlap across windows.\n\n"
    )
    for target in ["anxiety", "depression"]:
        text += f"## {TARGET_LABELS[target]} target\n\n"
        table_rows = []
        for row in rows_for_target(rows, target):
            table_rows.append(
                [
                    row["window_label"],
                    metric_ci_text(row, "bacc"),
                    metric_ci_text(row, "f1"),
                    metric_ci_text(row, "recall"),
                    metric_ci_text(row, "specificity"),
                    metric_ci_text(row, "roc_auc"),
                    metric_ci_text(row, "pr_auc"),
                ]
            )
        text += markdown_table(
            [
                "Window",
                "BACC (95% CI)",
                "F1 (95% CI)",
                "Recall (95% CI)",
                "Specificity (95% CI)",
                "ROC-AUC (95% CI)",
                "PR-AUC (95% CI)",
            ],
            table_rows,
        )
        text += "\n\n"
    path = output_dir / "figure4_caption_stats.md"
    path.write_text(text, encoding="utf-8")
    return path


def write_readme(output_dir: Path) -> Path:
    text = "# Mind-Echo revision guide figure outputs\n\n"
    text += (
        "This directory is additive. It is generated by `build_revision_figures.py` and "
        "does not overwrite the legacy `outputs/` directory or experiment summaries.\n\n"
    )
    text += "## Sources\n\n"
    text += f"- Main-model summary: `{MAIN_REVISION_SUMMARY_PATH}`\n"
    text += f"- Window summary: `{WINDOW_REVISION_SUMMARY_PATH}`\n"
    text += "- Shared plotting style: `plot_style.py`\n\n"
    text += "## Files\n\n"
    text += "- `figure1_1.md`: Figure 1 text-only sample and data-structure explanation.\n"
    text += "- `figure1_2.csv`: Machine-readable Figure 1 sample count table.\n"
    text += "- `figure3_1.svg/pdf/png`: Anxiety main-model OOF BACC and F1 with 95% CI.\n"
    text += "- `figure3_2.svg/pdf/png`: Depression main-model OOF BACC and F1 with 95% CI.\n"
    text += "- `figure3_3.svg/pdf/png`: Selected main-model confusion matrices.\n"
    text += "- `figure4_1.svg/pdf/png`: Anxiety context-window BACC and F1 with 95% CI.\n"
    text += "- `figure4_2.svg/pdf/png`: Depression context-window BACC and F1 with 95% CI.\n"
    text += "- `figure4_3.svg/pdf/png`: Recall and specificity by window for both targets.\n"
    text += "- `figure3_data.csv` and `figure4_data.csv`: Backing data for the charts, including ROC-AUC and PR-AUC.\n"
    text += "- `figure3_caption_stats.md` and `figure4_caption_stats.md`: Caption-ready statistics.\n"
    text += "- `revision_figures_validation.json`: Generation and validation record.\n\n"
    text += "Figure 1 intentionally has no SVG, PDF, or PNG files.\n"
    path = output_dir / "README.md"
    path.write_text(text, encoding="utf-8")
    return path


def figure3_fieldnames() -> List[str]:
    base = [
        "figure_id",
        "target",
        "target_label",
        "source_kind",
        "feature_mode",
        "method_label",
        "selected_main_model",
        "visits",
        "patients",
        "cv_grouping",
        "folds",
        "seed",
        "bootstrap_draws",
    ]
    metrics = []
    for metric in ["bacc", "f1", "recall", "specificity", "roc_auc", "pr_auc"]:
        metrics.extend([metric, f"{metric}_low", f"{metric}_high", f"{metric}_bootstrap_draws"])
    return [*base, *metrics, "tn", "fp", "fn", "tp", "n", "positive_rate"]


def figure4_fieldnames() -> List[str]:
    base = [
        "figure_id",
        "target",
        "target_label",
        "window",
        "window_label",
        "feature_mode",
        "visits",
        "patients",
        "cv_grouping",
        "folds",
        "seed",
        "bootstrap_draws",
    ]
    metrics = []
    for metric in ["bacc", "f1", "recall", "specificity", "roc_auc", "pr_auc"]:
        metrics.extend([metric, f"{metric}_low", f"{metric}_high", f"{metric}_bootstrap_draws"])
    return [*base, *metrics, "tn", "fp", "fn", "tp", "n", "positive_rate"]


def expected_output_names() -> List[str]:
    chart_names = []
    for stem in ["figure3_1", "figure3_2", "figure3_3", "figure4_1", "figure4_2", "figure4_3"]:
        chart_names.extend([f"{stem}.svg", f"{stem}.pdf", f"{stem}.png"])
    return [
        "figure1_1.md",
        "figure1_2.csv",
        *chart_names,
        "figure3_data.csv",
        "figure4_data.csv",
        "figure3_caption_stats.md",
        "figure4_caption_stats.md",
        "README.md",
        "revision_figures_validation.json",
    ]


def text_files_for_cjk_check(output_dir: Path) -> List[Path]:
    paths = [Path(__file__).resolve()]
    for suffix in ["*.md", "*.csv", "*.json", "*.svg"]:
        paths.extend(sorted(output_dir.glob(suffix)))
    return paths


def find_cjk(paths: Sequence[Path]) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []
    for path in paths:
        text = path.read_text(encoding="utf-8", errors="ignore")
        match = CJK_RE.search(text)
        if match:
            findings.append({"path": str(path), "index": match.start(), "character": match.group(0)})
    return findings


def validate_outputs(output_dir: Path, generated_files: Sequence[Path]) -> Dict[str, Any]:
    expected = expected_output_names()
    missing = [name for name in expected if not (output_dir / name).exists()]
    empty = [name for name in expected if (output_dir / name).exists() and (output_dir / name).stat().st_size == 0]
    figure1_images = []
    for ext in ["svg", "pdf", "png"]:
        figure1_images.extend(path.name for path in output_dir.glob(f"figure1_*.{ext}"))
    cjk_findings = find_cjk(text_files_for_cjk_check(output_dir))
    generated_under_legacy_outputs = [str(path) for path in generated_files if path.resolve().parent.name == "outputs"]
    validation = {
        "required_files_present": not missing,
        "required_files_nonzero": not empty,
        "missing_files": missing,
        "empty_files": empty,
        "figure1_image_files": sorted(figure1_images),
        "no_figure1_images": not figure1_images,
        "cjk_findings": cjk_findings,
        "no_cjk_in_script_docs_csv_svg_json": not cjk_findings,
        "legacy_outputs_written": generated_under_legacy_outputs,
        "no_legacy_outputs_written": not generated_under_legacy_outputs,
    }
    if missing or empty or figure1_images or cjk_findings or generated_under_legacy_outputs:
        raise RuntimeError(f"Revision output validation failed: {json.dumps(validation, ensure_ascii=True)}")
    return validation


def build_revision_outputs(output_dir: Path) -> Dict[str, Any]:
    apply_academic_style()
    output_dir.mkdir(parents=True, exist_ok=True)
    main_summary = load_json(MAIN_REVISION_SUMMARY_PATH)
    window_summary = load_json(WINDOW_REVISION_SUMMARY_PATH)
    validate_revision_summary(main_summary, "mind_echo_revision_minimal_main_v2_oof")
    validate_revision_summary(window_summary, "mind_echo_revision_minimal_window_oof")

    generated_files: List[Path] = []
    generated_files.extend(write_figure1_outputs(output_dir))

    main_rows = main_revision_rows(main_summary)
    window_rows = window_revision_rows(window_summary)
    generated_files.append(write_csv(output_dir / "figure3_data.csv", main_rows, figure3_fieldnames()))
    generated_files.append(write_csv(output_dir / "figure4_data.csv", window_rows, figure4_fieldnames()))
    generated_files.extend(plot_main_metric_bars(main_rows, "anxiety", output_dir, "figure3_1"))
    generated_files.extend(plot_main_metric_bars(main_rows, "depression", output_dir, "figure3_2"))
    generated_files.extend(plot_confusion_matrices(main_rows, output_dir))
    generated_files.extend(plot_window_metric_lines(window_rows, "anxiety", output_dir, "figure4_1"))
    generated_files.extend(plot_window_metric_lines(window_rows, "depression", output_dir, "figure4_2"))
    generated_files.extend(plot_recall_specificity(window_rows, output_dir))
    generated_files.append(write_figure3_caption_stats(output_dir, main_rows))
    generated_files.append(write_figure4_caption_stats(output_dir, window_rows))
    generated_files.append(write_readme(output_dir))

    validation_path = output_dir / "revision_figures_validation.json"
    validation: Dict[str, Any] = {
        "sources": {
            "main_revision_summary": str(MAIN_REVISION_SUMMARY_PATH),
            "window_revision_summary": str(WINDOW_REVISION_SUMMARY_PATH),
            "plot_style": str(PIPELINE_DIR / "plot_style.py"),
        },
        "output_dir": str(output_dir),
        "cohort": {
            "visits": 78,
            "patients": 63,
            "patient_records_from_dialogue_source_files": 84,
            "dialogue_source_files": 84,
            "visit_segments": 131,
            "paired_scale_records": 109,
            "patient_records_with_caregiver_labeled_turn": 63,
            "patient_records_without_caregiver_labeled_turn": 21,
            "patient_records_with_any_recorded_age": 35,
            "patient_records_with_pediatric_age_0_17": 21,
        },
        "model_protocol": {
            "cv": "patient-grouped five-fold OOF",
            "grouping_field": "patient_id",
            "seed": 2026,
            "patient_bootstrap_draws": 2000,
            "no_caregiver_or_family_grouped_cv_claim": True,
        },
        "figure1_text_only": True,
        "chart_outputs": [
            "figure3_1",
            "figure3_2",
            "figure3_3",
            "figure4_1",
            "figure4_2",
            "figure4_3",
        ],
        "generated_files": [str(path) for path in generated_files],
        "style_checks": {
            "svg_fonttype": plt.rcParams.get("svg.fonttype"),
            "pdf_fonttype": plt.rcParams.get("pdf.fonttype"),
            "minimum_configured_tick_font_size_pt": min(
                float(plt.rcParams.get("xtick.labelsize", 0)), float(plt.rcParams.get("ytick.labelsize", 0))
            ),
            "color_blind_aware_with_marker_hatch_line_encoding": True,
            "honest_axis_range_0_to_1_for_classification_metrics": True,
        },
    }
    validation_path.write_text(json.dumps(validation, ensure_ascii=True, indent=2), encoding="utf-8")
    generated_files.append(validation_path)
    validation["generated_files"] = [str(path) for path in generated_files]
    validation["output_validation"] = validate_outputs(output_dir, generated_files)
    validation_path.write_text(json.dumps(validation, ensure_ascii=True, indent=2), encoding="utf-8")
    return validation


def main() -> None:
    parser = argparse.ArgumentParser(description="Build additive Mind-Echo revision guide figure outputs.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Revision output directory")
    args = parser.parse_args()
    validation = build_revision_outputs(args.output_dir)
    print(f"Generated revision outputs in {validation['output_dir']}")
    print("Validated required files, nonzero sizes, Figure 1 text-only rule, and CJK-free text/vector outputs.")


if __name__ == "__main__":
    main()

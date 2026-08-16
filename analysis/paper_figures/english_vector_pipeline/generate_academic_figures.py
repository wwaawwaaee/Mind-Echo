#!/usr/bin/env python3
# pyright: reportImplicitRelativeImport=false
"""Generate English vector academic figures for Mind-Echo."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List

import matplotlib.pyplot as plt
import numpy as np

from figure_data import (
    FEATURE_MODE_LABELS,
    TARGET_LABELS,
    figure1_dataset,
    figure2_dataset,
    figure3_dataset,
    figure4_dataset,
    figure5_dataset,
    pearson_with_regression,
    section_3_2_correlation_rows,
    table1_feature_set_performance_rows,
)
from plot_style import COLORS, apply_academic_style, export_figure, panel_label, style_axis


PIPELINE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = PIPELINE_DIR / "outputs"

FIGURE1_SPLIT_STEMS = [
    "figure1a_role_turn_distribution",
    "figure1b_age_distribution",
    "figure1c_severity_distribution",
    "figure1d_gad_phq_correlation",
    "figure1e_clinical_risk",
]

FIGURE2_SPLIT_STEMS = [
    "figure2a_liwc_heatmap",
    "figure2b_gad7_anxiety_features",
    "figure2c_phq9_depression_features",
]

FIGURE3_STEM = "figure3_feature_set_f1_bacc"
FIGURE4_STEM = "figure4_available_window_curve_combined"
FIGURE5_STEM = "figure5_best_ml_vs_zero_shot"

REPORT_FILES = [
    "figures2_answer_report.md",
    "figures2_answer_report.csv",
    "section_3_2_correlations.csv",
    "table1_feature_set_performance.csv",
    "table1_feature_set_performance.md",
    "figure4_window_data_availability.md",
    "figures2_validation.json",
]

UNSUPPORTED_REQUIREMENTS = [
    {
        "requirement": "child-compliance extractor or lexicon",
        "status": "unsupported",
        "evidence": "No documented child-compliance extractor or lexicon is present in the current analysis pipeline.",
    },
    {
        "requirement": "separate Anxiety/Depression window trajectories",
        "status": "unsupported",
        "evidence": "window_comparison_summary.json provides one combined available-window series, not per-target classification series.",
    },
    {
        "requirement": "window balanced accuracy",
        "status": "unsupported",
        "evidence": "window_comparison_summary.json has acc and f1 but no balanced_acc; ACC is not relabeled as BACC.",
    },
    {
        "requirement": "cohort-level audio-data or multimodal CV results",
        "status": "unsupported",
        "evidence": "The current source files used by this pipeline do not contain cohort-level audio-data or multimodal cross-validation results.",
    },
]


def significance_marker(p_value: float | None) -> str:
    if p_value is None:
        return ""
    if p_value < 0.01:
        return "**"
    if p_value < 0.05:
        return "*"
    return ""


def p_text(p_value: float | None) -> str:
    if p_value is None:
        return "p=n/a"
    if p_value < 0.001:
        return "p<0.001"
    return f"p={p_value:.3f}"


def r_text(r_value: float | None) -> str:
    if r_value is None:
        return "r=n/a"
    return f"r={r_value:.2f}"


def add_bar_labels(ax, bars, total: int | None = None) -> None:
    for bar in bars:
        height = bar.get_height()
        label = f"{int(height)}"
        if total:
            label += f"\n({100.0 * height / total:.1f}%)"
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            height + max(1, height * 0.025),
            label,
            ha="center",
            va="bottom",
            fontsize=8.2,
        )


def plot_regression_panel(ax, x: List[float], y: List[float], x_label: str, y_label: str, title: str, color: str) -> Dict[str, Any]:
    corr = pearson_with_regression(x, y)
    x_arr = np.asarray(x, dtype=float)
    y_arr = np.asarray(y, dtype=float)
    ax.scatter(x_arr, y_arr, s=38, alpha=0.72, color=color, edgecolor="white", linewidth=0.5)
    if corr.slope is not None and corr.intercept is not None:
        x_line = np.linspace(float(np.min(x_arr)), float(np.max(x_arr)), 100)
        y_line = corr.slope * x_line + corr.intercept
        ax.plot(x_line, y_line, color=color, linewidth=1.8)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(title)
    style_axis(ax, grid_axis="both")
    r_value = corr.as_dict()["pearson_r"]
    p_value = corr.as_dict()["pearson_p"]
    ax.text(
        0.04,
        0.96,
        f"n={corr.n}\n{r_text(r_value)}\n{p_text(p_value)}{significance_marker(p_value)}",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=8.2,
        bbox={"boxstyle": "round,pad=0.3", "facecolor": "white", "edgecolor": "#D1D5DB", "alpha": 0.88},
    )
    return corr.as_dict()


def flatten_split_paths(split_paths: Dict[str, List[Path]]) -> List[Path]:
    return [path for paths in split_paths.values() for path in paths]


def split_panel_path_strings(split_paths: Dict[str, List[Path]]) -> Dict[str, List[str]]:
    return {panel: [str(path) for path in paths] for panel, paths in split_paths.items()}


def export_role_turn_panel(data: Dict[str, Any], output_dir: Path) -> List[Path]:
    fig, ax = plt.subplots(figsize=(4.8, 3.6))
    roles = ["doctor", "caregiver", "patient"]
    role_labels = ["Doctor", "Caregiver", "Patient"]
    role_counts = [data["role_turn_distribution"][role] for role in roles]
    bars = ax.bar(role_labels, role_counts, color=[COLORS[role] for role in roles], width=0.66)
    add_bar_labels(ax, bars, total=data["sample_counts"]["total_turns"])
    ax.set_ylabel("Dialogue turns")
    ax.set_title("Role turn distribution")
    ax.set_ylim(0, max(role_counts) * 1.22)
    ax.text(
        1,
        role_counts[1] * 0.55,
        "Caregiver involvement\n1,889 turns",
        ha="center",
        va="center",
        fontsize=8.5,
        color="white",
        fontweight="bold",
    )
    panel_label(ax, "1a")
    style_axis(ax)
    fig.subplots_adjust(left=0.18, right=0.98, top=0.84, bottom=0.16)
    generated = export_figure(fig, output_dir, "figure1a_role_turn_distribution")
    plt.close(fig)
    return generated


def export_age_panel(data: Dict[str, Any], output_dir: Path) -> List[Path]:
    fig, ax = plt.subplots(figsize=(4.6, 3.6))
    age_labels = list(data["pediatric_age_bins"].keys())
    age_counts = [data["pediatric_age_bins"][label] for label in age_labels]
    bars = ax.bar(age_labels, age_counts, color=COLORS["gad"], width=0.64)
    add_bar_labels(ax, bars)
    ax.set_ylabel("Patients")
    ax.set_xlabel("Age bin (years)")
    ax.set_title("Core pediatric cohort ages (n=21)")
    ax.set_ylim(0, max(age_counts) * 1.25)
    panel_label(ax, "1b")
    style_axis(ax)
    fig.subplots_adjust(left=0.18, right=0.98, top=0.84, bottom=0.18)
    generated = export_figure(fig, output_dir, "figure1b_age_distribution")
    plt.close(fig)
    return generated


def export_severity_panel(data: Dict[str, Any], output_dir: Path) -> List[Path]:
    fig, ax = plt.subplots(figsize=(5.2, 3.6))
    severity_labels = ["Mild", "Moderate", "Moderately\nsevere", "Severe"]
    gad_counts = [
        data["severity_distribution"]["GAD-7"].get("mild", 0),
        data["severity_distribution"]["GAD-7"].get("moderate", 0),
        0,
        data["severity_distribution"]["GAD-7"].get("severe", 0),
    ]
    phq_counts = [
        data["severity_distribution"]["PHQ-9"].get("mild", 0),
        data["severity_distribution"]["PHQ-9"].get("moderate", 0),
        data["severity_distribution"]["PHQ-9"].get("moderately severe", 0),
        data["severity_distribution"]["PHQ-9"].get("severe", 0),
    ]
    x = np.arange(len(severity_labels))
    width = 0.36
    ax.bar(x - width / 2, gad_counts, width, label="GAD-7", color=COLORS["gad"])
    ax.bar(x + width / 2, phq_counts, width, label="PHQ-9", color=COLORS["phq"])
    ax.set_xticks(x)
    ax.set_xticklabels(severity_labels)
    ax.set_ylabel("Scale records")
    ax.set_title("Severity distribution (n=109 scale records)")
    ax.legend(frameon=False)
    panel_label(ax, "1c")
    style_axis(ax)
    fig.subplots_adjust(left=0.16, right=0.98, top=0.84, bottom=0.18)
    generated = export_figure(fig, output_dir, "figure1c_severity_distribution")
    plt.close(fig)
    return generated


def export_gad_phq_correlation_panel(data: Dict[str, Any], output_dir: Path) -> List[Path]:
    fig, ax = plt.subplots(figsize=(5.4, 3.8))
    plot_regression_panel(
        ax,
        data["scale_pairs"]["gad7"],
        data["scale_pairs"]["phq9"],
        "GAD-7 total score",
        "PHQ-9 total score",
        "Paired anxiety and depression scores",
        COLORS["accent"],
    )
    panel_label(ax, "1d")
    fig.subplots_adjust(left=0.15, right=0.98, top=0.84, bottom=0.16)
    generated = export_figure(fig, output_dir, "figure1d_gad_phq_correlation")
    plt.close(fig)
    return generated


def export_clinical_risk_panel(data: Dict[str, Any], output_dir: Path) -> List[Path]:
    fig, ax = plt.subplots(figsize=(4.6, 3.6))
    risk = data["clinical_risk_thresholds"]
    risk_labels = ["GAD-7 >=10", "PHQ-9 >=10"]
    risk_values = [risk["GAD-7"]["percentage"], risk["PHQ-9"]["percentage"]]
    risk_counts = [risk["GAD-7"]["count"], risk["PHQ-9"]["count"]]
    bars = ax.bar(risk_labels, risk_values, color=[COLORS["gad"], COLORS["phq"]], width=0.58)
    for bar, count in zip(bars, risk_counts):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 2,
            f"{count}/109\n{bar.get_height():.1f}%",
            ha="center",
            va="bottom",
            fontsize=8.2,
        )
    ax.set_ylim(0, 100)
    ax.set_ylabel("Scale records above threshold (%)")
    ax.set_title("Clinical risk proportions")
    ax.tick_params(axis="x", rotation=12)
    panel_label(ax, "1e")
    style_axis(ax)
    fig.subplots_adjust(left=0.19, right=0.98, top=0.84, bottom=0.2)
    generated = export_figure(fig, output_dir, "figure1e_clinical_risk")
    plt.close(fig)
    return generated


def generate_figure1_split_panels(data: Dict[str, Any], output_dir: Path) -> Dict[str, List[Path]]:
    return {
        "figure1a_role_turn_distribution": export_role_turn_panel(data, output_dir),
        "figure1b_age_distribution": export_age_panel(data, output_dir),
        "figure1c_severity_distribution": export_severity_panel(data, output_dir),
        "figure1d_gad_phq_correlation": export_gad_phq_correlation_panel(data, output_dir),
        "figure1e_clinical_risk": export_clinical_risk_panel(data, output_dir),
    }


def generate_figure1(output_dir: Path) -> Dict[str, Any]:
    data = figure1_dataset()
    fig = plt.figure(figsize=(14.2, 8.8), constrained_layout=False)
    grid = fig.add_gridspec(2, 3, height_ratios=[1.0, 1.05], hspace=0.42, wspace=0.34)

    ax1 = fig.add_subplot(grid[0, 0])
    roles = ["doctor", "caregiver", "patient"]
    role_labels = ["Doctor", "Caregiver", "Patient"]
    role_counts = [data["role_turn_distribution"][role] for role in roles]
    bars = ax1.bar(role_labels, role_counts, color=[COLORS[role] for role in roles], width=0.66)
    add_bar_labels(ax1, bars, total=data["sample_counts"]["total_turns"])
    ax1.set_ylabel("Dialogue turns")
    ax1.set_title("Role turn distribution")
    ax1.set_ylim(0, max(role_counts) * 1.22)
    ax1.text(
        1,
        role_counts[1] * 0.55,
        "Caregiver involvement\n1,889 turns",
        ha="center",
        va="center",
        fontsize=8.5,
        color="white",
        fontweight="bold",
    )
    panel_label(ax1, "1a")
    style_axis(ax1)

    ax2 = fig.add_subplot(grid[0, 1])
    age_labels = list(data["pediatric_age_bins"].keys())
    age_counts = [data["pediatric_age_bins"][label] for label in age_labels]
    bars = ax2.bar(age_labels, age_counts, color="#60A5FA", width=0.64)
    add_bar_labels(ax2, bars)
    ax2.set_ylabel("Patients")
    ax2.set_xlabel("Age bin (years)")
    ax2.set_title("Core pediatric cohort ages (n=21)")
    ax2.set_ylim(0, max(age_counts) * 1.25)
    panel_label(ax2, "1b")
    style_axis(ax2)

    ax3 = fig.add_subplot(grid[0, 2])
    severity_labels = ["Mild", "Moderate", "Moderately\nsevere", "Severe"]
    gad_counts = [
        data["severity_distribution"]["GAD-7"].get("mild", 0),
        data["severity_distribution"]["GAD-7"].get("moderate", 0),
        0,
        data["severity_distribution"]["GAD-7"].get("severe", 0),
    ]
    phq_counts = [
        data["severity_distribution"]["PHQ-9"].get("mild", 0),
        data["severity_distribution"]["PHQ-9"].get("moderate", 0),
        data["severity_distribution"]["PHQ-9"].get("moderately severe", 0),
        data["severity_distribution"]["PHQ-9"].get("severe", 0),
    ]
    x = np.arange(len(severity_labels))
    width = 0.36
    ax3.bar(x - width / 2, gad_counts, width, label="GAD-7", color=COLORS["gad"])
    ax3.bar(x + width / 2, phq_counts, width, label="PHQ-9", color=COLORS["phq"])
    ax3.set_xticks(x)
    ax3.set_xticklabels(severity_labels)
    ax3.set_ylabel("Scale records")
    ax3.set_title("Severity distribution (n=109 scale records)")
    ax3.legend(frameon=False)
    panel_label(ax3, "1c")
    style_axis(ax3)

    ax4 = fig.add_subplot(grid[1, 0:2])
    gad = data["scale_pairs"]["gad7"]
    phq = data["scale_pairs"]["phq9"]
    corr = plot_regression_panel(
        ax4,
        gad,
        phq,
        "GAD-7 total score",
        "PHQ-9 total score",
        "Paired anxiety and depression scores",
        COLORS["accent"],
    )
    panel_label(ax4, "1d")

    ax5 = fig.add_subplot(grid[1, 2])
    risk = data["clinical_risk_thresholds"]
    risk_labels = ["GAD-7 >=10", "PHQ-9 >=10"]
    risk_values = [risk["GAD-7"]["percentage"], risk["PHQ-9"]["percentage"]]
    risk_counts = [risk["GAD-7"]["count"], risk["PHQ-9"]["count"]]
    bars = ax5.bar(risk_labels, risk_values, color=[COLORS["gad"], COLORS["phq"]], width=0.58)
    for bar, count in zip(bars, risk_counts):
        ax5.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 2,
            f"{count}/109\n{bar.get_height():.1f}%",
            ha="center",
            va="bottom",
            fontsize=8.2,
        )
    ax5.set_ylim(0, 100)
    ax5.set_ylabel("Scale records above threshold (%)")
    ax5.set_title("Clinical risk proportions")
    ax5.tick_params(axis="x", rotation=12)
    panel_label(ax5, "1e")
    style_axis(ax5)

    fig.suptitle(
        "Demographic baseline, dialogue characteristics, and clinical severity distribution of the outpatient cohort",
        y=0.985,
    )
    fig.text(
        0.5,
        0.012,
        "Scale records are paired GAD-7/PHQ-9 entries; pediatric age bins include valid integer ages from 0 to 17 years.",
        ha="center",
        fontsize=9,
    )
    fig.subplots_adjust(left=0.065, right=0.985, top=0.895, bottom=0.12, hspace=0.48, wspace=0.34)
    generated = export_figure(fig, output_dir, "figure1")
    plt.close(fig)

    split_generated = generate_figure1_split_panels(data, output_dir)
    split_paths = flatten_split_paths(split_generated)

    validation = {k: v for k, v in data.items() if k != "scale_pairs"}
    validation["gad7_phq9_correlation"] = corr
    validation["split_panel_files"] = split_panel_path_strings(split_generated)
    validation_path = output_dir / "figure1_validation.json"
    validation["generated_files"] = [str(path) for path in [*generated, *split_paths, validation_path]]
    validation_path.write_text(json.dumps(validation, ensure_ascii=False, indent=2), encoding="utf-8")
    assert all(path.exists() for path in [*generated, *split_paths]), "Figure 1 export failed"
    assert validation_path.exists(), "Figure 1 validation JSON missing"
    return validation


def heatmap_text_color(value: float) -> str:
    return "white" if abs(value) >= 0.28 else "#111827"


def export_liwc_heatmap_panel(data: Dict[str, Any], output_dir: Path) -> List[Path]:
    fig, ax = plt.subplots(figsize=(5.3, 5.8))
    features = data["heatmap"]["features"]
    scales = ["GAD-7_mean", "PHQ-9_mean"]
    scale_labels = ["GAD-7", "PHQ-9"]
    matrix = np.array(
        [[data["heatmap"]["correlations"][feature][scale]["pearson_r"] for scale in scales] for feature in features],
        dtype=float,
    )
    p_matrix = np.array(
        [[data["heatmap"]["correlations"][feature][scale]["pearson_p"] for scale in scales] for feature in features],
        dtype=float,
    )
    im = ax.imshow(matrix, cmap="RdBu_r", vmin=-0.35, vmax=0.35, aspect="auto")
    ax.set_xticks(np.arange(len(scales)))
    ax.set_xticklabels(scale_labels)
    ax.set_yticks(np.arange(len(features)))
    ax.set_yticklabels(features)
    ax.set_xlabel("Clinical scale")
    ax.set_ylabel("LIWC feature")
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            p_val = float(p_matrix[i, j])
            text = f"{matrix[i, j]:.2f}{significance_marker(p_val)}\n{p_text(p_val)}"
            ax.text(j, i, text, ha="center", va="center", fontsize=8.0, color=heatmap_text_color(matrix[i, j]))
    cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.035)
    cbar.set_label("Pearson r")
    ax.set_title("Top 10 LIWC features ranked by Pearson p-value")
    panel_label(ax, "2a")
    fig.subplots_adjust(left=0.24, right=0.92, top=0.9, bottom=0.12)
    generated = export_figure(fig, output_dir, "figure2a_liwc_heatmap")
    plt.close(fig)
    return generated


def export_gad7_feature_panel(data: Dict[str, Any], output_dir: Path) -> List[Path]:
    fig, axes = plt.subplots(1, 3, figsize=(12.6, 3.8))
    gad_feature_labels = [
        ("negemo", "Negative emotion density"),
        ("discrep", "Discrepancy word density"),
        ("absolute_density", "Absolutist expression density"),
    ]
    for idx, (feature, label) in enumerate(gad_feature_labels):
        payload = data["gad7_scatter_features"][feature]
        plot_regression_panel(
            axes[idx],
            payload["scale_values"],
            payload["feature_values"],
            "GAD-7 patient-level mean",
            label,
            f"GAD-7 vs {feature}",
            COLORS["gad"],
        )
    panel_label(axes[0], "2b")
    fig.suptitle("GAD-7 anxiety-related linguistic features", y=0.98)
    fig.subplots_adjust(left=0.065, right=0.985, top=0.8, bottom=0.18, wspace=0.36)
    generated = export_figure(fig, output_dir, "figure2b_gad7_anxiety_features")
    plt.close(fig)
    return generated


def export_phq9_feature_panel(data: Dict[str, Any], output_dir: Path) -> List[Path]:
    fig, axes = plt.subplots(1, 3, figsize=(12.6, 3.8))
    phq_labels = {
        "sad": "Sadness word density",
        "certain": "Certainty word density",
        "posemo": "Positive emotion density",
        "tentat": "Tentative word density",
    }
    phq_features = list(data["phq9_scatter_features"].keys())
    for idx, feature in enumerate(phq_features):
        payload = data["phq9_scatter_features"][feature]
        plot_regression_panel(
            axes[idx],
            payload["scale_values"],
            payload["feature_values"],
            "PHQ-9 patient-level mean",
            phq_labels.get(feature, f"{feature} density"),
            f"PHQ-9 vs {feature}",
            COLORS["phq"],
        )
    panel_label(axes[0], "2c")
    fig.suptitle("PHQ-9 depression-related linguistic features", y=0.98)
    fig.subplots_adjust(left=0.065, right=0.985, top=0.8, bottom=0.18, wspace=0.36)
    generated = export_figure(fig, output_dir, "figure2c_phq9_depression_features")
    plt.close(fig)
    return generated


def generate_figure2_split_panels(data: Dict[str, Any], output_dir: Path) -> Dict[str, List[Path]]:
    return {
        "figure2a_liwc_heatmap": export_liwc_heatmap_panel(data, output_dir),
        "figure2b_gad7_anxiety_features": export_gad7_feature_panel(data, output_dir),
        "figure2c_phq9_depression_features": export_phq9_feature_panel(data, output_dir),
    }


def generate_figure2(output_dir: Path) -> Dict[str, Any]:
    data = figure2_dataset()
    fig = plt.figure(figsize=(15.0, 12.4), constrained_layout=False)
    grid = fig.add_gridspec(3, 3, height_ratios=[1.45, 1.0, 1.0], hspace=0.52, wspace=0.34)

    ax_heat = fig.add_subplot(grid[0, :])
    features = data["heatmap"]["features"]
    scales = ["GAD-7_mean", "PHQ-9_mean"]
    scale_labels = ["GAD-7", "PHQ-9"]
    matrix = np.array(
        [[data["heatmap"]["correlations"][feature][scale]["pearson_r"] for scale in scales] for feature in features],
        dtype=float,
    )
    p_matrix = np.array(
        [[data["heatmap"]["correlations"][feature][scale]["pearson_p"] for scale in scales] for feature in features],
        dtype=float,
    )
    im = ax_heat.imshow(matrix, cmap="RdBu_r", vmin=-0.35, vmax=0.35, aspect="auto")
    ax_heat.set_xticks(np.arange(len(scales)))
    ax_heat.set_xticklabels(scale_labels)
    ax_heat.set_yticks(np.arange(len(features)))
    ax_heat.set_yticklabels(features)
    ax_heat.set_xlabel("Clinical scale")
    ax_heat.set_ylabel("LIWC feature")
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            p_val = float(p_matrix[i, j])
            text = f"{matrix[i, j]:.2f}{significance_marker(p_val)}\n{p_text(p_val)}"
            ax_heat.text(j, i, text, ha="center", va="center", fontsize=8.0, color=heatmap_text_color(matrix[i, j]))
    cbar = fig.colorbar(im, ax=ax_heat, fraction=0.025, pad=0.015)
    cbar.set_label("Pearson r")
    ax_heat.set_title("Top 10 LIWC features ranked by Pearson p-value")
    panel_label(ax_heat, "2a")

    gad_feature_labels = [
        ("negemo", "Negative emotion density"),
        ("discrep", "Discrepancy word density"),
        ("absolute_density", "Absolutist expression density"),
    ]
    for idx, (feature, label) in enumerate(gad_feature_labels):
        ax = fig.add_subplot(grid[1, idx])
        payload = data["gad7_scatter_features"][feature]
        plot_regression_panel(
            ax,
            payload["scale_values"],
            payload["feature_values"],
            "GAD-7 patient-level mean",
            label,
            f"GAD-7 vs {feature}",
            COLORS["gad"],
        )
        panel_label(ax, f"2{chr(ord('b') + idx)}")

    phq_features = list(data["phq9_scatter_features"].keys())
    phq_labels = {
        "sad": "Sadness word density",
        "certain": "Certainty word density",
        "posemo": "Positive emotion density",
        "tentat": "Tentative word density",
    }
    for idx, feature in enumerate(phq_features):
        ax = fig.add_subplot(grid[2, idx])
        payload = data["phq9_scatter_features"][feature]
        plot_regression_panel(
            ax,
            payload["scale_values"],
            payload["feature_values"],
            "PHQ-9 patient-level mean",
            phq_labels.get(feature, f"{feature} density"),
            f"PHQ-9 vs {feature}",
            COLORS["phq"],
        )
        panel_label(ax, f"2{chr(ord('e') + idx)}")

    fig.suptitle("Linguistic correlates of caregiver anxiety and depression", y=0.985)
    fig.text(
        0.5,
        0.012,
        "Pearson correlations are recomputed from patient-level raw arrays; * p<0.05, ** p<0.01. Absolutist density is operationalized from existing domain-rule patterns.",
        ha="center",
        fontsize=9,
    )
    fig.subplots_adjust(left=0.06, right=0.985, top=0.91, bottom=0.09, hspace=0.58, wspace=0.36)
    generated = export_figure(fig, output_dir, "figure2")
    plt.close(fig)

    split_generated = generate_figure2_split_panels(data, output_dir)
    split_paths = flatten_split_paths(split_generated)

    validation = data
    validation["heatmap_orientation"] = {
        "x_axis": "Clinical scales: GAD-7 and PHQ-9",
        "y_axis": "Top 10 LIWC features ranked by Pearson p-value",
    }
    validation["split_panel_files"] = split_panel_path_strings(split_generated)
    validation_path = output_dir / "figure2_validation.json"
    validation["generated_files"] = [str(path) for path in [*generated, *split_paths, validation_path]]
    validation_path.write_text(json.dumps(validation, ensure_ascii=False, indent=2), encoding="utf-8")
    assert all(path.exists() for path in [*generated, *split_paths]), "Figure 2 export failed"
    assert validation_path.exists(), "Figure 2 validation JSON missing"
    return validation


def format_float(value: Any, digits: int = 6) -> str:
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: format_float(row.get(key, "")) for key in fieldnames})
    return path


def markdown_table(headers: List[str], rows: List[List[str]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def by_target_and_mode(rows: List[Dict[str, Any]], target: str, mode: str) -> Dict[str, Any]:
    for row in rows:
        if row["target"] == target and row["feature_mode"] == mode:
            return row
    raise ValueError(f"Missing row for {target}/{mode}")


def generate_figure3(output_dir: Path) -> Dict[str, Any]:
    data = figure3_dataset()
    modes = data["feature_modes"]
    targets = data["targets"]
    fig, axes = plt.subplots(1, 2, figsize=(12.4, 4.4), sharey=True)
    metric_specs = [
        ("f1", "F1", COLORS["accent"]),
        ("balanced_acc", "Balanced accuracy", COLORS["caregiver"]),
    ]
    x = np.arange(len(modes))
    width = 0.36
    for target_idx, target in enumerate(targets):
        ax = axes[target_idx]
        for metric_idx, (metric_key, metric_label, color) in enumerate(metric_specs):
            means = [by_target_and_mode(data["rows"], target, mode)[f"{metric_key}_mean"] for mode in modes]
            stds = [by_target_and_mode(data["rows"], target, mode)[f"{metric_key}_std"] for mode in modes]
            offset = (metric_idx - 0.5) * width
            bars = ax.bar(
                x + offset,
                means,
                width,
                yerr=stds,
                capsize=3,
                label=metric_label,
                color=color,
                edgecolor=COLORS["text"],
                linewidth=0.45,
                alpha=0.9,
            )
            for bar, mean in zip(bars, means):
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    min(0.98, mean + 0.035),
                    f"{mean:.2f}",
                    ha="center",
                    va="bottom",
                    fontsize=7.5,
                )
        ax.set_xticks(x)
        ax.set_xticklabels([data["feature_mode_labels"][mode] for mode in modes], rotation=18, ha="right")
        ax.set_ylim(0, 1.02)
        ax.set_title(f"{data['target_labels'][target]} target")
        ax.set_ylabel("Cross-validation metric" if target_idx == 0 else "")
        panel_label(ax, f"3{chr(ord('a') + target_idx)}")
        style_axis(ax)
    axes[0].legend(frameon=False, loc="upper left")
    fig.suptitle("Feature-set performance with honest F1 and balanced accuracy labels", y=0.98)
    fig.text(
        0.5,
        0.02,
        "Bars show source means; error bars show source cross-validation standard deviations from the V2 with-caregiver summary.",
        ha="center",
        fontsize=9,
    )
    fig.subplots_adjust(left=0.07, right=0.985, top=0.82, bottom=0.24, wspace=0.18)
    generated = export_figure(fig, output_dir, FIGURE3_STEM)
    plt.close(fig)
    assert all(path.exists() for path in generated), "Figure 3 export failed"
    return {"data": data, "generated_files": [str(path) for path in generated]}


def generate_figure4(output_dir: Path) -> Dict[str, Any]:
    data = figure4_dataset()
    rows = data["rows"]
    x = np.arange(len(rows))
    labels = [row["window_label"] for row in rows]
    fig, axes = plt.subplots(1, 2, figsize=(12.6, 4.2))

    ax_cls = axes[0]
    ax_cls.plot(x, [row["f1"] for row in rows], marker="o", color=COLORS["accent"], label="F1")
    ax_cls.plot(x, [row["acc"] for row in rows], marker="s", color=COLORS["gad"], label="ACC")
    ax_cls.set_xticks(x)
    ax_cls.set_xticklabels(labels)
    ax_cls.set_ylim(0.65, 1.0)
    ax_cls.set_xlabel("Available context window")
    ax_cls.set_ylabel("Classification metric")
    ax_cls.set_title("Combined available-window classification")
    ax_cls.legend(frameon=False)
    panel_label(ax_cls, "4a")
    style_axis(ax_cls, grid_axis="both")

    ax_corr = axes[1]
    ax_corr.axhline(0, color=COLORS["muted"], linewidth=1.0, linestyle="--")
    ax_corr.plot(x, [row["gad_corr"] for row in rows], marker="o", color=COLORS["gad"], label="GAD-7 r")
    ax_corr.plot(x, [row["phq_corr"] for row in rows], marker="s", color=COLORS["phq"], label="PHQ-9 r")
    ax_corr.set_xticks(x)
    ax_corr.set_xticklabels(labels)
    ax_corr.set_ylim(-0.35, 0.45)
    ax_corr.set_xlabel("Available context window")
    ax_corr.set_ylabel("Pearson r")
    ax_corr.set_title("Combined available-window correlations")
    ax_corr.legend(frameon=False)
    panel_label(ax_corr, "4b")
    style_axis(ax_corr, grid_axis="both")

    fig.suptitle("Combined available-window analysis; ACC is not balanced accuracy", y=0.98)
    fig.text(
        0.5,
        0.02,
        "The source summary has no per-target window classification trajectories and no window balanced_acc field.",
        ha="center",
        fontsize=9,
    )
    fig.subplots_adjust(left=0.07, right=0.985, top=0.8, bottom=0.18, wspace=0.28)
    generated = export_figure(fig, output_dir, FIGURE4_STEM)
    plt.close(fig)
    assert all(path.exists() for path in generated), "Figure 4 export failed"
    return {"data": data, "generated_files": [str(path) for path in generated]}


def figure5_rows_by_target(rows: List[Dict[str, Any]], target: str) -> List[Dict[str, Any]]:
    selected = [row for row in rows if row["target"] == target]
    if len(selected) != 2:
        raise ValueError(f"Figure 5 expected two methods for {target}, found {len(selected)}")
    return selected


def generate_figure5(output_dir: Path) -> Dict[str, Any]:
    data = figure5_dataset()
    targets = ["anxiety", "depression"]
    metric_specs = [
        ("f1", "F1", COLORS["accent"]),
        ("balanced_acc", "Balanced accuracy", COLORS["caregiver"]),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(10.8, 4.4), sharey=True)
    width = 0.34
    for target_idx, target in enumerate(targets):
        ax = axes[target_idx]
        target_rows = figure5_rows_by_target(data["rows"], target)
        x = np.arange(len(target_rows))
        for metric_idx, (metric_key, metric_label, color) in enumerate(metric_specs):
            means = [row[metric_key] for row in target_rows]
            stds = [row[f"{metric_key}_std"] for row in target_rows]
            offset = (metric_idx - 0.5) * width
            bars = ax.bar(
                x + offset,
                means,
                width,
                yerr=stds,
                capsize=3,
                label=metric_label,
                color=color,
                edgecolor=COLORS["text"],
                linewidth=0.45,
                alpha=0.9,
            )
            for bar, mean in zip(bars, means):
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    min(0.98, mean + 0.035),
                    f"{mean:.2f}",
                    ha="center",
                    va="bottom",
                    fontsize=8.0,
                )
        ax.set_xticks(x)
        ax.set_xticklabels([f"{row['method']}\n{row['method_detail']}" for row in target_rows], fontsize=8.2)
        ax.set_ylim(0, 1.02)
        ax.set_title(f"{TARGET_LABELS[target]} target")
        ax.set_ylabel("Metric value" if target_idx == 0 else "")
        ax.text(
            0.98,
            0.06,
            "Direct LLM n=12",
            transform=ax.transAxes,
            ha="right",
            va="bottom",
            fontsize=8.4,
            color=COLORS["muted"],
        )
        panel_label(ax, f"5{chr(ord('a') + target_idx)}")
        style_axis(ax)
    axes[0].legend(frameon=False, loc="upper left")
    fig.suptitle("Best ML cross-validation performance versus direct zero-shot LLM", y=0.98)
    fig.text(
        0.5,
        0.02,
        "Best ML uses hybrid v2 for anxiety and LIWC only for depression from V2 CV; direct zero-shot LLM values come from the 12-sample test-set summary.",
        ha="center",
        fontsize=9,
    )
    fig.subplots_adjust(left=0.075, right=0.985, top=0.8, bottom=0.28, wspace=0.2)
    generated = export_figure(fig, output_dir, FIGURE5_STEM)
    plt.close(fig)
    assert all(path.exists() for path in generated), "Figure 5 export failed"
    return {"data": data, "generated_files": [str(path) for path in generated]}


def write_table1_outputs(output_dir: Path, rows: List[Dict[str, Any]]) -> List[Path]:
    csv_path = output_dir / "table1_feature_set_performance.csv"
    fieldnames = [
        "target",
        "feature_mode",
        "acc_mean",
        "acc_std",
        "f1_mean",
        "f1_std",
        "balanced_acc_mean",
        "balanced_acc_std",
        "precision_mean",
        "precision_std",
        "recall_mean",
        "recall_std",
        "specificity_mean",
        "specificity_std",
    ]
    write_csv(csv_path, rows, fieldnames)
    md_rows = [
        [
            row["target_label"],
            row["feature_mode_label"],
            f"{row['f1_mean']:.6f} +/- {row['f1_std']:.6f}",
            f"{row['balanced_acc_mean']:.6f} +/- {row['balanced_acc_std']:.6f}",
            f"{row['acc_mean']:.6f} +/- {row['acc_std']:.6f}",
        ]
        for row in rows
    ]
    md_text = "# Table 1. Feature-set classification performance\n\n"
    md_text += "Source: `analysis/emotion_model/experiments/with_caregiver_feature_v2_cv/summary.json`. Values are source means plus/minus source cross-validation standard deviations.\n\n"
    md_text += markdown_table(["Target", "Feature mode", "F1", "Balanced accuracy", "ACC"], md_rows)
    md_text += "\n"
    md_path = output_dir / "table1_feature_set_performance.md"
    md_path.write_text(md_text, encoding="utf-8")
    return [csv_path, md_path]


def write_section_3_2_correlations(output_dir: Path) -> Path:
    rows = section_3_2_correlation_rows()
    fieldnames = [
        "dataset",
        "respondent",
        "target_role",
        "sample_size",
        "scale",
        "feature",
        "pearson_r",
        "pearson_p",
        "spearman_r",
        "spearman_p",
        "significant",
    ]
    return write_csv(output_dir / "section_3_2_correlations.csv", rows, fieldnames)


def write_figure4_availability_report(output_dir: Path, figure4_data: Dict[str, Any]) -> Path:
    rows = figure4_data["rows"]
    md_rows = [
        [
            row["window_label"],
            str(row["samples"]),
            str(row["patients"]),
            f"{row['f1']:.6f}",
            f"{row['acc']:.6f}",
            f"{row['gad_corr']:.6f}",
            f"{row['phq_corr']:.6f}",
        ]
        for row in rows
    ]
    text = "# Figure 4 window data availability\n\n"
    text += "Figure 4 uses the available combined window series only. The current source summary does not contain separate anxiety/depression window trajectories.\n\n"
    text += "The source has `acc` and `f1`; it does not have window `balanced_acc`, so `acc` is labeled as ACC and is not renamed to balanced accuracy.\n\n"
    text += markdown_table(["Window", "Samples", "Patients", "F1", "ACC", "GAD-7 r", "PHQ-9 r"], md_rows)
    text += "\n"
    path = output_dir / "figure4_window_data_availability.md"
    path.write_text(text, encoding="utf-8")
    return path


def section32_row(rows: List[Dict[str, Any]], dataset: str, scale: str, feature: str) -> Dict[str, Any]:
    for row in rows:
        if row["dataset"] == dataset and row["scale"] == scale and row["feature"] == feature:
            return row
    raise ValueError(f"Missing Section 3.2 correlation row for {dataset}/{scale}/{feature}")


def section32_answer_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    caregiver_rows = [row for row in rows if row["dataset"] == "with_caregiver"]
    pearson_significant = [row for row in caregiver_rows if row["pearson_p"] < 0.05]
    spearman_significant = [row for row in caregiver_rows if row["spearman_p"] < 0.05]
    spearman_labels = [f"{row['scale']}/{row['feature']}" for row in spearman_significant]
    key_rows = {
        "GAD-7 negemo": section32_row(rows, "with_caregiver", "GAD-7_mean", "negemo"),
        "GAD-7 anx": section32_row(rows, "with_caregiver", "GAD-7_mean", "anx"),
        "GAD-7 discrep": section32_row(rows, "with_caregiver", "GAD-7_mean", "discrep"),
        "PHQ-9 sad": section32_row(rows, "with_caregiver", "PHQ-9_mean", "sad"),
        "PHQ-9 posemo": section32_row(rows, "with_caregiver", "PHQ-9_mean", "posemo"),
        "PHQ-9 tentat": section32_row(rows, "with_caregiver", "PHQ-9_mean", "tentat"),
        "PHQ-9 certain": section32_row(rows, "with_caregiver", "PHQ-9_mean", "certain"),
        "without-caregiver GAD-7 discrep": section32_row(rows, "without_caregiver", "GAD-7_mean", "discrep"),
    }
    return {
        "with_caregiver_pearson_p_lt_0_05_count": len(pearson_significant),
        "with_caregiver_spearman_p_lt_0_05_count": len(spearman_significant),
        "with_caregiver_spearman_p_lt_0_05_features": spearman_labels,
        "key_rows": key_rows,
    }


def section32_metric_text(row: Dict[str, Any]) -> str:
    return (
        f"Pearson r={row['pearson_r']:.4f}, p={row['pearson_p']:.4f}; "
        f"Spearman r={row['spearman_r']:.4f}, p={row['spearman_p']:.4f}"
    )


def write_answer_report(output_dir: Path, figure3_data: Dict[str, Any], figure4_data: Dict[str, Any], figure5_data: Dict[str, Any]) -> List[Path]:
    section32_rows = section_3_2_correlation_rows()
    section32 = section32_answer_summary(section32_rows)
    report_rows = [
        {
            "item": "Section 3.2 with-caregiver Pearson significance",
            "status": "answered",
            "evidence": f"Pearson p<0.05 count is {section32['with_caregiver_pearson_p_lt_0_05_count']} across with-caregiver LIWC rows.",
            "source": "analysis/results/summary_latest.json",
        },
        {
            "item": "Section 3.2 with-caregiver Spearman significance",
            "status": "answered",
            "evidence": "Spearman p<0.05 rows: " + "; ".join(section32["with_caregiver_spearman_p_lt_0_05_features"]),
            "source": "analysis/results/summary_latest.json",
        },
        {
            "item": "Section 3.2 caregiver linguistic claims",
            "status": "answered_with_limits",
            "evidence": "Requested caregiver Pearson claims are not statistically significant at p<0.05; PHQ-9 posemo is positive rather than negative.",
            "source": "analysis/results/summary_latest.json",
        },
        {
            "item": "Figure 3 feature-set F1 and balanced accuracy",
            "status": "supported",
            "evidence": "Generated from V2 with-caregiver CV summary using f1 and balanced_acc means with source standard deviations.",
            "source": "analysis/emotion_model/experiments/with_caregiver_feature_v2_cv/summary.json",
        },
        {
            "item": "Figure 4 combined available-window curve",
            "status": "supported_with_limits",
            "evidence": "Generated from the combined window series using F1, ACC, GAD-7 r, and PHQ-9 r; no per-target trajectories were inferred.",
            "source": "analysis/emotion_model/experiments/window_comparison/window_comparison_summary.json",
        },
        {
            "item": "Figure 5 Best ML versus direct zero-shot LLM",
            "status": "supported",
            "evidence": "Best ML anxiety uses hybrid_v2, Best ML depression uses liwc_only, and direct LLM values are taken from the 12-sample summary.",
            "source": "V2 CV summary and direct_llm_zero_shot_testset/summary.json",
        },
    ]
    for unsupported in UNSUPPORTED_REQUIREMENTS:
        report_rows.append(
            {
                "item": unsupported["requirement"],
                "status": unsupported["status"],
                "evidence": unsupported["evidence"],
                "source": "current pipeline source audit",
            }
        )
    csv_path = output_dir / "figures2_answer_report.csv"
    write_csv(csv_path, report_rows, ["item", "status", "evidence", "source"])

    best_rows = figure5_data["rows"]
    best_summary_rows = [
        [row["target_label"], row["method"], row["method_detail"], f"{row['f1']:.6f}", f"{row['balanced_acc']:.6f}", "12" if row["n"] == 12 else "CV"]
        for row in best_rows
    ]
    text = "# Figures 3-5 answer report\n\n"
    text += "This report records which requested outputs are supported by the existing experiment summaries and which requirements remain unsupported. No unavailable data were fabricated.\n\n"
    text += "## Supported outputs\n\n"
    text += "- Figure 3: grouped bars for feature modes and targets with F1 and balanced accuracy, using source CV standard deviations as error bars.\n"
    text += "- Figure 4: combined available-window analysis with F1 and ACC, plus GAD-7 and PHQ-9 correlations in a second panel.\n"
    text += "- Figure 5: Best ML versus direct zero-shot LLM for anxiety and depression, with direct LLM annotated as n=12.\n\n"
    text += "## Section 3.2 correlation answers\n\n"
    text += f"- With-caregiver Pearson p<0.05 count: {section32['with_caregiver_pearson_p_lt_0_05_count']}.\n"
    text += f"- With-caregiver Spearman p<0.05 count: {section32['with_caregiver_spearman_p_lt_0_05_count']} ({'; '.join(section32['with_caregiver_spearman_p_lt_0_05_features'])}).\n"
    text += "- Requested caregiver Pearson claims are not significant at p<0.05; PHQ-9 posemo is positive rather than negative in the existing result file.\n\n"
    key_rows = section32["key_rows"]
    text += markdown_table(
        ["Claim", "Existing-data answer"],
        [
            ["GAD-7 negative emotion", section32_metric_text(key_rows["GAD-7 negemo"])],
            ["GAD-7 anxiety words", section32_metric_text(key_rows["GAD-7 anx"])],
            ["GAD-7 discrepancy words", section32_metric_text(key_rows["GAD-7 discrep"])],
            ["PHQ-9 sadness words", section32_metric_text(key_rows["PHQ-9 sad"])],
            ["PHQ-9 positive emotion", section32_metric_text(key_rows["PHQ-9 posemo"])],
            ["PHQ-9 tentative words", section32_metric_text(key_rows["PHQ-9 tentat"])],
            ["PHQ-9 certainty words", section32_metric_text(key_rows["PHQ-9 certain"])],
            ["Without-caregiver GAD-7 discrepancy note", section32_metric_text(key_rows["without-caregiver GAD-7 discrep"]) + "; not a caregiver result"],
        ],
    )
    text += "\n\n"
    text += "## Unsupported requirements explicitly recorded\n\n"
    for unsupported in UNSUPPORTED_REQUIREMENTS:
        text += f"- {unsupported['requirement']}: {unsupported['evidence']}\n"
    text += "\n## Figure 5 source-derived values\n\n"
    text += markdown_table(["Target", "Method", "Detail", "F1", "Balanced accuracy", "Sample basis"], best_summary_rows)
    text += "\n\n"
    text += "## Source row counts\n\n"
    text += f"- Figure 3/Table 1 V2 CV rows: {len(figure3_data['rows'])}\n"
    text += f"- Figure 4 combined window rows: {len(figure4_data['rows'])}\n"
    text += f"- Figure 5 comparison rows: {len(figure5_data['rows'])}\n"
    md_path = output_dir / "figures2_answer_report.md"
    md_path.write_text(text, encoding="utf-8")
    return [md_path, csv_path]


def generate_reports(output_dir: Path, figure3_result: Dict[str, Any], figure4_result: Dict[str, Any], figure5_result: Dict[str, Any]) -> Dict[str, Any]:
    figure3_data = figure3_result["data"]
    figure4_data = figure4_result["data"]
    figure5_data = figure5_result["data"]
    section32_rows = section_3_2_correlation_rows()
    section32 = section32_answer_summary(section32_rows)
    table_paths = write_table1_outputs(output_dir, table1_feature_set_performance_rows())
    section_path = write_section_3_2_correlations(output_dir)
    availability_path = write_figure4_availability_report(output_dir, figure4_data)
    answer_paths = write_answer_report(output_dir, figure3_data, figure4_data, figure5_data)
    validation_path = output_dir / "figures2_validation.json"
    validation = {
        "input_paths": figure3_data["input_paths"],
        "generated_figures": {
            "figure3": figure3_result["generated_files"],
            "figure4": figure4_result["generated_files"],
            "figure5": figure5_result["generated_files"],
        },
        "generated_reports": [str(path) for path in [*answer_paths, section_path, *table_paths, availability_path, validation_path]],
        "unsupported_requirements": UNSUPPORTED_REQUIREMENTS,
        "section_3_2": {
            "source": str(Path(figure3_data["input_paths"]["summary_latest"])),
            "with_caregiver_pearson_p_lt_0_05_count": section32["with_caregiver_pearson_p_lt_0_05_count"],
            "with_caregiver_spearman_p_lt_0_05_count": section32["with_caregiver_spearman_p_lt_0_05_count"],
            "with_caregiver_spearman_p_lt_0_05_features": section32["with_caregiver_spearman_p_lt_0_05_features"],
            "note": "Requested caregiver Pearson claims are not significant at p<0.05; PHQ-9 posemo is positive rather than negative in the existing result file.",
        },
        "figure3": {
            "source": figure3_data["source"],
            "metrics": ["F1", "Balanced accuracy"],
            "error_bars": figure3_data["error_bar_note"],
            "row_count": len(figure3_data["rows"]),
        },
        "figure4": {
            "source": figure4_data["source"],
            "labeling": "Combined available-window analysis; ACC is labeled as ACC and not as balanced accuracy.",
            "row_count": len(figure4_data["rows"]),
        },
        "figure5": {
            "sources": figure5_data["sources"],
            "best_ml_specs": figure5_data["best_ml_specs"],
            "direct_llm_n": figure5_data["direct_llm_n"],
            "row_count": len(figure5_data["rows"]),
        },
    }
    validation_path.write_text(json.dumps(validation, ensure_ascii=True, indent=2), encoding="utf-8")
    return validation


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate English vector academic Mind-Echo figures.")
    parser.add_argument("--figure", choices=["all", "1", "2", "3", "4", "5"], default="all", help="Figure to generate")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory")
    args = parser.parse_args()

    apply_academic_style()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.figure in {"all", "1"}:
        validation = generate_figure1(args.output_dir)
        print(
            "Generated Figure 1: total_turns={total_turns}, pediatric_age_n={age_n}, scale_records={scale_n}".format(
                total_turns=validation["sample_counts"]["total_turns"],
                age_n=validation["sample_counts"]["valid_pediatric_age_n"],
                scale_n=validation["sample_counts"]["scale_record_n"],
            )
        )
    if args.figure in {"all", "2"}:
        validation = generate_figure2(args.output_dir)
        print(
            "Generated Figure 2: heatmap_features={features}, absolute_density_n={n}".format(
                features=", ".join(validation["heatmap"]["features"]),
                n=validation["sample_counts"]["absolute_density_n"],
            )
        )
    figure3_result = None
    figure4_result = None
    figure5_result = None
    if args.figure in {"all", "3"}:
        figure3_result = generate_figure3(args.output_dir)
        print(
            "Generated Figure 3: feature_modes={modes}, rows={rows}".format(
                modes=", ".join(FEATURE_MODE_LABELS.keys()),
                rows=len(figure3_result["data"]["rows"]),
            )
        )
    if args.figure in {"all", "4"}:
        figure4_result = generate_figure4(args.output_dir)
        print(
            "Generated Figure 4: combined_window_rows={rows}; ACC kept separate from balanced accuracy".format(
                rows=len(figure4_result["data"]["rows"]),
            )
        )
    if args.figure in {"all", "5"}:
        figure5_result = generate_figure5(args.output_dir)
        print(
            "Generated Figure 5: direct_llm_n={n}, comparison_rows={rows}".format(
                n=figure5_result["data"]["direct_llm_n"],
                rows=len(figure5_result["data"]["rows"]),
            )
        )
    if args.figure == "all":
        if figure3_result is None or figure4_result is None or figure5_result is None:
            raise RuntimeError("Reports require Figure 3, Figure 4, and Figure 5 results")
        report_validation = generate_reports(args.output_dir, figure3_result, figure4_result, figure5_result)
        print(
            "Generated reports: unsupported_requirements={count}".format(
                count=len(report_validation["unsupported_requirements"]),
            )
        )
    expected = []
    if args.figure in {"all", "1"}:
        expected.extend(["figure1.svg", "figure1.pdf", "figure1.png", "figure1_validation.json"])
        expected.extend(f"{stem}.{ext}" for stem in FIGURE1_SPLIT_STEMS for ext in ["svg", "pdf", "png"])
    if args.figure in {"all", "2"}:
        expected.extend(["figure2.svg", "figure2.pdf", "figure2.png", "figure2_validation.json"])
        expected.extend(f"{stem}.{ext}" for stem in FIGURE2_SPLIT_STEMS for ext in ["svg", "pdf", "png"])
    if args.figure in {"all", "3"}:
        expected.extend(f"{FIGURE3_STEM}.{ext}" for ext in ["svg", "pdf", "png"])
    if args.figure in {"all", "4"}:
        expected.extend(f"{FIGURE4_STEM}.{ext}" for ext in ["svg", "pdf", "png"])
    if args.figure in {"all", "5"}:
        expected.extend(f"{FIGURE5_STEM}.{ext}" for ext in ["svg", "pdf", "png"])
    if args.figure == "all":
        expected.extend(REPORT_FILES)
    missing = [name for name in expected if not (args.output_dir / name).exists()]
    assert not missing, f"Missing generated outputs: {missing}"
    print(f"Validated output files in {args.output_dir}")


if __name__ == "__main__":
    main()

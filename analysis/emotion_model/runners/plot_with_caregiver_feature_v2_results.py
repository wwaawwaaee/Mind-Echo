#!/usr/bin/env python3
"""Plot classification results for Feature Engineering V2."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np


def load_summary(summary_path: Path) -> dict:
    with open(summary_path, encoding='utf-8') as f:
        return json.load(f)


def plot_target(ax, rows: list[dict], target: str, metric_keys: list[str], metric_labels: list[str], colors: list[str]):
    feature_modes = [row['feature_mode'] for row in rows]
    x = np.arange(len(feature_modes))
    width = 0.16

    for idx, (metric_key, label, color) in enumerate(zip(metric_keys, metric_labels, colors)):
        means = [row['classification'][metric_key]['mean'] for row in rows]
        stds = [row['classification'][metric_key]['std'] for row in rows]
        offset = (idx - (len(metric_keys) - 1) / 2) * width
        bars = ax.bar(x + offset, means, width, yerr=stds, capsize=4, label=label, color=color, alpha=0.9)
        for bar, mean in zip(bars, means):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.015, f'{mean:.2f}', ha='center', va='bottom', fontsize=9)

    ax.set_title(f'{target.capitalize()} Classification', fontsize=13, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(feature_modes, rotation=15)
    ax.set_ylim(0, 1.05)
    ax.set_ylabel('Score')
    ax.grid(True, axis='y', alpha=0.25)


def main():
    runner_dir = Path(__file__).resolve().parent
    experiment_root = runner_dir.parent / 'experiments' / 'with_caregiver_feature_v2_cv'
    summary_path = experiment_root / 'summary.json'
    output_path = experiment_root / 'v2_classification_results.png'

    summary = load_summary(summary_path)

    metric_keys = ['f1', 'balanced_acc', 'recall', 'specificity']
    metric_labels = ['F1', 'Balanced ACC', 'Recall', 'Specificity']
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']

    fig, axes = plt.subplots(1, 2, figsize=(15, 6), sharey=True)
    for ax, target in zip(axes, ['anxiety', 'depression']):
        plot_target(ax, summary['targets'][target], target, metric_keys, metric_labels, colors)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper center', ncol=4, frameon=False, bbox_to_anchor=(0.5, 1.02))
    fig.suptitle('Feature Engineering V2: Classification Performance (with_caregiver, patient-level 5-fold CV)', fontsize=15, fontweight='bold', y=1.08)
    fig.text(0.5, 0.01, 'Bars show mean performance; error bars show standard deviation across folds.', ha='center', fontsize=10)
    plt.tight_layout(rect=[0, 0.04, 1, 0.95])
    plt.savefig(output_path, dpi=180, bbox_inches='tight')
    plt.close()

    print(f'Saved V2 classification figure to: {output_path}')


if __name__ == '__main__':
    main()

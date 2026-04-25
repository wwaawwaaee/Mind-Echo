#!/usr/bin/env python3
"""Plot best ML baseline versus direct LLM zero-shot results."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np


def load_json(path: Path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def extract_metric_block(metric_source: dict) -> tuple[list[str], list[float], list[float]]:
    keys = ['f1', 'balanced_acc', 'precision', 'recall', 'specificity']
    means = []
    stds = []
    for key in keys:
        value = metric_source[key]
        if isinstance(value, dict):
            means.append(value['mean'])
            stds.append(value.get('std', 0.0))
        else:
            means.append(value)
            stds.append(0.0)
    return keys, means, stds


def main():
    runner_dir = Path(__file__).resolve().parent
    exp_root = runner_dir.parent / 'experiments'

    zero_path = exp_root / 'direct_llm_zero_shot_testset' / 'summary.json'
    ml_path = exp_root / 'with_caregiver_feature_v2_cv' / 'summary.json'
    output_path = exp_root / 'direct_llm_zero_shot_testset' / 'ml_vs_direct_llm_zero_shot.png'

    zero = load_json(zero_path)
    ml = load_json(ml_path)

    ml_dep = None
    for row in ml['targets']['depression']:
        if row['feature_mode'] == 'liwc_only':
            ml_dep = row
            break
    if ml_dep is None:
        raise RuntimeError('Could not find liwc_only depression baseline in V2 summary.')

    metric_keys, anxiety_means, anxiety_stds = extract_metric_block(zero['anxiety'])
    _, dep_zero_means, dep_zero_stds = extract_metric_block(zero['depression'])
    _, dep_ml_means, dep_ml_stds = extract_metric_block(ml_dep['classification'])

    labels = ['F1', 'Balanced ACC', 'Precision', 'Recall', 'Specificity']
    x = np.arange(len(labels))
    width = 0.35

    fig, axes = plt.subplots(1, 2, figsize=(13.5, 5.5), sharey=True)

    axes[0].bar(x, anxiety_means, width=0.6, color='#ff7f0e', yerr=anxiety_stds, capsize=4)
    axes[0].set_title('Direct LLM Zero-shot (Anxiety)', fontsize=12, fontweight='bold')
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(labels, rotation=15)
    axes[0].set_ylim(0, 1.05)
    axes[0].set_ylabel('Score')
    axes[0].grid(True, axis='y', alpha=0.25)
    for xi, yi in zip(x, anxiety_means):
        axes[0].text(xi, yi + 0.015, f'{yi:.2f}', ha='center', va='bottom', fontsize=9)

    bars1 = axes[1].bar(x - width/2, dep_ml_means, width, color='#1f77b4', yerr=dep_ml_stds, capsize=4, label='Best ML baseline')
    bars2 = axes[1].bar(x + width/2, dep_zero_means, width, color='#ff7f0e', yerr=dep_zero_stds, capsize=4, label='Direct LLM zero-shot')
    axes[1].set_title('Depression: ML vs Direct LLM Zero-shot', fontsize=12, fontweight='bold')
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(labels, rotation=15)
    axes[1].grid(True, axis='y', alpha=0.25)
    axes[1].legend(frameon=False)
    for bars, means in [(bars1, dep_ml_means), (bars2, dep_zero_means)]:
        for bar, mean in zip(bars, means):
            axes[1].text(bar.get_x() + bar.get_width()/2, mean + 0.015, f'{mean:.2f}', ha='center', va='bottom', fontsize=8)

    fig.suptitle('Traditional ML vs Direct LLM Zero-shot', fontsize=15, fontweight='bold')
    fig.text(0.5, 0.01, 'Depression panel uses the strongest traditional baseline (LIWC-only, patient-level 5-fold CV).', ha='center', fontsize=10)
    plt.tight_layout(rect=[0, 0.04, 1, 0.95])
    plt.savefig(output_path, dpi=180, bbox_inches='tight')
    plt.close()

    print(f'Saved ML vs zero-shot figure to: {output_path}')


if __name__ == '__main__':
    main()

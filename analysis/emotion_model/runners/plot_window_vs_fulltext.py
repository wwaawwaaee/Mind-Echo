#!/usr/bin/env python3
"""Plot early-window versus full_text comparison results."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def main():
    runner_dir = Path(__file__).resolve().parent
    experiment_dir = runner_dir.parent / 'experiments' / 'window_comparison'
    summary_path = experiment_dir / 'window_comparison_summary.json'
    output_path = experiment_dir / 'window_vs_fulltext.png'

    with open(summary_path, encoding='utf-8') as f:
        summary = json.load(f)

    rows = summary['results']
    labels = [str(row['window']) for row in rows]
    acc = [row['acc'] for row in rows]
    f1 = [row['f1'] for row in rows]
    gad = [row['gad_corr'] for row in rows]
    phq = [row['phq_corr'] for row in rows]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))

    axes[0].plot(labels, acc, marker='o', linewidth=2, label='Accuracy', color='#1f77b4')
    axes[0].plot(labels, f1, marker='s', linewidth=2, label='F1', color='#ff7f0e')
    axes[0].set_title('Classification Performance')
    axes[0].set_ylabel('Score')
    axes[0].set_ylim(0, 1.0)
    axes[0].grid(True, alpha=0.25)
    axes[0].legend(frameon=False)

    axes[1].plot(labels, gad, marker='o', linewidth=2, label='GAD-7 correlation', color='#2ca02c')
    axes[1].plot(labels, phq, marker='s', linewidth=2, label='PHQ-9 correlation', color='#d62728')
    axes[1].axhline(0, color='gray', linewidth=0.8, alpha=0.6)
    axes[1].set_title('Regression Correlation')
    axes[1].set_ylabel('Pearson r')
    axes[1].grid(True, alpha=0.25)
    axes[1].legend(frameon=False)

    for ax in axes:
        ax.set_xlabel('Early-text window / full_text')

    fig.suptitle('Early-window vs Full-text Comparison (with_caregiver)', fontsize=15, fontweight='bold')
    fig.text(0.5, 0.01, 'Windows 4–15 represent the first N target-role turns; full_text uses the complete caregiver text within the visit.', ha='center', fontsize=10)
    plt.tight_layout(rect=[0, 0.04, 1, 0.95])
    plt.savefig(output_path, dpi=180, bbox_inches='tight')
    plt.close()

    print(f'Saved comparison figure to: {output_path}')


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Compare local zero-shot inference against the best current ML baseline."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import accuracy_score, balanced_accuracy_score, confusion_matrix, f1_score, precision_score, recall_score
from sklearn.model_selection import GroupKFold
from transformers import pipeline


def evaluate_classification(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
    tn, fp, fn, tp = cm.ravel()
    specificity = tn / (tn + fp) if (tn + fp) > 0 else 0.0
    return {
        'acc': float(accuracy_score(y_true, y_pred)),
        'f1': float(f1_score(y_true, y_pred, zero_division=0)),
        'precision': float(precision_score(y_true, y_pred, zero_division=0)),
        'recall': float(recall_score(y_true, y_pred, zero_division=0)),
        'specificity': float(specificity),
        'balanced_acc': float(balanced_accuracy_score(y_true, y_pred)),
        'confusion_matrix': cm.tolist(),
    }


def summarize_metric_dicts(metric_dicts: list[dict]) -> dict:
    summary = {}
    scalar_keys = [key for key, value in metric_dicts[0].items() if not isinstance(value, list)]
    for key in scalar_keys:
        values = [m[key] for m in metric_dicts]
        summary[key] = {'mean': float(np.mean(values)), 'std': float(np.std(values))}
    return summary


def load_all_data() -> list[dict]:
    base = Path(__file__).resolve().parent.parent / 'experiments' / 'with_caregiver_label_comparison' / 'data' / 'all_data.json'
    with open(base, encoding='utf-8') as f:
        return json.load(f)


def load_best_ml_baseline() -> dict:
    summary_path = Path(__file__).resolve().parent.parent / 'experiments' / 'with_caregiver_feature_v2_cv' / 'summary.json'
    with open(summary_path, encoding='utf-8') as f:
        summary = json.load(f)
    for row in summary['targets']['depression']:
        if row['feature_mode'] == 'liwc_only':
            return row
    raise RuntimeError('liwc_only baseline not found in V2 summary')


def run_zero_shot_cv(data: list[dict]) -> dict:
    texts = [item['full_text'] for item in data]
    y = np.array([item['depression_label'] for item in data], dtype=int)
    groups = np.array([item['patient_id'] for item in data])

    clf = pipeline('zero-shot-classification', model='MoritzLaurer/mDeBERTa-v3-base-mnli-xnli')
    candidate_labels = ['高抑郁风险', '低抑郁风险']
    positive_label = '高抑郁风险'
    hypothesis_template = '这段门诊对话体现的是{}。'

    fold_metrics = []
    fold_details = []

    gkf = GroupKFold(n_splits=5)
    for fold_idx, (_, test_idx) in enumerate(gkf.split(texts, y, groups), start=1):
        test_texts = [texts[i] for i in test_idx]
        outputs = clf(test_texts, candidate_labels, hypothesis_template=hypothesis_template, multi_label=False, batch_size=4)
        preds = []
        for out in outputs:
            label_to_score = {label: score for label, score in zip(out['labels'], out['scores'])}
            preds.append(1 if label_to_score.get(positive_label, 0.0) >= 0.5 else 0)

        preds = np.array(preds, dtype=int)
        metrics = evaluate_classification(y[test_idx], preds)
        fold_metrics.append(metrics)
        fold_details.append({'fold': fold_idx, 'size': int(len(test_idx)), 'metrics': metrics})

    return {
        'model': 'MoritzLaurer/mDeBERTa-v3-base-mnli-xnli',
        'task': 'depression',
        'classification': summarize_metric_dicts(fold_metrics),
        'folds': fold_details,
    }


def save_plot(ml_row: dict, zero_row: dict, output_path: Path):
    metric_order = ['f1', 'balanced_acc', 'precision', 'recall', 'specificity']
    metric_labels = ['F1', 'Balanced ACC', 'Precision', 'Recall', 'Specificity']

    ml_means = [ml_row['classification'][m]['mean'] for m in metric_order]
    ml_stds = [ml_row['classification'][m]['std'] for m in metric_order]
    zs_means = [zero_row['classification'][m]['mean'] for m in metric_order]
    zs_stds = [zero_row['classification'][m]['std'] for m in metric_order]

    x = np.arange(len(metric_order))
    width = 0.36
    fig, ax = plt.subplots(figsize=(11, 6))
    bars1 = ax.bar(x - width / 2, ml_means, width, yerr=ml_stds, capsize=4, label='Best ML baseline (LIWC-only)', color='#1f77b4')
    bars2 = ax.bar(x + width / 2, zs_means, width, yerr=zs_stds, capsize=4, label='Zero-shot', color='#ff7f0e')

    for bars, means in [(bars1, ml_means), (bars2, zs_means)]:
        for bar, mean in zip(bars, means):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.015, f'{mean:.2f}', ha='center', va='bottom', fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels(metric_labels)
    ax.set_ylim(0, 1.05)
    ax.set_ylabel('Score')
    ax.set_title('Zero-shot vs Best Traditional ML Baseline\n(with_caregiver, depression, patient-level 5-fold CV)', fontsize=14, fontweight='bold')
    ax.grid(True, axis='y', alpha=0.25)
    ax.legend(frameon=False)
    fig.text(0.5, 0.01, 'Error bars show standard deviation across folds.', ha='center', fontsize=10)
    plt.tight_layout(rect=[0, 0.04, 1, 1])
    plt.savefig(output_path, dpi=180, bbox_inches='tight')
    plt.close()


def main():
    runner_dir = Path(__file__).resolve().parent
    experiment_root = runner_dir.parent / 'experiments' / 'zero_shot_vs_best_ml'
    experiment_root.mkdir(parents=True, exist_ok=True)

    data = load_all_data()
    ml_baseline = load_best_ml_baseline()
    zero_shot = run_zero_shot_cv(data)

    payload = {
        'group': 'with_caregiver',
        'text_field': 'full_text',
        'classification_target': 'depression',
        'evaluation': 'patient_level_5fold_cv',
        'best_ml_baseline': {
            'feature_mode': 'liwc_only',
            'classification': ml_baseline['classification'],
        },
        'zero_shot': zero_shot,
    }

    with open(experiment_root / 'summary.json', 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    md = [
        '# Zero-shot vs Best Traditional ML Baseline',
        '',
        'Task: with_caregiver / full_text / depression / patient-level 5-fold CV',
        '',
        '| Method | F1 | Balanced ACC | Precision | Recall | Specificity |',
        '|---|---:|---:|---:|---:|---:|',
        f"| LIWC-only ML | {ml_baseline['classification']['f1']['mean']:.3f}±{ml_baseline['classification']['f1']['std']:.3f} | {ml_baseline['classification']['balanced_acc']['mean']:.3f}±{ml_baseline['classification']['balanced_acc']['std']:.3f} | {ml_baseline['classification']['precision']['mean']:.3f}±{ml_baseline['classification']['precision']['std']:.3f} | {ml_baseline['classification']['recall']['mean']:.3f}±{ml_baseline['classification']['recall']['std']:.3f} | {ml_baseline['classification']['specificity']['mean']:.3f}±{ml_baseline['classification']['specificity']['std']:.3f} |",
        f"| Zero-shot | {zero_shot['classification']['f1']['mean']:.3f}±{zero_shot['classification']['f1']['std']:.3f} | {zero_shot['classification']['balanced_acc']['mean']:.3f}±{zero_shot['classification']['balanced_acc']['std']:.3f} | {zero_shot['classification']['precision']['mean']:.3f}±{zero_shot['classification']['precision']['std']:.3f} | {zero_shot['classification']['recall']['mean']:.3f}±{zero_shot['classification']['recall']['std']:.3f} | {zero_shot['classification']['specificity']['mean']:.3f}±{zero_shot['classification']['specificity']['std']:.3f} |",
    ]
    (experiment_root / 'summary.md').write_text('\n'.join(md) + '\n', encoding='utf-8')

    save_plot(ml_baseline, zero_shot, experiment_root / 'zero_shot_vs_best_ml.png')
    print(f'Saved zero-shot comparison to: {experiment_root}')


if __name__ == '__main__':
    main()

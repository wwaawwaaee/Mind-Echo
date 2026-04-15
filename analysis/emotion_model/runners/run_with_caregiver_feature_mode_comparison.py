#!/usr/bin/env python3
"""Compare feature engineering modes within the with_caregiver subgroup."""

import json
import subprocess
import sys
from pathlib import Path


def run_command(command, workdir):
    result = subprocess.run(command, cwd=workdir, text=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(command)}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}")
    return result.stdout


def load_json(path: Path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def main():
    script_dir = Path(__file__).resolve().parent
    experiment_root = script_dir.parent
    analysis_dir = experiment_root.parent
    out_root = experiment_root / 'experiments' / 'with_caregiver_feature_mode_comparison'
    out_root.mkdir(exist_ok=True)

    data_dir = out_root / 'data'
    run_command([
        sys.executable,
        str(experiment_root / 'core' / '01_prepare_data.py'),
        '--datasets',
        'with_caregiver',
        '--output-dir',
        str(data_dir),
        '--max-role-turns',
        '10',
    ], script_dir)

    summary = []
    for feature_mode in ['text_only', 'liwc_only', 'hybrid']:
        result_dir = out_root / feature_mode / 'results'
        model_dir = out_root / feature_mode / 'models'
        run_command([
            sys.executable,
            str(experiment_root / 'core' / '02_train_model.py'),
            '--data-dir',
            str(data_dir),
            '--results-dir',
            str(result_dir),
            '--models-dir',
            str(model_dir),
            '--text-field',
            'full_text',
            '--classification-target',
            'anxiety',
            '--feature-mode',
            feature_mode,
        ], analysis_dir)

        results = load_json(result_dir / 'training_results.json')
        cls = results['test_results']['anxiety']
        summary.append({
            'feature_mode': feature_mode,
            'acc': cls['acc'],
            'f1': cls['f1'],
            'precision': cls['precision'],
            'recall': cls['recall'],
            'specificity': cls['specificity'],
            'balanced_acc': cls['balanced_acc'],
            'confusion_matrix': cls['confusion_matrix'],
            'gad_corr': results['test_results']['gad_corr'],
            'gad_mse': results['test_results']['gad_mse'],
            'phq_corr': results['test_results']['phq_corr'],
            'phq_mse': results['test_results']['phq_mse'],
        })

    payload = {
        'group': 'with_caregiver',
        'text_field': 'full_text',
        'classification_target': 'anxiety',
        'comparison': summary,
    }
    with open(out_root / 'summary.json', 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    lines = [
        '# With-caregiver Feature Mode Comparison',
        '',
        '| Feature Mode | ACC | F1 | Precision | Recall | Specificity | Balanced ACC | GAD r | PHQ r | Confusion Matrix |',
        '|---|---:|---:|---:|---:|---:|---:|---:|---:|---|',
    ]
    for row in summary:
        lines.append(
            f"| {row['feature_mode']} | {row['acc']:.3f} | {row['f1']:.3f} | {row['precision']:.3f} | {row['recall']:.3f} | {row['specificity']:.3f} | {row['balanced_acc']:.3f} | {row['gad_corr']:.3f} | {row['phq_corr']:.3f} | {row['confusion_matrix']} |"
        )

    with open(out_root / 'summary.md', 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')

    print(f'Saved feature-mode comparison to: {out_root}')


if __name__ == '__main__':
    main()

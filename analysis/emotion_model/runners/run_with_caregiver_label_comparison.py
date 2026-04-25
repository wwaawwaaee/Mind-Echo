#!/usr/bin/env python3
"""Run with_caregiver classification-target comparison experiments."""

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
    out_root = experiment_root / 'experiments' / 'with_caregiver_label_comparison'
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
    for target in ['combined', 'anxiety', 'depression']:
        result_dir = out_root / target / 'results'
        model_dir = out_root / target / 'models'
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
            'early_text',
            '--classification-target',
            target,
        ], analysis_dir)

        results = load_json(result_dir / 'training_results.json')
        cls = results['test_results'][target]
        summary.append({
            'target': target,
            'acc': cls['acc'],
            'f1': cls['f1'],
            'precision': cls['precision'],
            'recall': cls['recall'],
            'specificity': cls['specificity'],
            'balanced_acc': cls['balanced_acc'],
            'confusion_matrix': cls['confusion_matrix'],
            'baseline_acc': results['baseline_results']['acc'],
            'baseline_f1': results['baseline_results']['f1'],
        })

    payload = {'group': 'with_caregiver', 'text_field': 'early_text', 'comparison': summary}
    with open(out_root / 'summary.json', 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    lines = [
        '# With-caregiver Label Comparison',
        '',
        '| Target | ACC | F1 | Precision | Recall | Specificity | Balanced ACC | Confusion Matrix |',
        '|---|---:|---:|---:|---:|---:|---:|---|',
    ]
    for row in summary:
        lines.append(
            f"| {row['target']} | {row['acc']:.3f} | {row['f1']:.3f} | {row['precision']:.3f} | {row['recall']:.3f} | {row['specificity']:.3f} | {row['balanced_acc']:.3f} | {row['confusion_matrix']} |"
        )

    with open(out_root / 'summary.md', 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')

    print(f'Saved label comparison to: {out_root}')


if __name__ == '__main__':
    main()

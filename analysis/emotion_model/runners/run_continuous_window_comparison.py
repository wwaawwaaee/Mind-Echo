#!/usr/bin/env python3
"""Run continuous early-text window comparison experiments."""

import argparse
import json
import subprocess
import sys
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description='Run continuous early-text window comparison experiments')
    parser.add_argument('--windows', nargs='+', type=int, default=[4, 6, 8, 10, 12, 15], help='Target-role turn windows to compare')
    parser.add_argument('--max-chars', type=int, default=1000, help='Max chars per early_text sample')
    parser.add_argument('--min-chars', type=int, default=30, help='Min chars per early_text sample')
    parser.add_argument('--datasets', nargs='+', choices=['with_caregiver', 'without_caregiver'], default=['with_caregiver', 'without_caregiver'], help='Datasets to include')
    return parser.parse_args()


def run_command(command, workdir):
    result = subprocess.run(command, cwd=workdir, text=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(command)}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}")
    return result.stdout


def load_json(path: Path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def main():
    args = parse_args()

    script_dir = Path(__file__).resolve().parent
    experiment_root = script_dir.parent
    analysis_dir = experiment_root.parent
    comparison_root = experiment_root / 'experiments' / 'window_comparison'
    comparison_root.mkdir(exist_ok=True)

    summary_rows = []

    for window in args.windows:
        exp_dir = comparison_root / f'window_{window:02d}'
        data_dir = exp_dir / 'data'
        results_dir = exp_dir / 'results'
        models_dir = exp_dir / 'models'
        exp_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n{'=' * 70}\nRunning window={window}\n{'=' * 70}")

        prepare_cmd = [
            sys.executable,
            str(experiment_root / 'core' / '01_prepare_data.py'),
            '--datasets',
            *args.datasets,
            '--output-dir',
            str(data_dir),
            '--max-role-turns',
            str(window),
            '--max-chars',
            str(args.max_chars),
            '--min-chars',
            str(args.min_chars),
        ]
        run_command(prepare_cmd, script_dir)

        train_cmd = [
            sys.executable,
            str(experiment_root / 'core' / '02_train_model.py'),
            '--data-dir',
            str(data_dir),
            '--results-dir',
            str(results_dir),
            '--models-dir',
            str(models_dir),
        ]
        run_command(train_cmd, analysis_dir)

        metadata = load_json(data_dir / 'metadata.json')
        results = load_json(results_dir / 'training_results.json')

        summary_rows.append({
            'window': window,
            'samples': metadata['sample_count'],
            'patients': metadata['patient_count'],
            'acc': results['test_results']['combined']['acc'],
            'f1': results['test_results']['combined']['f1'],
            'gad_corr': results['test_results']['gad_corr'],
            'gad_mse': results['test_results']['gad_mse'],
            'phq_corr': results['test_results']['phq_corr'],
            'phq_mse': results['test_results']['phq_mse'],
            'baseline_acc': results['baseline_results']['acc'],
            'baseline_f1': results['baseline_results']['f1'],
            'best_cls_model': results['best_classification_model'],
            'best_gad_model': results['best_gad_model'],
            'best_phq_model': results['best_phq_model'],
        })

    summary = {
        'datasets': args.datasets,
        'windows': args.windows,
        'max_chars': args.max_chars,
        'min_chars': args.min_chars,
        'results': summary_rows,
    }

    with open(comparison_root / 'window_comparison_summary.json', 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    lines = [
        '# Continuous Early-Text Window Comparison',
        '',
        '| Window | Samples | Patients | ACC | F1 | GAD r | GAD MSE | PHQ r | PHQ MSE |',
        '|---|---:|---:|---:|---:|---:|---:|---:|---:|',
    ]
    for row in summary_rows:
        lines.append(
            f"| {row['window']} | {row['samples']} | {row['patients']} | {row['acc']:.3f} | {row['f1']:.3f} | {row['gad_corr']:.3f} | {row['gad_mse']:.2f} | {row['phq_corr']:.3f} | {row['phq_mse']:.2f} |"
        )

    with open(comparison_root / 'window_comparison_summary.md', 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')

    print(f"\nSaved summary to: {comparison_root / 'window_comparison_summary.json'}")
    print(f"Saved markdown to: {comparison_root / 'window_comparison_summary.md'}")


if __name__ == '__main__':
    main()

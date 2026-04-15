#!/usr/bin/env python3
"""Compare early_text vs full_text within the with_caregiver subgroup."""

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
    out_root = experiment_root / 'experiments' / 'with_caregiver_textfield_comparison'
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
    for text_field in ['early_text', 'full_text']:
        result_dir = out_root / text_field / 'results'
        model_dir = out_root / text_field / 'models'
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
            text_field,
        ], analysis_dir)

        results = load_json(result_dir / 'training_results.json')
        summary.append({
            'text_field': text_field,
            'acc': results['test_results']['combined']['acc'],
            'f1': results['test_results']['combined']['f1'],
            'gad_corr': results['test_results']['gad_corr'],
            'gad_mse': results['test_results']['gad_mse'],
            'phq_corr': results['test_results']['phq_corr'],
            'phq_mse': results['test_results']['phq_mse'],
        })

    payload = {'group': 'with_caregiver', 'comparison': summary}
    with open(out_root / 'summary.json', 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    md_lines = [
        '# With-caregiver Text Field Comparison',
        '',
        '| Text Field | ACC | F1 | GAD r | GAD MSE | PHQ r | PHQ MSE |',
        '|---|---:|---:|---:|---:|---:|---:|',
    ]
    for row in summary:
        md_lines.append(
            f"| {row['text_field']} | {row['acc']:.3f} | {row['f1']:.3f} | {row['gad_corr']:.3f} | {row['gad_mse']:.2f} | {row['phq_corr']:.3f} | {row['phq_mse']:.2f} |"
        )

    with open(out_root / 'summary.md', 'w', encoding='utf-8') as f:
        f.write('\n'.join(md_lines) + '\n')

    print(f'Saved text-field comparison to: {out_root}')


if __name__ == '__main__':
    main()

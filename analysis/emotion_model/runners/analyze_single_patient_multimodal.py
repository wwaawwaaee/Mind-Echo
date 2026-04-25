#!/usr/bin/env python3
"""Single-patient multimodal ablation framework with optional audio input."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

CURRENT_DIR = Path(__file__).resolve().parent
EXPERIMENT_ROOT = CURRENT_DIR.parent
CORE_DIR = EXPERIMENT_ROOT / 'core'
ANALYSIS_DIR = EXPERIMENT_ROOT.parent
if str(CORE_DIR) not in sys.path:
    sys.path.append(str(CORE_DIR))
if str(ANALYSIS_DIR) not in sys.path:
    sys.path.append(str(ANALYSIS_DIR))

from audio_features import AUDIO_FEATURE_KEYS, extract_audio_features
from domain_rule_features import DOMAIN_FEATURE_KEYS, extract_domain_rule_features
from liwc_analyzer import LIWCAnalyzer


LIWC_SUMMARY_KEYS = [
    'anxiety_index', 'depression_index', 'positive_affect', 'negative_affect',
    'cognitive_complexity', 'health_focus', 'social_reference', 'pronoun_density',
]


def parse_args():
    parser = argparse.ArgumentParser(description='Analyze one patient/visit with optional audio and multimodal ablation')
    parser.add_argument('--patient-id', required=True, help='Patient ID, e.g. P-000003')
    parser.add_argument('--visit-id', default=None, help='Optional visit ID, e.g. V-000003-1')
    parser.add_argument('--data-path', default=str(EXPERIMENT_ROOT / 'experiments' / 'with_caregiver_label_comparison' / 'data' / 'all_data.json'))
    parser.add_argument('--audio-root', default=str(EXPERIMENT_ROOT / 'inputs' / 'audio'))
    parser.add_argument('--output-root', default=str(EXPERIMENT_ROOT / 'experiments' / 'single_patient_multimodal'))
    parser.add_argument('--liwc-dict', default='D:\\vscode-project\\git-project\\Auto_CLIWC\\Auto_CLIWC\\datasets\\sc_liwc.dic')
    return parser.parse_args()


def load_records(data_path: str | Path) -> list[dict]:
    with open(data_path, encoding='utf-8') as f:
        return json.load(f)


def find_visit(records: list[dict], patient_id: str, visit_id: str | None) -> dict:
    matches = [r for r in records if r['patient_id'] == patient_id]
    if not matches:
        raise ValueError(f'Patient not found: {patient_id}')
    if visit_id is None:
        return matches[0]
    for record in matches:
        if record['visit_id'] == visit_id:
            return record
    raise ValueError(f'Visit not found for {patient_id}: {visit_id}')


def resolve_audio_path(audio_root: Path, patient_id: str, visit_id: str) -> Path | None:
    candidates = [
        audio_root / patient_id / f'{visit_id}.wav',
        audio_root / patient_id / f'{visit_id}_caregiver.wav',
        audio_root / f'{patient_id}_{visit_id}.wav',
        audio_root / f'{visit_id}.wav',
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def extract_text_features(text: str, liwc_dict: str) -> dict:
    analyzer = LIWCAnalyzer(liwc_dict)
    liwc_full = analyzer.extract_features(text)
    liwc_full.update(analyzer.extract_category_ratios(liwc_full))
    liwc_full['pronoun_density'] = analyzer.extract_pronoun_density(text)
    liwc_summary = analyzer.get_key_indicators(liwc_full)
    liwc_summary['pronoun_density'] = liwc_full['pronoun_density']
    domain = extract_domain_rule_features(text)
    return {
        'liwc_summary': {k: float(liwc_summary.get(k, 0.0)) for k in LIWC_SUMMARY_KEYS},
        'domain_summary': {k: float(domain.get(k, 0.0)) for k in DOMAIN_FEATURE_KEYS},
    }


def build_ablation_payload(record: dict, audio_path: Path | None, liwc_dict: str) -> dict:
    text = record['full_text']
    text_features = extract_text_features(text, liwc_dict)
    audio_features = extract_audio_features(audio_path) if audio_path else None

    available_modalities = ['text'] + (['audio'] if audio_features else [])
    ablations = {
        'text_only': {
            'available': True,
            'features': text_features,
        },
        'audio_only': {
            'available': audio_features is not None,
            'features': audio_features,
        },
        'multimodal': {
            'available': audio_features is not None,
            'features': {
                'text': text_features,
                'audio': audio_features,
            } if audio_features is not None else None,
        },
    }

    return {
        'patient_id': record['patient_id'],
        'visit_id': record['visit_id'],
        'labels': {
            'gad_score': record['gad_score'],
            'phq_score': record['phq_score'],
            'anxiety_label': record['anxiety_label'],
            'depression_label': record['depression_label'],
            'combined_label': record['combined_label'],
        },
        'text_length': len(text),
        'target_role_turns': record.get('target_role_turns'),
        'dialogue_turns': record.get('dialogue_turns'),
        'audio_path': str(audio_path) if audio_path else None,
        'available_modalities': available_modalities,
        'ablations': ablations,
    }


def make_plot(report: dict, output_path: Path):
    text_liwc = report['ablations']['text_only']['features']['liwc_summary']
    text_domain = report['ablations']['text_only']['features']['domain_summary']
    audio_feats = report['ablations']['audio_only']['features']

    fig, axes = plt.subplots(1, 3 if audio_feats else 2, figsize=(16 if audio_feats else 11, 5))
    if not isinstance(axes, np.ndarray):
        axes = np.array([axes])

    liwc_keys = ['anxiety_index', 'depression_index', 'negative_affect', 'health_focus', 'pronoun_density']
    axes[0].bar(liwc_keys, [text_liwc.get(k, 0.0) for k in liwc_keys], color='#4c78a8')
    axes[0].set_title('LIWC Summary (Text)')
    axes[0].tick_params(axis='x', rotation=20)

    domain_keys = ['absolute_density', 'symptom_density', 'symptom_amplification_density', 'reassurance_density', 'question_density']
    axes[1].bar(domain_keys, [text_domain.get(k, 0.0) for k in domain_keys], color='#f58518')
    axes[1].set_title('Domain-rule Summary (Text)')
    axes[1].tick_params(axis='x', rotation=20)

    if audio_feats:
        audio_keys = ['duration_sec', 'rms_mean', 'zero_crossing_rate', 'silence_ratio', 'spectral_centroid_hz']
        axes[2].bar(audio_keys, [audio_feats.get(k, 0.0) for k in audio_keys], color='#54a24b')
        axes[2].set_title('Audio Summary')
        axes[2].tick_params(axis='x', rotation=20)

    fig.suptitle(f"Single-patient multimodal ablation: {report['patient_id']} / {report['visit_id']}", fontsize=14, fontweight='bold')
    fig.text(0.5, 0.01, f"Available modalities: {', '.join(report['available_modalities'])}", ha='center', fontsize=10)
    plt.tight_layout(rect=[0, 0.04, 1, 0.95])
    plt.savefig(output_path, dpi=180, bbox_inches='tight')
    plt.close()


def main():
    args = parse_args()
    records = load_records(args.data_path)
    record = find_visit(records, args.patient_id, args.visit_id)
    audio_root = Path(args.audio_root)
    audio_path = resolve_audio_path(audio_root, record['patient_id'], record['visit_id'])

    report = build_ablation_payload(record, audio_path, args.liwc_dict)

    output_root = Path(args.output_root) / record['patient_id'] / record['visit_id']
    output_root.mkdir(parents=True, exist_ok=True)
    report_path = output_root / 'multimodal_ablation_report.json'
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    plot_path = output_root / 'multimodal_ablation_summary.png'
    make_plot(report, plot_path)

    md_lines = [
        f"# Single-patient multimodal analysis: {record['patient_id']} / {record['visit_id']}",
        '',
        f"- Available modalities: {', '.join(report['available_modalities'])}",
        f"- Audio file: {report['audio_path'] or 'not found (text-only fallback)'}",
        f"- GAD-7: {report['labels']['gad_score']}",
        f"- PHQ-9: {report['labels']['phq_score']}",
        '',
        '## Ablation status',
        '',
        f"- text_only: {report['ablations']['text_only']['available']}",
        f"- audio_only: {report['ablations']['audio_only']['available']}",
        f"- multimodal: {report['ablations']['multimodal']['available']}",
        '',
        '## Notes',
        '',
        '- This framework is ready for WAV input. If audio is absent, it falls back to text-only analysis.',
        '- Audio naming convention is described in `inputs/audio/README_audio_import.md`.',
    ]
    (output_root / 'README.md').write_text('\n'.join(md_lines) + '\n', encoding='utf-8')

    print(f'Saved single-patient multimodal report to: {output_root}')


if __name__ == '__main__':
    main()

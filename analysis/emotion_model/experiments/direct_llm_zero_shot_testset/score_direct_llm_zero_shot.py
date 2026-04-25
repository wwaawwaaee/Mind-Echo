import json
from pathlib import Path

from sklearn.metrics import accuracy_score, balanced_accuracy_score, confusion_matrix, f1_score, precision_score, recall_score


BASE = Path(__file__).resolve().parent
TRUTH_PATH = BASE.parent / 'with_caregiver_label_comparison' / 'data' / 'test.json'
PRED_PATH = BASE / 'llm_direct_predictions.json'


def metrics(y_true, y_pred):
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


def main():
    truth = json.loads(TRUTH_PATH.read_text(encoding='utf-8'))
    preds = json.loads(PRED_PATH.read_text(encoding='utf-8'))['predictions']

    truth_map = {(item['patient_id'], item['visit_id']): item for item in truth}
    merged = []
    for pred in preds:
        gold = truth_map[(pred['patient_id'], pred['visit_id'])]
        merged.append({
            **pred,
            'true_anxiety': gold['anxiety_label'],
            'true_depression': gold['depression_label'],
        })

    result = {
        'method': 'direct_current_model_zero_shot_manual_judgment',
        'dataset': 'with_caregiver_test',
        'n_samples': len(merged),
        'anxiety': metrics([m['true_anxiety'] for m in merged], [m['pred_anxiety'] for m in merged]),
        'depression': metrics([m['true_depression'] for m in merged], [m['pred_depression'] for m in merged]),
        'predictions': merged,
    }

    (BASE / 'summary.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')

    lines = [
        '# Direct current-model zero-shot on with_caregiver test set',
        '',
        'This is a single-pass direct LLM judgment on the held-out test set, not a trained classifier.',
        '',
        '## Anxiety',
        '',
        '| ACC | F1 | Precision | Recall | Specificity | Balanced ACC | Confusion Matrix |',
        '|---:|---:|---:|---:|---:|---:|---|',
        f"| {result['anxiety']['acc']:.3f} | {result['anxiety']['f1']:.3f} | {result['anxiety']['precision']:.3f} | {result['anxiety']['recall']:.3f} | {result['anxiety']['specificity']:.3f} | {result['anxiety']['balanced_acc']:.3f} | {result['anxiety']['confusion_matrix']} |",
        '',
        '## Depression',
        '',
        '| ACC | F1 | Precision | Recall | Specificity | Balanced ACC | Confusion Matrix |',
        '|---:|---:|---:|---:|---:|---:|---|',
        f"| {result['depression']['acc']:.3f} | {result['depression']['f1']:.3f} | {result['depression']['precision']:.3f} | {result['depression']['recall']:.3f} | {result['depression']['specificity']:.3f} | {result['depression']['balanced_acc']:.3f} | {result['depression']['confusion_matrix']} |",
    ]
    (BASE / 'summary.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    print(json.dumps({'anxiety': result['anxiety'], 'depression': result['depression']}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

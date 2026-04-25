#!/usr/bin/env python3
"""Feature Engineering V2 with patient-level cross-validation for with_caregiver."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
from scipy.sparse import csr_matrix, hstack
from scipy.stats import pearsonr
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import accuracy_score, balanced_accuracy_score, confusion_matrix, f1_score, mean_squared_error, precision_score, recall_score
from sklearn.model_selection import GroupKFold
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

CURRENT_DIR = Path(__file__).resolve().parent
EXPERIMENT_ROOT = CURRENT_DIR.parent
ANALYSIS_DIR = EXPERIMENT_ROOT.parent
CORE_DIR = EXPERIMENT_ROOT / 'core'
if str(CORE_DIR) not in sys.path:
    sys.path.append(str(CORE_DIR))
if str(ANALYSIS_DIR) not in sys.path:
    sys.path.append(str(ANALYSIS_DIR))

from domain_rule_features import DOMAIN_FEATURE_KEYS, extract_domain_rule_features
from liwc_analyzer import LIWCAnalyzer


LIWC_FEATURE_KEYS = [
    'negemo', 'posemo', 'anx', 'sad', 'anger', 'health', 'humans', 'insight', 'cause', 'body',
    'family', 'funct', 'negate', 'quant', 'number', 'PastM', 'PresentM', 'FutureM', 'certain',
    'tentat', 'discrep', 'inhib', 'friend', 'see', 'hear', 'feel', 'motion', 'space', 'time',
    'affect_ratio', 'cogmech_ratio', 'bio_ratio', 'social_ratio', 'anxiety_index', 'depression_index',
    'positive_affect', 'negative_affect', 'pronoun_density', 'cognitive_complexity', 'health_focus',
    'social_reference', '_match_ratio'
]


def load_dataset() -> list[dict]:
    data_path = EXPERIMENT_ROOT / 'experiments' / 'with_caregiver_label_comparison' / 'data' / 'all_data.json'
    with open(data_path, encoding='utf-8') as f:
        return json.load(f)


def build_liwc_matrix(texts: list[str], analyzer: LIWCAnalyzer) -> np.ndarray:
    rows = []
    for text in texts:
        features = analyzer.extract_features(text)
        features.update(analyzer.extract_category_ratios(features))
        features['pronoun_density'] = analyzer.extract_pronoun_density(text)
        features.update(analyzer.get_key_indicators(features))
        rows.append([features.get(key, 0.0) for key in LIWC_FEATURE_KEYS])
    return np.array(rows, dtype=float)


def build_domain_matrix(texts: list[str]) -> np.ndarray:
    rows = []
    for text in texts:
        feats = extract_domain_rule_features(text)
        rows.append([feats.get(key, 0.0) for key in DOMAIN_FEATURE_KEYS])
    return np.array(rows, dtype=float)


def safe_corr(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    if len(y_true) < 2:
        return 0.0
    corr, _ = pearsonr(y_true, y_pred)
    return 0.0 if np.isnan(corr) else float(corr)


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
        summary[key] = {
            'mean': float(np.mean(values)),
            'std': float(np.std(values)),
        }
    return summary


def run_feature_mode(data: list[dict], text_field: str, feature_mode: str, target: str, liwc_dict: str) -> dict:
    texts = [item[text_field] for item in data]
    groups = np.array([item['patient_id'] for item in data])
    y_cls = np.array([item[f'{target}_label'] for item in data], dtype=int)
    y_gad = np.array([item['gad_score'] for item in data], dtype=float)
    y_phq = np.array([item['phq_score'] for item in data], dtype=float)

    liwc_analyzer = LIWCAnalyzer(liwc_dict)
    gkf = GroupKFold(n_splits=5)

    cls_metrics = []
    gad_corrs, gad_mses = [], []
    phq_corrs, phq_mses = [], []

    for train_idx, test_idx in gkf.split(texts, y_cls, groups):
        train_texts = [texts[i] for i in train_idx]
        test_texts = [texts[i] for i in test_idx]

        X_parts_train = []
        X_parts_test = []

        if feature_mode in {'tfidf_svd', 'hybrid_v2'}:
            vectorizer = TfidfVectorizer(max_features=1000, ngram_range=(1, 2), min_df=2, max_df=0.95, sublinear_tf=True)
            X_train_tfidf = vectorizer.fit_transform(train_texts)
            X_test_tfidf = vectorizer.transform(test_texts)
            svd_components = min(80, max(2, X_train_tfidf.shape[1] - 1))
            svd = TruncatedSVD(n_components=svd_components, random_state=42)
            X_train_svd = svd.fit_transform(X_train_tfidf)
            X_test_svd = svd.transform(X_test_tfidf)
            scaler_text = StandardScaler()
            X_parts_train.append(csr_matrix(scaler_text.fit_transform(X_train_svd)))
            X_parts_test.append(csr_matrix(scaler_text.transform(X_test_svd)))

        if feature_mode in {'liwc_only', 'hybrid_v2'}:
            X_train_liwc = build_liwc_matrix(train_texts, liwc_analyzer)
            X_test_liwc = build_liwc_matrix(test_texts, liwc_analyzer)
            scaler_liwc = StandardScaler()
            X_parts_train.append(csr_matrix(scaler_liwc.fit_transform(X_train_liwc)))
            X_parts_test.append(csr_matrix(scaler_liwc.transform(X_test_liwc)))

        if feature_mode in {'domain_only', 'hybrid_v2'}:
            X_train_domain = build_domain_matrix(train_texts)
            X_test_domain = build_domain_matrix(test_texts)
            scaler_domain = StandardScaler()
            X_parts_train.append(csr_matrix(scaler_domain.fit_transform(X_train_domain)))
            X_parts_test.append(csr_matrix(scaler_domain.transform(X_test_domain)))

        X_train = hstack(X_parts_train).tocsr() if len(X_parts_train) > 1 else X_parts_train[0]
        X_test = hstack(X_parts_test).tocsr() if len(X_parts_test) > 1 else X_parts_test[0]

        cls_model = LogisticRegression(max_iter=2000, class_weight='balanced', random_state=42)
        cls_model.fit(X_train, y_cls[train_idx])
        cls_pred = cls_model.predict(X_test)
        cls_metrics.append(evaluate_classification(y_cls[test_idx], cls_pred))

        gad_model = Ridge(alpha=1.0)
        gad_model.fit(X_train, y_gad[train_idx])
        gad_pred = gad_model.predict(X_test)
        gad_corrs.append(safe_corr(y_gad[test_idx], gad_pred))
        gad_mses.append(float(mean_squared_error(y_gad[test_idx], gad_pred)))

        phq_model = Ridge(alpha=1.0)
        phq_model.fit(X_train, y_phq[train_idx])
        phq_pred = phq_model.predict(X_test)
        phq_corrs.append(safe_corr(y_phq[test_idx], phq_pred))
        phq_mses.append(float(mean_squared_error(y_phq[test_idx], phq_pred)))

    return {
        'feature_mode': feature_mode,
        'classification': summarize_metric_dicts(cls_metrics),
        'gad_corr': {'mean': float(np.mean(gad_corrs)), 'std': float(np.std(gad_corrs))},
        'gad_mse': {'mean': float(np.mean(gad_mses)), 'std': float(np.std(gad_mses))},
        'phq_corr': {'mean': float(np.mean(phq_corrs)), 'std': float(np.std(phq_corrs))},
        'phq_mse': {'mean': float(np.mean(phq_mses)), 'std': float(np.std(phq_mses))},
    }


def main():
    data = load_dataset()
    out_dir = EXPERIMENT_ROOT / 'experiments' / 'with_caregiver_feature_v2_cv'
    out_dir.mkdir(exist_ok=True)
    liwc_dict = r'D:\vscode-project\git-project\Auto_CLIWC\Auto_CLIWC\datasets\sc_liwc.dic'

    all_targets = {}
    for target in ['anxiety', 'depression']:
        comparison = []
        for feature_mode in ['tfidf_svd', 'liwc_only', 'domain_only', 'hybrid_v2']:
            comparison.append(run_feature_mode(data, text_field='full_text', feature_mode=feature_mode, target=target, liwc_dict=liwc_dict))
        all_targets[target] = comparison

    payload = {
        'group': 'with_caregiver',
        'evaluation': 'patient_level_5fold_cv',
        'text_field': 'full_text',
        'targets': all_targets,
    }

    with open(out_dir / 'summary.json', 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    lines = ['# With-caregiver Feature Engineering V2 (Patient-level 5-fold CV)', '']
    for target, comparison in all_targets.items():
        lines.extend([
            f'## Target: {target}',
            '',
            '| Feature Mode | ACC | F1 | Precision | Recall | Specificity | Balanced ACC | GAD r | PHQ r |',
            '|---|---:|---:|---:|---:|---:|---:|---:|---:|',
        ])
        for row in comparison:
            cls = row['classification']
            lines.append(
                f"| {row['feature_mode']} | {cls['acc']['mean']:.3f}±{cls['acc']['std']:.3f} | {cls['f1']['mean']:.3f}±{cls['f1']['std']:.3f} | {cls['precision']['mean']:.3f}±{cls['precision']['std']:.3f} | {cls['recall']['mean']:.3f}±{cls['recall']['std']:.3f} | {cls['specificity']['mean']:.3f}±{cls['specificity']['std']:.3f} | {cls['balanced_acc']['mean']:.3f}±{cls['balanced_acc']['std']:.3f} | {row['gad_corr']['mean']:.3f}±{row['gad_corr']['std']:.3f} | {row['phq_corr']['mean']:.3f}±{row['phq_corr']['std']:.3f} |"
            )
        lines.append('')

    (out_dir / 'summary.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    print(f'Saved Feature Engineering V2 summary to: {out_dir}')


if __name__ == '__main__':
    main()

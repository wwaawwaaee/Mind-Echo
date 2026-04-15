#!/usr/bin/env python3
"""
模型训练脚本
使用TF-IDF + 传统ML模型进行情绪分类和量表回归
"""

import json
import argparse
import os
import sys
from pathlib import Path
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import Ridge, LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.svm import SVC
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import cross_val_score, GridSearchCV
from sklearn.metrics import accuracy_score, balanced_accuracy_score, confusion_matrix, f1_score, mean_squared_error, precision_score, recall_score, r2_score
from scipy.stats import pearsonr
from scipy.sparse import csr_matrix, hstack
import pickle
import warnings
warnings.filterwarnings('ignore')

CURRENT_DIR = Path(__file__).resolve().parent
EXPERIMENT_ROOT = CURRENT_DIR.parent
ANALYSIS_DIR = EXPERIMENT_ROOT.parent
if str(ANALYSIS_DIR) not in sys.path:
    sys.path.append(str(ANALYSIS_DIR))
if str(CURRENT_DIR) not in sys.path:
    sys.path.append(str(CURRENT_DIR))

from liwc_analyzer import LIWCAnalyzer


LIWC_FEATURE_KEYS = [
    'negemo', 'posemo', 'anx', 'sad', 'anger',
    'health', 'humans', 'insight', 'cause', 'body', 'family',
    'funct', 'negate', 'quant', 'number',
    'PastM', 'PresentM', 'FutureM',
    'certain', 'tentat', 'discrep', 'inhib', 'friend',
    'see', 'hear', 'feel', 'motion', 'space', 'time',
    'affect_ratio', 'cogmech_ratio', 'bio_ratio', 'social_ratio',
    'anxiety_index', 'depression_index', 'positive_affect', 'negative_affect',
    'pronoun_density', 'cognitive_complexity', 'health_focus', 'social_reference',
    '_match_ratio'
]


def parse_args():
    parser = argparse.ArgumentParser(description='Train TF-IDF baselines for early-text data')
    parser.add_argument('--data-dir', default=str(EXPERIMENT_ROOT / 'outputs' / 'default' / 'data'), help='Prepared data directory')
    parser.add_argument('--results-dir', default=str(EXPERIMENT_ROOT / 'outputs' / 'default' / 'results'), help='Output results directory')
    parser.add_argument('--models-dir', default=str(EXPERIMENT_ROOT / 'outputs' / 'default' / 'models'), help='Output models directory')
    parser.add_argument('--text-field', default='early_text', choices=['early_text', 'full_text'], help='Text field to vectorize')
    parser.add_argument('--classification-target', default='combined', choices=['combined', 'anxiety', 'depression'], help='Binary classification target')
    parser.add_argument('--feature-mode', default='text_only', choices=['text_only', 'liwc_only', 'hybrid'], help='Feature engineering mode')
    parser.add_argument('--liwc-dict', default='D:\\vscode-project\\git-project\\Auto_CLIWC\\Auto_CLIWC\\datasets\\sc_liwc.dic', help='LIWC dictionary path for liwc/hybrid features')
    return parser.parse_args()


def load_data(base_path):
    """加载训练数据"""
    with open(f'{base_path}/train.json', encoding='utf-8') as f:
        train_data = json.load(f)
    with open(f'{base_path}/val.json', encoding='utf-8') as f:
        val_data = json.load(f)
    with open(f'{base_path}/test.json', encoding='utf-8') as f:
        test_data = json.load(f)
    
    return train_data, val_data, test_data


def prepare_text_features(data_list, text_field='early_text', vectorizer=None, fit=True):
    """准备TF-IDF文本特征"""
    texts = [d.get(text_field, '')[:1000] for d in data_list]  # 限制文本长度
    
    if fit:
        vectorizer = TfidfVectorizer(
            max_features=500,
            ngram_range=(1, 2),
            min_df=2,
            max_df=0.95,
            sublinear_tf=True
        )
        X = vectorizer.fit_transform(texts)
    else:
        X = vectorizer.transform(texts)
    
    return X, vectorizer


def compute_liwc_features(text, analyzer):
    features = analyzer.extract_features(text)
    features.update(analyzer.extract_category_ratios(features))
    features['pronoun_density'] = analyzer.extract_pronoun_density(text)
    features.update(analyzer.get_key_indicators(features))
    return np.array([features.get(key, 0.0) for key in LIWC_FEATURE_KEYS], dtype=float)


def prepare_liwc_features(data_list, analyzer, text_field='early_text', scaler=None, fit=True):
    rows = []
    for item in data_list:
        text = item.get(text_field, '')[:1000]
        rows.append(compute_liwc_features(text, analyzer))

    X_dense = np.vstack(rows) if rows else np.zeros((0, len(LIWC_FEATURE_KEYS)))

    if fit:
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_dense)
    else:
        X_scaled = scaler.transform(X_dense)

    return csr_matrix(X_scaled), scaler


def prepare_features(data_list, feature_mode='text_only', text_field='early_text', vectorizer=None, liwc_analyzer=None, scaler=None, fit=True):
    """准备特征：text_only / liwc_only / hybrid。"""
    if feature_mode == 'text_only':
        X, vectorizer = prepare_text_features(data_list, text_field=text_field, vectorizer=vectorizer, fit=fit)
        return X, vectorizer, scaler

    if feature_mode == 'liwc_only':
        X, scaler = prepare_liwc_features(data_list, analyzer=liwc_analyzer, text_field=text_field, scaler=scaler, fit=fit)
        return X, vectorizer, scaler

    X_text, vectorizer = prepare_text_features(data_list, text_field=text_field, vectorizer=vectorizer, fit=fit)
    X_liwc, scaler = prepare_liwc_features(data_list, analyzer=liwc_analyzer, text_field=text_field, scaler=scaler, fit=fit)
    return hstack([X_text, X_liwc]).tocsr(), vectorizer, scaler


def prepare_labels(data_list):
    """准备标签"""
    gad_scores = np.array([d['gad_score'] for d in data_list])
    phq_scores = np.array([d['phq_score'] for d in data_list])
    anxiety_labels = np.array([d['anxiety_label'] for d in data_list])
    depression_labels = np.array([d['depression_label'] for d in data_list])
    combined_labels = np.array([d['combined_label'] for d in data_list])
    
    return {
        'gad': gad_scores,
        'phq': phq_scores,
        'anxiety': anxiety_labels,
        'depression': depression_labels,
        'combined': combined_labels
    }


def train_baseline_model(X_train, y_train, model_type='random'):
    """
    训练基线模型 (未训练/随机猜测)
    用于对比
    """
    class BaselineClassifier:
        def __init__(self, strategy='random'):
            self.strategy = strategy
            self.class_ratio = None
        
        def fit(self, X, y):
            self.class_ratio = np.bincount(y) / len(y)
            return self
        
        def predict(self, X):
            n = X.shape[0]
            if self.strategy == 'random':
                return np.random.choice([0, 1], size=n, p=self.class_ratio)
            elif self.strategy == 'majority':
                return np.zeros(n, dtype=int)
            elif self.strategy == 'positive':
                return np.ones(n, dtype=int)
    
    class BaselineRegressor:
        def __init__(self, strategy='mean'):
            self.strategy = strategy
            self.mean_value = None
        
        def fit(self, X, y):
            self.mean_value = np.mean(y)
            return self
        
        def predict(self, X):
            return np.full(X.shape[0], self.mean_value)
    
    return BaselineClassifier(model_type), BaselineRegressor('mean')


def train_classification_models(X_train, y_train, X_val, y_val):
    """训练分类模型"""
    print("\n--- 训练分类模型 ---")
    
    models = {
        'Logistic Regression': LogisticRegression(max_iter=1000, C=1.0, random_state=42),
        'SVM': SVC(kernel='linear', C=1.0, random_state=42, class_weight='balanced'),
        'Random Forest': RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42, class_weight='balanced'),
        'Gradient Boosting': GradientBoostingClassifier(n_estimators=50, max_depth=3, random_state=42)
    }
    models['Logistic Regression'].set_params(class_weight='balanced')
    
    results = {}
    
    for name, model in models.items():
        # 训练
        model.fit(X_train, y_train)
        
        # 验证集预测
        y_pred = model.predict(X_val)
        
        # 计算指标
        acc = accuracy_score(y_val, y_pred)
        f1 = f1_score(y_val, y_pred, zero_division=0)
        bal_acc = balanced_accuracy_score(y_val, y_pred)
        
        # 交叉验证
        cv_scores = cross_val_score(model, X_train, y_train, cv=3, scoring='f1')
        
        results[name] = {
            'model': model,
            'val_acc': acc,
            'val_f1': f1,
            'val_bal_acc': bal_acc,
            'cv_f1_mean': cv_scores.mean(),
            'cv_f1_std': cv_scores.std()
        }
        
        print(f"  {name}: ACC={acc:.3f}, F1={f1:.3f}, BAL-ACC={bal_acc:.3f}, CV-F1={cv_scores.mean():.3f}±{cv_scores.std():.3f}")
    
    # 选择最佳模型
    best_name = max(results, key=lambda x: results[x]['val_f1'])
    print(f"\n最佳分类模型: {best_name}")
    
    return results, best_name


def train_regression_models(X_train, y_train, X_val, y_val):
    """训练回归模型"""
    print("\n--- 训练回归模型 ---")
    
    models = {
        'Ridge Regression': Ridge(alpha=1.0),
        'Ridge (alpha=0.5)': Ridge(alpha=0.5),
        'Ridge (alpha=5)': Ridge(alpha=5.0)
    }
    
    results = {}
    
    for name, model in models.items():
        # 训练
        model.fit(X_train, y_train)
        
        # 验证集预测
        y_pred = model.predict(X_val)
        
        # 计算指标
        mse = mean_squared_error(y_val, y_pred)
        r2 = r2_score(y_val, y_pred)
        
        if len(y_val) >= 2:
            corr, p_value = pearsonr(y_val, y_pred)
        else:
            corr, p_value = 0, 1
        
        results[name] = {
            'model': model,
            'val_mse': mse,
            'val_r2': r2,
            'val_corr': corr,
            'val_p': p_value
        }
        
        print(f"  {name}: MSE={mse:.2f}, R2={r2:.3f}, r={corr:.3f}")
    
    # 选择最佳模型
    best_name = max(results, key=lambda x: abs(results[x]['val_corr']))
    print(f"\n最佳回归模型: {best_name}")
    
    return results, best_name


def evaluate_on_testset(models, X_test, y_test):
    """在测试集上评估"""
    print("\n--- 测试集评估 ---")
    
    results = {}
    
    # 分类评估
    for task, model_info in models['classification'].items():
        model = model_info['best_model']
        y_pred = model.predict(X_test)
        
        acc = accuracy_score(y_test[task], y_pred)
        f1 = f1_score(y_test[task], y_pred, zero_division=0)
        precision = precision_score(y_test[task], y_pred, zero_division=0)
        recall = recall_score(y_test[task], y_pred, zero_division=0)
        bal_acc = balanced_accuracy_score(y_test[task], y_pred)
        cm = confusion_matrix(y_test[task], y_pred, labels=[0, 1])
        tn, fp, fn, tp = cm.ravel()
        specificity = tn / (tn + fp) if (tn + fp) > 0 else 0.0
        
        results[task] = {
            'acc': acc,
            'f1': f1,
            'precision': precision,
            'recall': recall,
            'specificity': specificity,
            'balanced_acc': bal_acc,
            'confusion_matrix': cm.tolist(),
        }
        print(f"  {task}分类: ACC={acc:.3f}, F1={f1:.3f}, Precision={precision:.3f}, Recall={recall:.3f}, Specificity={specificity:.3f}, BAL-ACC={bal_acc:.3f}")
    
    # 回归评估
    for task, model_info in models['regression'].items():
        model = model_info['best_model']
        y_pred = model.predict(X_test)
        
        mse = mean_squared_error(y_test[task], y_pred)
        r2 = r2_score(y_test[task], y_pred)
        
        if len(y_test[task]) >= 2:
            corr, p_value = pearsonr(y_test[task], y_pred)
        else:
            corr, p_value = 0, 1
        
        results[f'{task}_mse'] = mse
        results[f'{task}_corr'] = corr
        print(f"  {task}回归: r={corr:.3f}, MSE={mse:.2f}")
    
    return results


def evaluate_baseline(X_test, y_test, task_type='classification'):
    """评估基线模型 (随机猜测)"""
    print("\n--- 基线模型评估 (随机猜测) ---")
    
    results = {}
    
    if task_type == 'classification':
        # 随机猜测基线
        np.random.seed(42)
        class_ratio = np.mean(y_test)
        
        # 多次随机取平均
        accs = []
        f1s = []
        for _ in range(100):
            y_pred = np.random.choice([0, 1], size=len(y_test), p=[1-class_ratio, class_ratio])
            accs.append(accuracy_score(y_test, y_pred))
            f1s.append(f1_score(y_test, y_pred, zero_division=0))
        
        results['acc'] = np.mean(accs)
        results['f1'] = np.mean(f1s)
        print(f"  随机基线: ACC={results['acc']:.3f}, F1={results['f1']:.3f}")
        
    return results


def main():
    args = parse_args()
    print("="*60)
    print("Step 2: 模型训练")
    print("="*60)
    
    # 加载数据
    print("\n加载数据...")
    train_data, val_data, test_data = load_data(args.data_dir)
    print(f"训练集: {len(train_data)}, 验证集: {len(val_data)}, 测试集: {len(test_data)}")
    
    # 准备特征
    print(f"\n提取特征: mode={args.feature_mode}, text_field={args.text_field} ...")
    vectorizer = None
    scaler = None
    liwc_analyzer = LIWCAnalyzer(args.liwc_dict) if args.feature_mode in {'liwc_only', 'hybrid'} else None
    X_train, vectorizer, scaler = prepare_features(
        train_data,
        feature_mode=args.feature_mode,
        text_field=args.text_field,
        vectorizer=vectorizer,
        liwc_analyzer=liwc_analyzer,
        scaler=scaler,
        fit=True,
    )
    X_val, _, _ = prepare_features(
        val_data,
        feature_mode=args.feature_mode,
        text_field=args.text_field,
        vectorizer=vectorizer,
        liwc_analyzer=liwc_analyzer,
        scaler=scaler,
        fit=False,
    )
    X_test, _, _ = prepare_features(
        test_data,
        feature_mode=args.feature_mode,
        text_field=args.text_field,
        vectorizer=vectorizer,
        liwc_analyzer=liwc_analyzer,
        scaler=scaler,
        fit=False,
    )
    
    # 准备标签
    y_train = prepare_labels(train_data)
    y_val = prepare_labels(val_data)
    y_test = prepare_labels(test_data)
    
    # 训练分类模型
    cls_results, best_cls_name = train_classification_models(
        X_train, y_train[args.classification_target], X_val, y_val[args.classification_target]
    )
    
    # 训练回归模型 (GAD-7)
    print("\n--- GAD-7回归 ---")
    reg_gad_results, best_gad_name = train_regression_models(
        X_train, y_train['gad'], X_val, y_val['gad']
    )
    
    # 训练回归模型 (PHQ-9)
    print("\n--- PHQ-9回归 ---")
    reg_phq_results, best_phq_name = train_regression_models(
        X_train, y_train['phq'], X_val, y_val['phq']
    )
    
    # 收集最佳模型
    models = {
        'classification': {
            args.classification_target: {
                'best_model': cls_results[best_cls_name]['model'],
                'best_name': best_cls_name
            }
        },
        'regression': {
            'gad': {
                'best_model': reg_gad_results[best_gad_name]['model'],
                'best_name': best_gad_name
            },
            'phq': {
                'best_model': reg_phq_results[best_phq_name]['model'],
                'best_name': best_phq_name
            }
        }
    }
    
    # 测试集评估
    test_results = evaluate_on_testset(models, X_test, y_test)
    
    # 基线评估
    baseline_acc = evaluate_baseline(X_test, y_test[args.classification_target], 'classification')
    
    # 保存结果
    output = {
        'text_field': args.text_field,
        'classification_target': args.classification_target,
        'feature_mode': args.feature_mode,
        'best_classification_model': best_cls_name,
        'best_gad_model': best_gad_name,
        'best_phq_model': best_phq_name,
        'test_results': test_results,
        'baseline_results': baseline_acc
    }

    os.makedirs(args.results_dir, exist_ok=True)
    os.makedirs(args.models_dir, exist_ok=True)
    
    with open(f'{args.results_dir}/training_results.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    # 保存模型
    with open(f'{args.models_dir}/vectorizer.pkl', 'wb') as f:
        pickle.dump(vectorizer, f)
    with open(f'{args.models_dir}/scaler.pkl', 'wb') as f:
        pickle.dump(scaler, f)
    with open(f'{args.models_dir}/classifier.pkl', 'wb') as f:
        pickle.dump(models['classification'][args.classification_target]['best_model'], f)
    with open(f'{args.models_dir}/regressor_gad.pkl', 'wb') as f:
        pickle.dump(models['regression']['gad']['best_model'], f)
    with open(f'{args.models_dir}/regressor_phq.pkl', 'wb') as f:
        pickle.dump(models['regression']['phq']['best_model'], f)
    
    print("\n模型和结果已保存!")
    print(f"  - results/training_results.json")
    print(f"  - models/vectorizer.pkl")
    print(f"  - models/classifier.pkl")
    print(f"  - models/regressor_gad.pkl")
    print(f"  - models/regressor_phq.pkl")
    
    return output


if __name__ == '__main__':
    main()
